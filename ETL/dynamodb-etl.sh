#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

if [[ ! -x $(type -p jq) ]]; then
	cat >&2 <<-MISSING_JQ
		Please install "jq". On Amazon AMI Linux, install it with:

		    sudo yum install -y jq

		See https://stedolan.github.io/jq/download/ for information on other
		platforms.
	MISSING_JQ
	exit 3
fi

if [[ $(jq --version) == jq-1.[0-4] ]]; then
	echo >&2 "Please update jq to version 1.5 or higher"
	exit 4
fi

usage() {
	cat >&2 <<-USAGE
		Usage: $0 [-?|-h|--help] [<binary_path> <string_path>]

		-? | -h | --help             Prints this message
		-a | --all                   Process all data (overrides total)
		-m N | --max-items N         Process data in batches of N (defaults to 25)
		-q | --quiet                 Do not print progress information
		-t N | --total N             Limits processing to the first N entries (defaults to 100)
		-T NAME | --table NAME       DynamoDB table name (defaults to "projects")
		-w N | --workers N           Number of concurrent workers

		Paths are specified as .x.y.z for { "x": { "y": { "z": data }}}. More
		generally, they must be valid "jq" paths.

		Binary paths must point to a string that contains a base64-encoded,
		gzipped json, so that "base64 --decode | gzip -d" will turn that
		string into valid json.

		String paths must point to a string that contains valid json. For
		example, .x in { "x": "{ \"a\": 5 }" }.

		Use a non-existing path if there's no binary or string path. For
		example, ".no.binary.path .path.to.string" if there's string data
		on the .path.to.string, but not binary data, and ".no.binary.path"
		is not an existing path in the input data.
	USAGE
	exit 1
}

while [[ $# -gt 0 && $1 == -* ]]; do
	case "$1" in
	-\? | -h | --help) usage ;;
	-a | --all) ALL=1 ;;
	-m | --max-items)
		shift
		MAX_ITEMS="${1}"
		;;
	-t | --total)
		shift
		TOTAL="${1}"
		;;
	-T | --table)
		shift
		TABLE="${1}"
		;;
	-q | --quiet)
		QUIET=1
		;;
	-w | --workers)
		shift
		WORKERS="${1}"
		;;
	# These options are intentionally undocumented
	---no-timer)
		NO_TIMER=1
		;;
	---read-from)
		shift
		READ_FROM="${1}"
		;;
	*)
		echo >&2 "Invalid parameter '$1'"$'\n'
		usage
		;;
	esac
	shift
done

if [[ $# -ne 0 && $# -ne 2 ]]; then
	echo >&2 "Either both paths or no paths must be passed!"
	exit 2
fi

BINARY_PATH="${1:-.projectBinaryData.B}"
STRING_PATH="${2:-.projectData.S}"
BINARY_DEFAULT_QUERY="${BINARY_PATH}"' // "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA="'
STRING_DEFAULT_QUERY="${STRING_PATH}"' // "{}"'
BINARY_JQ_PATH="$(jq -n -c "path($BINARY_PATH)")"
STRING_JQ_PATH="$(jq -n -c "path($STRING_PATH)")"
# False positive on -d 'EOL':
# shellcheck disable=SC2034
read -r -d 'EOL' OUTPUT_QUERY <<-QUERY
	. as \$uncompressed
	| (\$line | ${STRING_DEFAULT_QUERY} | fromjson) as \$literal
	| \$line
	| if getpath(${BINARY_JQ_PATH}) then setpath(${BINARY_JQ_PATH}; \$uncompressed) else . end
	| if getpath(${STRING_JQ_PATH}) then setpath(${STRING_JQ_PATH}; \$literal) else . end
	EOL
QUERY

: "${ALL:=}"
: "${MAX_ITEMS:=25}"
: "${NO_TIMER:=}"
: "${QUIET:=}"
: "${READ_FROM:=}"
: "${TABLE:=projects}"
: "${TOTAL:=100}"
: "${WORKERS:=1}"

# Fetch total if using --all
if [[ -n $ALL ]]; then
	TOTAL="$(aws dynamodb describe-table --table-name "${TABLE}" | jq .Table.ItemCount)"
	[[ -n $QUIET ]] || echo >&2 "Total ${TOTAL}"
fi

# Adjust total for number of workers; we can't predict which workers will get remainders
TOTAL=$((TOTAL / WORKERS))

worker() {
	declare -g COUNT
	declare -g NEXTTOKEN
	declare -g -a SEGMENTATION
	declare -g WORKER_INFO

	COUNT=0
	NEXTTOKEN='null'
	SECONDS=0

	# Adjust max items if greater than total
	curbMaxItems

	if [[ $WORKERS -eq 1 ]]; then
		SEGMENTATION=()
		WORKER_INFO=""
	else
		SEGMENTATION=('--segment' "$1" '--total-segments' "$WORKERS")
		WORKER_INFO="Worker #$1: "
	fi

	if [[ -z $READ_FROM ]]; then
		readData
	else
		FILES=( "${READ_FROM}"* )
		FILE="${FILES[$1]}"
		processData < "${FILE}"
	fi

}

scan() {
	if [[ $# -eq 1 ]]; then
		aws dynamodb scan --output json --table-name "$TABLE" --max-items "$MAX_ITEMS" \
			"${SEGMENTATION[@]}" --starting-token "$1"
	else
		aws dynamodb scan --output json --table-name "$TABLE" --max-items "$MAX_ITEMS" \
			"${SEGMENTATION[@]}"
	fi
}

readData() {
	DATA=$(scan)
	jq -r -c '.Items[]' <<<"$DATA" | processData
	NEXTTOKEN=$(jq -r '.NextToken' <<<"$DATA")
	ITEMS_READ=$(jq -r -c '.Items|length' <<<"$DATA")
	COUNT=$ITEMS_READ
	showCount

	while [[ ${NEXTTOKEN} != 'null' && (${TOTAL} -gt ${COUNT}) ]]; do
		curbMaxItems
		DATA=$(scan "${NEXTTOKEN}")
		jq -r -c '.Items[]' <<<"$DATA" | processData
		NEXTTOKEN=$(jq -r '.NextToken' <<<"$DATA")
		ITEMS_READ=$(jq -r -c '.Items|length' <<<"$DATA")
		COUNT=$((COUNT + ITEMS_READ))
		showCount
		showEllapsed
	done
}

processData() {
	TMP="$(getTMP)"
	while read -r line; do
		jq -r -c "${BINARY_DEFAULT_QUERY}" <<< "$line" | base64 --decode | gzip -d |
			jq -r -c ". as \$line | input | ${OUTPUT_QUERY}" <(cat <<<"$line") <(cat) || {
				echo >&2 'Invalid JSON!'
				cat >&2 <<<"$line"
			}
	done > "$TMP"
	echo "$TMP"
}

curbMaxItems() {
	[[ -n $ALL ]] && return
	REMAINING=$((TOTAL - COUNT))
	if [[ $REMAINING -lt $MAX_ITEMS ]]; then
		MAX_ITEMS=$REMAINING
	fi
}

showEllapsed() {
	if [[ -z $NO_TIMER && -z $QUIET && -n $ALL ]]; then
		duration=$SECONDS
		estimate=$((TOTAL * duration / COUNT))
		echo >&2 -n "Time ellapsed: $(showTimer $duration)"
		echo >&2 " estimated: $(showTimer $estimate)"
	fi
}

showTimer() {
	duration="$1"
	seconds=$((duration % 60))
	minutes=$((duration / 60 % 60))
	hours=$((duration / 3600))
	echo -n "$hours hour$(ss $hours) $minutes minute$(ss $minutes) $seconds second$(ss $seconds)"
}

showCount() {
	if [[ -z $QUIET ]]; then
		if [[ -n $ALL ]]; then
			perc=$((COUNT * 100 / TOTAL))
			echo >&2 "${WORKER_INFO}${COUNT} (${perc}%)"
		else
			echo >&2 "${WORKER_INFO}${COUNT}"
		fi
	fi
}

ss() {
	s="${2-s}"
	[[ $1 -ne 1 ]] && echo -n "$s"
}

getPipe() {
	PIPE="$(getTMP)"
	rm -f "$PIPE"
	mkfifo "$PIPE"
	echo "$PIPE"
}

getTMP() {
  mktemp -t "dynamodb-etl.XXXXXXXXXX"
}

declare -a PIPE_NAME
declare -a PIPE_FD

trap 'kill $(jobs -p)' EXIT

for worker in $(seq 1 ${WORKERS}); do
	segment=$((worker - 1))
	PIPE="$(getPipe "$worker")"
	PIPE_NAME[$worker]="$PIPE"
	worker "${segment}" > "$PIPE" &
	: {FD}< "${PIPE}"
	PIPE_FD["${worker}"]="${FD}"
done

while [[ ${#PIPE_FD[@]} -gt 0 ]]; do
	for index in "${!PIPE_FD[@]}"; do
		if read -r -t 1 -u "${PIPE_FD[$index]}" file; then
			cat "$file"
			rm "$file"
		elif [[ $? -lt 128 ]]; then
			unset "PIPE_FD[$index]"
		fi
	done
done

wait

trap - EXIT

for pipe in "${PIPE_NAME[@]}"; do
	rm "$pipe"
done

# vim: set ts=4 sw=4 tw=100 noet :
