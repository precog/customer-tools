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
		Usage: $0 [-?|-h|--help] [<binary_path> <string_path> <merged_path]

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
	--profiling)
		PROFILING=1
		;;
	*)
		echo >&2 "Invalid parameter '$1'"$'\n'
		usage
		;;
	esac
	shift
done

if [[ $# -ne 0 && $# -ne 3 ]]; then
	echo >&2 "Either all paths or no paths must be passed!"
	exit 2
fi

BINARY_PATH="${1:-.projectBinaryData.B}"
STRING_PATH="${2:-.projectData.S}"
MERGED_PATH="${3:-.mergedProjectData}"
BINARY_DEFAULT_QUERY="${BINARY_PATH}"' // "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA="'
STRING_DEFAULT_QUERY="${STRING_PATH}"' // "{}"'
BINARY_JQ_PATH="$(jq -n -c "path($BINARY_PATH)")"
STRING_JQ_PATH="$(jq -n -c "path($STRING_PATH)")"
MERGED_JQ_PATH="$(jq -n -c "path($MERGED_PATH)")"
# False positive on -d 'EOL':
# shellcheck disable=SC2034
read -r -d 'EOL' OUTPUT_QUERY <<-QUERY
	. as \$uncompressed
	| (\$line | ${STRING_DEFAULT_QUERY} | fromjson) as \$literal
	| \$line
	| setpath(${MERGED_JQ_PATH};
	    if getpath(${BINARY_JQ_PATH}) then \$uncompressed
		else if getpath(${STRING_JQ_PATH}) then \$literal
		else null end
        end
	  )
    | del(${BINARY_PATH})
	| del(${STRING_PATH})
	EOL
QUERY

: "${ALL:=}"
: "${MAX_ITEMS:=25}"
: "${NO_TIMER:=}"
: "${QUIET:=}"
: "${READ_FROM:=}"
: "${TABLE:=projects}"
: "${TOTAL:=100}"
: "${PROFILING:=}"
: "${WORKERS:=1}"

# If Bash 5
if ( : "$EPOCHREALTIME" ) 2> /dev/null; then
	timestamp() {
		echo "$EPOCHREALTIME"
	}
	since() {
		bc -l <<<"$EPOCHREALTIME - $1"
	}
elif [[ "$(date +%N)" =~ [0-9]+ ]]; then
	timestamp() {
		date +%s.%N
	}
	since() {
		bc -l <<<"$(timestamp) - $1"
	}
else
	timestamp() {
		date +%s
	}
	since() {
		echo $(($(timestamp) - $1))
	}
fi

# If BSD
if ( stat -f "%z" "${BASH_SOURCE[0]}" > /dev/null 2>&1 ); then
	filesize() {
		stat -f "%z" "$1"
    }
else
	filesize() {
		stat -c "%s" "$1"
	}
fi

# Fetch total if using --all
if [[ -n $ALL ]]; then
	TOTAL="$(aws dynamodb describe-table --table-name "${TABLE}" | jq .Table.ItemCount)"
	[[ -n $QUIET ]] || echo >&2 "Total ${TOTAL}"
fi

# Extra scan flags on verbose
if [[ -n $PROFILING ]]; then
	PROFILING_SCAN=( '--return-consumed-capacity' 'TOTAL' )
else
	PROFILING_SCAN=( )
fi

# Adjust total for number of workers; we can't predict which workers will get remainders
REMAINDER=$((TOTAL % WORKERS))
TOTAL=$((TOTAL / WORKERS))

worker() {
	declare -g COUNT
	declare -g NEXTTOKEN
	declare -g -a SEGMENTATION
	declare -g WORKER_INFO

	COUNT=0
	NEXTTOKEN='null'
	SECONDS=0

	if [[ -n $PROFILING ]]; then
		exec 5> "worker$1.profiling"
		echo >&5 "epochseconds,scan,math,size,tsize,count,consumed,scanned,osize,decode,pipe,loop"
	fi

	# Last worker gets total remainders
	if [[ $1 -eq $((WORKERS - 1))  ]]; then
		TOTAL=$((TOTAL + REMAINDER))
	fi

	# Adjust max items if greater than total
	curbMaxItems

	if [[ $WORKERS -eq 1 ]]; then
		SEGMENTATION=( )
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
	local t1
	t1=$(timestamp)
	profiling -n "$(timestamp),"
	if [[ $# -eq 1 ]]; then
		aws dynamodb scan --output json --table-name "$TABLE" --max-items "$MAX_ITEMS" \
			"${SEGMENTATION[@]}" --starting-token "$1" \
			"${PROFILING_SCAN[@]}"
	else
		aws dynamodb scan --output json --table-name "$TABLE" --max-items "$MAX_ITEMS" \
			"${SEGMENTATION[@]}" \
			"${PROFILING_SCAN[@]}"
	fi
	profiling -n "$(since "$t1"),"
}

readData() {
	local BYTES KBYTES t1 t2
	t1=$(timestamp)
	DATA=$(scan)
	t2=$(timestamp)
	BYTES=${#DATA}
	KBYTES=$(((BYTES + 512) / 1024))
	NEXTTOKEN=$(jq -r '.NextToken' <<<"$DATA")
	ITEMS_READ=$(jq -r -c '.Items|length' <<<"$DATA")
	COUNT=$ITEMS_READ
	profiling -n "$(since "$t2"),$BYTES,$KBYTES,$COUNT,$(
		jq -rc '[.ConsumedCapacity.CapacityUnits,.ScannedCount]|map(tostring)|join(",")' <<<"$DATA"
	),"
	jq -r -c '.Items[]' <<<"$DATA" | processData
	showCount

	while [[ ${NEXTTOKEN} != 'null' && (${TOTAL} -gt ${COUNT}) ]]; do
		profiling "$(since "$t1")"
		t1=$(timestamp)
		curbMaxItems
		DATA=$(scan "${NEXTTOKEN}")
		t2=$(timestamp)
		BYTES=${#DATA}
		KBYTES=$(((BYTES + 512) / 1024 + KBYTES))
		NEXTTOKEN=$(jq -r '.NextToken' <<<"$DATA")
		ITEMS_READ=$(jq -r -c '.Items|length' <<<"$DATA")
		COUNT=$((COUNT + ITEMS_READ))
		profiling -n "$(since "$t2"),$BYTES,$KBYTES,$COUNT,$(
			jq -rc '[.ConsumedCapacity.CapacityUnits,.ScannedCount]|map(tostring)|join(",")' <<<"$DATA"
		),"
		jq -r -c '.Items[]' <<<"$DATA" | processData
		showCount
		showEllapsed
	done
	profiling "$(since "$t1")"
}

processData() {
	local t1 t2
	t1=$(timestamp)
	TMP="$(getTMP)"
	while read -r line; do
		jq -r -c "${BINARY_DEFAULT_QUERY}" <<< "$line" | base64 --decode | gzip -d |
			jq -r -c ". as \$line | input | ${OUTPUT_QUERY}" <(cat <<<"$line") <(cat) || {
				echo >&2 'Invalid JSON!'
				cat >&2 <<<"$line"
			}
	done > "$TMP"
	profiling -n "$(filesize "${TMP}"),$(since "$t1")," || :
	t2=$(timestamp)
	echo "$TMP"
	profiling -n "$(since "$t2"),"
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

profiling() {
	if [[ -n $PROFILING ]]; then
		echo >&5 "$@"
	fi
}

declare -a PIPE_NAME
declare -a PIPE_FD
declare -a WORKER_PARTIAL

trap 'kill $(jobs -p)' EXIT

for worker in $(seq 0 $((WORKERS - 1))); do
	segment="$worker"
	PIPE="$(getPipe "$worker")"
	PIPE_NAME[$worker]="$PIPE"
	worker "${segment}" > "$PIPE" &
	: {FD}< "${PIPE}"
	PIPE_FD["${worker}"]="${FD}"
done

if [[ -n $PROFILING ]]; then
	exec 5> main.profiling
	echo >&5 "timestamp,wait,worker,send"
fi

t0=$(timestamp)
profiling -n "$(timestamp),"
while [[ ${#PIPE_FD[@]} -gt 0 ]]; do
	for index in "${!PIPE_FD[@]}"; do
		if read -r -t 1 -u "${PIPE_FD[$index]}" file; then
			profiling -n "$(since "$t0"),$index,"
			t0=$(timestamp)
			cat "${WORKER_PARTIAL[$index]:-}$file"
			unset WORKER_PARTIAL["$index"]
			profiling "$(since "$t0")"
			t0=$(timestamp)
			profiling -n "$(timestamp),"
			rm "$file"
		elif [[ $? -lt 128 ]]; then
			unset "PIPE_FD[$index]"
		else
			WORKER_PARTIAL[$index]="${WORKER_PARTIAL[$index]:-}${file}"
		fi
	done
done

wait

trap - EXIT

for pipe in "${PIPE_NAME[@]}"; do
	rm "$pipe"
done

profiling "$(since "$t0"),end,"

# vim: set ts=4 sw=4 tw=100 noet :
