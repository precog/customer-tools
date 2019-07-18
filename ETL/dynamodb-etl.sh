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
: "${QUIET:=}"
: "${TABLE:=projects}"
: "${TOTAL:=100}"

NEXTTOKEN='null'
COUNT=0
{
	if [[ ${TOTAL} -lt ${MAX_ITEMS} ]]; then
		MAX_ITEMS=${TOTAL}
	fi
	DATA=$(aws dynamodb scan --output json --table-name "$TABLE" --max-items "$MAX_ITEMS")
	jq -r -c '.Items[]' <<<"$DATA"
	NEXTTOKEN=$(jq -r '.NextToken' <<<"$DATA")
	ITEMS_READ=$(jq -r -c '.Items|length' <<<"$DATA")
	TOTAL=$((TOTAL - ITEMS_READ))
	COUNT=$ITEMS_READ
	[[ -n $QUIET ]] || echo >&2 "$COUNT"
	while [[ ${NEXTTOKEN} != 'null' && (-n ${ALL} || ${TOTAL} -gt 0) ]]; do
		if [[ ${TOTAL} -lt ${MAX_ITEMS} ]]; then
			MAX_ITEMS=${TOTAL}
		fi
		DATA=$(aws dynamodb scan --output json --table-name "$TABLE" --starting-token "$NEXTTOKEN" --max-items "$MAX_ITEMS")
		jq -r -c '.Items[]' <<<"$DATA"
		NEXTTOKEN=$(jq -r '.NextToken' <<<"$DATA")
		ITEMS_READ=$(jq -r -c '.Items|length' <<<"$DATA")
		TOTAL=$((TOTAL - ITEMS_READ))
		COUNT=$((COUNT + ITEMS_READ))
		[[ -n $QUIET ]] || echo >&2 $COUNT
	done
} | while read -r line; do
	echo "$line" | jq -r -c "${BINARY_DEFAULT_QUERY}" | base64 --decode | gzip -d |
		jq --argjson line "$line" -r -c "${OUTPUT_QUERY}"
done

# vim: set ts=4 sw=4 tw=100 noet :
