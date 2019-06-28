#!/usr/bin/env bash
# aws dynamodb scan --output json --table-name "$TABLE" --max-items "$BATCH_SIZE"
# --starting-token "$NEXTTOKEN"

#set -x

DATA="$1"
shift

while [[ $# -gt 0 ]]; do
case "$1" in
	--table-name)
		shift
		echo >&2 "TABLE($1)"
		shift
		;;
	--max-items)
		shift
		echo >&2 "MAX_ITEMS($1)"
		shift
		;;
	--starting-token)
		shift
		echo >&2 "STARTING_TOKEN($1)"
		NEXT="$1"
		shift
		;;
	*) shift ;;
esac
done

mapfile -t FILES < <(cd "${DATA}" && ls)
: "${NEXT:="${FILES[0]}"}"
cat "${DATA}/${NEXT}"
#rm -f "${NEXT}"

#set +x
