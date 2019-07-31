#!/usr/bin/env bash
# aws dynamodb scan --output json --table-name "$TABLE" --max-items "$BATCH_SIZE"
# --starting-token "$NEXTTOKEN"

DATA="$1"
shift

declare DESCRIBE_TABLE
declare SCAN

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
		LENGTH="$1"
		shift
		;;
	--starting-token)
		shift
		echo >&2 "STARTING_TOKEN($1)"
		INDEX="$1"
		shift
		;;
	scan)
		shift
		echo >&2 "SCAN"
		SCAN=1
		;;
	describe-table)
		shift
		echo >&2 "DESCRIBE_TABLE"
		DESCRIBE_TABLE=1
		;;
	*) shift ;;
esac
done

cd "${DATA}" || exit 1
mapfile -t FILES < <(ls)

if [[ -n $DESCRIBE_TABLE ]]; then
	jq -n --argjson count "${#FILES[@]}" '{ Table: { ItemCount: $count } }'
	exit 0
elif [[ -n $SCAN ]]; then
	: "${INDEX:=0}"
	: "${LENGTH:=5}"
	NEXT=$((INDEX+LENGTH))
	cat "${FILES[@]:${INDEX}:${LENGTH}}" |
		jq --slurp "{Items:.} | if $NEXT < ${#FILES[@]} then .+{NextToken:$NEXT} else . end"
fi


# vim: set ts=4 sw=4 tw=100 noet :

