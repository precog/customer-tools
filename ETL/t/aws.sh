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
	--segment)
		shift
		echo >&2 "SEGMENT($1)"
		SEGMENT="$1"
		shift
		;;
	--total-segments)
		shift
		echo >&2 "SEGMENTS($1)"
		SEGMENTS="$1"
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
	---segments-size)
		shift
		SEGMENTS_SIZE=( "$@" )
		shift "${#SEGMENTS_SIZE[@]}"
		echo >&2 "SEGMENTS_SIZE(${SEGMENTS_SIZE[*]})"
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
	: "${SEGMENT:=0}"
	: "${SEGMENTS:=1}"
	if [[ -z "${SEGMENTS_SIZE[*]:-}" ]]; then
		PARTITION=$((${#FILES[@]} / SEGMENTS))
		START=$((PARTITION * SEGMENT))
		if [[ $((SEGMENT + 1)) -eq $SEGMENTS ]]; then
			END="${#FILES[@]}"
		else
			END=$((START + PARTITION))
		fi
	else
		START_SIZES=("${SEGMENTS_SIZE[@]:0:${SEGMENT}}")
		# shellcheck disable=SC1102
		START=$(("${START_SIZES[@]/%/+}" 0))
		END=$((START + ${SEGMENTS_SIZE[$SEGMENT]}))
	fi
	POSITION=$((START + INDEX))
	REMAINING=$((END - POSITION))
	if [[ $LENGTH -gt $REMAINING ]]; then
		LENGTH="${REMAINING}"
	fi
	NEXT=$((INDEX + LENGTH))
	#echo >&2 "FILES ${#FILES[@]} SEGMENT $SEGMENT SEGMENTS $SEGMENTS PARTITION $PARTITION START $START LENGTH $LENGTH END $END REMAINING $REMAINING INDEX $INDEX POSITION $POSITION NEXT $NEXT"
	cat "${FILES[@]:${POSITION}:${LENGTH}}" |
		jq --slurp "{Items:.} | if $LENGTH != $REMAINING then .+{NextToken:$NEXT} else . end" #| tee /dev/fd/2
fi


# vim: set ts=4 sw=4 tw=100 noet :

