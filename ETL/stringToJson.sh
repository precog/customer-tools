#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

TOTAL=100
i=1
j=0
k=all
NEXTTOKEN="null"
TABLE=projects
MAXITEMS=25
{
	DATA=$(aws dynamodb scan --output json --table-name $TABLE --max-items $MAXITEMS)
	ITEMS=$(echo "$DATA" | jq -r -c ".Items | .[]")
	echo "$ITEMS"
	NEXTTOKEN="$(echo "$DATA" | jq -r -c '.NextToken')" && j=$MAXITEMS
	echo $j >&2
	while [ "${NEXTTOKEN}" != "null" ] && [ $j -lt $TOTAL ]; do
		DATA=$(aws dynamodb scan --output json --table-name $TABLE --starting-token "$NEXTTOKEN" --max-items $MAXITEMS)
		ITEMS=$(echo "$DATA" | jq -r -c ".Items | .[]")
		echo "$ITEMS"
		NEXTTOKEN="$(echo "$DATA" | jq -r -c '.NextToken')"
		j=$((j + MAXITEMS))
		echo $j >&2
	done
} | jq -r -c '"\(.)\n\(.)\n\(.)"' | while read -r line; do case $k in all)
	echo "$line"
	k=pbd
	;;
pbd)
	echo "$line" | jq -r -c '.projectBinaryData.B // "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA="' | base64 --decode | gzip -d | jq -r -c '{ projectBinaryData: { B: . } }'
	k=pd
	;;
pd)
	echo "$line" | jq -r -c '.projectData.S // "{}" | fromjson | { projectData: { S: . } }'
	k=all
	;;
esac done | paste - - - | jq -r -c '. + input + input'
