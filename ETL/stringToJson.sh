#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

TOTAL=100
j=0
NEXTTOKEN='null'
TABLE=projects
MAXITEMS=25
{
	DATA=$(aws dynamodb scan --output json --table-name $TABLE --max-items $MAXITEMS)
	jq -r -c '.Items[]' <<< "$DATA"
	NEXTTOKEN=$(jq -r '.NextToken' <<< "$DATA") && j=$MAXITEMS
	echo $j >&2
	while [[ "${NEXTTOKEN}" != 'null' && $j -lt $TOTAL ]]; do
		DATA=$(aws dynamodb scan --output json --table-name $TABLE --starting-token "$NEXTTOKEN" --max-items $MAXITEMS)
		jq -r -c '.Items[]' <<< "$DATA"
		NEXTTOKEN=$(jq -r '.NextToken' <<< "$DATA")
		j=$((j + MAXITEMS))
		echo $j >&2
	done
} | while read -r line; do
    echo "$line" | jq -r -c '.projectBinaryData.B // "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA="' | base64 --decode | gzip -d | 
	    jq --argjson line "$line" -r -c '$line + { projectBinaryData: { B: . } } + ($line.projectData.S // "{}" | fromjson | { projectData: { S: . } })'
done
