#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

if [[ $# -ne 2 ]]; then
	echo >&2 "$0 <table> <# of workers>"
	exit 1
fi

TABLE="$1"
WORKERS="$2"

scan() {
	local worker
	worker="$1"
	aws dynamodb scan \
		--output json \
		--table-name "$TABLE" \
		--segment "$worker" --total-segments "$WORKERS" \
		| gzip -9 \
		| aws s3 cp - "s3://au-reform/${TABLE}/worker$worker.jzon.gz"
	echo >&2 "Worker $worker finished"
}

for worker in $(seq 0 $((WORKERS - 1))); do
	scan "$worker" 2>"worker$worker.nohup.log" &
done

