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
		| aws s3 cp - "s3://au-reform/${TABLE}/worker$worker.json.gz"
	echo "Worker $worker finished"
}

trap 'kill $(jobs -p)' EXIT

echo "Starting $WORKERS workers"
date

for worker in $(seq 0 $((WORKERS - 1))); do
	scan "$worker" &
done

echo "All workers started, waiting for them to finish"

wait

echo "All workers finished"
date

