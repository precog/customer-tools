#!/usr/bin/env bash
# Bash unofficial lint mode for test setup
set -euo pipefail
IFS=$'\n\t'

# Helper functions and test environment setup
# shellcheck source=ETL/t/helper.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/helper.sh"

# Test framework
unset IFS # osht depends on default IFS
# shellcheck disable=SC1094 disable=SC1090
source "$(dirname "$0")/osht.sh"

# Tests run unlinted
set +euo pipefail

# Tests
PLAN 135

# Empty input for error tests
clearData

# Simulate jq not installed
jq() { echo "Why?"; exit 1; }
export -f jq
NRUNS "${SCRIPT}"  # abort if jq is not installed
EGREP 'install "jq"'
unset -f jq

# Simulate wrong jq version
JQ="${BIN}/jq"
echo 'echo "jq-1.4"' > "${JQ}"
chmod +x "${JQ}"
NRUNS "${SCRIPT}"  # abort if jq version is not compatible
EGREP '1.5'
rm "${JQ}"

# Invalid parameters
NRUNS "${SCRIPT}" --mistaken-parameter  # Invalid parameter
EGREP -i 'invalid parameter.*--mistaken-parameter'

# Workers has to be a positive integer
NRUNS "${SCRIPT}" --workers ''  # workers can't be empty
NRUNS "${SCRIPT}" --workers many  # workers has to be a number
NRUNS "${SCRIPT}" --workers -1  # workers has to be positive
NRUNS "${SCRIPT}" --workers 0  # workers has to be greater than zero
NRUNS "${SCRIPT}" --workers 1.5  # workers has to be an integer

# Output file has to be non-empty
NRUNS "${SCRIPT}" --output ''  # output file has to be non-empty

# --stdout and --output are mutually exclusive
NRUNS "${SCRIPT}" --output "${TMP}/test" --stdout  # --stdout and --output are mutually exclusive
EGREP -- "--stdout and --output are mutually exclusive"
NRUNS "${SCRIPT}" -s -o "${TMP}/test2"
EGREP -- "--stdout and --output are mutually exclusive"

# --stdout and --pipe are mutually exclusive
NRUNS "${SCRIPT}" --pipe "cat > ${TMP}/test" --stdout  # --stdout and --pipe are mutually exclusive
EGREP -- "--stdout and --pipe are mutually exclusive"
NRUNS "${SCRIPT}" -s -p "cat > ${TMP}/test2"
EGREP -- "--stdout and --pipe are mutually exclusive"

# --pipe and --output are mutually exclusive
NRUNS "${SCRIPT}" --output "${TMP}/test" --pipe "cat > ${TMP}/test"  # --pipe and --output are mutually exclusive
EGREP -- "--pipe and --output are mutually exclusive"
NRUNS "${SCRIPT}" -p "cat > ${TMP}/test" -o "${TMP}/test2"
EGREP -- "--pipe and --output are mutually exclusive"

# Empty input
clearData
addData < /dev/null
RUNS "${SCRIPT}" --stdout -t 5 -M 1  # does not fail on empty input
NOGREP .
EDIFF <<< $'SCAN\nTABLE(projects)\nMAX_ITEMS(5)\n0'

# No text or binary data in input
clearData
addData <<< '{"neither":"exists"}'
RUNS "${SCRIPT}"  # no text or binary data
OGREP '"mergedProjectData":null'

# Convert string literals to json
clearData
addData <<< '{"projectData": {"S": "{\"a\": \"b\"}"}}'
RUNS "${SCRIPT}" -s  # converts text data into json
OGREP '"projectData":{}'
OGREP '"mergedProjectData":{"a":"b"}'

# Survives bad string data
clearData
BADDATA='{"projectData": {"S": "bad data"}}'
addData <<< '{"projectData": {"S": "{\"a\": \"b\"}"}}'
addData <<< "${BADDATA}"
addData <<< '{"projectData": {"S": "{\"a\": \"b\"}"}}'
RUNS "${SCRIPT}" -q --verbose  # survives bad string data
EGREP 'Invalid JSON!'
EGREP "$(jq -c . <<<"${BADDATA}")"
RUNS countLines.sh  # good records still read
ODIFF <<< $'2'

# Convert base64-encoded, gzipped string literals to json
clearData
addData <<< '{"projectBinaryData": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="}}'
RUNS "${SCRIPT}"  # converts binary data into json
OGREP '"projectBinaryData":{}'
OGREP '"mergedProjectData":{"a":"b"}'

# Survives bad binary data
clearData
BAD1='{"projectBinaryData": {"B": "not base64-encoded"}}'
BAD2='{"projectBinaryData": {"B": "'"$(echo "not gzipped" | base64)"'"}}'
BAD3='{"projectBinaryData": {"B": "'"$(echo "not a json" | gzip -c | base64)"'"}}'
addData <<< '{"projectBinaryData": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="}}'
addData <<< "${BAD1}"
addData <<< "${BAD2}"
addData <<< "${BAD3}"
addData <<< '{"projectBinaryData": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="}}'
RUNS "${SCRIPT}" -q --verbose  # survives bad binary data
EGREP 'Invalid JSON!'
EGREP "$(jq -c . <<<"${BAD1}")"
EGREP "$(jq -c . <<<"${BAD2}")"
EGREP "$(jq -c . <<<"${BAD3}")"
RUNS countLines.sh  # good records still read
ODIFF <<< $'2'

# Discard text data if binary data is present
clearData
addData <<-BOTH_FIELDS
	{
		"projectBinaryData": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="},
		"projectData": {"S": "{\"c\": \"d\"}"}
	}
BOTH_FIELDS
RUNS "${SCRIPT}"  # Discard text data if binary data is present
OGREP '"projectData":{}'
OGREP '"projectBinaryData":{}'
OGREP '"mergedProjectData":{"a":"b"}'

# Do not add string path unless present on input
clearData
addData <<< '{"projectBinaryData": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="}}'
RUNS "${SCRIPT}"  # does not add binary path if not present on input
NOGREP 'projectData'

# Do not add binary path unless present on input
clearData
addData <<< '{"projectData": {"S": "{\"a\": \"b\"}"}}'
RUNS "${SCRIPT}"  # does not add text path if not present on input
NOGREP 'projectBinaryData'

# Specify paths as parameters
addData <<< '{"text": "{\"a\": \"b\"}"}'
RUNS "${SCRIPT}" .binary .text .merged  # paths as parameters, text input
NOGREP '"text"'
OGREP '"merged":{"a":"b"}'

addData <<< '{"binary": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="}}'
RUNS "${SCRIPT}" .binary .text .merged  # paths as parameters, binary input
NOGREP '"binary"'
OGREP '"merged":{"a":"b"}'

# Handles data over 400 KB in length
clearData
addData <<< "{\"key\": [$(seq -s , 1 100000)0]}"
RUNS "${SCRIPT}" -M 1  # handles records bigger than 400 KB
EDIFF <<< $'SCAN\nTABLE(projects)\nMAX_ITEMS(25)\n1'

# Do not process data if asked not to
clearData
addData <<< '{"projectData": {"S": "{\"a\": \"b\"}"}}'
RUNS "${SCRIPT}" --raw  # does not convert data
OGREP '{"projectData":{"S":"{\\"a\\": \\"b\\"}"}}'
NOGREP '"mergedProjectData":{"a":"b"}'

# Setup for tests counting data
clearData
# shellcheck disable=SC2034
for ignore in {1..20}; do
	addData <<< '{"projectData": {"S": "{\"a\": \"b\"}"}}'
	addData <<< '{"projectBinaryData": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="}}'
done

# Quiet running
RUNS "${SCRIPT}" -q -t 5 -M 1  # quiet running
EDIFF <<< $'SCAN\nTABLE(projects)\nMAX_ITEMS(5)'
RUNS "${SCRIPT}" --quiet --all -M 1
EDIFF <<-ALL_QUIET
	DESCRIBE_TABLE
	TABLE(projects)
	SCAN
	TABLE(projects)
	MAX_ITEMS(25)
	SCAN
	TABLE(projects)
	MAX_ITEMS(25)
	STARTING_TOKEN(25)
ALL_QUIET

# Basic parameters
RUNS "${SCRIPT}" --table testTable --total 19 --max-items 7 -M 1  # does not go beyond total
EGREP 'TABLE(testTable)'
EGREP 'MAX_ITEMS(7)'
EGREP 19
NEGREP 21

# Test Total/Max Items
RUNS "${SCRIPT}" --total 30 --max-items 12 -M 1  # reads all available data up to total
EGREP 'STARTING_TOKEN(12)'
EGREP 'STARTING_TOKEN(24)'
NEGREP 'STARTING_TOKEN(30)'
NEGREP 'STARTING_TOKEN(36)'
EGREP 30

# Test Total/Max Items for Total < Max Items
RUNS countLines.sh -t 5 -M 1  # 5 total out of 40 with 25 increments
ODIFF <<< $'5'

# Test Total/Max Items for Total > Max Items
RUNS countLines.sh -t 10 -m 7 -M 1  # 10 total out of 40 with 7 increments
ODIFF <<< $'10'

# Test --all
RUNS countLines.sh --all --total 10 -M 1  # reads all data despite total
ODIFF <<< $'40'  # fetches all content
RUNS "${SCRIPT}" --all --total 10 -M 1 ---no-timer  # shows how many records are going to be fetched
EDIFF <<-ALL_OUTPUT
	DESCRIBE_TABLE
	TABLE(projects)
	Total 40
	SCAN
	TABLE(projects)
	MAX_ITEMS(25)
	25 (62%)
	SCAN
	TABLE(projects)
	MAX_ITEMS(25)
	STARTING_TOKEN(25)
	40 (100%)
ALL_OUTPUT

# Test two parallel workers
RUNS "${SCRIPT}" --total 20 --max-items 5 --workers 2 -M 1  # two parallel workers
EGREP "SEGMENTS(2)"
EGREP "SEGMENT(0)"
EGREP "SEGMENT(1)"
NEGREP "SEGMENT(2)"
EGREP "Worker #0: 5"
EGREP "Worker #1: 5"
EGREP "Worker #0: 10"
EGREP "Worker #1: 10"

# Test three parallel workers
RUNS countLines.sh --total 20 --max-items 5 --workers 3 -M 1  # three parallel workers
EGREP "SEGMENTS(3)"
EGREP "SEGMENT(0)"
EGREP "SEGMENT(1)"
EGREP "SEGMENT(2)"
ODIFF <<< $'20'

# Test output to file
RUNS "${SCRIPT}" --output "${TMP}/output"  # output to file
NOGREP .
OK -f "${TMP}/output"
RUNS grep -c '"mergedProjectData":{"a":"b"}' "${TMP}/output"
OGREP "40"

# Test output to file with parallel workers
RUNS "${SCRIPT}" --workers 2 -o "${TMP}/worker_%d_output"  # output to file with parallel workers
NOGREP .
OK -f "${TMP}/worker_0_output"
OK -f "${TMP}/worker_1_output"
RUNS grep -c '"mergedProjectData":{"a":"b"}' "${TMP}/worker_0_output"
OGREP "20"
RUNS grep -c '"mergedProjectData":{"a":"b"}' "${TMP}/worker_1_output"
OGREP "20"

# Test pipe to command
RUNS "${SCRIPT}" --pipe "wc -l"  # pipe to command
OGREP "40"

# Test pipe to command with multiple workers
RUNS "${SCRIPT}" --workers 2 -p "gzip -9 | cat > ${TMP}/worker_%d_pipe.gz"
OK -f "${TMP}/worker_0_pipe.gz"
OK -f "${TMP}/worker_1_pipe.gz"
RUNS gunzip "${TMP}/worker_0_pipe.gz" "${TMP}/worker_1_pipe.gz"
RUNS grep -c '"mergedProjectData":{"a":"b"}' "${TMP}/worker_0_pipe"
OGREP "20"
RUNS grep -c '"mergedProjectData":{"a":"b"}' "${TMP}/worker_1_pipe"
OGREP "20"

# Unequal sized partitions CH10813
RUNS "${SCRIPT}" --all --workers 2 --pipe "wc -l | tr -d ' ' > '${TMP}/partition%d'" ---segments-size 10 30 # Unequal sized partitions
RUNS cat "${TMP}/partition0"
ODIFF <<< $'10'
RUNS cat "${TMP}/partition1"
ODIFF <<< $'30'

# Report pipe errors
NRUNS "${SCRIPT}" --all --workers 1 --pipe "false"  # Report pipe errors
EGREP "'false' exited with error code 1"
EGREP 'Pipe #0: \*\*\* ABORTED \*\*\*'

# vim: set ts=4 sw=4 sts=4 tw=100 noet filetype=sh :
