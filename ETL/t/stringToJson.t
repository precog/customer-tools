#!/usr/bin/env bash
# Bash unofficial lint mode for test setup
set -euo pipefail
IFS=$'\n\t'

# Find ourselves and script to be tested
TEST_NAME="$(basename "$0" .t)"
TEST_DIR="$(dirname "$0")"
SCRIPT_NAME="${TEST_NAME}.sh"
SCRIPT_DIR="$(cd "${TEST_DIR}/.." && pwd)"
SCRIPT="${SCRIPT_DIR}/${SCRIPT_NAME}"

# Validate environment
[[ ! -d "${TEST_DIR}" ]] ||
	[[ ! -d "${SCRIPT_DIR}" ]] ||
	[[ ! -f "${TEST_DIR}/osht.sh" ]] ||
	[[ ! ( -f "${SCRIPT}" || -x "${SCRIPT}" ) ]] &&
	exit 1

# Clean up on exit
unset TMP
cleanUp() {
	[[ -d "${TMP}" ]]  && rm -fr "${TMP}"
}
trap cleanUp EXIT

# Set temporary directory and fake aws cli
TMP=$(mktemp -d -t "${TEST_NAME}.XXXXXXXXXX")
BIN="${TMP}/bin"
DATA="${TMP}/data"
mkdir -p "${BIN}" "${DATA}"
cat > "${BIN}/aws" <<-AWS
	#!/usr/bin/env bash
	exec ${BIN}/aws.sh "${DATA}" "\${@}"
AWS
cp "${TEST_DIR}/aws.sh" "${BIN}/"
chmod +x "${BIN}/aws" "${BIN}/aws.sh"
export PATH="${BIN}:${PATH}"

# Load test framework
unset IFS # osht depends on default IFS
# shellcheck disable=SC1094 disable=SC1090
source "$(dirname "$0")/osht.sh"

# Tests run unlinted
set +euo pipefail

# Test helpers
clearData() {
	[[ -d "${DATA}" ]] && rm -fr "${DATA}"
	mkdir -p "${DATA}"
}

addData() {
	mapfile -t < <(cd "${DATA}" && ls)
	if [[ "${#MAPFILE[@]}" -eq 0 ]]; then
		FILE="00.json"
	else
		LAST="${MAPFILE[-1]%.json}"
		[[ "${LAST}" == +([0-9]) ]] || { echo >&2 "Invalid data file: '${LAST}'"; return 1; }
		FILE=$(printf "%02d.json" $(("10#$LAST" + 1)))
	fi
	cat > "${DATA}/${FILE}"
}

# Tests
PLAN 28

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

# Empty input
clearData
addData < /dev/null
RUNS "${SCRIPT}" -m 5  # does not fail on empty input
NOGREP .
EDIFF <<< $'TABLE(projects)\nMAX_ITEMS(25)\n25'

# Basic parameters
clearData
for ignore in {1..90}; do
	addData </dev/null
done
RUNS "${SCRIPT}" --table testTable --max-items 47 --batch-size 13  # does not go beyond max items
EGREP 'TABLE(testTable)'
EGREP 'MAX_ITEMS(13)'
EGREP 52 # 3 * 13 <= 47 < 4 * 13
NEGREP 65 # 47 < 4 * 13 < 5 * 13

# Invalid parameters
NRUNS "${SCRIPT}" --mistaken-parameter  # Invalid parameter
EGREP -i 'invalid parameter.*--mistaken-parameter'

# Convert string literals to json
clearData
addData <<< '{"projectData": {"S": "{\"a\": \"b\"}"}}'
RUNS "${SCRIPT}"  # converts text data into json
OGREP '"projectData":{"S":{"a":"b"}}'

# Convert base64-encoded, gzipped string literals to json
clearData
addData <<< '{"projectBinaryData": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="}}'
RUNS "${SCRIPT}"  # converts binary data into json
OGREP '"projectBinaryData":{"B":{"a":"b"}}'

# Test Max Items/Batch Size
clearData
for ignore in {1..20}; do
	addData <<< '{"projectData": {"S": "{\"a\": \"b\"}"}}'
	addData <<< '{"projectBinaryData": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="}}'
done
RUNS "${SCRIPT}" --max-items 30 --batch-size 12  # reads all available data up to max items
EGREP 'STARTING_TOKEN(12)'
EGREP 'STARTING_TOKEN(24)'
NEGREP 'STARTING_TOKEN(30)'
NEGREP 'STARTING_TOKEN(36)'
EGREP 36

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

# vim: set ts=4 sw=4 tw=100 noet filetype=sh :

