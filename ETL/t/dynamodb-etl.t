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
cat > "${BIN}/countLines.sh" <<-COUNTLINES
	#!/usr/bin/env bash
	"${SCRIPT}" "\$@" | tee >(cat >&2) | wc -l | tr -d ' '
COUNTLINES
chmod +x "${BIN}/countLines.sh"
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
		FILE=$(printf "%02d.json" $((10#$LAST + 1)))
	fi
	cat > "${DATA}/${FILE}"
}

# Tests
PLAN 36

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
RUNS "${SCRIPT}" -t 5  # does not fail on empty input
NOGREP .
EDIFF <<< $'TABLE(projects)\nMAX_ITEMS(5)\n0'

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

# Handles data over 400 KB in length
clearData
addData <<< "{\"key\": [$(seq -s , 1 100000)0]}"
RUNS "${SCRIPT}"  # handles records bigger than 400 KB
EDIFF <<< $'TABLE(projects)\nMAX_ITEMS(25)\n1'

# Setup for tests counting data
clearData
# shellcheck disable=SC2034
for ignore in {1..20}; do
	addData <<< '{"projectData": {"S": "{\"a\": \"b\"}"}}'
	addData <<< '{"projectBinaryData": {"B": "H4sIAMzyFV0CA6tWUEpUslJQSlJSqAUACEgasgwAAAA="}}'
done

# Quiet running
RUNS "${SCRIPT}" -q -t 5
EDIFF <<< $'TABLE(projects)\nMAX_ITEMS(5)'
RUNS "${SCRIPT}" --quiet -t 5
EDIFF <<< $'TABLE(projects)\nMAX_ITEMS(5)'

# Basic parameters
RUNS "${SCRIPT}" --table testTable --total 19 --max-items 7  # does not go beyond total
EGREP 'TABLE(testTable)'
EGREP 'MAX_ITEMS(7)'
EGREP 19
NEGREP 21

# Test Total/Max Items
RUNS "${SCRIPT}" --total 30 --max-items 12  # reads all available data up to total
EGREP 'STARTING_TOKEN(12)'
EGREP 'STARTING_TOKEN(24)'
NEGREP 'STARTING_TOKEN(30)'
NEGREP 'STARTING_TOKEN(36)'
EGREP 30

# Test Total/Max Items for Total < Max Items
RUNS countLines.sh -t 5  # 5 total out of 40 with 25 increments
ODIFF <<< $'5'

# Test Total/Max Items for Total > Max Items
RUNS countLines.sh -t 10 -m 7  # 10 total out of 40 with 7 increments
ODIFF <<< $'10'

# vim: set ts=4 sw=4 tw=100 noet filetype=sh :

