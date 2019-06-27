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

	NULLGLOB=\$(shopt -p nullglob) || :
	shopt -s nullglob
	declare -a FILES
	FILES=("${DATA}"/*)
	NEXT="\${FILES[0]}"
	cat "\${NEXT}"
	rm -f "\${NEXT}"
	eval "\${NULLGLOB}"
AWS
chmod +x "${BIN}/aws"
export PATH="${BIN}:${PATH}"

# Load test framework
unset IFS # osht depends on default IFS
# shellcheck source=t/osht.sh disable=SC1094
source "$(dirname "$0")/osht.sh"

# Tests run unlinted
set +euo pipefail

# Test helpers
clearData() {
	[[ -d "${DATA}" ]] && rm -fr "${DATA}"
	mkdir -p "${DATA}"
}

addData() {
	mapfile -t < <(cd "${DATA}"; ls)
	if [[ "${#MAPFILE[@]}" -eq 0 ]]; then
		FILE="00.json"
	else
		LAST="${MAPFILE[-1]%.json}"
		[[ "${LAST}" == +([0-9]) ]] || { echo >&2 "Invalid data file: '${LAST}'"; return 1; }
		FILE=$(printf "%02d.json" $((LAST + 1)))
	fi
	cat > "${DATA}/${FILE}"
}

# Tests
PLAN 7

# Simulate jq not installed
jq() { echo "Why?"; exit 1; }
export -f jq
NRUNS "${SCRIPT}"
EGREP 'install "jq"'
unset -f jq

# Simulate wrong jq version
JQ="${BIN}/jq"
echo 'echo "jq-1.4"' > "${JQ}"
chmod +x "${JQ}"
NRUNS "${SCRIPT}"
EGREP '1.5'
rm "${JQ}"

# Empty input
clearData; addData < /dev/null
RUNS "${SCRIPT}" -m 5
NOGREP .
EDIFF <<< "25"

# vim: set ts=4 sw=4 tw=100 noet filetype=sh :

