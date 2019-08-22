#!/usr/bin/env bash
# Bash unofficial lint mode for test setup
set -euo pipefail
IFS=$'\n\t'

# Test helper functions

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

# Test execution environment

(return 0 2>/dev/null) && sourced=1 || sourced=0

getPath() {
	(cd "$(dirname "$1")" && pwd)
}

# Find ourselves/our caller and the script to be tested
TEST_NAME="$(basename "${BASH_SOURCE[$sourced]}" .t)"
TEST_DIR="$(getPath "${BASH_SOURCE[$sourced]}")"
SCRIPT_NAME="${TEST_NAME}.sh"
SCRIPT_DIR="$(getPath "${TEST_DIR}/../${SCRIPT_NAME}")"
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

