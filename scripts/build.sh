#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Lint shell scripts
find . \( -name "*.sh" -or -name "*.ksh" -or -name "*.bash" \) -not -name osht.sh -prune -print0 |
	xargs -0 -I % bash -c 'echo "Linting script %"; shellcheck %'

# Run shell test
find . -path '*/t/*.t' -perm -u+x -print0 |
	xargs -0 -I % bash -c 'echo "Running Test %"; %'

