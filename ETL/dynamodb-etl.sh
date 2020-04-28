#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

if [[ ! -x $(type -p jq) ]]; then
	cat >&2 <<-MISSING_JQ
		Please install "jq". On Amazon AMI Linux, install it with:

		    sudo yum install -y jq

		See https://stedolan.github.io/jq/download/ for information on other
		platforms.
	MISSING_JQ
	exit 3
fi

if [[ $(jq --version) == jq-1.[0-4] ]]; then
	echo >&2 "Please update jq to version 1.5 or higher"
	exit 4
fi

usage() {
	cat >&2 <<-USAGE
		Usage: $0 [-?|-h|--help] [<binary_path> <string_path> <merged_path>]

		-? | -h | --help             Prints this message
		-a | --all                   Process all data (overrides total)
		-m N | --max-items N         Load data from DynamoDB in batches of N (defaults to 25)
		-M N | --multiple N          Process loaded data in batches of -m * -M (defaults to 100)
		-o <file> | --output <file>  Send output to "file" (use %d to represent worker number)
		-p <cmd> | --pipe <cmd>      Pipes output to a command (use %d to represent worker number)
		-q | --quiet                 Do not print progress information
		-r | --raw                   Do not decode data
		-s | --stdout                Sends output to stdout (default)
		-t N | --total N             Limits processing to the first N entries (defaults to 100)
		-T NAME | --table NAME       DynamoDB table name (defaults to "projects")
		-v | --verbose               Prints extra information on errors
		-w N | --workers N           Number of concurrent workers

		Paths are specified as .x.y.z for { "x": { "y": { "z": data }}}. More
		generally, they must be valid "jq" paths.

		Binary paths must point to a string that contains a base64-encoded,
		gzipped json, so that "base64 --decode | gzip -d" will turn that
		string into valid json.

		String paths must point to a string that contains valid json. For
		example, .x in { "x": "{ \"a\": 5 }" }.

		Use a non-existing path if there's no binary or string path. For
		example, ".no.binary.path .path.to.string" if there's string data
		on the .path.to.string, but not binary data, and ".no.binary.path"
		is not an existing path in the input data.

		The output is sent to stdout by default. When running with multiple workers,
		that process is coordinated to race issues on the output.

		When sending output to a file, the file name is created by doing
		"printf <file> <worker id>", with worker id numbering from 0 to the total
		number of workers minus 1.

		When sending output to a pipe, the command is created by doing
		"printf <cmd> <worker id>", with worker id numbering from 0 to the total
		number of workers minus 1. That command is evaluated to allow further pipes
		and redirections, but misuse can break the script.
	USAGE
	exit 1
}

POSITIVE_INTEGER='^[1-9][0-9]*$'
while [[ $# -gt 0 && $1 == -* ]]; do
	case "$1" in
	-\? | -h | --help) usage ;;
	-a | --all) ALL=1 ;;
	-m | --max-items)
		shift
		MAX_ITEMS="${1}"
		if [[ ! "$MAX_ITEMS" =~ $POSITIVE_INTEGER ]]; then
			echo >&2 "Max items must be a positive integer, got '$MAX_ITEMS'"$'\n'
			usage
		fi
		;;
	-M | --multiple)
		shift
		MULTIPLE="${1}"
		if [[ ! "$MULTIPLE" =~ $POSITIVE_INTEGER ]]; then
			echo >&2 "Multiple must be a positive integer, got '$MAX_ITEMS'"$'\n'
			usage
		fi
		;;
	-t | --total)
		shift
		TOTAL="${1}"
		if [[ ! "$TOTAL" =~ $POSITIVE_INTEGER ]]; then
			echo >&2 "Total must be a positive integer, got '$TOTAL'"$'\n'
			usage
		fi
		;;
	-T | --table)
		shift
		TABLE="${1}"
		;;
	-o | --output)
		shift
		OUTPUT="${1}"
		if [[ -z $OUTPUT ]]; then
			echo >&2 $'Output file cannot be empty\n'
			usage
		fi
		;;
	-p | --pipe)
		shift
		PIPE_TO="${1}"
		if [[ -z $PIPE_TO ]]; then
			echo >&2 $'Pipe command cannot be empty\n'
			usage
		fi
		;;
	-q | --quiet)
		QUIET=1
		;;
	-r | --raw)
		RAW=1
		;;
	-s | --stdout)
		STDOUT=1
		;;
	-v | --verbose)
		VERBOSE=1
		;;
	-w | --workers)
		shift
		WORKERS="${1}"
		if [[ ! "$WORKERS" =~ $POSITIVE_INTEGER ]]; then
			echo >&2 "Workers must be a positive integer, got '$WORKERS'"$'\n'
			usage
		fi
		;;
	# These options are intentionally undocumented
	---no-timer)
		NO_TIMER=1
		;;
	---read-from)
		shift
		READ_FROM="${1}"
		;;
	---segments-size)
		shift
		SEGMENTS_SIZE=( '---segments-size' "$@" )
		shift $(("${#SEGMENTS_SIZE[@]}" - 2))
		;;
	--profiling)
		PROFILING=1
		;;
	*)
		echo >&2 "Invalid parameter '$1'"$'\n'
		usage
		;;
	esac
	shift
done

if [[ $# -ne 0 && $# -ne 3 ]]; then
	echo >&2 "Either all paths or no paths must be passed!"
	exit 2
fi

BINARY_PATH="${1:-.projectBinaryData.B}"
STRING_PATH="${2:-.projectData.S}"
MERGED_PATH="${3:-.mergedProjectData}"
BINARY_DEFAULT_QUERY="${BINARY_PATH}"' // "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA="'
STRING_DEFAULT_QUERY="${STRING_PATH}"' // "{}"'
BINARY_JQ_PATH="$(jq -n -c "path($BINARY_PATH)")"
STRING_JQ_PATH="$(jq -n -c "path($STRING_PATH)")"
MERGED_JQ_PATH="$(jq -n -c "path($MERGED_PATH)")"
# False positive on -d 'EOL':
# shellcheck disable=SC2034
read -r -d 'EOL' OUTPUT_QUERY <<-QUERY
	. as \$uncompressed
	| (\$line | ${STRING_DEFAULT_QUERY} | fromjson) as \$literal
	| \$line
	| setpath(${MERGED_JQ_PATH};
		if getpath(${BINARY_JQ_PATH}) then \$uncompressed
		else if getpath(${STRING_JQ_PATH}) then \$literal
		else null end
		end
	  )
	| del(${BINARY_PATH})
	| del(${STRING_PATH})
	EOL
QUERY

: "${ALL:=}"
: "${MAX_ITEMS:=25}"
: "${MULTIPLE:=100}"
: "${NO_TIMER:=}"
: "${OUTPUT:=}"
: "${PIPE_TO:=}"
: "${QUIET:=}"
: "${VERBOSE:=}"
: "${RAW:=}"
: "${READ_FROM:=}"
if [[ -z $OUTPUT && -z $PIPE_TO ]]; then
	: "${STDOUT:=1}"
else
	: "${STDOUT:=}"
fi
: "${TABLE:=projects}"
: "${TOTAL:=100}"
: "${PROFILING:=}"
: "${WORKERS:=1}"

if [[ -n $STDOUT && -n $OUTPUT ]]; then
	echo >&2 "Parameters --stdout and --output are mutually exclusive"
	exit 1
fi

if [[ -n $STDOUT && -n $PIPE_TO ]]; then
	echo >&2 "Parameters --stdout and --pipe are mutually exclusive"
	exit 1
fi

if [[ -n $PIPE_TO && -n $OUTPUT ]]; then
	echo >&2 "Parameters --pipe and --output are mutually exclusive"
	exit 1
fi

if [[ $WORKERS -gt 1 && -n $STDOUT ]]; then
	read -r -t 1 < <(echo -n "X"; sleep 2; echo "Y") || :
	if [[ "${REPLY:-}" != "X" ]]; then
		echo >&2 "Multiple workers unsupported for stdout in this bash version"
		exit 6
	fi
fi

# If Bash 5
if ( : "$EPOCHREALTIME" ) 2> /dev/null; then
	timestamp() {
		echo "$EPOCHREALTIME"
	}
	since() {
		bc -l <<<"$EPOCHREALTIME - $1"
	}
elif [[ "$(date +%N)" =~ [0-9]+ ]]; then
	timestamp() {
		date +%s.%N
	}
	since() {
		bc -l <<<"$(timestamp) - $1"
	}
else
	timestamp() {
		date +%s
	}
	since() {
		echo $(($(timestamp) - $1))
	}
fi

# If BSD
if ( stat -f "%z" "${BASH_SOURCE[0]}" > /dev/null 2>&1 ); then
	filesize() {
		stat -f "%z" "$1"
    }
else
	filesize() {
		stat -c "%s" "$1"
	}
fi

if [[ "${BASH_VERSINFO[0]}" -gt 4 || ( "${BASH_VERSINFO[0]}" -eq 4 && "${BASH_VERSINFO[1]}" -ge 3 ) ]]; then
	waitChildren() {
		declare -g WAIT_FOR
		declare ignored
		# "wait -n" will go through terminated children, "jobs -p" will not list them
		for ignored in $(seq 1 "${WAIT_FOR}"); do
			wait -n
		done
	}
else
	waitChildren() {
		# won't do early exit, but what can you do?
		declare -g WORKER_PID
		declare -g MAIN_PID
		declare index
		for index in "${!WORKER_PID[@]}"; do
			wait "${WORKER_PID[$index]}"
		done

		for index in "${!MAIN_PID[@]}"; do
			wait "${MAIN_PID[$index]}"
		done
	}
fi

# Fetch total if using --all
if [[ -n $ALL ]]; then
	if [[ -z $READ_FROM ]]; then
		TOTAL="$(aws dynamodb describe-table --table-name "${TABLE}" | jq .Table.ItemCount)"
		[[ -n $QUIET ]] || echo >&2 "Total ${TOTAL}"
	else
		FILES=( "${READ_FROM}"* )
		TOTAL="${#FILES[@]}"
	fi
fi

# Extra scan flags on verbose
if [[ -n $PROFILING ]]; then
	PROFILING_SCAN=( '--return-consumed-capacity' 'TOTAL' )
else
	PROFILING_SCAN=( )
fi

# Adjust total for number of workers; we can't predict which workers will get remainders
REMAINDER=$((TOTAL % WORKERS))
TOTAL=$((TOTAL / WORKERS))

main_stdout() {
	if [[ -n $PROFILING ]]; then
		exec 5> main.profiling
		echo >&5 "timestamp,wait,worker,send"
	fi

	if [[ $WORKERS -gt 1 ]]; then
		OPT_TIMEOUT=("-t" "1")
	else
		OPT_TIMEOUT=( )
	fi

	t0=$(timestamp)
	profiling -n "$(timestamp),"
	while [[ ${#PIPE_FD[@]} -gt 0 ]]; do
		for index in "${!PIPE_FD[@]}"; do
			if read -r ${OPT_TIMEOUT[@]+"${OPT_TIMEOUT[@]}"} -u "${PIPE_FD[$index]}" file; then
				profiling -n "$(since "$t0"),$index,"
				t0=$(timestamp)
				TMP_FILE="${WORKER_PARTIAL[$index]:-}$file"
				if [[ $TMP_FILE != /* ]]; then
					TMP_FILE="/${TMP_FILE}"
				fi
				cat "${TMP_FILE}"
				unset WORKER_PARTIAL["$index"]
				profiling "$(since "$t0")"
				t0=$(timestamp)
				profiling -n "$(timestamp),"
				rm "${TMP_FILE}"
			elif [[ $? -lt 128 ]]; then
				unset "PIPE_FD[$index]"
			else
				WORKER_PARTIAL[$index]="${WORKER_PARTIAL[$index]:-}${file}"
			fi
		done
	done

	profiling "$(since "$t0"),end,"
}

main_pipe() {
	declare index
	declare CMD
	declare TMP
	declare -g PIPE_TO
	declare -g PIPE_NAME
	declare -g OUTPUT
	declare -g WORKER_INFO

	index="$1"
	if [[ -n $PIPE_TO ]]; then
		# shellcheck disable=SC2059
		CMD="$(printf "$PIPE_TO" "$index")"
		WORKER_INFO="Pipe #${index}:"
	else
		# shellcheck disable=SC2059
		TMP="$(printf "$OUTPUT" "$index")"
		CMD="cat > $TMP"
		WORKER_INFO="Output #${index}:"
	fi

	trap 'showAborted' EXIT

	eval "$CMD" < "${PIPE_NAME[$index]}" || {
		echo >&2 "'$CMD' exited with error code $?"
		exit 7
	}

	trap - EXIT

	showFinished
}

worker() {
	declare -g COUNT
	declare -g NEXTTOKEN
	declare -g -a SEGMENTATION
	declare -g WORKER
	declare -g WORKER_INFO

	COUNT=0
	NEXTTOKEN='null'
	SECONDS=0
	WORKER=$1

	if [[ -n $PROFILING ]]; then
		exec 5> "worker${WORKER}.profiling"
		echo >&5 "epochseconds,scan,math,size,tsize,count,consumed,scanned,osize,decode,pipe,loop"
	fi

	# Last worker gets total remainders
	if [[ $1 -eq $((WORKERS - 1))  ]]; then
		TOTAL=$((TOTAL + REMAINDER))
	fi

	# Adjust max items if greater than total
	curbMaxItems

	if [[ $WORKERS -eq 1 ]]; then
		SEGMENTATION=( )
		WORKER_INFO=""
	else
		SEGMENTATION=('--segment' "$WORKER" '--total-segments' "$WORKERS")
		WORKER_INFO="Worker #$WORKER: "
	fi

	trap 'showAborted' EXIT

	if [[ -z $READ_FROM ]]; then
		readData
	else
		FILES=( "${READ_FROM}"* )
		FILE="${FILES[$WORKER]}"
		processData < "${FILE}"
	fi

	trap - EXIT

	showFinished
}

scan() {
	local t1
	t1=$(timestamp)
	ACTUAL_MAX_ITEMS=$((MULTIPLE * MAX_ITEMS))
	profiling -n "$(timestamp),"
	if [[ $# -eq 1 ]]; then
		aws dynamodb scan --output json --table-name "$TABLE" --page-size "$MAX_ITEMS" \
			--max-items "$ACTUAL_MAX_ITEMS"\
			${SEGMENTATION[@]+"${SEGMENTATION[@]}"} --starting-token "$1" \
			${PROFILING_SCAN[@]+"${PROFILING_SCAN[@]}"} \
			${SEGMENTS_SIZE[@]+"${SEGMENTS_SIZE[@]}"} || {
			echo >&2 "Scan failed with exit code $?"
			if [[ -n $VERBOSE ]]; then
				echo >&2 "Command attempted:"
				echo >&2 "aws dynamodb scan --output json --table-name \"$TABLE\" --page-size \"$MAX_ITEMS\" \\"
				echo >&2 "--max-items \"$ACTUAL_MAX_ITEMS\" \\"
				echo >&2 "${SEGMENTATION[@]+"${SEGMENTATION[@]}"} --starting-token \"$1\" \\"
				echo >&2 "${PROFILING_SCAN[@]+"${PROFILING_SCAN[@]}"} \\"
				echo >&2 "${SEGMENTS_SIZE[@]+"${SEGMENTS_SIZE[@]}"}"
			fi
			exit 6
		}
	else
		aws dynamodb scan --output json --table-name "$TABLE" --page-size "$MAX_ITEMS" \
			--max-items "$ACTUAL_MAX_ITEMS"\
			${SEGMENTATION[@]+"${SEGMENTATION[@]}"} \
			${PROFILING_SCAN[@]+"${PROFILING_SCAN[@]}"} \
			${SEGMENTS_SIZE[@]+"${SEGMENTS_SIZE[@]}"} || {
			echo >&2 "Scan failed with exit code $?"
			if [[ -n $VERBOSE ]]; then
				echo >&2 "Command attempted:"
				echo >&2 "aws dynamodb scan --output json --table-name \"$TABLE\" --page-size \"$MAX_ITEMS\" \\"
				echo >&2 "--max-items \"$ACTUAL_MAX_ITEMS\" \\"
				echo >&2 "${SEGMENTATION[@]+"${SEGMENTATION[@]}"} \\"
				echo >&2 "${PROFILING_SCAN[@]+"${PROFILING_SCAN[@]}"} \\"
				echo >&2 "${SEGMENTS_SIZE[@]+"${SEGMENTS_SIZE[@]}"}"
			fi
			exit 6
		}
	fi
	profiling -n "$(since "$t1"),"
}

readData() {
	local BYTES KBYTES t1 t2
	t1=$(timestamp)
	DATA=$(scan)
	t2=$(timestamp)
	BYTES=${#DATA}
	KBYTES=$(((BYTES + 512) / 1024))
	NEXTTOKEN=$(jq -r '.NextToken' <<<"$DATA")
	ITEMS_READ=$(jq -r -c '.Items|length' <<<"$DATA")
	COUNT=$ITEMS_READ
	profiling -n "$(since "$t2"),$BYTES,$KBYTES,$COUNT,$(
		jq -rc '[.ConsumedCapacity.CapacityUnits,.ScannedCount]|map(tostring)|join(",")' <<<"$DATA"
	),"
	jq -r -c '.Items[]' <<<"$DATA" | processData
	showCount

	while [[ ${NEXTTOKEN} != 'null' && (${TOTAL} -gt ${COUNT} || $ALL) ]]; do
		profiling "$(since "$t1")"
		t1=$(timestamp)
		curbMaxItems
		DATA=$(scan "${NEXTTOKEN}")
		t2=$(timestamp)
		BYTES=${#DATA}
		KBYTES=$(((BYTES + 512) / 1024 + KBYTES))
		NEXTTOKEN=$(jq -r '.NextToken' <<<"$DATA")
		ITEMS_READ=$(jq -r -c '.Items|length' <<<"$DATA")
		COUNT=$((COUNT + ITEMS_READ))
		profiling -n "$(since "$t2"),$BYTES,$KBYTES,$COUNT,$(
			jq -rc '[.ConsumedCapacity.CapacityUnits,.ScannedCount]|map(tostring)|join(",")' <<<"$DATA"
		),"
		jq -r -c '.Items[]' <<<"$DATA" | processData
		showCount
		showEllapsed
	done
	profiling "$(since "$t1")"
}

processData() {
	local t1 t2
	t1=$(timestamp)

	if [[ -n $STDOUT ]]; then
		TMP="$(getTMP)"
		if [[ $TMP != /* ]]; then
			echo >&2 "Temporary file not on absolute path: ${TMP}"
			exit 5
		fi
	else
		TMP="$PIPE"
	fi

	while read -r line; do
		if [[ -z $RAW ]]; then
			jq -r -c "${BINARY_DEFAULT_QUERY}" <<< "$line" | base64 --decode | gzip -d |
				jq -r -c ". as \$line | input | ${OUTPUT_QUERY}" <(cat <<<"$line") <(cat) || {
					echo >&2 "Decode failed with exit code $?"
					echo >&2 'Invalid JSON!'
					[[ -z $VERBOSE ]] || cat >&2 <<<"$line"
				}
		else
			echo "$line"
		fi
	done >> "$TMP"
	if [[ -n $STDOUT ]]; then
		profiling -n "$(filesize "${TMP}"),$(since "$t1")," || :
	else
		profiling -n "0,0,"
	fi
	t2=$(timestamp)
	if [[ -n $STDOUT ]]; then
		echo "$TMP"
	fi
	profiling -n "$(since "$t2"),"
}

curbMaxItems() {
	[[ -n $ALL ]] && return
	REMAINING=$((TOTAL - COUNT))
	if [[ $REMAINING -lt $MAX_ITEMS ]]; then
		MAX_ITEMS=$REMAINING
	fi
}

showEllapsed() {
	if [[ -z $NO_TIMER && -z $QUIET && -n $ALL ]]; then
		duration=$SECONDS
		estimate=$((TOTAL * duration / COUNT))
		echo >&2 -n "Time ellapsed: $(showTimer $duration)"
		echo >&2 " estimated: $(showTimer $estimate)"
	fi
}

showFinished() {
	if [[ -z $NO_TIMER && -z $QUIET && -n $ALL ]]; then
		echo >&2 "${WORKER_INFO} Finished"
	fi
}

showAborted() {
	if [[ -z $NO_TIMER && -z $QUIET ]]; then
		echo >&2 "${WORKER_INFO} *** ABORTED ***"
	fi
}

showTimer() {
	duration="$1"
	seconds=$((duration % 60))
	minutes=$((duration / 60 % 60))
	hours=$((duration / 3600))
	echo -n "$hours hour$(ss $hours) $minutes minute$(ss $minutes) $seconds second$(ss $seconds)"
}

showCount() {
	if [[ -z $QUIET ]]; then
		if [[ -n $ALL ]]; then
			perc=$((COUNT * 100 / TOTAL))
			echo >&2 "${WORKER_INFO}${COUNT} (${perc}%)"
		else
			echo >&2 "${WORKER_INFO}${COUNT}"
		fi
	fi
}

ss() {
	s="${2-s}"
	[[ $1 -ne 1 ]] && echo -n "$s"
}

getPipe() {
	declare PIPE
	PIPE="$(getTMP)"
	rm -f "$PIPE"
	mkfifo "$PIPE"
	echo "$PIPE"
}

getTMP() {
	mktemp -t "dynamodb-etl.XXXXXXXXXX"
}

profiling() {
	if [[ -n $PROFILING ]]; then
		echo >&5 "$@"
	fi
}

declare -a PIPE_NAME
declare -a PIPE_FD
declare -a WORKER_PARTIAL
declare -a WORKER_PID

trap 'kill $(jobs -p)' EXIT

for worker in $(seq 0 $((WORKERS - 1))); do
	segment="$worker"
	PIPE="$(getPipe "$worker")"
	PIPE_NAME[$worker]="$PIPE"
	worker "${segment}" > "$PIPE" &
	: {FD}< "${PIPE}"
	WORKER_PID["${worker}"]=$!
	PIPE_FD["${worker}"]="${FD}"
done

declare -a MAIN_PID

if [[ -n $STDOUT ]]; then
	main_stdout
else
	for index in "${!PIPE_NAME[@]}"; do
		main_pipe "$index" &
		MAIN_PID["${index}"]=$!
	done
fi

declare WAIT_FOR

if [[ -n $STDOUT ]]; then
	WAIT_FOR="${WORKERS}"
else
	WAIT_FOR=$((WORKERS * 2))
fi

waitChildren

trap - EXIT

for pipe in "${PIPE_NAME[@]}"; do
	rm "$pipe"
done

# vim: set sts=4 ts=4 sw=4 tw=100 noet :
