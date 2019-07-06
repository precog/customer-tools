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
		Usage: $0 [-?|-h|--help] <wordpress_base_url> <basic_auth_username> <basic_auth_password>
		
		-? | -h | --help                      Prints this message
		-a | --all                            Process all orders (overrides total)
		-p N | --per-page N                   Process orders in batches of N (defaults to 100)
		-q | --quiet                          Do not print progress information
		-t N | --total N                      Only include the first N orders (defaults to 1000)
		-E NAME | --endpoint NAME             WooCommerce endpoint name (defaults to "orders")
		-A START_DATE | --after START_DATE    Only include orders after this date (E.g. "2017-03-22")
		-B END_DATE | --before END_DATE       Only include orders before this date (E.g. "2018-04-14")
		
	USAGE
	exit 1
}

while [[ $# -gt 0 && $1 == -* ]]; do
	case "$1" in
	-h | --help) usage ;;
	-a | --all) ALL=1 ;;
	-p | --per-page)
		shift
		PER_PAGE="${1}"
		;;
	-t | --total)
		shift
		TOTAL="${1}"
		;;
	-E | --endpoint)
		shift
		ENDPOINT="${1}"
		;;
	-A | --after)
		shift
		AFTER="${1}"
		;;
	-B | --before)
		shift
		BEFORE="${1}"
		;;
	-q | --quiet)
		QUIET=1
		;;
	*)
		echo >&2 "Invalid parameter '$1'"$'\n'
		usage
		;;
	esac
	shift
done

WORDPRESS_BASE_URL="${1:-https://example.com}"
BASIC_AUTH_USER="${2:-example_username}"
BASIC_AUTH_PASSWORD="${2:-example_password}"

: "${ALL:=}"
: "${PER_PAGE:=100}"
: "${QUIET:=}"
: "${ENDPOINT:=orders}"
: "${TOTAL:=1000}"
: "${AFTER:=}"
: "${BEFORE:=}"

NEXTTOKEN='null'
COUNT=0
PAGE=1
{
	if [[ ${TOTAL} -lt ${PER_PAGE} ]]; then
		PER_PAGE=${TOTAL}
	fi
	BEFORE_PARAM=""
	AFTER_PARAM=""
	if [[ ! -z $BEFORE ]]; then
		BEFORE_PARAM="before=$BEFORE&"
	fi
	if [[ ! -z $AFTER ]]; then
		AFTER_PARAM="after=$AFTER&"
	fi
	URL="$WORDPRESS_BASE_URL/wp-json/wc/v2/${ENDPOINT}?${BEFORE_PARAM}${AFTER_PARAM}"
	ITEMS_READ=$PER_PAGE
	while [[ $ITEMS_READ -gt $(($PER_PAGE - 1)) && (-n ${ALL} || ${TOTAL} -gt 0) ]]; do
		if [[ ${TOTAL} -lt ${PER_PAGE} ]]; then
			PER_PAGE=${TOTAL}
		fi
		DATA=$(curl --silent --user "$BASIC_AUTH_USER:$BASIC_AUTH_PASSWORD" "${URL}per-page=${PER_PAGE}&page=${PAGE}")
		cat <<< "$DATA" | jq -c '.[]' 
		ITEMS_READ=$(jq -r -c 'length' <<< "$DATA")
		TOTAL=$((TOTAL - ITEMS_READ))
		COUNT=$((COUNT + ITEMS_READ))
		PAGE=$(($PAGE + 1))
		[[ -n $QUIET ]] || echo >&2 $COUNT
	done
}
# vim: set ts=4 sw=4 tw=1000 noet :
