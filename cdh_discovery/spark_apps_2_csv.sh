#!/usr/bin/env bash
# CDH 6.3.3 discovery: Spark History Server applications -> CSV
# Uses curl with --negotiate (SPNEGO/Kerberos). No Python dependencies needed.
#
# Prerequisites:
#   - kinit with a valid keytab before running
#   - curl and jq installed
#   - Spark History Server accessible
#
# Usage:
#   ./spark_apps_2_csv.sh --shs-url https://spark-history-host:18488 --out-access spark_access.csv
#   ./spark_apps_2_csv.sh --shs-url https://spark-history-host:18488 --since 2025-01-01 --until 2026-03-11
#   ./spark_apps_2_csv.sh --shs-url https://spark-history-host:18488 --last-months 6

set -euo pipefail

SHS_URL=""
OUT_ACCESS="spark_access.csv"
SINCE=""
UNTIL=""
LAST_MONTHS=3
LIMIT=50000
SLEEP_SEC=0.2
CURL_OPTS="-s -k"

usage() {
    cat <<USAGE
Usage: $0 --shs-url URL [OPTIONS]

  --shs-url       Spark History Server URL (e.g. https://shs-host:18488)
  --out-access     Output CSV (default: spark_access.csv)
  --since          Start date YYYY-MM-DD (optional)
  --until          End date YYYY-MM-DD (optional)
  --last-months    If --since/--until not given, fetch last N months (default: 3)
  --limit          Max apps per request (default: 50000)
  --sleep          Sleep between requests in seconds (default: 0.2)
USAGE
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --shs-url)     SHS_URL="$2";     shift 2 ;;
        --out-access)  OUT_ACCESS="$2";   shift 2 ;;
        --since)       SINCE="$2";        shift 2 ;;
        --until)       UNTIL="$2";        shift 2 ;;
        --last-months) LAST_MONTHS="$2";  shift 2 ;;
        --limit)       LIMIT="$2";        shift 2 ;;
        --sleep)       SLEEP_SEC="$2";    shift 2 ;;
        -h|--help)     usage ;;
        *)             echo "Unknown arg: $1" >&2; usage ;;
    esac
done

if [[ -z "$SHS_URL" ]]; then
    echo "ERROR: --shs-url is required" >&2
    usage
fi

if ! command -v jq &>/dev/null; then
    echo "ERROR: jq is required. Install with: sudo yum install jq" >&2
    exit 1
fi

SHS_URL="${SHS_URL%/}"

shs_curl() {
    curl $CURL_OPTS --negotiate -u : "$@"
}

# Resolve date range
if [[ -n "$SINCE" && -n "$UNTIL" ]]; then
    START_DATE="$SINCE"
    END_DATE="$UNTIL"
elif [[ -z "$SINCE" && -z "$UNTIL" ]]; then
    END_DATE=$(date -u +%Y-%m-%d)
    START_DATE=$(date -u -v-${LAST_MONTHS}m +%Y-%m-%d 2>/dev/null || \
                 date -u -d "${LAST_MONTHS} months ago" +%Y-%m-%d 2>/dev/null || \
                 date -u +%Y-%m-%d)
else
    echo "ERROR: Provide both --since and --until, or neither." >&2
    exit 1
fi

echo "[spark] Date range: ${START_DATE} to ${END_DATE}" >&2

# Write CSV header
echo "window_start,service,user,do_as,client_ip,app_name,op,object_type,object_id,cnt" > "$OUT_ACCESS"

# Helper: advance date by 1 day (portable across macOS/Linux)
next_day() {
    local d="$1"
    if date -v+1d &>/dev/null 2>&1; then
        date -j -f "%Y-%m-%d" "$d" -v+1d +%Y-%m-%d
    else
        date -u -d "$d + 1 day" +%Y-%m-%d
    fi
}

# Helper: convert SHS timestamp to ISO
ts_to_iso() {
    local ts="$1"
    if [[ -z "$ts" || "$ts" == "null" ]]; then
        echo ""
        return
    fi
    echo "$ts" | sed -E 's/\.[0-9]+GMT$/Z/; s/GMT$/Z/; s/\.[0-9]+$/Z/; s/([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}).*/\1Z/'
}

# Helper: escape a value for CSV
csv_escape() {
    local val="$1"
    if [[ "$val" == *,* ]] || [[ "$val" == *\"* ]] || [[ "$val" == *$'\n'* ]]; then
        val="\"${val//\"/\"\"}\""
    fi
    echo "$val"
}

TOTAL=0
SEEN_FILE=$(mktemp)
trap "rm -f $SEEN_FILE" EXIT

CURRENT="$START_DATE"
DAY_COUNT=0

while [[ "$CURRENT" < "$END_DATE" || "$CURRENT" == "$END_DATE" ]]; do
    NEXT=$(next_day "$CURRENT")
    URL="${SHS_URL}/api/v1/applications?minDate=${CURRENT}&maxDate=${NEXT}&limit=${LIMIT}"

    RESPONSE=$(shs_curl "$URL" 2>/dev/null) || {
        echo "[spark] WARN: failed to fetch ${CURRENT}" >&2
        CURRENT="$NEXT"
        sleep "$SLEEP_SEC"
        continue
    }

    if [[ -z "$RESPONSE" || "$RESPONSE" == "null" ]]; then
        CURRENT="$NEXT"
        sleep "$SLEEP_SEC"
        continue
    fi

    COUNT=$(echo "$RESPONSE" | jq 'length' 2>/dev/null) || COUNT=0

    if [[ "$COUNT" -gt 0 ]]; then
        NEW=0
        echo "$RESPONSE" | jq -c '.[]' 2>/dev/null | while IFS= read -r APP; do
            APP_ID=$(echo "$APP" | jq -r '.id // ""')

            if [[ -z "$APP_ID" ]]; then
                continue
            fi
            if grep -qF "$APP_ID" "$SEEN_FILE" 2>/dev/null; then
                continue
            fi
            echo "$APP_ID" >> "$SEEN_FILE"

            APP_NAME=$(echo "$APP" | jq -r '.name // ""')
            SPARK_USER=$(echo "$APP" | jq -r '.attempts[-1].sparkUser // ""')
            START_TIME=$(echo "$APP" | jq -r '.attempts[-1].startTime // ""')

            ISO_TIME=$(ts_to_iso "$START_TIME")
            APP_NAME_CSV=$(csv_escape "$APP_NAME")

            echo "${ISO_TIME},spark,${SPARK_USER},,,${APP_NAME_CSV},SUBMIT,application,${APP_ID},1" >> "$OUT_ACCESS"
        done

        TOTAL=$(wc -l < "$SEEN_FILE" | tr -d ' ')
    fi

    DAY_COUNT=$((DAY_COUNT + 1))
    if [[ "$COUNT" -gt 0 ]] || [[ $((DAY_COUNT % 7)) -eq 0 ]]; then
        echo "[spark] ${CURRENT} fetched=${COUNT} total=${TOTAL}" >&2
    fi

    if [[ "$COUNT" -ge "$LIMIT" ]]; then
        echo "[spark] WARNING: ${CURRENT} returned ${COUNT} apps (= limit). May be truncated." >&2
    fi

    CURRENT="$NEXT"
    sleep "$SLEEP_SEC"
done

TOTAL=$(wc -l < "$SEEN_FILE" | tr -d ' ')
echo "[spark] Done. Wrote ${TOTAL} rows to ${OUT_ACCESS}" >&2
