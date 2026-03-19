#!/usr/bin/env bash
# CDH 6.3.3 discovery: YARN ResourceManager + Job History Server -> CSV
# Uses curl with --negotiate (SPNEGO/Kerberos). No Python dependencies needed.
#
# Prerequisites:
#   - kinit with a valid keytab before running
#   - curl and jq installed
#
# Usage:
#   ./yarn_apps_2_csv.sh --rm-url https://rm-host:8090 --out-access yarn_access.csv
#   ./yarn_apps_2_csv.sh --rm-url https://rm-host:8090 --jhs-url https://jhs-host:19890 --out-access yarn_access.csv
#   ./yarn_apps_2_csv.sh --rm-url https://rm-host:8090 --since 2025-01-01 --until 2026-03-11 --last-months 6

set -euo pipefail

RM_URL=""
JHS_URL=""
OUT_ACCESS="yarn_access.csv"
SINCE=""
UNTIL=""
LAST_MONTHS=3
LIMIT=5000
SLEEP_SEC=0.1
CURL_OPTS="-s -k"
STATES="FINISHED,KILLED,FAILED,RUNNING"

usage() {
    cat <<USAGE
Usage: $0 [OPTIONS]

  --rm-url         YARN ResourceManager URL (e.g. https://rm-host:8090)
  --jhs-url        Job History Server URL (e.g. https://jhs-host:19890)
  --out-access     Output CSV (default: yarn_access.csv)
  --since          Start date YYYY-MM-DD (optional)
  --until          End date YYYY-MM-DD (optional)
  --last-months    If --since/--until not given, fetch last N months (default: 3)
  --states         App states for RM query (default: FINISHED,KILLED,FAILED,RUNNING)
  --limit          Page size per request (default: 5000)
  --sleep          Sleep between requests in seconds (default: 0.1)

At least one of --rm-url or --jhs-url is required.
USAGE
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rm-url)      RM_URL="$2";      shift 2 ;;
        --jhs-url)     JHS_URL="$2";     shift 2 ;;
        --out-access)  OUT_ACCESS="$2";   shift 2 ;;
        --since)       SINCE="$2";        shift 2 ;;
        --until)       UNTIL="$2";        shift 2 ;;
        --last-months) LAST_MONTHS="$2";  shift 2 ;;
        --states)      STATES="$2";       shift 2 ;;
        --limit)       LIMIT="$2";        shift 2 ;;
        --sleep)       SLEEP_SEC="$2";    shift 2 ;;
        -h|--help)     usage ;;
        *)             echo "Unknown arg: $1" >&2; usage ;;
    esac
done

if [[ -z "$RM_URL" && -z "$JHS_URL" ]]; then
    echo "ERROR: at least one of --rm-url or --jhs-url is required" >&2
    usage
fi

if ! command -v jq &>/dev/null; then
    echo "ERROR: jq is required. Install with: sudo yum install jq" >&2
    exit 1
fi

RM_URL="${RM_URL%/}"
JHS_URL="${JHS_URL%/}"

yarn_curl() {
    curl $CURL_OPTS --negotiate -u : "$@"
}

# Date -> epoch milliseconds (portable)
date_to_epoch_ms() {
    local d="$1"
    local epoch
    if date -j &>/dev/null 2>&1; then
        epoch=$(date -j -f "%Y-%m-%d" "$d" +%s 2>/dev/null)
    else
        epoch=$(date -u -d "$d" +%s 2>/dev/null)
    fi
    echo $(( epoch * 1000 ))
}

# Epoch ms -> ISO timestamp
epoch_ms_to_iso() {
    local ms="$1"
    if [[ -z "$ms" || "$ms" == "null" || "$ms" == "0" ]]; then
        echo ""
        return
    fi
    local sec=$(( ms / 1000 ))
    if date -j &>/dev/null 2>&1; then
        date -j -u -f "%s" "$sec" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null
    else
        date -u -d "@$sec" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null
    fi
}

# Epoch ms -> YYYY-MM-DD for logging
epoch_ms_to_date() {
    local ms="$1"
    local sec=$(( ms / 1000 ))
    if date -j &>/dev/null 2>&1; then
        date -j -u -f "%s" "$sec" +"%Y-%m-%d" 2>/dev/null
    else
        date -u -d "@$sec" +"%Y-%m-%d" 2>/dev/null
    fi
}

csv_escape() {
    local val="$1"
    if [[ "$val" == *,* ]] || [[ "$val" == *\"* ]] || [[ "$val" == *$'\n'* ]]; then
        val="\"${val//\"/\"\"}\""
    fi
    echo "$val"
}

# Resolve date range to epoch ms
if [[ -n "$SINCE" && -n "$UNTIL" ]]; then
    STARTED_BEGIN=$(date_to_epoch_ms "$SINCE")
    STARTED_END=$(date_to_epoch_ms "$UNTIL")
elif [[ -z "$SINCE" && -z "$UNTIL" ]]; then
    NOW_SEC=$(date -u +%s)
    AGO_SEC=$(( LAST_MONTHS * 30 * 86400 ))
    STARTED_BEGIN=$(( (NOW_SEC - AGO_SEC) * 1000 ))
    STARTED_END=""
else
    echo "ERROR: provide both --since and --until, or neither." >&2
    exit 1
fi

echo "[yarn] Date range: $(epoch_ms_to_date "$STARTED_BEGIN") to ${STARTED_END:+$(epoch_ms_to_date "$STARTED_END")}${STARTED_END:-now}" >&2

# Write CSV header
echo "window_start,service,user,do_as,client_ip,app_name,op,object_type,object_id,cnt" > "$OUT_ACCESS"

SEEN_FILE=$(mktemp)
RAW_FILE=$(mktemp)
trap "rm -f $SEEN_FILE $RAW_FILE" EXIT
TOTAL=0

# ------------------------------------------------------------------ #
#  Batch jq: convert an entire page of RM apps to CSV in one call     #
# ------------------------------------------------------------------ #
JQ_RM_BATCH='
.apps.app // [] | .[] |
  (.startedTime // 0) as $ts |
  ($ts / 1000 | floor | strftime("%Y-%m-%dT%H:%M:%SZ")) as $iso |
  [ $iso, "yarn", (.user // ""), "", "",
    (.name // ""), "SUBMIT", (.applicationType // "application"),
    (.id // ""), "1",
    ($ts | tostring),
    (.queue // ""), (.finalStatus // .state // "")
  ] | @csv
'

# Batch jq: convert an entire page of JHS jobs to CSV in one call
JQ_JHS_BATCH='
.jobs.job // [] | .[] |
  ((.startTime // .submitTime // 0)) as $ts |
  ($ts / 1000 | floor | strftime("%Y-%m-%dT%H:%M:%SZ")) as $iso |
  (.id // "" | gsub("^job_"; "application_")) as $appid |
  [ $iso, "yarn", (.user // ""), "", "",
    (.name // ""), "SUBMIT", "mapreduce",
    $appid, "1",
    ($ts | tostring),
    "", ""
  ] | @csv
'

# Extract max startedTime from RM response
JQ_RM_MAX='.apps.app // [] | [.[].startedTime // 0] | max'
JQ_JHS_MAX='.jobs.job // [] | [.[]|(.startTime // .submitTime // 0)] | max'

# Count apps in response
JQ_RM_COUNT='.apps.app // [] | length'
JQ_JHS_COUNT='.jobs.job // [] | length'

# ------------------------------------------------------------------ #
#  ResourceManager: /ws/v1/cluster/apps                               #
# ------------------------------------------------------------------ #
if [[ -n "$RM_URL" ]]; then
    echo "[yarn] Fetching from ResourceManager: ${RM_URL}" >&2
    CURSOR="$STARTED_BEGIN"
    RM_COUNT=0
    PAGE=0

    while true; do
        URL="${RM_URL}/ws/v1/cluster/apps?limit=${LIMIT}&states=${STATES}"
        if [[ -n "$CURSOR" ]]; then
            URL="${URL}&startedTimeBegin=${CURSOR}"
        fi
        if [[ -n "$STARTED_END" ]]; then
            URL="${URL}&startedTimeEnd=${STARTED_END}"
        fi

        RESPONSE=$(yarn_curl "$URL" 2>/dev/null) || {
            echo "[yarn] WARN: RM request failed at cursor=$(epoch_ms_to_date "${CURSOR:-0}")" >&2
            break
        }

        if [[ -z "$RESPONSE" || "$RESPONSE" == "null" ]]; then
            break
        fi

        COUNT=$(echo "$RESPONSE" | jq "$JQ_RM_COUNT" 2>/dev/null) || COUNT=0
        if [[ "$COUNT" -eq 0 ]]; then
            break
        fi

        # Single jq call to transform entire page to CSV rows
        echo "$RESPONSE" | jq -r "$JQ_RM_BATCH" 2>/dev/null >> "$RAW_FILE"

        MAX_TS=$(echo "$RESPONSE" | jq "$JQ_RM_MAX" 2>/dev/null) || MAX_TS=0
        RM_COUNT=$((RM_COUNT + COUNT))
        PAGE=$((PAGE + 1))

        echo "[yarn] RM page=${PAGE} fetched=${COUNT} total=${RM_COUNT} cursor=$(epoch_ms_to_date "${CURSOR:-0}")" >&2

        if [[ "$COUNT" -lt "$LIMIT" ]]; then
            break
        fi
        if [[ "$MAX_TS" -eq 0 ]]; then
            break
        fi

        NEXT_CURSOR=$((MAX_TS + 1))
        if [[ -n "$CURSOR" && "$NEXT_CURSOR" -le "$CURSOR" ]]; then
            NEXT_CURSOR=$((MAX_TS + 1001))
        fi
        CURSOR="$NEXT_CURSOR"

        if [[ -n "$STARTED_END" && "$CURSOR" -ge "$STARTED_END" ]]; then
            break
        fi

        sleep "$SLEEP_SEC"
    done

    echo "[yarn] ResourceManager: ${RM_COUNT} apps fetched" >&2
fi

# ------------------------------------------------------------------ #
#  Job History Server: /ws/v1/history/mapreduce/jobs                  #
# ------------------------------------------------------------------ #
if [[ -n "$JHS_URL" ]]; then
    echo "[yarn] Fetching from Job History Server: ${JHS_URL}" >&2
    CURSOR="$STARTED_BEGIN"
    JHS_COUNT=0
    PAGE=0

    while true; do
        URL="${JHS_URL}/ws/v1/history/mapreduce/jobs?limit=${LIMIT}"
        if [[ -n "$CURSOR" ]]; then
            URL="${URL}&startedTimeBegin=${CURSOR}"
        fi
        if [[ -n "$STARTED_END" ]]; then
            URL="${URL}&startedTimeEnd=${STARTED_END}"
        fi

        RESPONSE=$(yarn_curl "$URL" 2>/dev/null) || {
            echo "[yarn] WARN: JHS request failed at cursor=$(epoch_ms_to_date "${CURSOR:-0}")" >&2
            break
        }

        if [[ -z "$RESPONSE" || "$RESPONSE" == "null" ]]; then
            break
        fi

        COUNT=$(echo "$RESPONSE" | jq "$JQ_JHS_COUNT" 2>/dev/null) || COUNT=0
        if [[ "$COUNT" -eq 0 ]]; then
            break
        fi

        echo "$RESPONSE" | jq -r "$JQ_JHS_BATCH" 2>/dev/null >> "$RAW_FILE"

        MAX_TS=$(echo "$RESPONSE" | jq "$JQ_JHS_MAX" 2>/dev/null) || MAX_TS=0
        JHS_COUNT=$((JHS_COUNT + COUNT))
        PAGE=$((PAGE + 1))

        echo "[yarn] JHS page=${PAGE} fetched=${COUNT} total=${JHS_COUNT}" >&2

        if [[ "$COUNT" -lt "$LIMIT" ]]; then
            break
        fi
        if [[ "$MAX_TS" -eq 0 ]]; then
            break
        fi

        NEXT_CURSOR=$((MAX_TS + 1))
        if [[ -n "$CURSOR" && "$NEXT_CURSOR" -le "$CURSOR" ]]; then
            NEXT_CURSOR=$((MAX_TS + 1001))
        fi
        CURSOR="$NEXT_CURSOR"

        if [[ -n "$STARTED_END" && "$CURSOR" -ge "$STARTED_END" ]]; then
            break
        fi

        sleep "$SLEEP_SEC"
    done

    echo "[yarn] Job History Server: ${JHS_COUNT} jobs fetched" >&2
fi

# ------------------------------------------------------------------ #
#  Dedup by app-id (column 9) and write final CSV                     #
# ------------------------------------------------------------------ #
# Raw file has 13 columns (10 CSV + startedTime, queue, status as extras)
# Dedup on column 9 (app id), keep first occurrence, output columns 1-10
if [[ -f "$RAW_FILE" && -s "$RAW_FILE" ]]; then
    awk -F',' '!seen[$9]++ { print }' "$RAW_FILE" | \
        cut -d',' -f1-10 >> "$OUT_ACCESS"
fi

TOTAL=$(( $(wc -l < "$OUT_ACCESS" | tr -d ' ') - 1 ))
[[ "$TOTAL" -lt 0 ]] && TOTAL=0
echo "[yarn] Done. Wrote ${TOTAL} unique rows to ${OUT_ACCESS}" >&2
