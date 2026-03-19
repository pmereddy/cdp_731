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
LIMIT=1000
SLEEP_SEC=0.2
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
  --limit          Page size per request (default: 1000)
  --sleep          Sleep between requests in seconds (default: 0.2)

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
trap "rm -f $SEEN_FILE" EXIT
TOTAL=0

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

        APPS=$(echo "$RESPONSE" | jq -c '.apps.app // []' 2>/dev/null)
        COUNT=$(echo "$APPS" | jq 'length' 2>/dev/null) || COUNT=0

        if [[ "$COUNT" -eq 0 ]]; then
            break
        fi

        MAX_TS=0
        NEW=0

        echo "$APPS" | jq -c '.[]' 2>/dev/null | while IFS= read -r APP; do
            APP_ID=$(echo "$APP" | jq -r '.id // ""')
            [[ -z "$APP_ID" ]] && continue

            if grep -qF "$APP_ID" "$SEEN_FILE" 2>/dev/null; then
                continue
            fi
            echo "$APP_ID" >> "$SEEN_FILE"

            APP_USER=$(echo "$APP" | jq -r '.user // ""')
            APP_NAME=$(echo "$APP" | jq -r '.name // ""')
            STARTED_TIME=$(echo "$APP" | jq -r '.startedTime // 0')
            APP_TYPE=$(echo "$APP" | jq -r '.applicationType // ""')
            QUEUE=$(echo "$APP" | jq -r '.queue // ""')
            STATE=$(echo "$APP" | jq -r '.finalStatus // .state // ""')

            ISO_TIME=$(epoch_ms_to_iso "$STARTED_TIME")
            APP_NAME_CSV=$(csv_escape "$APP_NAME")

            echo "${ISO_TIME},yarn,${APP_USER},,,${APP_NAME_CSV},SUBMIT,application,${APP_ID},1" >> "$OUT_ACCESS"
        done

        MAX_TS=$(echo "$APPS" | jq '[.[].startedTime // 0] | max' 2>/dev/null) || MAX_TS=0
        RM_COUNT=$(wc -l < "$SEEN_FILE" | tr -d ' ')
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

    RM_COUNT=$(wc -l < "$SEEN_FILE" | tr -d ' ')
    echo "[yarn] ResourceManager: ${RM_COUNT} unique apps" >&2
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

        JOBS=$(echo "$RESPONSE" | jq -c '.jobs.job // []' 2>/dev/null)
        COUNT=$(echo "$JOBS" | jq 'length' 2>/dev/null) || COUNT=0

        if [[ "$COUNT" -eq 0 ]]; then
            break
        fi

        MAX_TS=0

        echo "$JOBS" | jq -c '.[]' 2>/dev/null | while IFS= read -r JOB; do
            JOB_ID=$(echo "$JOB" | jq -r '.id // ""')
            [[ -z "$JOB_ID" ]] && continue

            # Convert job_xxx to application_xxx for dedup with RM
            APP_ID="${JOB_ID/job_/application_}"

            if grep -qF "$APP_ID" "$SEEN_FILE" 2>/dev/null; then
                continue
            fi
            echo "$APP_ID" >> "$SEEN_FILE"

            JOB_USER=$(echo "$JOB" | jq -r '.user // ""')
            JOB_NAME=$(echo "$JOB" | jq -r '.name // ""')
            START_TIME=$(echo "$JOB" | jq -r '.startTime // .submitTime // 0')

            ISO_TIME=$(epoch_ms_to_iso "$START_TIME")
            JOB_NAME_CSV=$(csv_escape "$JOB_NAME")

            echo "${ISO_TIME},yarn,${JOB_USER},,,${JOB_NAME_CSV},SUBMIT,application,${APP_ID},1" >> "$OUT_ACCESS"
        done

        MAX_TS=$(echo "$JOBS" | jq '[.[].startTime // .[].submitTime // 0] | max' 2>/dev/null) || MAX_TS=0
        TOTAL_NOW=$(wc -l < "$SEEN_FILE" | tr -d ' ')
        PAGE=$((PAGE + 1))

        echo "[yarn] JHS page=${PAGE} fetched=${COUNT} total=${TOTAL_NOW}" >&2

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

    TOTAL_NOW=$(wc -l < "$SEEN_FILE" | tr -d ' ')
    JHS_NEW=$((TOTAL_NOW - ${RM_COUNT:-0}))
    echo "[yarn] Job History Server: ${JHS_NEW} new jobs" >&2
fi

TOTAL=$(wc -l < "$SEEN_FILE" | tr -d ' ')
echo "[yarn] Done. Wrote ${TOTAL} rows to ${OUT_ACCESS}" >&2
