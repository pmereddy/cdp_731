#!/usr/bin/env bash
# CDH 6.3.3 discovery: Oozie workflows, coordinators, bundles -> CSV
# Uses curl with --negotiate (SPNEGO/Kerberos). No Python dependencies needed.
#
# Prerequisites:
#   - kinit with a valid keytab before running
#   - curl and jq installed
#   - Oozie REST API accessible via HTTPS
#
# Usage:
#   ./oozie_2_csv.sh --oozie-url https://oozie-host:11443/oozie --out-objects oozie_objects.csv
#   ./oozie_2_csv.sh --oozie-url https://oozie-host:11443/oozie --out-objects oozie_objects.csv --out-access oozie_access.csv
#
# The script paginates through all workflows, coordinators, and bundles,
# extracts metadata, and writes the same CSV schemas as oozie_2_csv.py.

set -euo pipefail

OOZIE_URL=""
OUT_OBJECTS="oozie_objects.csv"
OUT_ACCESS=""
PAGE_LEN=500
SLEEP_SEC=0.1
CURL_OPTS="-s -k"

usage() {
    echo "Usage: $0 --oozie-url URL [--out-objects FILE] [--out-access FILE] [--page-len N] [--sleep SEC]"
    echo ""
    echo "  --oozie-url     Oozie URL (e.g. https://oozie-host:11443/oozie)"
    echo "  --out-objects   Output objects CSV (default: oozie_objects.csv)"
    echo "  --out-access    Output access CSV (optional; omit to skip)"
    echo "  --page-len      Jobs per API page (default: 500)"
    echo "  --sleep         Sleep between API calls in seconds (default: 0.1)"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --oozie-url)   OOZIE_URL="$2";    shift 2 ;;
        --out-objects) OUT_OBJECTS="$2";   shift 2 ;;
        --out-access)  OUT_ACCESS="$2";   shift 2 ;;
        --page-len)    PAGE_LEN="$2";     shift 2 ;;
        --sleep)       SLEEP_SEC="$2";    shift 2 ;;
        -h|--help)     usage ;;
        *)             echo "Unknown arg: $1"; usage ;;
    esac
done

if [[ -z "$OOZIE_URL" ]]; then
    echo "ERROR: --oozie-url is required" >&2
    usage
fi

if ! command -v jq &>/dev/null; then
    echo "ERROR: jq is required. Install with: sudo yum install jq" >&2
    exit 1
fi

OOZIE_URL="${OOZIE_URL%/}"

oozie_curl() {
    curl $CURL_OPTS --negotiate -u : -H "Content-Type: application/json" "$@"
}

normalize_app_path() {
    local path="$1"
    path="${path%/}"
    if [[ "$path" == hdfs://* ]]; then
        path=$(echo "$path" | sed 's|^hdfs://[^/]*||')
    fi
    [[ -z "$path" ]] && path="/"
    echo "$path"
}

echo "service,object_type,object_id,owner,group,extra" > "$OUT_OBJECTS"

if [[ -n "$OUT_ACCESS" ]]; then
    echo "window_start,service,user,do_as,client_ip,app_name,op,object_type,object_id,cnt" > "$OUT_ACCESS"
fi

TOTAL_OBJECTS=0
TOTAL_ACCESS=0

declare -A SEEN_PATHS

for JOBTYPE in wf coordinator bundle; do
    case "$JOBTYPE" in
        wf)          OBJECT_TYPE="oozie_workflow";    JSON_KEY="workflows" ;;
        coordinator) OBJECT_TYPE="oozie_coordinator"; JSON_KEY="coordinatorjobs" ;;
        bundle)      OBJECT_TYPE="oozie_bundle";      JSON_KEY="bundlejobs" ;;
    esac

    OFFSET=0
    while true; do
        URL="${OOZIE_URL}/v1/jobs?jobtype=${JOBTYPE}&len=${PAGE_LEN}&offset=${OFFSET}"
        RESPONSE=$(oozie_curl "$URL" 2>/dev/null) || {
            echo "[oozie] WARN: failed to fetch $JOBTYPE offset=$OFFSET" >&2
            break
        }

        if [[ -z "$RESPONSE" ]] || [[ "$RESPONSE" == "null" ]]; then
            break
        fi

        JOBS=$(echo "$RESPONSE" | jq -r ".${JSON_KEY} // [] | .[]" 2>/dev/null) || break
        COUNT=$(echo "$RESPONSE" | jq -r ".${JSON_KEY} // [] | length" 2>/dev/null) || break

        if [[ "$COUNT" -eq 0 ]] || [[ -z "$COUNT" ]]; then
            break
        fi

        echo "$RESPONSE" | jq -r --arg otype "$OBJECT_TYPE" "
            .${JSON_KEY} // [] | .[] |
            {
                id:       (.id // .jobId // \"\"),
                appPath:  (.appPath // \"\"),
                appName:  (.appName // .name // \"\"),
                user:     (.user // .userName // \"\"),
                status:   (.status // \"\"),
                created:  (.createdTime // \"\")
            } |
            select(.id != \"\")
        " | jq -c '.' | while IFS= read -r JOB; do
            JOB_ID=$(echo "$JOB" | jq -r '.id')
            APP_PATH=$(echo "$JOB" | jq -r '.appPath')
            APP_NAME=$(echo "$JOB" | jq -r '.appName')
            USER=$(echo "$JOB" | jq -r '.user')
            STATUS=$(echo "$JOB" | jq -r '.status')
            CREATED=$(echo "$JOB" | jq -r '.created')

            if [[ -n "$APP_PATH" && "$APP_PATH" != "null" ]]; then
                OBJ_ID="${APP_PATH%/}"
            else
                OBJ_ID="$JOB_ID"
            fi
            [[ "$APP_NAME" == "null" ]] && APP_NAME=""
            [[ "$STATUS" == "null" ]] && STATUS=""
            [[ "$CREATED" == "null" ]] && CREATED=""

            EXTRA="job_id=${JOB_ID}"
            [[ -n "$STATUS" ]] && EXTRA="${EXTRA}|status=${STATUS}"
            [[ -n "$CREATED" ]] && EXTRA="${EXTRA}|created=${CREATED}"

            # Escape commas in fields for CSV
            escape_csv() {
                local val="$1"
                if [[ "$val" == *,* ]] || [[ "$val" == *\"* ]] || [[ "$val" == *$'\n'* ]]; then
                    val="\"${val//\"/\"\"}\""
                fi
                echo "$val"
            }

            OBJ_ID_CSV=$(escape_csv "$OBJ_ID")
            USER_CSV=$(escape_csv "$USER")
            EXTRA_CSV=$(escape_csv "$EXTRA")
            APP_NAME_CSV=$(escape_csv "${APP_NAME:-$OBJ_ID}")

            echo "oozie,${OBJECT_TYPE},${OBJ_ID_CSV},${USER_CSV},,${EXTRA_CSV}" >> "$OUT_OBJECTS"
            TOTAL_OBJECTS=$((TOTAL_OBJECTS + 1))

            # HDFS app-path row
            if [[ -n "$APP_PATH" && "$APP_PATH" != "null" ]]; then
                HDFS_PATH=$(normalize_app_path "$APP_PATH")
                if [[ -n "$HDFS_PATH" && "$HDFS_PATH" != "/" ]]; then
                    if [[ -z "${SEEN_PATHS[$HDFS_PATH]+x}" ]]; then
                        SEEN_PATHS["$HDFS_PATH"]=1
                        HDFS_CSV=$(escape_csv "$HDFS_PATH")
                        REF_EXTRA=$(escape_csv "referenced_by_oozie|${JOB_ID}")
                        echo "hdfs,hdfs_path,${HDFS_CSV},${USER_CSV},,${REF_EXTRA}" >> "$OUT_OBJECTS"
                        TOTAL_OBJECTS=$((TOTAL_OBJECTS + 1))
                    fi
                fi
            fi

            # Access row (optional)
            if [[ -n "$OUT_ACCESS" && -n "$CREATED" ]]; then
                WINDOW_START="$CREATED"
                echo "${WINDOW_START},oozie,${USER_CSV},,,${APP_NAME_CSV},SUBMIT,${OBJECT_TYPE},${OBJ_ID_CSV},1" >> "$OUT_ACCESS"
                TOTAL_ACCESS=$((TOTAL_ACCESS + 1))
            fi
        done

        if [[ "$COUNT" -lt "$PAGE_LEN" ]]; then
            break
        fi
        OFFSET=$((OFFSET + COUNT))
        echo "[oozie] $JOBTYPE offset=$OFFSET fetched=$COUNT total_objects=$TOTAL_OBJECTS" >&2
        sleep "$SLEEP_SEC"
    done
done

echo "[oozie] Wrote $TOTAL_OBJECTS rows to $OUT_OBJECTS" >&2
if [[ -n "$OUT_ACCESS" ]]; then
    echo "[oozie] Wrote $TOTAL_ACCESS access rows to $OUT_ACCESS" >&2
fi
