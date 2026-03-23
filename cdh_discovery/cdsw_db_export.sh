#!/usr/bin/env bash
# ============================================================================
# CDSW Database Export Script
# ============================================================================
# Exports CDSW dependency data from the internal PostgreSQL database.
#
# Usage:
#   ./cdsw_db_export.sh -h <host> -p <port> -U <user> -d <dbname>
#
# Defaults (CDSW internal DB):
#   host: localhost
#   port: 5432
#   dbname: sense
#   user: sense
#
# Output: CSV files in ./peanut_results/cdsw/ (or wherever -o points)
# ============================================================================

set -euo pipefail

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-sense}"
DB_NAME="${DB_NAME:-sense}"
OUT_DIR="${OUT_DIR:-./peanut_results/cdsw}"

usage() {
    echo "Usage: $0 [-h host] [-p port] [-U user] [-d dbname] [-o outdir]"
    echo ""
    echo "  -h  Database host       (default: localhost)"
    echo "  -p  Database port       (default: 5432)"
    echo "  -U  Database user       (default: sense)"
    echo "  -d  Database name       (default: sense)"
    echo "  -o  Output directory    (default: ./peanut_results/cdsw)"
    echo ""
    echo "Environment variables DB_HOST, DB_PORT, DB_USER, DB_NAME, OUT_DIR"
    echo "can also be used."
    exit 1
}

while getopts "h:p:U:d:o:" opt; do
    case $opt in
        h) DB_HOST="$OPTARG" ;;
        p) DB_PORT="$OPTARG" ;;
        U) DB_USER="$OPTARG" ;;
        d) DB_NAME="$OPTARG" ;;
        o) OUT_DIR="$OPTARG" ;;
        *) usage ;;
    esac
done

PSQL="psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME --no-password"

echo "=== CDSW Database Export ==="
echo "Host: $DB_HOST:$DB_PORT  DB: $DB_NAME  User: $DB_USER"
echo "Output: $OUT_DIR"
echo ""

# Test connection
if ! $PSQL -c "SELECT 1" > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to database."
    echo "Check connection parameters and ensure .pgpass or PGPASSWORD is set."
    exit 1
fi

mkdir -p "$OUT_DIR"

run_query() {
    local label="$1"
    local outfile="$2"
    local query="$3"
    echo -n "  Exporting $label ... "
    $PSQL -c "\\COPY ($query) TO STDOUT WITH CSV HEADER" > "$OUT_DIR/$outfile" 2>/dev/null
    local rows
    rows=$(( $(wc -l < "$OUT_DIR/$outfile") - 1 ))
    echo "$rows rows -> $outfile"
}

# -------------------------------------------------------
echo "[1/11] Objects: Projects"
run_query "projects as objects" "cdsw_objects.csv" "
SELECT
    'cdsw' AS service,
    'cdsw_project' AS object_type,
    u.username || '/' || p.name AS object_id,
    u.username AS owner,
    '' AS grp,
    'desc=' || COALESCE(REPLACE(REPLACE(LEFT(p.description, 120), '|', ' '), E'\n', ' '), '')
    || '|engine=' || COALESCE(p.default_project_engine_type::text, '')
    || '|created=' || COALESCE(TO_CHAR(p.created_at, 'YYYY-MM-DD'), '')
    || '|visibility=' || COALESCE(p.project_visibility::text, '')
    AS extra
FROM public.projects p
JOIN public.users u ON p.user_id = u.id
WHERE p.creation_status IS NULL OR p.creation_status <> 'failed'
ORDER BY p.created_at
"

# -------------------------------------------------------
echo "[2/11] Access: Jobs"
run_query "job definitions" "cdsw_access_jobs.csv" "
SELECT
    COALESCE(TO_CHAR(j.updated_at, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS usr,
    '' AS do_as,
    '' AS client_ip,
    j.name AS app_name,
    CASE j.type
        WHEN 'manual' THEN 'JOB_MANUAL'
        WHEN 'dependent' THEN 'JOB_DEPENDENT'
        ELSE 'JOB_SCHEDULED'
    END AS op,
    'cdsw_project' AS object_type,
    owner_u.username || '/' || p.name AS object_id,
    1 AS cnt
FROM public.jobs j
JOIN public.projects p ON j.project_id = p.id
JOIN public.users owner_u ON p.user_id = owner_u.id
LEFT JOIN public.users u ON j.creator_id = u.id
ORDER BY j.updated_at DESC
"

# -------------------------------------------------------
echo "[3/11] Access: Job runs"
run_query "job run history" "cdsw_access_jobruns.csv" "
SELECT
    COALESCE(TO_CHAR(br.created_at, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS usr,
    '' AS do_as,
    '' AS client_ip,
    br.script AS app_name,
    'JOB_RUN' AS op,
    'cdsw_project' AS object_type,
    owner_u.username || '/' || p.name AS object_id,
    1 AS cnt
FROM public.batch_runs br
JOIN public.projects p ON br.project_id = p.id
JOIN public.users owner_u ON p.user_id = owner_u.id
JOIN public.users u ON br.user_id = u.id
ORDER BY br.created_at DESC
"

# -------------------------------------------------------
echo "[4/11] Access: Models"
run_query "models" "cdsw_access_models.csv" "
SELECT
    COALESCE(TO_CHAR(m.created_at, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS usr,
    '' AS do_as,
    '' AS client_ip,
    m.name AS app_name,
    'MODEL' AS op,
    'cdsw_project' AS object_type,
    owner_u.username || '/' || p.name AS object_id,
    1 AS cnt
FROM public.models m
JOIN public.projects p ON m.project_id = p.id
JOIN public.users owner_u ON p.user_id = owner_u.id
JOIN public.users u ON m.creator_id = u.id
WHERE m.deletion_status IS NULL
ORDER BY m.created_at DESC
"

# -------------------------------------------------------
echo "[5/11] Access: Applications"
run_query "web applications" "cdsw_access_applications.csv" "
SELECT
    COALESCE(TO_CHAR(a.created_at, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS usr,
    '' AS do_as,
    '' AS client_ip,
    a.name AS app_name,
    'APPLICATION' AS op,
    'cdsw_project' AS object_type,
    owner_u.username || '/' || p.name AS object_id,
    1 AS cnt
FROM public.applications a
JOIN public.projects p ON a.project_id = p.id
JOIN public.users owner_u ON p.user_id = owner_u.id
LEFT JOIN public.users u ON a.creator_id = u.id
WHERE a.deleted_at IS NULL
ORDER BY a.created_at DESC
"

# -------------------------------------------------------
echo "[6/11] Access: Experiments"
run_query "experiment runs" "cdsw_access_experiments.csv" "
SELECT
    COALESCE(TO_CHAR(r.start_time, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS usr,
    '' AS do_as,
    '' AS client_ip,
    COALESCE(e.name, '') || '/' || COALESCE(r.source_name, r.entry_point_name, '') AS app_name,
    'EXPERIMENT_RUN' AS op,
    'cdsw_project' AS object_type,
    r.project_id AS object_id,
    1 AS cnt
FROM public.runs r
JOIN public.users u ON r.user_id = u.id
LEFT JOIN public.experiments e ON r.experiment_id = e.experiment_id
ORDER BY r.start_time DESC
"

# -------------------------------------------------------
echo "[7/11] Users"
run_query "user list" "cdsw_users.csv" "
SELECT
    u.id,
    u.username,
    u.name,
    u.email,
    u.type::text AS user_type,
    u.admin,
    u.hadoop_username,
    u.last_login_at,
    u.last_seen_at,
    u.created_at,
    u.deactivated,
    u.business_user,
    (SELECT COUNT(*) FROM public.projects p WHERE p.user_id = u.id) AS project_count,
    (SELECT COUNT(*) FROM public.batch_runs br WHERE br.user_id = u.id) AS job_run_count
FROM public.users u
ORDER BY u.last_seen_at DESC NULLS LAST
"

# -------------------------------------------------------
echo "[8/11] Projects detail"
run_query "project details" "cdsw_projects_detail.csv" "
SELECT
    p.id AS project_id,
    owner_u.username AS owner,
    p.name AS project_name,
    LEFT(REPLACE(REPLACE(COALESCE(p.description,''), E'\n', ' '), ',', ';'), 200) AS description,
    p.project_visibility::text AS visibility,
    p.default_project_engine_type::text AS engine_type,
    p.created_at,
    p.updated_at,
    (SELECT COUNT(*) FROM public.jobs j WHERE j.project_id = p.id) AS job_count,
    (SELECT COUNT(*) FROM public.jobs j WHERE j.project_id = p.id AND j.schedule IS NOT NULL AND j.schedule <> '') AS scheduled_job_count,
    (SELECT COUNT(*) FROM public.batch_runs br WHERE br.project_id = p.id) AS total_runs,
    (SELECT COUNT(*) FROM public.models m WHERE m.project_id = p.id AND m.deletion_status IS NULL) AS model_count,
    (SELECT COUNT(*) FROM public.applications a WHERE a.project_id = p.id AND a.deleted_at IS NULL) AS app_count
FROM public.projects p
JOIN public.users owner_u ON p.user_id = owner_u.id
ORDER BY (SELECT COUNT(*) FROM public.batch_runs br WHERE br.project_id = p.id) DESC
"

# -------------------------------------------------------
echo "[9/11] Job schedules"
run_query "job schedules" "cdsw_schedules.csv" "
SELECT
    j.id AS job_id,
    owner_u.username AS owner,
    p.name AS project_name,
    j.name AS job_name,
    j.script,
    j.schedule,
    j.timezone,
    j.type::text AS job_type,
    j.paused,
    j.kernel,
    j.cpu,
    j.memory,
    j.nvidia_gpu,
    j.created_at,
    j.updated_at,
    (SELECT COUNT(*) FROM public.batch_runs br WHERE br.project_id = j.project_id AND br.script = j.script) AS run_count,
    (SELECT MAX(br.created_at) FROM public.batch_runs br WHERE br.project_id = j.project_id AND br.script = j.script) AS last_run
FROM public.jobs j
JOIN public.projects p ON j.project_id = p.id
JOIN public.users owner_u ON p.user_id = owner_u.id
ORDER BY j.updated_at DESC
"

# -------------------------------------------------------
echo "[10/11] Model deployments"
run_query "model deployments" "cdsw_model_deployments.csv" "
SELECT
    m.name AS model_name,
    owner_u.username AS owner,
    p.name AS project_name,
    mb.target_file_path,
    mb.target_function_name,
    mb.kernel,
    mb.status AS build_status,
    mb.created_at AS build_created,
    md.status AS deploy_status,
    md.cpu,
    md.memory,
    md.nvidia_gpus,
    md.deployed_at,
    md.stopped_at,
    deployer_u.username AS deployer
FROM public.model_deployments md
JOIN public.model_builds mb ON md.model_build_id = mb.id
JOIN public.models m ON mb.model_id = m.id
JOIN public.projects p ON m.project_id = p.id
JOIN public.users owner_u ON p.user_id = owner_u.id
JOIN public.users deployer_u ON md.deployer_id = deployer_u.id
ORDER BY md.created_at DESC
"

# -------------------------------------------------------
echo "[11/11] Summary stats"
$PSQL -t -A -F'|' -c "
SELECT 'Total users', COUNT(*) FROM public.users
UNION ALL SELECT 'Active users (90d)', COUNT(*) FROM public.users WHERE last_login_at > NOW() - INTERVAL '90 days'
UNION ALL SELECT 'Admin users', COUNT(*) FROM public.users WHERE admin = true
UNION ALL SELECT 'Total projects', COUNT(*) FROM public.projects
UNION ALL SELECT 'Total jobs', COUNT(*) FROM public.jobs
UNION ALL SELECT 'Scheduled jobs', COUNT(*) FROM public.jobs WHERE schedule IS NOT NULL AND schedule <> ''
UNION ALL SELECT 'Paused jobs', COUNT(*) FROM public.jobs WHERE paused = true
UNION ALL SELECT 'Total job runs', COUNT(*) FROM public.batch_runs
UNION ALL SELECT 'Total models', COUNT(*) FROM public.models WHERE deletion_status IS NULL
UNION ALL SELECT 'Total model deployments', COUNT(*) FROM public.model_deployments
UNION ALL SELECT 'Total applications', COUNT(*) FROM public.applications WHERE deleted_at IS NULL
UNION ALL SELECT 'Total experiments', COUNT(*) FROM public.experiments
UNION ALL SELECT 'Total experiment runs', COUNT(*) FROM public.runs
UNION ALL SELECT 'Earliest job run', COALESCE(MIN(created_at)::text,'none') FROM public.batch_runs
UNION ALL SELECT 'Latest job run', COALESCE(MAX(created_at)::text,'none') FROM public.batch_runs
UNION ALL SELECT 'Earliest project', COALESCE(MIN(created_at)::text,'none') FROM public.projects
UNION ALL SELECT 'Latest project', COALESCE(MAX(created_at)::text,'none') FROM public.projects
" | tee "$OUT_DIR/cdsw_summary.txt"

echo ""
echo "=== Export complete ==="
echo "Files:"
ls -lh "$OUT_DIR"/cdsw_*.csv "$OUT_DIR"/cdsw_summary.txt 2>/dev/null
echo ""
echo "Total rows exported:"
for f in "$OUT_DIR"/cdsw_*.csv; do
    rows=$(( $(wc -l < "$f") - 1 ))
    printf "  %-40s %s rows\n" "$(basename "$f")" "$rows"
done
