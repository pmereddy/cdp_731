-- ============================================================================
-- CDSW Database Queries for CDP Dependency Discovery
-- ============================================================================
-- Run against the CDSW internal PostgreSQL database.
--
-- Usage (from the CDSW master node or wherever the DB is accessible):
--
--   # Find the CDSW DB connection details:
--   #   Host: usually localhost or the CDSW master
--   #   Port: 5432 (default) or check CDSW config
--   #   DB name: usually "sense" or "cdsw"
--   #   User: usually "sense" or "cdsw"
--
--   # Export each query to CSV:
--   psql -h <host> -p <port> -U <user> -d <dbname> \
--     -c "\COPY (<query>) TO STDOUT WITH CSV HEADER" > output.csv
--
--   # Or run this entire file to generate all CSVs at once:
--   chmod +x cdsw_db_export.sh && ./cdsw_db_export.sh
-- ============================================================================


-- ============================================================================
-- QUERY 1: cdsw_objects.csv
-- All CDSW projects as dependency objects
-- ============================================================================
-- \COPY output: cdsw_objects.csv
SELECT
    'cdsw' AS service,
    'cdsw_project' AS object_type,
    u.username || '/' || p.name AS object_id,
    u.username AS owner,
    '' AS "group",
    'desc=' || COALESCE(REPLACE(REPLACE(LEFT(p.description, 120), '|', ' '), E'\n', ' '), '')
    || '|engine=' || COALESCE(p.default_project_engine_type::text, '')
    || '|created=' || COALESCE(TO_CHAR(p.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), '')
    || '|visibility=' || COALESCE(p.project_visibility::text, '')
    AS extra
FROM public.projects p
JOIN public.users u ON p.user_id = u.id
WHERE p.creation_status IS NULL OR p.creation_status <> 'failed'
ORDER BY p.created_at;


-- ============================================================================
-- QUERY 2: cdsw_access.csv
-- Jobs (scheduled and manual) with their latest run info
-- ============================================================================
-- \COPY output: cdsw_access_jobs.csv
SELECT
    COALESCE(TO_CHAR(j.updated_at, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS "user",
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
ORDER BY j.updated_at DESC;


-- ============================================================================
-- QUERY 3: cdsw_access_jobruns.csv
-- Individual job runs (batch_runs) — shows actual execution history
-- ============================================================================
-- \COPY output: cdsw_access_jobruns.csv
SELECT
    COALESCE(TO_CHAR(br.created_at, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS "user",
    '' AS do_as,
    '' AS client_ip,
    COALESCE(j.name, br.script) AS app_name,
    'JOB_RUN' AS op,
    'cdsw_project' AS object_type,
    owner_u.username || '/' || p.name AS object_id,
    1 AS cnt
FROM public.batch_runs br
JOIN public.projects p ON br.project_id = p.id
JOIN public.users owner_u ON p.user_id = owner_u.id
JOIN public.users u ON br.user_id = u.id
LEFT JOIN public.jobs j ON br.id = br.id AND j.project_id = br.project_id AND j.script = br.script
ORDER BY br.created_at DESC;


-- ============================================================================
-- QUERY 4: cdsw_access_models.csv
-- ML models
-- ============================================================================
-- \COPY output: cdsw_access_models.csv
SELECT
    COALESCE(TO_CHAR(m.created_at, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS "user",
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
ORDER BY m.created_at DESC;


-- ============================================================================
-- QUERY 5: cdsw_access_applications.csv
-- CDSW web applications
-- ============================================================================
-- \COPY output: cdsw_access_applications.csv
SELECT
    COALESCE(TO_CHAR(a.created_at, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS "user",
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
ORDER BY a.created_at DESC;


-- ============================================================================
-- QUERY 6: cdsw_access_experiments.csv
-- ML experiments and runs
-- ============================================================================
-- \COPY output: cdsw_access_experiments.csv
SELECT
    COALESCE(TO_CHAR(r.start_time, 'YYYY-MM-DD HH24:MI:SS'), '') AS window_start,
    'cdsw' AS service,
    u.username AS "user",
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
ORDER BY r.start_time DESC;


-- ============================================================================
-- QUERY 7: cdsw_users.csv
-- All CDSW users with activity info
-- ============================================================================
-- \COPY output: cdsw_users.csv
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
ORDER BY u.last_seen_at DESC NULLS LAST;


-- ============================================================================
-- QUERY 8: cdsw_projects_detail.csv
-- Projects with owner, stats, and schedule info
-- ============================================================================
-- \COPY output: cdsw_projects_detail.csv
SELECT
    p.id AS project_id,
    owner_u.username AS owner,
    p.name AS project_name,
    LEFT(REPLACE(REPLACE(p.description, E'\n', ' '), ',', ';'), 200) AS description,
    p.project_visibility::text AS visibility,
    p.default_project_engine_type::text AS engine_type,
    p.created_at,
    p.updated_at,
    (SELECT COUNT(*) FROM public.jobs j WHERE j.project_id = p.id) AS job_count,
    (SELECT COUNT(*) FROM public.jobs j WHERE j.project_id = p.id AND j.schedule IS NOT NULL AND j.schedule <> '') AS scheduled_job_count,
    (SELECT COUNT(*) FROM public.batch_runs br WHERE br.project_id = p.id) AS total_runs,
    (SELECT COUNT(*) FROM public.models m WHERE m.project_id = p.id AND m.deletion_status IS NULL) AS model_count,
    (SELECT COUNT(*) FROM public.applications a WHERE a.project_id = p.id AND a.deleted_at IS NULL) AS app_count,
    (SELECT COUNT(*) FROM public.experiments e WHERE e.project_id = p.public_identifier) AS experiment_count
FROM public.projects p
JOIN public.users owner_u ON p.user_id = owner_u.id
ORDER BY (SELECT COUNT(*) FROM public.batch_runs br WHERE br.project_id = p.id) DESC;


-- ============================================================================
-- QUERY 9: cdsw_schedules.csv
-- Job schedules (cron expressions)
-- ============================================================================
-- \COPY output: cdsw_schedules.csv
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
ORDER BY j.updated_at DESC;


-- ============================================================================
-- QUERY 10: cdsw_model_deployments.csv
-- Model deployment history
-- ============================================================================
-- \COPY output: cdsw_model_deployments.csv
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
ORDER BY md.created_at DESC;


-- ============================================================================
-- QUERY 11: cdsw_summary.txt
-- Quick cluster summary stats
-- ============================================================================
SELECT 'Total users' AS metric, COUNT(*)::text AS value FROM public.users
UNION ALL
SELECT 'Active users (logged in last 90 days)', COUNT(*)::text FROM public.users WHERE last_login_at > NOW() - INTERVAL '90 days'
UNION ALL
SELECT 'Admin users', COUNT(*)::text FROM public.users WHERE admin = true
UNION ALL
SELECT 'Total projects', COUNT(*)::text FROM public.projects
UNION ALL
SELECT 'Total jobs', COUNT(*)::text FROM public.jobs
UNION ALL
SELECT 'Scheduled jobs (with cron)', COUNT(*)::text FROM public.jobs WHERE schedule IS NOT NULL AND schedule <> ''
UNION ALL
SELECT 'Paused jobs', COUNT(*)::text FROM public.jobs WHERE paused = true
UNION ALL
SELECT 'Total job runs', COUNT(*)::text FROM public.batch_runs
UNION ALL
SELECT 'Total models', COUNT(*)::text FROM public.models WHERE deletion_status IS NULL
UNION ALL
SELECT 'Total model deployments', COUNT(*)::text FROM public.model_deployments
UNION ALL
SELECT 'Total applications', COUNT(*)::text FROM public.applications WHERE deleted_at IS NULL
UNION ALL
SELECT 'Total experiments', COUNT(*)::text FROM public.experiments
UNION ALL
SELECT 'Total experiment runs', COUNT(*)::text FROM public.runs
UNION ALL
SELECT 'Earliest job run', MIN(created_at)::text FROM public.batch_runs
UNION ALL
SELECT 'Latest job run', MAX(created_at)::text FROM public.batch_runs
UNION ALL
SELECT 'Earliest project', MIN(created_at)::text FROM public.projects
UNION ALL
SELECT 'Latest project', MAX(created_at)::text FROM public.projects;
