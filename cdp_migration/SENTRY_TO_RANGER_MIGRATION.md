# Sentry (CDH 6.3.3) to Ranger (CDP 7.1.9) Migration Guide

Migrate Apache Sentry authorization policies from a CDH 6.3.3 cluster to Apache
Ranger on a CDP 7.1.9 SP1 cluster using the side-car (side-by-side) approach.

> **Sources**: This guide incorporates lessons from Cloudera PS engagements
> (Santander, Citibank), Cloudera official documentation, and the Solr policy
> migration automation toolkit. Reference materials are in `sentry_2_ranger/`.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture Comparison](#2-architecture-comparison)
3. [Prerequisites](#3-prerequisites)
4. [Migration Approach](#4-migration-approach)
5. [Phase 1 — Inventory and Backup](#5-phase-1--inventory-and-backup)
6. [Phase 2 — Export Sentry Permissions](#6-phase-2--export-sentry-permissions)
7. [Phase 3 — Prepare and Initialize Ranger](#7-phase-3--prepare-and-initialize-ranger)
8. [Phase 4 — Import into Ranger (Two-Step)](#8-phase-4--import-into-ranger-two-step)
9. [Phase 5 — Policy Consolidation and Optimization](#9-phase-5--policy-consolidation-and-optimization)
10. [Phase 6 — Solr Policy Migration](#10-phase-6--solr-policy-migration)
11. [Phase 7 — HDFS, HBase, YARN, and UDF Policies](#11-phase-7--hdfs-hbase-yarn-and-udf-policies)
12. [Phase 8 — Post-Import Adjustments](#12-phase-8--post-import-adjustments)
13. [Phase 9 — Validation and Comparison](#13-phase-9--validation-and-comparison)
14. [Phase 10 — Cutover Checklist](#14-phase-10--cutover-checklist)
15. [Rollback Plan](#15-rollback-plan)
16. [Automation Script](#16-automation-script)
17. [Known Issues and Workarounds](#17-known-issues-and-workarounds)
18. [References](#18-references)

---

## 1. Overview

### Why Migrate?

Sentry was the authorization engine in CDH. CDP replaces Sentry with Apache Ranger,
which provides:

- **Unified policy management** — single UI for HDFS, Hive, Impala, HBase, Kafka, Solr, YARN, Knox, and more
- **Fine-grained access control** — column-level, row-level filtering, and data masking
- **Tag-based policies** — policies that follow data via Atlas classifications
- **Centralized auditing** — all access decisions logged to Solr with a searchable UI
- **Delegated administration** — grant policy management to teams without full admin access

### Migration Strategy

This guide uses the **side-car (side-by-side)** migration strategy:

```
CDH 6.3.3 (Source)                    CDP 7.1.9 SP1 (Target)
┌──────────────────┐                  ┌──────────────────┐
│  Sentry Server   │                  │  Ranger Admin    │
│  (MySQL 5.x DB)  │                  │  (MySQL/PG DB)   │
│                  │   authzmigrator  │                  │
│  Roles → Groups  │ ──────────────►  │  Policies        │
│  Privileges      │  permissions.json│  Audit (Solr)    │
│                  │                  │                  │
│  Hive Metastore  │   cross-ref for │  Ranger REST API │
│  (MySQL 5.x DB)  │   ADHOC policies│  (v2 per-policy) │
└──────────────────┘                  └──────────────────┘
```

### What Gets Migrated Automatically

| Object Type | Tool | Notes |
|---|---|---|
| Hive/Impala privileges | `authzmigrator` | Database, table, column, URI, UDF |
| Kafka permissions | `authzmigrator` | Topic, consumer-group, cluster |
| Kudu permissions | `authzmigrator` | If Kudu Sentry integration enabled |

### What Requires Separate Migration

| Object Type | Approach | Automation Level |
|---|---|---|
| Solr permissions | Automated scripts (`gen-sentry-script.sh` + Python) | Semi-automated |
| HDFS ACLs | Ranger Policy Migration Utility (`transform.sh`) | Semi-automated |
| HBase permissions | Manual — export from HBase shell, create in Ranger | Manual |
| YARN queue ACLs | Manual — configure in Ranger YARN service | Manual |
| UDF policies | Manual — configure Hive Auxiliary JARs + Ranger UDF policies | Manual |
| Sentry roles with no privileges | Not migrated (by design) — recreate via API if needed | Manual |

---

## 2. Architecture Comparison

| Aspect | Sentry (CDH 6.3.3) | Ranger (CDP 7.1.9) |
|---|---|---|
| Policy model | Role-based (roles → groups → privileges) | Policy-based (policies → users/groups/roles) |
| Supported services | Hive, Impala, Solr, Kafka, Kudu, HDFS (via ACL sync) | Hive, Impala, HDFS, HBase, Kafka, Solr, Knox, YARN, Atlas, and more |
| Admin interface | `beeline` / `impala-shell` (SQL grants) | Ranger Admin Web UI + REST API |
| Audit | Sentry audit log (limited) | Solr-backed audit with searchable UI |
| Column-level security | Partial (SELECT on specific columns) | Full column masking and row filtering |
| Ownership | OWNER privilege (CDH 5.16+) | `{OWNER}` default policy (automatic for all table/db creators) |
| Grant delegation | WITH GRANT OPTION | Delegated Admin flag |
| Storage | Sentry DB (MySQL/PostgreSQL) | Ranger DB (MySQL/PostgreSQL) |
| Database visibility | `show databases` shows all by default | Only databases the user has policies for (once "public" group removed) |

### Privilege Translation Map

| Sentry Action | Ranger Action | Notes |
|---|---|---|
| SELECT | SELECT | |
| INSERT | UPDATE | Name change |
| CREATE | CREATE | |
| ALTER | CONFIGURE | Kafka cluster/topic only |
| REFRESH | REFRESH | |
| ALL | ALL | |
| SELECT WITH GRANT | SELECT + Delegated Admin | |
| INSERT WITH GRANT | UPDATE + Delegated Admin | |
| ALL WITH GRANT | ALL + Delegated Admin | |
| OWNER | `{OWNER}` default policy | Automatic in Ranger |

### Sentry-to-Ranger Solr Permission Mapping

| Sentry Privilege | Ranger Policy |
|---|---|
| `admin=collections - action=UPDATE`, `collection=<alias> - action=UPDATE` | All collections — permission: SolrAdmin |
| `admin=collections - action=UPDATE`, `collection=<name> - action=UPDATE` | Policy for `<name>` — permissions: SolrAdmin |
| `admin=collections - action=QUERY`, `collection=<name> - action=QUERY` | Policy for `<name>` — permissions: SolrAdmin |
| `admin=cores - action=UPDATE`, `collection=<core> - action=UPDATE` | All collections — permission: SolrAdmin |
| `admin=cores - action=QUERY`, `collection=<core> - action=QUERY` | All collections — permission: SolrAdmin |
| `config=<configName> - action=*` | All collections — permission: SolrAdmin |
| `collection=<name> - action=QUERY` | Policy for `<name>` — permissions: Query, Others |
| `collection=<name> - action=UPDATE` | Policy for `<name>` — permissions: Update |

---

## 3. Prerequisites

### Source Cluster (CDH 6.3.3)

| Requirement | How to Verify |
|---|---|
| Sentry service running and healthy | CM UI → Sentry → Status = GOOD |
| Sentry database accessible (MySQL 5.x) | `mysql -h <sentry_db_host> -u sentry -p sentry_db -e "SELECT COUNT(*) FROM SENTRY_DB_PRIVILEGE"` |
| Hive Metastore database accessible | `mysql -h <hms_db_host> -u hive -p metastore -e "SELECT COUNT(*) FROM DBS"` |
| DB user has access to **both** Sentry and Hive Metastore DBs | Grant: `GRANT ALL ON metastore.* TO 'sentry'@'%'; FLUSH PRIVILEGES;` |
| `authz_export.tar.gz` obtained | Contact Cloudera Support or download from Cloudera portal |
| Java 8 on Sentry host | `java -version` |
| CM Admin credentials | Needed to locate process directories |
| SSH access to Sentry host | `ssh <sentry_host> hostname -f` |
| Sentry keytab available (for Solr export) | `klist -kt /path/to/sentry.keytab` |

### Target Cluster (CDP 7.1.9 SP1)

| Requirement | How to Verify |
|---|---|
| Ranger service installed and healthy | CM UI → Ranger → Status = GOOD |
| Ranger Admin UI accessible | `curl -sk -u admin:<password> https://<ranger_host>:6182/` |
| **MySQL isolation = READ-COMMITTED** | See [MySQL Isolation Check](#mysql-isolation-check-critical) below |
| Kerberos enabled | `klist` returns valid ticket |
| HDFS accessible | `hdfs dfs -ls /` |
| LDAP/AD user-group sync complete | Ranger Admin → Settings → User/Groups shows expected groups |
| Ranger Solr audit configured | Ranger Admin → Audit tab returns results |
| Ranger RMS service installed (if applicable) | CM UI → Ranger RMS → Status |

### MySQL Isolation Check (CRITICAL)

Before importing any policies, the Ranger MySQL database **must** use `READ-COMMITTED`
isolation. The default MySQL `REPEATABLE-READ` causes lock-wait timeouts during import.

```sql
-- Check current isolation
SELECT @@GLOBAL.tx_isolation, @@tx_isolation, @@session.tx_isolation;

-- If REPEATABLE-READ, change to READ-COMMITTED:
SET tx_isolation = 'READ-COMMITTED';
SET GLOBAL tx_isolation = 'READ-COMMITTED';
```

> **Important**: Also add `transaction-isolation = READ-COMMITTED` to the MySQL
> config (`my.cnf`) so it persists across restarts.

### Tooling

| Tool | Purpose | Location |
|---|---|---|
| `authzmigrator` (`authz_export.tar.gz`) | Export Sentry Hive/Kafka permissions to JSON | Cloudera Support download |
| `beeline` / `impala-shell` | Query Sentry roles on source | CDH cluster |
| Ranger REST API | Import policies, export for comparison, per-policy CRUD | `https://<ranger_host>:6182/service/` |
| Ranger Admin UI | Visual policy import and review | `https://<ranger_host>:6182/` |
| Solr migration scripts | Automate Solr Sentry→Ranger conversion | `sentry_2_ranger/` (see Phase 6) |
| `rangerscripts/` | Template-based batch policy creation via API | `sentry_2_ranger/rangerscripts/` |
| Ranger Policy Migration Utility | Convert HDFS native ACLs to Ranger JSON | `/opt/cloudera/parcels/CDH/lib/ranger-admin/policymigration/` |
| `sentry_to_ranger_migrate.sh` | Automation script (this guide) | `cdp_migration/` |
| `consolidate_policies.py` | Sentry policy consolidation tool | `cdp_migration/` |

---

## 4. Migration Approach

### End-to-End Flow

```
Phase 1: Inventory & Backup
  ├── Capture Sentry roles, groups, privileges (source — via beeline AND direct DB)
  ├── Cross-reference Sentry DB with Hive Metastore DB (for ADHOC policies)
  ├── Capture Ranger baseline policies (target)
  └── Store backups with timestamps

Phase 2: Export Sentry Permissions
  ├── Configure authzmigrator on source cluster
  ├── Set authorization.migration.role.permissions = true
  ├── Run authz_export.sh → permissions.json
  └── Checkpoint: validate JSON — policy count, role-group mappings, service types

Phase 3: Prepare and Initialize Ranger (Target)
  ├── Verify MySQL isolation = READ-COMMITTED
  ├── (Optional) Reinitialize Ranger DB for clean-slate migration
  ├── Recreate default policies (Ranger > Actions > Setup Ranger Plugin Service)
  └── Wait for LDAP/AD user/group sync to complete

Phase 4: Import into Ranger (Two-Step)
  ├── Step A: Import roles first (modified permissions.json with dummy policy)
  ├── Step B: Import full policies (via CM UI or REST API)
  ├── Use "Override Policy" if re-importing
  └── Checkpoint: compare imported policy count vs exported count

Phase 5: Policy Consolidation & Optimization
  ├── Run consolidate_policies.py (pre-import or post-export)
  │     ├── Aggregate roles → one policy per database
  │     ├── Filter redundant server-level URI grants
  │     └── Pattern-merge identical-access databases
  ├── Import consolidated_policies.json into Ranger
  └── (Optional) Post-import optimization scripts

Phase 6: Solr Policy Migration
  ├── gen-sentry-script.sh → sentry_db.txt
  ├── solr_roles_mapping_privileges.py → JSON
  ├── sentry_ranger_migration_tool.py → ranger_policies.json
  └── Import into Ranger cm_solr service

Phase 7: HDFS, HBase, YARN, UDF Policies
  ├── HDFS: Extract native ACLs → transform.sh convert → import
  ├── HBase: Export grants → recreate in Ranger
  ├── YARN: Review capacity-scheduler ACLs → Ranger YARN policies
  └── UDF: Configure Hive Auxiliary JARs, create Ranger UDF policies

Phase 8: Post-Import Adjustments
  ├── Remove "public" group from default Hive policies
  ├── Verify {OWNER} default policies
  ├── Add db-level and url-level userspace policies
  ├── Verify show databases / use database behavior
  └── Remove temporary dummy policy from role import

Phase 9: Validation & Comparison
  ├── Side-by-side role/policy comparison report (CSV + MD)
  ├── Functional access tests (positive + negative for Hive, HDFS, Kafka, Solr)
  ├── show databases / use database tests
  ├── Insert Overwrite / URL permission tests
  └── Audit log verification

Phase 10: Cutover
  ├── Freeze Sentry changes, re-export, final import
  ├── Switch applications to target cluster
  └── Monitor Ranger audit logs for 24-48 hours
```

---

## 5. Phase 1 — Inventory and Backup

### 5.1 Capture Sentry Inventory on Source Cluster

#### Via beeline (all roles and grants)

```bash
beeline -u "jdbc:hive2://<hiveserver2_host>:10000/default;principal=hive/<hs2_fqdn>@<REALM>" \
  -e "SHOW ROLES;" > /opt/backup/sentry_roles.txt

for role in $(cat /opt/backup/sentry_roles.txt | tail -n +2); do
  echo "=== Role: ${role} ==="
  beeline -u "jdbc:hive2://<hs2_host>:10000/default;principal=hive/<hs2_fqdn>@<REALM>" \
    --silent=true -e "SHOW GRANT ROLE ${role};"
done > /opt/backup/sentry_all_grants.txt
```

#### Via Sentry Database (Direct Query — Recommended)

Direct DB access is more reliable and captures everything including column-level and
URI-level grants:

```bash
mysql -h <sentry_db_host> -u sentry -p<password> sentry_db <<'SQL'
-- Roles
SELECT role_name FROM SENTRY_ROLE ORDER BY role_name;

-- Role-Group mappings
SELECT r.role_name, g.group_name
FROM SENTRY_ROLE r
JOIN SENTRY_ROLE_GROUP_MAP rgm ON r.role_id = rgm.role_id
JOIN SENTRY_GROUP g ON rgm.group_id = g.group_id
ORDER BY r.role_name;

-- All privileges (including scope, column, URI)
SELECT r.role_name, p.privilege_scope, p.server_name, p.db_name,
       p.table_name, p.column_name, p.URI, p.action,
       p.grant_option, p.with_grant_option
FROM SENTRY_ROLE r
JOIN SENTRY_ROLE_DB_PRIVILEGE_MAP rp ON r.role_id = rp.role_id
JOIN SENTRY_DB_PRIVILEGE p ON rp.db_privilege_id = p.db_privilege_id
ORDER BY r.role_name, p.db_name, p.table_name;
SQL
```

#### Cross-Reference with Hive Metastore (for ADHOC Policy Detection)

This is critical for identifying non-standard policies that reference specific HDFS
locations and need special handling:

```bash
mysql -h <sentry_db_host> -u sentry -p<password> <<'SQL'
SELECT
    sr.role_name,
    sdp.privilege_scope,
    sdp.db_name,
    sdp.table_name,
    sdp.column_name,
    dbs.DB_LOCATION_URI AS uri,
    sdp.action
FROM
    sentry_db.SENTRY_ROLE sr
    INNER JOIN sentry_db.SENTRY_ROLE_DB_PRIVILEGE_MAP srdpm
        ON sr.role_id = srdpm.role_id
    INNER JOIN sentry_db.SENTRY_DB_PRIVILEGE sdp
        ON sdp.db_privilege_id = srdpm.db_privilege_id
    INNER JOIN metastore.DBS dbs
        ON sdp.db_name = dbs.NAME
WHERE
    sdp.privilege_scope IN ('DATABASE','COLUMN','TABLE')
ORDER BY sr.role_name, sdp.db_name;
SQL
```

> **Note**: The DB user must have `SELECT` on both `sentry_db` and `metastore`
> databases. Grant access if needed:
> ```sql
> GRANT SELECT ON metastore.* TO 'sentry'@'%';
> FLUSH PRIVILEGES;
> ```

### 5.2 Capture Solr Sentry Permissions (Source)

Solr policies are stored separately in Sentry. Capture them using `solrctl`:

```bash
kinit -kt /path/to/sentry.keytab sentry/<hostname>@<REALM>
solrctl sentry --list-roles > /opt/backup/sentry_solr_roles.txt
for role in $(awk '{print $1}' /opt/backup/sentry_solr_roles.txt); do
  echo "=== Role: ${role} ==="
  solrctl sentry --list-privileges ${role}
done > /opt/backup/sentry_solr_privileges.txt
```

### 5.3 Capture Ranger Baseline on Target Cluster

Export existing Ranger policies **before** any import (for rollback and comparison):

```bash
RANGER_URL="https://<ranger_host>:6182"
RANGER_USER="admin"
RANGER_PASS="<ranger_admin_password>"
BACKUP_DIR="/opt/backup/ranger_baseline_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${BACKUP_DIR}"

for svc in cm_hive cm_hdfs cm_kafka cm_solr; do
  curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
    "${RANGER_URL}/service/plugins/policies/exportJson?serviceName=${svc}" \
    -o "${BACKUP_DIR}/ranger_${svc}_baseline.json" 2>/dev/null
  cnt=$(python3 -c "import json; print(len(json.load(open('${BACKUP_DIR}/ranger_${svc}_baseline.json')).get('policies',[])))" 2>/dev/null || echo "N/A")
  echo "  ${svc}: ${cnt} policies"
done

echo "Baseline policies saved to ${BACKUP_DIR}"
```

### 5.4 Checkpoint: Inventory Summary

```bash
echo "=== Sentry Inventory ==="
echo "Roles:       $(mysql -N -h <sentry_db_host> -u sentry -p<pw> sentry_db -e 'SELECT COUNT(*) FROM SENTRY_ROLE')"
echo "Privileges:  $(mysql -N -h <sentry_db_host> -u sentry -p<pw> sentry_db -e 'SELECT COUNT(*) FROM SENTRY_DB_PRIVILEGE')"
echo "Role-Groups: $(mysql -N -h <sentry_db_host> -u sentry -p<pw> sentry_db -e 'SELECT COUNT(*) FROM SENTRY_ROLE_GROUP_MAP')"
echo ""
echo "=== Ranger Baseline ==="
for f in "${BACKUP_DIR}"/ranger_*_baseline.json; do
  svc=$(basename "$f" | sed 's/ranger_//;s/_baseline.json//')
  cnt=$(python3 -c "import json; d=json.load(open('${f}')); print(len(d.get('policies',[])))" 2>/dev/null || echo 0)
  echo "  ${svc}: ${cnt} policies"
done
```

---

## 6. Phase 2 — Export Sentry Permissions

### 6.1 Prepare the authzmigrator Tool

On the **source CDH 6.3.3 Sentry host**:

```bash
cd /opt
tar xzf authz_export.tar.gz
cd authzmigrator

SENTRY_PROC=$(ls -td /var/run/cloudera-scm-agent/process/*-sentry-SENTRY_SERVER 2>/dev/null | head -1)
echo "Sentry process dir: ${SENTRY_PROC}"
```

### 6.2 Configure authzmigrator

```bash
cd /opt/authzmigrator

cp "${SENTRY_PROC}/sentry-site.xml"  config/sentry-site.xml
cp "${SENTRY_PROC}/core-site.xml"    config/core-site.xml
```

#### Edit `config/sentry-site.xml`

```xml
<property>
  <name>sentry.store.jdbc.user</name>
  <value>sentry</value>
</property>
<property>
  <name>sentry.store.jdbc.password</name>
  <value>YOUR_SENTRY_DB_PASSWORD</value>
</property>
<!-- REMOVE hadoop.security.credential.provider.path if present -->
```

#### Edit `config/core-site.xml`

```xml
<property>
  <name>fs.defaultFS</name>
  <value>file:///</value>
</property>
<!-- REMOVE hadoop.security.credential.provider.path if present -->
```

#### Edit `config/authorization-migration-site.xml`

**Pay special attention** to these settings:

```xml
<property>
  <name>authorization.migration.export.target_services</name>
  <value>HIVE,KAFKA</value>
  <!-- Add KUDU if Kudu-Sentry integration is active -->
</property>

<property>
  <name>authorization.migration.export.output_file</name>
  <value>/opt/backup/permissions.json</value>
</property>

<!-- CRITICAL: Include role-group mappings in the export -->
<property>
  <name>authorization.migration.role.permissions</name>
  <value>true</value>
</property>

<!-- Skip individual owner policies to avoid policy explosion -->
<property>
  <name>authorization.migration.skip.owner.policy</name>
  <value>true</value>
</property>
```

> **Key**: The `authorization.migration.role.permissions = true` setting ensures
> that Sentry role-to-group mappings are included in the exported JSON. Without
> this, roles are not exported and the import will create policies without proper
> role assignments. (Source: Cloudera PS — Santander engagement)

### 6.3 Verify Java Path

```bash
ps -ef | grep org.apache.sentry.SentryMain

# If Java path differs from /usr/java/default, update JAVA_HOME in authz_export.sh
# Example: if Sentry uses /usr/java/jdk1.8.0_141-cloudera/bin/java,
#   set JAVA_HOME=/usr/java/jdk1.8.0_141-cloudera in authz_export.sh
```

### 6.4 Run the Export

```bash
cd /opt/authzmigrator
mkdir -p /opt/backup
sh authz_export.sh 2>&1 | tee /opt/backup/authz_export.log
```

Expected output:

```
ccea.PolicyStore.fetchDbPermissionInfo Total privileges retrieved [N]
ccea.PolicyStore.lambda$getRoleGroupMap$4 Fetched M roles
ccea.PolicyStore.lambda$getRoleGroupMap$4 Roles to group mapping retrieved [M]
ccea.SentryExportTask.execute Exporting permission information to location /opt/backup/permissions.json successful
ccea.Main.main Exporting the permissions is complete
```

> The tool can be re-run whenever Sentry changes are made that need re-migration.

### 6.5 Checkpoint: Validate the Export

```bash
ls -lh /opt/backup/permissions.json

python3 <<'PY'
import json, sys

with open("/opt/backup/permissions.json") as f:
    data = json.load(f)

# Check for role-group mappings
rgm = data.get("roleGroupMapping", {})
print(f"Role-Group mappings: {len(rgm)}")
for role, groups in sorted(rgm.items()):
    print(f"  {role} → {', '.join(groups)}")

# Policy summary
policies = data.get("dbPolicies", []) or data.get("policies", [])
print(f"\nTotal policies exported: {len(policies)}")

by_scope = {}
for p in policies:
    scope = p.get("resource", {}).get("authorizableType", "unknown")
    by_scope[scope] = by_scope.get(scope, 0) + 1
for scope, cnt in sorted(by_scope.items()):
    print(f"  {scope}: {cnt}")

kafka = data.get("kafkaPolicies", [])
print(f"\nKafka policies: {len(kafka)}")

if len(policies) == 0 and len(kafka) == 0:
    print("WARNING: No policies exported!", file=sys.stderr)
    sys.exit(1)

if len(rgm) == 0:
    print("WARNING: No role-group mappings! Check authorization.migration.role.permissions=true", file=sys.stderr)
PY
```

---

## 7. Phase 3 — Prepare and Initialize Ranger

### 7.1 MySQL Isolation Check

On the **target cluster** Ranger database:

```sql
SELECT @@GLOBAL.tx_isolation, @@tx_isolation, @@session.tx_isolation;
-- If REPEATABLE-READ:
SET tx_isolation = 'READ-COMMITTED';
SET GLOBAL tx_isolation = 'READ-COMMITTED';
```

### 7.2 (Optional) Reinitialize Ranger for Clean-Slate Migration

If you need a completely clean starting point (e.g., previous failed imports left
stale policies), follow the Ranger reinitialization procedure:

1. **Stop** Ranger and Ranger RMS services (CM UI)
2. **Truncate** all tables from Ranger's database:
   ```sql
   -- CAUTION: This removes ALL Ranger data. Only do this for clean-slate.
   -- Back up first!
   mysqldump -h <ranger_db_host> -u ranger -p ranger_db > /opt/backup/ranger_db_before_truncate.sql
   ```
3. **Recreate Ranger schema**: CM → Ranger → Actions → **Upgrade Ranger Database and Apply Patches**
4. **Recreate default settings**: CM → Ranger → Actions → **Setup Ranger Admin Component**
5. **Start Ranger**
6. **Recreate default policies**: CM → Ranger → Actions → **Setup Ranger Plugin Service**
7. **Wait** for LDAP/AD user/group sync to complete (check Ranger Admin → Settings → Users/Groups)
8. **Start Ranger RMS**

> Source: Cloudera PS — Santander engagement. This step ensures no conflicting
> policies from previous attempts interfere with the import.

---

## 8. Phase 4 — Import into Ranger (Two-Step)

The recommended approach imports **roles first**, then **full policies**. This ensures
all role-to-group mappings exist in Ranger before policies reference them.

### 8.1 Step A: Import Roles

Create a modified `permissions_roles_only.json` that contains:
- All role-group mappings (the `roleGroupMapping` section from the original file)
- A single dummy policy (temporary — will be removed after the full import)

```bash
python3 <<'PY'
import json

with open("/opt/backup/permissions.json") as f:
    data = json.load(f)

rgm = data.get("roleGroupMapping", {})
all_roles = [{"principalType": "ROLE", "principalName": r} for r in rgm.keys()]

roles_only = {
    "metaData": data.get("metaData", {}),
    "dbPolicies": [{
        "resource": {
            "authorizableType": "TABLE",
            "database": "default",
            "table": "_dummy_role_import_",
            "column": ""
        },
        "serviceType": "HIVE",
        "permissions": [{
            "principals": all_roles,
            "action": "SELECT"
        }]
    }],
    "roleGroupMapping": rgm,
    "kafkaPolicies": []
}

with open("/opt/backup/permissions_roles_only.json", "w") as f:
    json.dump(roles_only, f, indent=2)

print(f"Created permissions_roles_only.json with {len(rgm)} role mappings")
PY
```

Upload and import the roles-only file:

```bash
scp /opt/backup/permissions_roles_only.json <target_host>:/tmp/

# On target cluster
hdfs dfs -mkdir -p /user/sentry/export-permissions
hdfs dfs -put -f /tmp/permissions_roles_only.json /user/sentry/export-permissions/permissions.json
hdfs dfs -setfacl -m -R user:ranger:rwx /user/sentry/export-permissions/permissions.json
```

Import via CM: **Ranger → Actions → Import Sentry Policies**

### 8.2 Step B: Import Full Policies

Now import the complete `permissions.json`:

```bash
scp /opt/backup/permissions.json <target_host>:/tmp/

hdfs dfs -put -f /tmp/permissions.json /user/sentry/export-permissions/permissions.json
hdfs dfs -setfacl -m -R user:ranger:rwx /user/sentry/export-permissions/permissions.json
```

**Import via CM UI** (recommended):
1. CM → Ranger → Actions → **Import Sentry Policies**
2. The process reads `/user/sentry/export-permissions/permissions.json`
3. View progress in Ranger Admin UI

**Import via Ranger Admin UI** (alternative):
1. Log in to Ranger Admin: `https://<ranger_host>:6182`
2. Navigate to the Hive service (e.g., `cm_hive`)
3. Click **Import**
4. Check **"Override Policy"** if re-importing (the JSON already contains default policies)
5. Click **Import**

**Import via REST API** (alternative for automation):

```bash
RANGER_URL="https://<ranger_host>:6182"
RANGER_USER="admin"
RANGER_PASS="<ranger_admin_password>"

curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  -X POST \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/tmp/permissions.json" \
  "${RANGER_URL}/service/plugins/policies/importPoliciesFromFile?serviceName=cm_hive&updateIfExists=true"
```

> **Note**: If policies already exist that are defined in the import file and
> `updateIfExists` is not set, the import will fail. Use `updateIfExists=true`
> or check "Override Policy" in the UI.

### 8.3 Import Individual Policies via API v2

For more control (e.g., handling ADHOC or exception policies one at a time), use the
Ranger REST API v2:

```bash
curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  -X POST \
  -H "Content-Type: application/json" \
  -d @policy.json \
  "${RANGER_URL}/service/public/v2/api/policy"
```

Example policy JSON (from `sentry_2_ranger/rangerscripts/json-create-policy.template`):

```json
{
  "isEnabled": true,
  "service": "cm_hive",
  "name": "my_database_select_policy",
  "policyType": 0,
  "description": "Migrated from Sentry",
  "isAuditEnabled": true,
  "resources": {
    "database": {"values": ["my_database"], "isExcludes": false, "isRecursive": false},
    "column": {"values": ["*"], "isExcludes": false, "isRecursive": false},
    "table": {"values": ["*"], "isExcludes": false, "isRecursive": false}
  },
  "policyItems": [{
    "accesses": [{"type": "select", "isAllowed": true}],
    "users": [],
    "groups": ["my_ad_group"],
    "roles": [],
    "conditions": [],
    "delegateAdmin": false
  }],
  "denyPolicyItems": [],
  "allowExceptions": [],
  "denyExceptions": []
}
```

For batch creation from a list of JSON files:

```bash
while read json_file; do
  curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
    -H "Content-Type: application/json" \
    -d @"${json_file}" \
    -X POST "${RANGER_URL}/service/public/v2/api/policy"
done < policy_file_list.txt
```

> **Note**: Ranger's API returns a maximum of 200 results per request. When
> listing or deleting policies in bulk, iterate until all are processed.
> (Source: Citibank engagement)

### 8.4 Checkpoint: Verify Import

```bash
BACKUP_DIR="/opt/backup/ranger_post_import_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${BACKUP_DIR}"

curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  "${RANGER_URL}/service/plugins/policies/exportJson?serviceName=cm_hive" \
  -o "${BACKUP_DIR}/ranger_hive_policies_post_import.json"

python3 <<PY
import json, glob

baseline_files = sorted(glob.glob("/opt/backup/ranger_baseline_*/ranger_cm_hive_baseline.json"))
baseline = json.load(open(baseline_files[-1])) if baseline_files else {"policies": []}
imported = json.load(open("${BACKUP_DIR}/ranger_hive_policies_post_import.json"))

base_cnt = len(baseline.get("policies", []))
imp_cnt  = len(imported.get("policies", []))
print(f"Baseline Hive policies:    {base_cnt}")
print(f"Post-import Hive policies: {imp_cnt}")
print(f"New policies added:        {imp_cnt - base_cnt}")
PY
```

---

## 9. Phase 5 — Policy Consolidation and Optimization

The `authzmigrator` creates one Ranger policy per Sentry role, which for this
environment would produce ~5,662 policies.  The custom **consolidation tool**
(`consolidate_policies.py`) reduces this to ~2,200 policies by:

1. Aggregating multiple roles into **one policy per database** (all groups and
   permissions merged).
2. Filtering out redundant **server-level URI grants** that are no longer needed
   in CDP (Insert Overwrite does not require URI permission in the default
   warehouse).
3. **Pattern-merging** databases with identical access patterns into a single
   multi-database policy.

### 9.1 Pre-Import Consolidation (Recommended)

Run the consolidation tool **before** importing into Ranger.  This produces a
clean, optimized JSON that can be imported directly.

#### Option A — From `permissions.json` (after authzmigrator export)

```bash
python3 cdp_migration/consolidate_policies.py \
    --input /opt/backup/permissions.json \
    --output /opt/backup/consolidated_policies.json \
    --report /opt/backup/consolidation_report.md \
    --report-csv /opt/backup/consolidation_report.csv \
    --hive-service cm_hive
```

#### Option B — From CSV extracts (pre-migration dry run)

When the Sentry analytics CSVs are available but `authzmigrator` has not been
run yet, use `--from-csv` to preview the consolidation:

```bash
python3 cdp_migration/consolidate_policies.py \
    --from-csv \
    --access-csv peanut_results/sentry/sentry_access.csv \
    --objects-csv peanut_results/sentry/sentry_objects.csv \
    --report /opt/backup/consolidation_report.md \
    --report-csv /opt/backup/consolidation_report.csv \
    --dry-run
```

#### CLI Reference

| Flag | Description |
|------|-------------|
| `--input FILE` | Path to authzmigrator `permissions.json` |
| `--from-csv` | Read from CSV extracts instead of permissions.json |
| `--access-csv FILE` | Path to `sentry_access.csv` (required with `--from-csv`) |
| `--objects-csv FILE` | Path to `sentry_objects.csv` (optional context) |
| `--output FILE` | Output Ranger policy JSON (default: `consolidated_policies.json`) |
| `--report FILE` | Output Markdown report (default: `consolidation_report.md`) |
| `--report-csv FILE` | Output CSV report (default: `consolidation_report.csv`) |
| `--hive-service NAME` | Ranger Hive service name (default: `cm_hive`) |
| `--dry-run` | Analyze only; skip Ranger JSON generation |

### 9.2 Import Consolidated Policies

```bash
curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  -X POST \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/opt/backup/consolidated_policies.json" \
  "${RANGER_URL}/service/plugins/policies/importPoliciesFromFile?serviceName=cm_hive&updateIfExists=true"
```

### 9.3 Post-Import Optimization Scripts (Optional)

If further refinement is needed after import, these scripts from Cloudera PS
engagements can be chained (each takes a Ranger export JSON and produces a
modified JSON):

| Script | Purpose |
|---|---|
| `userspace_policies.py` | Consolidates per-user pattern policies into fewer, broader policies |
| `governedzone_policies_v3.py` | Groups governed-zone policies into a single policy |
| `remove_udf_policies.py` | Removes UDF policies that are not needed |
| `url_long_format_converter.py` | Normalizes URLs to a consistent format |

```bash
curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  "${RANGER_URL}/service/plugins/policies/exportJson?serviceName=cm_hive" \
  -o /opt/backup/current_ranger_policies.json

cd /opt/backup
python3 userspace_policies.py current_ranger_policies.json
python3 governedzone_policies_v3.py userspace_policies.json
python3 remove_udf_policies.py governedzone_policies.json
python3 url_long_format_converter.py remove_udf_policies.json
```

### 9.4 Re-Import After Optimization

```bash
curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  -X POST \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/opt/backup/url_long_format_converter.json" \
  "${RANGER_URL}/service/plugins/policies/importPoliciesFromFile?serviceName=cm_hive&isOverride=true"
```

> When using "Override Policy" (either via UI checkbox or `isOverride=true`),
> the JSON file being imported must contain **all** policies you want, including
> defaults. Any existing policies not in the file will be removed.
> (Source: Cloudera PS — Santander engagement)

---

## 10. Phase 6 — Solr Policy Migration

Solr policies are **not** handled by `authzmigrator` and must be migrated separately.
An automated 3-script pipeline reduces this from ~40 hours to ~5 minutes per cluster.

### 10.1 Automated Solr Migration Pipeline

> Source: Cloudera PS Solr policy migration toolkit (Bhagya Gummalla).
> Used by Citi to migrate 2000+ policies.

#### Script 1: Extract Sentry Solr Policies (CDH Cluster)

```bash
# Requires a Sentry keytab
kinit -kt /path/to/sentry.keytab sentry/<hostname>@<REALM>
./gen-sentry-script.sh &> sentry_db.txt
```

This reverse-engineers Sentry Solr policies into a text representation.

#### Script 2: Convert to JSON with Role/Group Mappings

```bash
python3 solr_roles_mapping_privileges.py
# Output: solr_roles_mapping_with_privileges.json
```

#### Script 3: Generate Ranger-Compatible Policies

```bash
python3 sentry_ranger_migration_tool.py
# Output: ranger_policies.json
```

### 10.2 Import Solr Policies into Ranger

1. Ranger Admin UI → `cm_solr` service → **Import**
2. Select `ranger_policies.json`
3. Click **Import**

### 10.3 Manual Solr Migration (Alternative)

If the automated scripts are not available, use the mapping table in
[Section 2](#sentry-to-ranger-solr-permission-mapping) and create policies via
Ranger Admin UI or REST API:

```bash
curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  -X POST -H "Content-Type: application/json" \
  -d '{
    "isEnabled": true,
    "service": "cm_solr",
    "name": "collection_<name>_query",
    "resources": {"collection": {"values": ["<collection_name>"]}},
    "policyItems": [{
      "accesses": [{"type": "query", "isAllowed": true}, {"type": "others", "isAllowed": true}],
      "groups": ["<group_name>"]
    }]
  }' \
  "${RANGER_URL}/service/public/v2/api/policy"
```

---

## 11. Phase 7 — HDFS, HBase, YARN, and UDF Policies

### 11.1 HDFS ACL → Ranger Policy Conversion

HDFS POSIX ACLs remain on the NameNode and are unaffected by the migration. To
additionally manage HDFS access via Ranger, use the **Ranger Policy Migration Utility**.

#### Extract HDFS Native Permissions

```bash
hdfs dfs -ls -R / | python3 -c "
import sys, csv, io
w = csv.writer(sys.stdout)
w.writerow(['path','owner','group','permissions'])
for line in sys.stdin:
    parts = line.strip().split()
    if len(parts) >= 8:
        perms, _, owner, group = parts[0], parts[1], parts[2], parts[3]
        path = parts[-1]
        w.writerow([path, owner, group, perms])
" > /tmp/HDFS_Permissions_Export.csv
```

#### Convert to Ranger HDFS Policies

```bash
# Locate the utility on the CDP cluster
POLICYMIG_TAR=$(find /opt/cloudera/parcels/CDH/lib/ranger-admin/policymigration/ \
  -name 'ranger-*-policymigration.tar.gz' 2>/dev/null | head -1)

mkdir -p /opt/policymigration && cd /opt/policymigration
tar xzf "${POLICYMIG_TAR}"
cd ranger-*-policymigration/

# Edit env.sh with your environment values
# Run conversion
./transform.sh convert /tmp/HDFS_Permissions_Export.csv
# Output: HDFS_Permissions_Export_convert.json
```

#### Import HDFS Policies

Use the Ranger Import UI or API for `cm_hdfs` service.

> **Important**: In Ranger HDFS policies, always use **paths** (e.g.,
> `/databases/sample/username`), **never** URLs (e.g.,
> `hdfs://nameservice1/databases/sample/username`). Do not include a trailing `/`.
> (Source: Cloudera official documentation)

### 11.2 HBase Policies

```bash
# Capture HBase grants on source
echo "user_permission '.*'" | hbase shell > /opt/backup/hbase_grants.txt
```

Recreate in Ranger → HBase service with equivalent table/column-family/column policies.

### 11.3 YARN Queue ACLs

Review existing `capacity-scheduler.xml` ACLs and create Ranger YARN policies:

```bash
# Export current queue ACLs
grep -E 'acl_submit_applications|acl_administer_queue' \
  /etc/hadoop/conf/capacity-scheduler.xml > /opt/backup/yarn_queue_acls.txt
```

Create corresponding Ranger YARN policies for queue submit/admin access.

### 11.4 UDF Policies

UDF access in Ranger requires both Hive configuration and Ranger policies:

**Step 1**: Configure Hive Auxiliary JARs in CM:
1. CM → Hive → Configuration → **Hive Auxiliary JARs Directory**
2. CM → Hive on Tez → Configuration → **Hive Auxiliary JARs Directory**
3. Set both to the directory containing your UDF JARs
4. Restart Hive services

**Step 2**: Create UDF functions:

```sql
-- In beeline (with a user that has CREATE permission)
USE <database>;
CREATE FUNCTION <func_name> AS '<fully.qualified.ClassName>';
SHOW FUNCTIONS;
```

**Step 3**: Create Ranger UDF policy if needed:
- Ranger Admin → cm_hive → Add Policy → Resource Type = UDF

---

## 12. Phase 8 — Post-Import Adjustments

### 12.1 Remove "public" Group from Default Hive Policies

Default Hive policies include the "public" group, which makes all databases visible to
all Hive/Impala users. This is typically **not** desired in production.

In Ranger Admin UI:
1. Navigate to `cm_hive` service
2. Find policies: `default database tables columns` and `Information_schema database tables columns`
3. Remove **public** group from both policies
4. Save

After this change, users will only see databases they have explicit policy access to
via `SHOW DATABASES` and `USE <database>`.

### 12.2 Verify {OWNER} Default Policies

Ranger creates default `{OWNER}` policies for:

| Policy Name | Scope |
|---|---|
| all - database, table, column | `{OWNER}` → ALL permissions |
| all - database, table | `{OWNER}` → ALL permissions |
| all - database, udf | `{OWNER}` → ALL permissions |
| all - database | `{OWNER}` → ALL permissions |

These policies give table/database creators full access to objects they own.

> **Warning**: Removing `{OWNER}` from these default policies restricts access to
> only users with specific permissions listed explicitly in policies. Removing
> `{OWNER}` is **not recommended** unless you understand the implications.
> (Source: Cloudera official documentation)

### 12.3 Remove Dummy Policy from Role Import

If you used the two-step import, remove the temporary dummy policy:

```bash
# Find and delete the dummy policy
DUMMY_ID=$(curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  "${RANGER_URL}/service/public/v2/api/policy?serviceName=cm_hive&policyName=_dummy_role_import_" \
  | python3 -c "import json,sys; policies=json.load(sys.stdin); print(policies[0]['id'] if policies else '')")

if [[ -n "${DUMMY_ID}" ]]; then
  curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
    -X DELETE "${RANGER_URL}/service/public/v2/api/policy/${DUMMY_ID}"
  echo "Deleted dummy policy ID: ${DUMMY_ID}"
fi
```

### 12.4 Verify URL/URI Policies

Sentry URL-level privileges are translated to Ranger Hive URL policies. Verify
that HDFS locations referenced in Ranger policies use **paths, not URLs**:

```bash
# Check for policies with hdfs:// in resource values (should be paths only)
curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  "${RANGER_URL}/service/plugins/policies/exportJson?serviceName=cm_hive" \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
for p in data.get('policies', []):
    for rtype, rval in p.get('resources', {}).items():
        for v in rval.get('values', []):
            if 'hdfs://' in v:
                print(f'  WARN: Policy [{p[\"name\"]}] has URL-style resource: {v}')
"
```

---

## 13. Phase 9 — Validation and Comparison

### 13.1 Policy Count Comparison

```bash
python3 <<'PY'
import json, os, glob

sentry_file = "/opt/backup/permissions.json"
with open(sentry_file) as f:
    sentry_data = json.load(f)

sentry_policies = sentry_data.get("dbPolicies", []) or sentry_data.get("policies", [])
sentry_kafka    = sentry_data.get("kafkaPolicies", [])
sentry_roles    = sentry_data.get("roleGroupMapping", {})

ranger_dir = sorted(glob.glob("/opt/backup/ranger_post_import_*"))[-1]
ranger_counts = {}
for f in glob.glob(os.path.join(ranger_dir, "ranger_*_post_import.json")):
    svc = os.path.basename(f).replace("ranger_","").replace("_policies_post_import.json","")
    with open(f) as fh:
        data = json.load(fh)
    ranger_counts[svc] = len(data.get("policies", []))

print("=" * 60)
print("SENTRY → RANGER POLICY MIGRATION COMPARISON")
print("=" * 60)
print(f"Sentry Hive/Impala policies: {len(sentry_policies)}")
print(f"Sentry Kafka policies:       {len(sentry_kafka)}")
print(f"Sentry role-group mappings:  {len(sentry_roles)}")
print()
for svc, cnt in sorted(ranger_counts.items()):
    print(f"Ranger {svc}: {cnt} policies")
PY
```

### 13.2 Detailed Policy Diff Report

```bash
python3 <<'DIFF_PY'
import json, csv

sentry_file = "/opt/backup/permissions.json"
report_csv  = "/opt/backup/migration_comparison_report.csv"
report_md   = "/opt/backup/migration_comparison_report.md"

with open(sentry_file) as f:
    sentry = json.load(f)

rgm = sentry.get("roleGroupMapping", {})
rows = []
for p in sentry.get("dbPolicies", []) or sentry.get("policies", []):
    res = p.get("resource", {})
    db    = res.get("database", "")
    table = res.get("table", "")
    col   = res.get("column", "")
    scope = res.get("authorizableType", "")
    for perm in p.get("permissions", []):
        action = perm.get("action", "")
        roles  = [pr["principalName"] for pr in perm.get("principals", []) if pr.get("principalType") == "ROLE"]
        groups = set()
        for r in roles:
            groups.update(rgm.get(r, []))
        rows.append({
            "scope":    scope,
            "database": db,
            "table":    table,
            "column":   col,
            "action":   action,
            "roles":    ", ".join(sorted(roles)),
            "groups":   ", ".join(sorted(groups)),
        })

with open(report_csv, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["scope","database","table","column","action","roles","groups"])
    w.writeheader()
    w.writerows(rows)

with open(report_md, "w") as f:
    f.write("# Sentry to Ranger Migration Comparison Report\n\n")
    f.write(f"Total exported privileges: {len(rows)}\n\n")
    f.write("| Scope | Database | Table | Column | Action | Roles | Groups |\n")
    f.write("|---|---|---|---|---|---|---|\n")
    for r in rows:
        f.write(f"| {r['scope']} | {r['database']} | {r['table']} | {r['column']} "
                f"| {r['action']} | {r['roles']} | {r['groups']} |\n")

print(f"CSV:  {report_csv}")
print(f"MD:   {report_md}")
print(f"Rows: {len(rows)}")
DIFF_PY
```

### 13.3 Functional Access Tests

#### Show Databases / Use Database (Behavior Change)

After removing "public" from default policies, verify database visibility:

```bash
kinit testuser@<REALM>

# Should only show databases the user has access to
beeline -u "jdbc:hive2://<hs2>:10000/;principal=hive/<hs2_fqdn>@<REALM>" \
  -e "SHOW DATABASES;"

# Should succeed for authorized database
beeline -u "jdbc:hive2://<hs2>:10000/;principal=hive/<hs2_fqdn>@<REALM>" \
  -e "USE <authorized_database>;"

# Should fail for unauthorized database
beeline -u "jdbc:hive2://<hs2>:10000/;principal=hive/<hs2_fqdn>@<REALM>" \
  -e "USE <restricted_database>;" 2>&1 | head -5
```

#### Hive / Impala SELECT + INSERT Tests

```bash
# Positive: SELECT on authorized table
beeline -u "jdbc:hive2://<hs2>:10000/<db>;principal=hive/<hs2_fqdn>@<REALM>" \
  -e "SELECT * FROM <table> LIMIT 5;"

# Negative: SELECT on unauthorized table
beeline -u "jdbc:hive2://<hs2>:10000/<db>;principal=hive/<hs2_fqdn>@<REALM>" \
  -e "SELECT * FROM <restricted_table> LIMIT 5;" 2>&1 | head -5

# Insert Overwrite test (does NOT require URL/URI permission for default warehouse)
beeline -u "jdbc:hive2://<hs2>:10000/<db>;principal=hive/<hs2_fqdn>@<REALM>" \
  -e "INSERT OVERWRITE TABLE <target_table> PARTITION(part_col)
      SELECT * FROM <source_table>;"
```

> **Note**: Insert Overwrite within the default warehouse location does NOT
> require URL/URI permission in Ranger. URL policies are only needed when
> creating external tables or databases with custom HDFS locations.
> (Source: Citibank engagement — tested and verified)

#### HDFS Access Test

```bash
kinit testuser@<REALM>
hdfs dfs -ls /user/testuser/             # should succeed
hdfs dfs -ls /user/admin_only/ 2>&1      # should fail
```

#### Kafka Access Test

```bash
echo "test" | kafka-console-producer \
  --broker-list <broker>:9093 --topic <authorized_topic> \
  --producer.config /etc/kafka/conf/producer.properties

kafka-console-consumer \
  --bootstrap-server <broker>:9093 --topic <authorized_topic> \
  --from-beginning --max-messages 1 \
  --consumer.config /etc/kafka/conf/consumer.properties
```

### 13.4 Audit Verification

```bash
curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  "${RANGER_URL}/service/assets/accessAudit?pageSize=50" \
  | python3 -m json.tool | head -100
```

Or via Ranger Admin UI: **Audit → Access** tab, filter by user/service/time.

### 13.5 Checkpoint: Validation Summary

```
=============================================================
  MIGRATION VALIDATION CHECKLIST
=============================================================

[ ] Phase 1:  Sentry inventory captured (roles, privileges, Solr, Hive Metastore cross-ref)
[ ] Phase 2:  permissions.json exported with role.permissions=true
[ ] Phase 3:  MySQL isolation = READ-COMMITTED
[ ] Phase 3:  Ranger initialized, LDAP/AD sync complete
[ ] Phase 4A: Roles imported successfully
[ ] Phase 4B: Full policies imported, count matches expectations
[ ] Phase 5:  Policies consolidated and optimized
[ ] Phase 6:  Solr policies migrated
[ ] Phase 7:  HDFS / HBase / YARN / UDF policies created
[ ] Phase 8:  "public" group removed from default Hive policies
[ ] Phase 8:  Dummy policy from role import removed
[ ] Phase 8:  {OWNER} default policies verified
[ ] Phase 9:  show databases / use database behavior verified
[ ] Phase 9:  Positive access tests pass (Hive, HDFS, Kafka, Solr)
[ ] Phase 9:  Negative access tests pass (unauthorized denied)
[ ] Phase 9:  Insert Overwrite test passed
[ ] Phase 9:  Ranger audit logs show correct decisions
[ ] Phase 9:  Policy comparison CSV/MD report reviewed
=============================================================
```

---

## 14. Phase 10 — Cutover Checklist

| Step | Action | Verification |
|---|---|---|
| 1 | Freeze Sentry changes on source cluster | Announce change freeze to teams |
| 2 | Re-run `authz_export.sh` if changes made since Phase 2 | Compare new `permissions.json` with previous |
| 3 | Re-run Solr migration scripts if Solr changes made | Compare policy counts |
| 4 | Final import into Ranger (if re-exported) | Policy count matches |
| 5 | Run full validation suite (Phase 9) | All tests pass |
| 6 | Update application connection strings to target cluster | Apps connecting to CDP |
| 7 | Disable Sentry service on source cluster | CM UI → Sentry → Stop |
| 8 | Monitor Ranger audit logs for 24-48 hours | No unexpected DENIED entries |
| 9 | Archive source cluster Sentry database backup | Stored in `/opt/backup/` |

---

## 15. Rollback Plan

### Restore Ranger Baseline

```bash
BASELINE_DIR=$(ls -td /opt/backup/ranger_baseline_* | head -1)

for svc in cm_hive cm_hdfs cm_kafka cm_solr; do
  baseline_file="${BASELINE_DIR}/ranger_${svc}_baseline.json"
  [[ -f "${baseline_file}" ]] || continue

  curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
    -X POST \
    -H "Content-Type: multipart/form-data" \
    -F "file=@${baseline_file}" \
    "${RANGER_URL}/service/plugins/policies/importPoliciesFromFile?serviceName=${svc}&isOverride=true"
  echo "Restored ${svc} from baseline"
done
```

### Bulk Cleanup via API (if needed)

Ranger does not support bulk delete by prefix natively. Iterate through results
(max 200 per page) and delete by policy ID:

```bash
python3 <<'PY'
import requests, urllib3
urllib3.disable_warnings()

RANGER_URL = "https://<ranger_host>:6182"
AUTH = ("admin", "<password>")
SERVICE = "cm_hive"
PREFIX = "migrated_"  # delete policies with this name prefix

page = 0
while True:
    resp = requests.get(
        f"{RANGER_URL}/service/public/v2/api/policy",
        params={"serviceName": SERVICE, "page": page, "pageSize": 200},
        auth=AUTH, verify=False
    )
    policies = resp.json()
    if not policies:
        break
    for p in policies:
        if p["name"].startswith(PREFIX):
            requests.delete(f"{RANGER_URL}/service/public/v2/api/policy/{p['id']}", auth=AUTH, verify=False)
            print(f"  Deleted: {p['name']} (id={p['id']})")
    page += 1
PY
```

### Re-enable Sentry on Source

1. CM UI → Sentry → Start
2. Verify roles and privileges are intact:
   ```bash
   beeline -e "SHOW ROLES;"
   ```

---

## 16. Automation Script

An automation script `sentry_to_ranger_migrate.sh` is provided in the `cdp_migration/`
directory. It automates Phases 1, 2, 4, 5, and 9.

### Usage

```bash
vi sentry_to_ranger_migrate.sh    # Edit configuration at top

./sentry_to_ranger_migrate.sh --dry-run                # Show what would be done
./sentry_to_ranger_migrate.sh                           # Full run
./sentry_to_ranger_migrate.sh --phase inventory         # Phase 1 only
./sentry_to_ranger_migrate.sh --phase export            # Phase 2 only
./sentry_to_ranger_migrate.sh --phase consolidate       # Phase 5 only
./sentry_to_ranger_migrate.sh --phase import            # Phase 4 only
./sentry_to_ranger_migrate.sh --phase validate          # Phase 9 only
```

See the [script source](sentry_to_ranger_migrate.sh) for full configuration options.

### Helper Scripts (from sentry_2_ranger/rangerscripts/)

| Script | Purpose |
|---|---|
| `create-select-policy.sh` | Creates a single SELECT policy JSON from a template |
| `json-create-policy.template` | JSON template with `$1`-`$4` placeholders |
| `create_ranger_policy.sh` | Batch-POSTs a list of JSON files to the Ranger API |
| `json-prov-gen.sh` / `listb` | Example batch generation and file listing |

---

## 17. Known Issues and Workarounds

### MySQL Transaction Rollback During Import

**Symptom**: `MySQLTransactionRollbackException: Lock wait timeout exceeded`

**Root Cause**: MySQL `REPEATABLE-READ` isolation causes lock contention.

**Fix** (do **both**):
1. Set MySQL to `READ-COMMITTED` (see [Prerequisites](#mysql-isolation-check-critical))
2. Set `ranger.client.pool.size` to `1`:
   - CM → Configuration → Search `core-site.xml`
   - In `CORE_SETTINGS-1 (Service-Wide)`, add: Name = `ranger.client.pool.size`, Value = `1`
   - Save → Restart cluster

### Empty Sentry Roles Not Migrated

Roles created with `CREATE ROLE <name>` but no `GRANT` statements are intentionally
skipped. Recreate via API if needed:

```bash
curl -sk -u "${RANGER_USER}:${RANGER_PASS}" \
  -X POST -H "Content-Type: application/json" \
  -d '{"name":"<role_name>","description":"Migrated empty role","groups":[],"users":[]}' \
  "${RANGER_URL}/service/roles/roles"
```

### Owner Policy Explosion

If `authorization.migration.skip.owner.policy` is not set to `true`, the migrator
creates individual policies for every table owner. This can result in thousands of
policies. Set the flag before export, or consolidate after import.

### Kafka ALTER → CONFIGURE Translation

For Kafka cluster and topic resources, Sentry's `ALTER` permission maps to Ranger's
`CONFIGURE`. After import, verify Kafka policies show `configure` (not `alter`).

> The migrated Ranger policy for Kafka "cluster" resource may be missing the
> "alter" permission when the source Sentry policy had `action=ALL`. Add it
> manually if needed. The `configure` permission allows alter operations, so
> the functional impact is minimal.

### Hive Views Treated as Tables

Sentry does not distinguish between tables and views. Migrated view permissions appear
as table-type policies in Ranger. This is functionally correct.

### Policy Import Fails if Policies Already Exist

If you re-run an import without `updateIfExists=true` or the "Override Policy"
checkbox, the import will fail for any policy names that already exist in Ranger.

### Exception Policies Overwriting Groups

During Citibank testing, exception/ADHOC policies were found to incorrectly overwrite
groups on existing policies. To mitigate:
- Always check for existing policies by name before updating
- Use `overwrite_policy_if_exists=false` in automation scripts
- Verify group assignments after ADHOC policy import

### Large Permission Sets

For clusters with thousands of Sentry privileges, the export can take several minutes.
Increase JVM heap if needed:

```bash
# In authz_export.sh, find the java command and add/increase -Xmx
JAVA_OPTS="-Xmx2g"
```

### Ranger API 200-Result Pagination Limit

When listing policies via the REST API, results are capped at 200 per request.
Always paginate using `page` and `pageSize` parameters when iterating over large
policy sets.

---

## 18. References

| Resource | URL / Location |
|---|---|
| Migrating from Sentry to Ranger (overview) | https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/security-authorization/topics/security-migrate-sentry-to-ranger-overview.html |
| Authzmigrator tool | https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/security-authorization/topics/rm-dc-authzmigrator-tool.html |
| Exporting Permissions from Sentry | https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/security-authorization/topics/rm-dc-authzmigrator-tool-step1.html |
| Importing Sentry privileges into Ranger | https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/tools-and-methods/topics/rm-sentry-ranger-permissions.html |
| Manual migration with authzmigrator | https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/security-authorization/topics/rm-dc-migrate-perm-manually-authzmigrator.html |
| Sentry to Ranger Concise Guide (blog) | https://www.cloudera.com/blog/technical/sentry-to-ranger-a-concise-guide.html |
| Side-car migration | https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/sidecar-migration-cdh/topics/cdppvc-sidecar-migrate-sentry_to_ranger.html |
| Converting HDFS ACLs to Ranger | https://docs.cloudera.com/cdp-public-cloud/cloud/cdppc-data-migration/topics/cdp-one-hdfs-acls-convert.html |
| Ranger REST API | https://ranger.apache.org/apidocs/index.html |
| Cloudera PS — Santander Migration Guide | `sentry_2_ranger/Cloudera PS - Santander Technology - Sentry to Ranger Policies Migration Guide.pdf` |
| Cloudera PS — Citibank Migration Guide | `sentry_2_ranger/SentryToRanger.docx` |
| Cloudera PS — Solr Policy Migration | `sentry_2_ranger/Solr Policy Migration CDH - CDP.pptx` |
| Cloudera Official — Sentry to Ranger PDF | `sentry_2_ranger/cdppvc-data-migration-sentry-ranger (1).pdf` |
| Ranger Scripts (batch policy templates) | `sentry_2_ranger/rangerscripts/` |
