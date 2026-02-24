#!/usr/bin/env bash
###############################################################################
# 07_distribute_cdp_runtime.sh
#
# Reminder and helper script for distributing and installing CDP Runtime
# 7.3.1.600 SP3 CHF1 via Cloudera Manager.
#
# Parcel distribution and activation is performed through the CM Admin
# Console or CM API.  This script:
#   1. Prints step-by-step instructions for the UI workflow
#   2. Optionally triggers parcel operations via the CM API
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars CM_SERVER_HOST CDP_VERSION CDP_PARCEL_URL CLOUDERA_REPO_USER CLOUDERA_REPO_PASS

banner "Phase 6: Distribute and Install CDP Runtime 7.3.1.600 SP3 CHF1"

AUTH_PARCEL_URL="https://${CLOUDERA_REPO_USER}:${CLOUDERA_REPO_PASS}@archive.cloudera.com/p/cdh7/${CDP_VERSION}/"

# ---------------------------------------------------------------------------
# UI-based instructions
# ---------------------------------------------------------------------------
log_step "Option A: Distribute CDP Runtime via CM Admin Console (Recommended)"

cat <<INSTRUCTIONS

  Follow these steps in the Cloudera Manager Admin Console:

  1. LOG IN to CM Admin Console
     URL: http://${CM_SERVER_HOST}:7180  (or https://:7183 if TLS is enabled)
     Default credentials: admin / admin

  2. ADD THE CDP PARCEL REPOSITORY
     a. Navigate to:  Parcels  (from the top nav bar)
     b. Click:  Parcel Repositories & Network Settings
     c. In "Remote Parcel Repository URLs", click the "+" icon
     d. Add the following URL:
          ${AUTH_PARCEL_URL}
     e. Click "Save & Verify Configuration"
     f. Wait for the verification to succeed

  3. DOWNLOAD THE PARCEL
     a. Return to the Parcels page
     b. Find "CDH ${CDP_VERSION}" (or "Cloudera Runtime ${CDP_VERSION}")
     c. Click "Download"
     d. Wait for the download to complete on the CM server host
        (this may take 15-30 minutes depending on network speed)

  4. DISTRIBUTE THE PARCEL
     a. Once downloaded, click "Distribute"
     b. This pushes the parcel to all cluster hosts
     c. Wait for distribution to complete (progress bars will show)

  5. ACTIVATE THE PARCEL
     a. Once distributed, click "Activate"
     b. Confirm activation
     c. Services will need to be restarted after activation

  6. VERIFY ACTIVATION
     a. The parcel status should show "Active" with a green checkmark
     b. Verify on the Parcels page that all hosts show the parcel as active

INSTRUCTIONS

# ---------------------------------------------------------------------------
# API-based option
# ---------------------------------------------------------------------------
log_step "Option B: Distribute CDP Runtime via CM API (Advanced)"

CM_API_BASE="http://${CM_SERVER_HOST}:${CM_API_PORT:-7180}/api/v54"

cat <<API_INSTRUCTIONS

  If you prefer to use the CM API, here are the relevant endpoints.
  Adjust the cluster name and credentials as needed.

  0. CHECK API VERSION:
     curl -u ${CM_API_USER:-admin}:${CM_API_PASS:-admin} \\
       "${CM_API_BASE}/cm/version"

  1. ADD PARCEL REPO (update CM config):
     curl -X PUT -u ${CM_API_USER:-admin}:${CM_API_PASS:-admin} \\
       -H "Content-Type: application/json" \\
       "${CM_API_BASE}/cm/config" \\
       -d '{
         "items": [{
           "name": "REMOTE_PARCEL_REPO_URLS",
           "value": "${AUTH_PARCEL_URL}"
         }]
       }'

  2. LIST AVAILABLE PARCELS for cluster (replace CLUSTER_NAME):
     curl -u ${CM_API_USER:-admin}:${CM_API_PASS:-admin} \\
       "${CM_API_BASE}/clusters/CLUSTER_NAME/parcels"

  3. START DOWNLOAD:
     curl -X POST -u ${CM_API_USER:-admin}:${CM_API_PASS:-admin} \\
       "${CM_API_BASE}/clusters/CLUSTER_NAME/parcels/products/CDH/versions/${CDP_VERSION}/commands/startDownload"

  4. START DISTRIBUTION:
     curl -X POST -u ${CM_API_USER:-admin}:${CM_API_PASS:-admin} \\
       "${CM_API_BASE}/clusters/CLUSTER_NAME/parcels/products/CDH/versions/${CDP_VERSION}/commands/startDistribution"

  5. ACTIVATE:
     curl -X POST -u ${CM_API_USER:-admin}:${CM_API_PASS:-admin} \\
       "${CM_API_BASE}/clusters/CLUSTER_NAME/parcels/products/CDH/versions/${CDP_VERSION}/commands/activate"

  6. CHECK PARCEL STATUS:
     curl -u ${CM_API_USER:-admin}:${CM_API_PASS:-admin} \\
       "${CM_API_BASE}/clusters/CLUSTER_NAME/parcels/products/CDH/versions/${CDP_VERSION}"

API_INSTRUCTIONS

# ---------------------------------------------------------------------------
# Post-distribution checklist
# ---------------------------------------------------------------------------
log_step "Post-Distribution Checklist"

cat <<'CHECKLIST'

  After parcel activation, verify:

  [ ] 1. Parcels page shows CDH parcel as "Active" on all hosts
  [ ] 2. Run the cluster setup wizard to assign roles to hosts
  [ ] 3. Configure services (HDFS, YARN, Hive, etc.) with appropriate settings
  [ ] 4. Provide database connection details for services that need them:
         - Hive Metastore  -> hivemetastore DB
         - Hue             -> hue DB
         - Oozie           -> oozie DB
         - Ranger          -> ranger DB
         - Reports Manager -> rman DB
         - Activity Monitor -> amon DB
  [ ] 5. Start all services
  [ ] 6. Run the cluster health check from CM
  [ ] 7. Verify HDFS, YARN, and other services are operational

CHECKLIST

log_warn "This script is a reminder. Parcel operations are performed through CM."

banner "Phase 6 Complete"
