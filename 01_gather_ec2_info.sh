#!/usr/bin/env bash
###############################################################################
# 01_gather_ec2_info.sh
#
# Gather detailed information about the EC2 instances that will form the
# CDP cluster.  Outputs a summary table to stdout and a CSV file.
#
# Requires:
#   - AWS CLI v2 configured with appropriate IAM permissions
#   - SSH key-based passwordless access to every host
#   - .env populated with EC2_INSTANCE_IDS, ALL_HOSTS, AWS_REGION
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
load_env

require_vars EC2_INSTANCE_IDS AWS_REGION ALL_HOSTS SSH_USER SSH_KEY

OUT_DIR="${SCRIPT_DIR}/output"
mkdir -p "${OUT_DIR}"
REPORT="${OUT_DIR}/ec2_instance_report.csv"

banner "Phase 1a: Gather EC2 Instance Information"

# ---------------------------------------------------------------------------
# Part 1 -- AWS API metadata
# ---------------------------------------------------------------------------
log_step "Querying AWS EC2 API for instance metadata"

read -ra INSTANCE_IDS <<< "${EC2_INSTANCE_IDS}"

echo "InstanceId,InstanceType,PrivateIp,PublicIp,VpcId,SubnetId,SecurityGroups,AMI,State,AvailabilityZone,CPUs,MemoryMB,EBSVolumes" > "${REPORT}"

for iid in "${INSTANCE_IDS[@]}"; do
  log_info "Describing instance ${iid} ..."

  INSTANCE_JSON=$(aws ec2 describe-instances \
    --region "${AWS_REGION}" \
    --instance-ids "${iid}" \
    --query 'Reservations[0].Instances[0]' \
    --output json 2>/dev/null) || { log_warn "Failed to describe ${iid}"; continue; }

  INSTANCE_TYPE=$(echo "${INSTANCE_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('InstanceType',''))" 2>/dev/null)
  PRIVATE_IP=$(echo "${INSTANCE_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('PrivateIpAddress',''))" 2>/dev/null)
  PUBLIC_IP=$(echo "${INSTANCE_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('PublicIpAddress','N/A'))" 2>/dev/null)
  VPC_ID=$(echo "${INSTANCE_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('VpcId',''))" 2>/dev/null)
  SUBNET_ID=$(echo "${INSTANCE_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('SubnetId',''))" 2>/dev/null)
  AMI_ID=$(echo "${INSTANCE_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('ImageId',''))" 2>/dev/null)
  STATE=$(echo "${INSTANCE_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['State']['Name'])" 2>/dev/null)
  AZ=$(echo "${INSTANCE_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['Placement']['AvailabilityZone'])" 2>/dev/null)
  CPUS=$(echo "${INSTANCE_JSON}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('CpuOptions',{}).get('CoreCount','N/A'))" 2>/dev/null)

  SG_NAMES=$(echo "${INSTANCE_JSON}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
sgs = d.get('SecurityGroups', [])
print(' | '.join([sg.get('GroupName','') + '(' + sg.get('GroupId','') + ')' for sg in sgs]))
" 2>/dev/null)

  EBS_VOLS=$(echo "${INSTANCE_JSON}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
devs = d.get('BlockDeviceMappings', [])
print(' | '.join([bm['DeviceName'] + ':' + bm['Ebs']['VolumeId'] for bm in devs if 'Ebs' in bm]))
" 2>/dev/null)

  # Get memory from instance type
  MEM_MB=$(aws ec2 describe-instance-types \
    --region "${AWS_REGION}" \
    --instance-types "${INSTANCE_TYPE}" \
    --query 'InstanceTypes[0].MemoryInfo.SizeInMiB' \
    --output text 2>/dev/null || echo "N/A")

  echo "\"${iid}\",\"${INSTANCE_TYPE}\",\"${PRIVATE_IP}\",\"${PUBLIC_IP}\",\"${VPC_ID}\",\"${SUBNET_ID}\",\"${SG_NAMES}\",\"${AMI_ID}\",\"${STATE}\",\"${AZ}\",\"${CPUS}\",\"${MEM_MB}\",\"${EBS_VOLS}\"" >> "${REPORT}"

  echo ""
  echo "  Instance:       ${iid}"
  echo "  Type:           ${INSTANCE_TYPE}"
  echo "  Private IP:     ${PRIVATE_IP}"
  echo "  Public IP:      ${PUBLIC_IP}"
  echo "  VPC:            ${VPC_ID}"
  echo "  Subnet:         ${SUBNET_ID}"
  echo "  Security Groups:${SG_NAMES}"
  echo "  AMI:            ${AMI_ID}"
  echo "  State:          ${STATE}"
  echo "  AZ:             ${AZ}"
  echo "  vCPUs (cores):  ${CPUS}"
  echo "  Memory (MiB):   ${MEM_MB}"
  echo "  EBS Volumes:    ${EBS_VOLS}"
  echo ""
done

log_info "EC2 API report written to ${REPORT}"

# ---------------------------------------------------------------------------
# Part 2 -- OS-level details via SSH
# ---------------------------------------------------------------------------
log_step "Gathering OS-level details via SSH"

OS_REPORT="${OUT_DIR}/ec2_os_report.txt"
: > "${OS_REPORT}"

read -ra _hosts <<< "${ALL_HOSTS}"
for h in "${_hosts[@]}"; do
  log_host "OS details for ${h}"
  {
    echo "================================================================"
    echo "Host: ${h}"
    echo "================================================================"
    ssh_cmd "${h}" "
      echo '--- Hostname ---'
      hostname -f

      echo '--- OS Version ---'
      cat /etc/redhat-release 2>/dev/null || cat /etc/os-release 2>/dev/null

      echo '--- Kernel ---'
      uname -r

      echo '--- CPU ---'
      lscpu | grep -E '^(Architecture|CPU\(s\)|Model name|Thread)'

      echo '--- Memory ---'
      free -h | head -2

      echo '--- Block Devices ---'
      lsblk -d -o NAME,SIZE,TYPE,MOUNTPOINT 2>/dev/null || lsblk

      echo '--- Disk Usage ---'
      df -h

      echo '--- Default Python ---'
      python3 --version 2>&1 || echo 'python3 not found'
      which python3 2>/dev/null || true

      echo '--- Default Java ---'
      java -version 2>&1 || echo 'java not found'
      echo \"JAVA_HOME=\${JAVA_HOME:-not set}\"

      echo '--- Network Interfaces ---'
      ip -4 addr show | grep -E 'inet ' | awk '{print \$NF, \$2}'
    " 2>&1
  } | tee -a "${OS_REPORT}"
  echo ""
done

log_info "OS-level report written to ${OS_REPORT}"

banner "Phase 1a Complete"
