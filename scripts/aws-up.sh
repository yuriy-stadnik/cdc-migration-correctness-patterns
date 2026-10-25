#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/aws-common.sh"

REGION="${1:-${AWS_REGION:-us-east-1}}"
IP_INPUT="${2:-${MY_IP:-${PUBLIC_IP:-${TF_VAR_ssh_cidr:-}}}}"
MAPPING_APPLY_ATTEMPTS="${MAPPING_APPLY_ATTEMPTS:-12}"
RETRY_DELAY_SECONDS="${RETRY_DELAY_SECONDS:-15}"
TOPIC_INIT_SETTLE_SECONDS="${TOPIC_INIT_SETTLE_SECONDS:-20}"

require_positive_integer MAPPING_APPLY_ATTEMPTS "$MAPPING_APPLY_ATTEMPTS"
if ! [[ "$RETRY_DELAY_SECONDS" =~ ^[0-9]+$ && "$TOPIC_INIT_SETTLE_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "RETRY_DELAY_SECONDS and TOPIC_INIT_SETTLE_SECONDS must be non-negative integers." >&2
  exit 1
fi

if [[ -z "$IP_INPUT" ]]; then
  require_command curl
  IP_INPUT="$(curl -fsS https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]' || true)"
fi
if [[ -z "$IP_INPUT" ]]; then
  echo "Could not determine an SSH allowlist address." >&2
  echo "Usage: $0 [aws-region] [public-ip-or-cidr]" >&2
  echo "You may also set MY_IP, PUBLIC_IP, or TF_VAR_ssh_cidr." >&2
  exit 1
fi

if [[ "$IP_INPUT" == */* ]]; then
  SSH_CIDR="$IP_INPUT"
else
  SSH_CIDR="$IP_INPUT/32"
fi
if ! [[ "$SSH_CIDR" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}/([0-9]|[12][0-9]|3[0-2])$ ]]; then
  echo "Expected an IPv4 address or CIDR, got: $SSH_CIDR" >&2
  exit 1
fi

export TF_VAR_ssh_cidr="$SSH_CIDR"
export TF_VAR_enable_local_deployment=false
export TF_VAR_enable_msk=true

echo "Starting or repairing the Terraform-managed AWS lab in $REGION."
echo "SSH allowlist: $SSH_CIDR"
echo "This creates billable AWS resources."

terraform_init
prepare_start_artifacts
terraform_run validate

COMMON_VARS=(
  -var "aws_region=$REGION"
  -var "ssh_cidr=$SSH_CIDR"
  -var "enable_local_deployment=false"
  -var "enable_msk=true"
)

STATE="$(terraform_state_list)"
if grep -q '^aws_lambda_event_source_mapping\.msk_operational\[' <<<"$STATE"; then
  echo "At least one Lambda mapping is already in state; preserving it and converging directly."
else
  echo "Phase 1/2: converging infrastructure with Lambda MSK mappings disabled."
  export TF_VAR_enable_msk_event_source_mappings=false
  terraform_apply "${COMMON_VARS[@]}" -var "enable_msk_event_source_mappings=false"
  if [[ "$TOPIC_INIT_SETTLE_SECONDS" -gt 0 ]]; then
    echo "Allowing EC2 topic initialization $TOPIC_INIT_SETTLE_SECONDS seconds before mapping activation."
    sleep "$TOPIC_INIT_SETTLE_SECONDS"
  fi
fi

echo "Phase 2/2: enabling Lambda mappings after MSK topic initialization."
export TF_VAR_enable_msk_event_source_mappings=true
for ((attempt=1; attempt<=MAPPING_APPLY_ATTEMPTS; attempt++)); do
  if terraform_apply "${COMMON_VARS[@]}" -var "enable_msk_event_source_mappings=true"; then
    echo "AWS lab is converged and Lambda MSK mappings are enabled."
    terraform_run output
    exit 0
  fi

  if [[ "$attempt" -eq "$MAPPING_APPLY_ATTEMPTS" ]]; then
    echo "Mapping activation did not succeed after $MAPPING_APPLY_ATTEMPTS attempts." >&2
    echo "Inspect EC2 /var/log/user-data.log and the msk-topics-init container, then rerun this script." >&2
    exit 1
  fi
  echo "Mapping activation attempt $attempt failed; topics or AWS resources may still be converging. Retrying in $RETRY_DELAY_SECONDS seconds." >&2
  sleep "$RETRY_DELAY_SECONDS"
done
