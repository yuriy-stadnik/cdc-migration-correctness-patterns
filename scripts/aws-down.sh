#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/aws-common.sh"

REGION="${1:-${AWS_REGION:-us-east-1}}"
DESTROY_ATTEMPTS="${DESTROY_ATTEMPTS:-3}"
RETRY_DELAY_SECONDS="${RETRY_DELAY_SECONDS:-20}"

require_positive_integer DESTROY_ATTEMPTS "$DESTROY_ATTEMPTS"
if ! [[ "$RETRY_DELAY_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "RETRY_DELAY_SECONDS must be a non-negative integer." >&2
  exit 1
fi

export TF_VAR_enable_local_deployment=false
export TF_VAR_enable_msk=false
export TF_VAR_enable_msk_event_source_mappings=false

echo "Removing the Terraform-managed AWS lab in $REGION."
terraform_init

STATE="$(terraform_state_list)"
if [[ -z "$STATE" ]]; then
  echo "Terraform state contains no managed resources; the AWS lab is already absent."
  exit 0
fi

prepare_destroy_artifacts

DESTROY_VARS=(
  -var "aws_region=$REGION"
  -var "enable_local_deployment=false"
  -var "enable_msk=false"
  -var "enable_msk_event_source_mappings=false"
)

for ((attempt=1; attempt<=DESTROY_ATTEMPTS; attempt++)); do
  if terraform_destroy "${DESTROY_VARS[@]}"; then
    REMAINING_STATE="$(terraform_state_list)"
    if [[ -z "$REMAINING_STATE" ]]; then
      echo "AWS lab removal is complete."
      exit 0
    fi
    echo "Terraform destroy returned successfully but state still contains resources:" >&2
    printf '%s\n' "$REMAINING_STATE" >&2
  fi

  if [[ "$attempt" -eq "$DESTROY_ATTEMPTS" ]]; then
    echo "AWS lab removal remains incomplete after $DESTROY_ATTEMPTS attempts." >&2
    echo "Review the Terraform error and rerun this script after correcting the reported dependency or permission issue." >&2
    exit 1
  fi
  echo "Destroy attempt $attempt was incomplete; retrying in $RETRY_DELAY_SECONDS seconds." >&2
  sleep "$RETRY_DELAY_SECONDS"
done
