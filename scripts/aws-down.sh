#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/aws-common.sh"

REGION="${1:-${AWS_REGION:-us-east-1}}"
DESTROY_ATTEMPTS="${DESTROY_ATTEMPTS:-3}"
RETRY_DELAY_SECONDS="${RETRY_DELAY_SECONDS:-20}"
CLEANUP_ORPHAN_ENIS="${CLEANUP_ORPHAN_ENIS:-0}"
AWS_BIN="${AWS_BIN:-aws}"
export AWS_PAGER=""

require_positive_integer DESTROY_ATTEMPTS "$DESTROY_ATTEMPTS"
require_binary_option CLEANUP_ORPHAN_ENIS "$CLEANUP_ORPHAN_ENIS"
if ! [[ "$RETRY_DELAY_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "RETRY_DELAY_SECONDS must be a non-negative integer." >&2
  exit 1
fi

terraform_state_resource_id() {
  local address="$1" output
  if ! output="$(terraform_run state show -no-color "$address")"; then
    echo "Unable to inspect Terraform state for $address." >&2
    return 1
  fi
  sed -nE 's/^[[:space:]]*id[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' <<<"$output" | head -n 1
}

state_has_eni_owners() {
  local state="$1"
  grep -Eq '^aws_(db_instance|efs_mount_target|instance|lambda_event_source_mapping|lambda_function|msk_serverless_cluster|nat_gateway|rds_cluster|rds_cluster_instance|vpc_endpoint)\.' <<<"$state"
}

handle_orphan_network_interfaces() {
  local state="$1" address resource_id subnet_ids="" security_group_ids=""
  local candidate_ids="" eni_id eni_group_ids group_id all_groups_managed
  local blocking_candidates=0 unsafe_candidates=0

  [[ -n "$state" ]] || return 0
  if state_has_eni_owners "$state"; then
    return 0
  fi

  while IFS= read -r address; do
    case "$address" in
      aws_subnet.*)
        resource_id="$(terraform_state_resource_id "$address")" || return 1
        [[ -n "$resource_id" ]] && subnet_ids+="${resource_id}"$'\n'
        ;;
      aws_security_group.*)
        resource_id="$(terraform_state_resource_id "$address")" || return 1
        [[ -n "$resource_id" ]] && security_group_ids+="${resource_id}"$'\n'
        ;;
    esac
  done <<<"$state"

  [[ -n "$subnet_ids" ]] || return 0
  require_command "$AWS_BIN"

  while IFS= read -r resource_id; do
    [[ -n "$resource_id" ]] || continue
    if ! candidate_ids+="$("$AWS_BIN" ec2 describe-network-interfaces \
      --region "$REGION" \
      --filters "Name=subnet-id,Values=$resource_id" \
      --query 'NetworkInterfaces[?Status==`available` && RequesterManaged==`false` && Attachment==null].NetworkInterfaceId' \
      --output text)"$'\n'; then
      echo "Unable to inspect network interfaces in subnet $resource_id." >&2
      return 1
    fi
  done <<<"$subnet_ids"

  candidate_ids="$(tr '\t' '\n' <<<"$candidate_ids" | sed '/^[[:space:]]*$/d' | sort -u)"
  [[ -n "$candidate_ids" ]] || return 0

  while IFS= read -r eni_id; do
    [[ -n "$eni_id" ]] || continue
    blocking_candidates=$((blocking_candidates + 1))
    if ! eni_group_ids="$("$AWS_BIN" ec2 describe-network-interfaces \
      --region "$REGION" \
      --network-interface-ids "$eni_id" \
      --query 'NetworkInterfaces[0].Groups[].GroupId' \
      --output text)"; then
      echo "Unable to inspect security groups for detached ENI $eni_id." >&2
      return 1
    fi

    all_groups_managed=1
    for group_id in $eni_group_ids; do
      if ! grep -Fxq "$group_id" <<<"$security_group_ids"; then
        all_groups_managed=0
        break
      fi
    done
    if [[ -z "$eni_group_ids" || "$all_groups_managed" -ne 1 ]]; then
      unsafe_candidates=$((unsafe_candidates + 1))
      echo "Detached ENI $eni_id is blocking teardown but references a security group outside the remaining Terraform state; refusing automatic deletion." >&2
      continue
    fi

    echo "Detected safe detached teardown ENI $eni_id (security groups: $eni_group_ids)." >&2
    if [[ "$CLEANUP_ORPHAN_ENIS" == "1" ]]; then
      echo "Deleting opted-in orphan ENI $eni_id." >&2
      "$AWS_BIN" ec2 delete-network-interface --region "$REGION" --network-interface-id "$eni_id"
    else
      echo "Rerun with CLEANUP_ORPHAN_ENIS=1 to delete this ENI before Terraform continues." >&2
    fi
  done <<<"$candidate_ids"

  if [[ "$CLEANUP_ORPHAN_ENIS" != "1" || "$unsafe_candidates" -gt 0 ]]; then
    echo "Teardown stopped before another long Terraform retry because $blocking_candidates detached ENI(s) require cleanup or review." >&2
    return 2
  fi
}

handle_orphan_result() {
  local state="$1" status=0
  handle_orphan_network_interfaces "$state" || status=$?
  case "$status" in
    0) return 0 ;;
    2) exit 1 ;;
    *)
      echo "Orphan ENI inspection failed; leaving resources unchanged and stopping teardown." >&2
      exit 1
      ;;
  esac
}

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

handle_orphan_result "$STATE"

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

  REMAINING_STATE="$(terraform_state_list)"
  handle_orphan_result "$REMAINING_STATE"

  if [[ "$attempt" -eq "$DESTROY_ATTEMPTS" ]]; then
    echo "AWS lab removal remains incomplete after $DESTROY_ATTEMPTS attempts." >&2
    echo "Review the Terraform error and rerun this script after correcting the reported dependency or permission issue." >&2
    exit 1
  fi
  echo "Destroy attempt $attempt was incomplete; retrying in $RETRY_DELAY_SECONDS seconds." >&2
  sleep "$RETRY_DELAY_SECONDS"
done
