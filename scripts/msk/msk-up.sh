#!/usr/bin/env bash
set -euo pipefail

REGION="${1:-${AWS_REGION:-us-east-1}}"

export TF_VAR_enable_msk=true
export TF_VAR_enable_msk_event_source_mappings=false

echo "Applying MSK infrastructure with event-source mappings disabled in region ${REGION}..."
terraform apply -auto-approve -var "aws_region=${REGION}" -var "enable_msk=true" -var "enable_msk_event_source_mappings=false"

echo
echo "MSK outputs:"
terraform output msk_cluster_arn || true
terraform output msk_bootstrap_sasl_iam || true
echo
echo "After EC2 msk-topics-init completes, run scripts/msk/msk-enable-mappings.sh ${REGION}."
