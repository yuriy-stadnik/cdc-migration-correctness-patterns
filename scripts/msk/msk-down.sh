#!/usr/bin/env bash
set -euo pipefail

REGION="${1:-${AWS_REGION:-us-east-1}}"

export TF_VAR_enable_msk=false
export TF_VAR_enable_msk_event_source_mappings=false

echo "Applying with enable_msk=false in region ${REGION} (destroys MSK and MSK mappings)..."
terraform apply -auto-approve -var "aws_region=${REGION}" -var "enable_msk=false" -var "enable_msk_event_source_mappings=false"

echo
echo "MSK outputs after disable:"
terraform output msk_cluster_arn || true
terraform output msk_bootstrap_sasl_iam || true
