#!/usr/bin/env bash
set -euo pipefail

REGION="${1:-${AWS_REGION:-us-east-1}}"

export TF_VAR_enable_msk=true

echo "Applying with enable_msk=true in region ${REGION}..."
terraform apply -auto-approve -var "aws_region=${REGION}" -var "enable_msk=true"

echo
echo "MSK outputs:"
terraform output msk_cluster_arn || true
terraform output msk_bootstrap_sasl_iam || true
