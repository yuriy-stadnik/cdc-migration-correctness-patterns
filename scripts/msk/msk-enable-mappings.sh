#!/usr/bin/env bash
set -euo pipefail

REGION="${1:-${AWS_REGION:-us-east-1}}"

export TF_VAR_enable_msk=true
export TF_VAR_enable_msk_event_source_mappings=true

echo "Enabling Lambda MSK event-source mappings in region ${REGION}..."
terraform apply -auto-approve \
  -var "aws_region=${REGION}" \
  -var "enable_msk=true" \
  -var "enable_msk_event_source_mappings=true"
