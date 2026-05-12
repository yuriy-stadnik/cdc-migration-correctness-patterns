#!/usr/bin/env bash
set -euo pipefail

MY_IP="${1:-${MY_IP:-${PUBLIC_IP:-${TF_VAR_ssh_cidr:-}}}}"
CURRENT_IP="$(curl -fsS https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]' || true)"

if [[ -z "$MY_IP" ]]; then
  if [[ -z "$CURRENT_IP" ]]; then
    echo "Usage: $0 <your-public-ip>" >&2
    echo "Or set MY_IP, PUBLIC_IP, or TF_VAR_ssh_cidr before running this script." >&2
    exit 1
  fi
  MY_IP="$CURRENT_IP"
fi

if [[ "$MY_IP" == */* ]]; then
  SSH_CIDR="$MY_IP"
else
  SSH_CIDR="$MY_IP/32"
fi

export TF_VAR_ssh_cidr="$SSH_CIDR"
echo "TF_VAR_ssh_cidr=$TF_VAR_ssh_cidr"

terraform apply \
  -auto-approve \
  -target=aws_internet_gateway.igw \
  -target=aws_route.public_default \
  -target=aws_route_table_association.public_assoc \
  -target=aws_security_group.ec2 \
  -target=aws_instance.kafka_mm2 \
  -target=aws_iam_user_policy.ec2_instance_connect
