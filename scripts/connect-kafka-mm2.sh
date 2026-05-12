#!/usr/bin/env bash
set -euo pipefail

MY_IP="${1:-}"
AWS_REGION="${2:-us-east-1}"
KEY_PATH="${3:-$HOME/.ssh/temp_ec2_key}"

CURRENT_IP="$(curl -fsS https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]' || true)"
if [[ -z "$MY_IP" ]]; then
  if [[ -z "$CURRENT_IP" ]]; then
    echo "Usage: $0 <your-public-ip> [aws-region] [ssh-key-path]" >&2
    exit 1
  fi
  MY_IP="$CURRENT_IP"
fi

if [[ -n "$CURRENT_IP" && "$MY_IP" != "$CURRENT_IP" && "$MY_IP" != "$CURRENT_IP/32" ]]; then
  echo "Warning: current outbound IPv4 appears to be $CURRENT_IP, but SSH CIDR was set from $MY_IP." >&2
  echo "If SSH times out, rerun with: $0 $CURRENT_IP $AWS_REGION $KEY_PATH" >&2
fi

if [[ "$MY_IP" == */* ]]; then
  SSH_CIDR="$MY_IP"
else
  SSH_CIDR="$MY_IP/32"
fi

mkdir -p "$(dirname "$KEY_PATH")"

if [[ ! -f "$KEY_PATH" ]]; then
  ssh-keygen -t ed25519 -f "$KEY_PATH" -N "" -C "temp-ec2-instance-connect"
fi

chmod 600 "$KEY_PATH"

terraform apply \
  -var="ssh_cidr=$SSH_CIDR" \
  -target=aws_internet_gateway.igw \
  -target=aws_route.public_default \
  -target=aws_route_table_association.public_assoc \
  -target=aws_security_group.ec2 \
  -target=aws_instance.kafka_mm2 \
  -target=aws_iam_user_policy.ec2_instance_connect

INSTANCE_ID="$(terraform output -raw ec2_instance_id)"
INSTANCE_PUBLIC_IP="$(terraform output -raw ec2_public_ip)"
EC2_SECURITY_GROUP_ID="$(terraform output -raw sg_ec2_id)"

echo "EC2 instance: $INSTANCE_ID"
echo "EC2 public IP: $INSTANCE_PUBLIC_IP"
echo "EC2 security group: $EC2_SECURITY_GROUP_ID"
echo "Allowed SSH CIDR: $SSH_CIDR"

for attempt in {1..6}; do
  if aws ec2-instance-connect send-ssh-public-key \
    --region "$AWS_REGION" \
    --instance-id "$INSTANCE_ID" \
    --instance-os-user ec2-user \
    --ssh-public-key "file://$KEY_PATH.pub"; then
    break
  fi

  if [[ "$attempt" -eq 6 ]]; then
    echo "Failed to send SSH public key after waiting for IAM policy propagation." >&2
    exit 1
  fi

  echo "EC2 Instance Connect is not authorized yet. Waiting for IAM policy propagation..."
  sleep 10
done

ssh -i "$KEY_PATH" "ec2-user@$INSTANCE_PUBLIC_IP"
