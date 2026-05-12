#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${1:-us-east-1}"
KEY_PATH="${2:-$HOME/.ssh/temp_ec2_key}"

if [[ ! -f "$KEY_PATH" ]]; then
  echo "SSH private key not found: $KEY_PATH" >&2
  echo "Run scripts/connect-kafka-mm2.sh first, or create the key before running this script." >&2
  exit 1
fi

if [[ ! -f "$KEY_PATH.pub" ]]; then
  echo "SSH public key not found: $KEY_PATH.pub" >&2
  ssh-keygen -y -f "$KEY_PATH" > "$KEY_PATH.pub"
fi

INSTANCE_ID="$(terraform output -raw ec2_instance_id)"
INSTANCE_PUBLIC_IP="$(terraform output -raw ec2_public_ip)"

aws ec2-instance-connect send-ssh-public-key \
  --region "$AWS_REGION" \
  --instance-id "$INSTANCE_ID" \
  --instance-os-user ec2-user \
  --ssh-public-key "file://$KEY_PATH.pub"

ssh \
  -o StrictHostKeyChecking=accept-new \
  -i "$KEY_PATH" \
  "ec2-user@$INSTANCE_PUBLIC_IP" \
  "sudo /opt/lab/integration-test-cdc-to-kafka.sh"
