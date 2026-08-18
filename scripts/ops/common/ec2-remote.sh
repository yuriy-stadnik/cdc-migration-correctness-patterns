#!/usr/bin/env bash

set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
KEY_PATH="${KEY_PATH:-$HOME/.ssh/temp_ec2_key}"

require_cmd() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "${cmd} is required." >&2
    exit 1
  fi
}

repo_root() {
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  cd "${script_dir}/../../.." && pwd
}

terraform_output() {
  local name="$1"
  terraform -chdir="$(repo_root)" output -raw "${name}"
}

ensure_ssh_key() {
  if [[ ! -f "${KEY_PATH}" ]]; then
    echo "SSH private key not found: ${KEY_PATH}" >&2
    exit 1
  fi

  if [[ ! -f "${KEY_PATH}.pub" ]]; then
    ssh-keygen -y -f "${KEY_PATH}" > "${KEY_PATH}.pub"
  fi
}

send_ec2_instance_connect_key() {
  local instance_id="$1"

  aws ec2-instance-connect send-ssh-public-key \
    --region "${AWS_REGION}" \
    --instance-id "${instance_id}" \
    --instance-os-user ec2-user \
    --ssh-public-key "file://${KEY_PATH}.pub" >/dev/null
}

remote_ssh() {
  local instance_public_ip="$1"
  shift

  ssh -o StrictHostKeyChecking=accept-new -i "${KEY_PATH}" "ec2-user@${instance_public_ip}" "$@"
}

run_on_ec2() {
  require_cmd terraform
  require_cmd aws
  require_cmd ssh
  require_cmd ssh-keygen
  ensure_ssh_key

  local instance_id instance_public_ip
  instance_id="$(terraform_output ec2_instance_id)"
  instance_public_ip="$(terraform_output ec2_public_ip)"

  send_ec2_instance_connect_key "${instance_id}"
  remote_ssh "${instance_public_ip}" "$@"
}

run_script_on_ec2() {
  require_cmd terraform
  require_cmd aws
  require_cmd ssh
  require_cmd ssh-keygen
  ensure_ssh_key

  local instance_id instance_public_ip
  instance_id="$(terraform_output ec2_instance_id)"
  instance_public_ip="$(terraform_output ec2_public_ip)"

  send_ec2_instance_connect_key "${instance_id}"
  remote_ssh "${instance_public_ip}" bash -s
}

require_jq() {
  require_cmd jq
}
