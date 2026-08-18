#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

MSK_BOOTSTRAP="$(terraform_output msk_bootstrap_sasl_iam)"

run_on_ec2 "sudo docker exec lab-mirrormaker2-1 kafka-topics --bootstrap-server '${MSK_BOOTSTRAP}' --command-config /tmp/client.properties --list | sort"
