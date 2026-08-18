#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

run_on_ec2 "sudo docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'"
