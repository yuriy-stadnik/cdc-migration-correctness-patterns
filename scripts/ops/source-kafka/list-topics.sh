#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/ec2-remote.sh
source "${SCRIPT_DIR}/../common/ec2-remote.sh"

run_on_ec2 "sudo docker exec lab-kafka-1 kafka-topics --bootstrap-server kafka:29092 --list | sort"
