#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-flink-jobmanager
require_container_running local-source-flink-taskmanager
curl -fsS http://localhost:8081/jobs | grep -F '"jobs"' >/dev/null

curl -fsS http://localhost:8081/jobs
echo
echo "flink: ok"
