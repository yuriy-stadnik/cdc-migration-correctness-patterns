#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-debezium-connect
curl -fsS http://localhost:8083/connectors/legacy-postgres-source/status | grep -F '"state":"RUNNING"' >/dev/null

curl -fsS http://localhost:8083/connectors/legacy-postgres-source/status
echo
echo "debezium-connect: ok"
