#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-debezium-connect
curl -fsS http://localhost:8083/connectors/source-version-v2/status | grep -F '"state":"RUNNING"' >/dev/null
curl -fsS http://localhost:8083/connectors/source-version-v2/config | grep -F '"provide.transaction.metadata":"true"' >/dev/null
curl -fsS http://localhost:8083/connectors/source-version-v2/config | grep -F '"topic.prefix":"pg1v2"' >/dev/null
curl -fsS http://localhost:8083/connectors/source-version-v2/config | grep -F '"snapshot.mode":"no_data"' >/dev/null

curl -fsS http://localhost:8083/connectors/source-version-v2/status
echo
echo "debezium-connect: ok"
