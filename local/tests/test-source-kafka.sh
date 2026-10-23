#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-kafka
docker exec local-source-kafka kafka-topics --bootstrap-server source-kafka:29092 --list >/tmp/local-source-kafka-topics.txt

grep -Fx "cdc.source.capture.v2" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "cdc.source.baseline.control.v2" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "cdc.tx.control.v2" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "cdc.tx.mutations.v2" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "cdc.tx.baseline.v2" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "cdc.tx.dlq.v2" /tmp/local-source-kafka-topics.txt >/dev/null

cat /tmp/local-source-kafka-topics.txt
echo "source-kafka: ok"
