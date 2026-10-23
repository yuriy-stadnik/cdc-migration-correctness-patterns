#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-cloud-kafka
docker exec local-cloud-kafka kafka-topics --bootstrap-server cloud-kafka:39092 --list >/tmp/local-cloud-kafka-topics.txt

grep -Fx "cdc.tx.control.v2" /tmp/local-cloud-kafka-topics.txt >/dev/null
grep -Fx "cdc.tx.mutations.v2" /tmp/local-cloud-kafka-topics.txt >/dev/null
grep -Fx "cdc.tx.dlq.v2" /tmp/local-cloud-kafka-topics.txt >/dev/null

cat /tmp/local-cloud-kafka-topics.txt
echo "cloud-kafka: ok"
