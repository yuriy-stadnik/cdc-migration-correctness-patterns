#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-kafka
docker exec local-source-kafka kafka-topics --bootstrap-server source-kafka:29092 --list >/tmp/local-source-kafka-topics.txt

while IFS=: read -r topic partitions _; do
  [ -n "$topic" ] || continue
  grep -Fx "$topic" /tmp/local-source-kafka-topics.txt >/dev/null
  docker exec local-source-kafka kafka-topics --bootstrap-server source-kafka:29092 \
    --describe --topic "$topic" | grep -F "PartitionCount: $partitions" >/dev/null
done < config/transaction-v2-topics.txt

cat /tmp/local-source-kafka-topics.txt
echo "source-kafka: ok"
