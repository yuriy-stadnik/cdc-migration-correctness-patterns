#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-mirrormaker2
grep -Fx "target.exactly.once.source.support = enabled" local/source/mm2.properties >/dev/null
grep -Fx "dedicated.mode.enable.internal.rest = true" local/source/mm2.properties >/dev/null
grep -Fx "source.consumer.isolation.level = read_committed" local/source/mm2.properties >/dev/null

if docker logs local-source-mirrormaker2 --tail 200 2>&1 \
  | grep -E 'ERROR|Exception|FAILED' \
  | grep -vE 'connect-log4j[.]properties|log4j:ERROR'; then
  echo "MirrorMaker2 logs contain errors" >&2
  exit 1
fi

require_topic local-cloud-kafka cloud-kafka:39092 cdc.tx.control.v2
require_topic local-cloud-kafka cloud-kafka:39092 cdc.tx.mutations.v2
require_topic local-cloud-kafka cloud-kafka:39092 cdc.tx.dlq.v2

echo "mirrormaker2: ok"
