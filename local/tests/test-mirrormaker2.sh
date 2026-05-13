#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-mirrormaker2
if docker logs local-source-mirrormaker2 --tail 200 2>&1 \
  | grep -E 'ERROR|Exception|FAILED' \
  | grep -vE 'connect-log4j[.]properties|log4j:ERROR'; then
  echo "MirrorMaker2 logs contain errors" >&2
  exit 1
fi

require_topic local-cloud-kafka cloud-kafka:39092 client.customers
require_topic local-cloud-kafka cloud-kafka:39092 client.addresses
require_topic local-cloud-kafka cloud-kafka:39092 operational.orders

echo "mirrormaker2: ok"
