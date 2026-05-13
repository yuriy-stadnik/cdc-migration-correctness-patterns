#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-cloud-lambda
if docker logs local-cloud-lambda --tail 200 2>&1 | grep -E 'failed|Exception|ERROR'; then
  echo "local Lambda logs contain errors" >&2
  exit 1
fi

docker logs local-cloud-lambda --tail 20
docker exec -i local-cloud-operational-postgres psql -U appuser -d operationaldb -tAc \
  "SELECT count(*) FROM cdc.transaction_metadata WHERE status = 'END'" | grep -v '^0$' >/dev/null
echo "local-lambda: ok"
