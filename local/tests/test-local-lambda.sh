#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_container_running local-cloud-lambda
if docker logs local-cloud-lambda --tail 200 2>&1 | grep -E 'failed|Exception|ERROR'; then
  echo "transaction apply worker logs contain errors" >&2
  exit 1
fi
docker logs local-cloud-lambda --tail 20
echo "transaction apply worker: ok"
