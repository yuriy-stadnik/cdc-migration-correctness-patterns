#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-flink-jobmanager
require_container_running local-source-flink-taskmanager
test -f transaction-projector/build/libs/transaction-projector-all.jar
grep -F 'DeliveryGuarantee.EXACTLY_ONCE' transaction-projector/src/main/java/com/example/cdc/projector/TransactionProjectorJob.java >/dev/null
grep -F "FOR UPDATE LIMIT 1" transaction-apply/src/main/java/com/example/cdc/apply/V2TransactionApplyService.java >/dev/null

curl -fsS http://localhost:8081/jobs | grep -F 'transaction-preserving-cdc-projector' >/dev/null

curl -fsS http://localhost:8081/jobs
echo
echo "flink: ok"
