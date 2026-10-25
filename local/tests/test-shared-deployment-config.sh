#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

test "$(wc -l < config/transaction-v2-topics.txt | tr -d '[:space:]')" = "4"
test "$(cut -d: -f1 config/transaction-v2-topics.txt | grep -c '^cdc\.tx\.')" = "3"

grep -F 'config/transaction-v2-topics.txt' local/source/docker-compose.yml >/dev/null
grep -F 'config/transaction-v2-topics.txt' ec2.tf >/dev/null
grep -F 'config/transaction-v2-topics.txt' lambda.tf >/dev/null
grep -F 'config/mm2-common.properties' local/source/docker-compose.yml >/dev/null
grep -F 'config/mm2-common.properties' ec2.tf >/dev/null
grep -F 'scripts/ec2-cdc-to-kafka-test.sh' ec2.tf >/dev/null
grep -F 'transaction-protocol --no-daemon clean test' scripts/build-deployment-artifacts.sh >/dev/null
grep -F 'transaction-apply --no-daemon clean test' scripts/build-deployment-artifacts.sh >/dev/null

for sql in e2e-source-insert.sql e2e-source-update.sql; do
  grep -F "/config/$sql" local/scripts/run-local-e2e.sh >/dev/null
  grep -F "/config/$sql" scripts/run-aws-e2e-test.sh >/dev/null
done

grep -F 'reserved_concurrent_executions = 1' lambda.tf >/dev/null
grep -F 'enable_msk_event_source_mappings' lambda.tf >/dev/null
grep -F 'msk-topics-init:' user_data.sh.tpl >/dev/null
test -f lambda_java/src/test/java/com/example/HandlerPostgresIntegrationTest.java
for action in \
  'lambda:AddPermission' \
  'lambda:PutFunctionConcurrency' \
  'events:PutRule' \
  'events:PutTargets' \
  's3:*'
do
  grep -F -- "- $action" iam-terraform-deployer-user.yaml >/dev/null
done
for assertion in \
  'one transaction boundary across both schemas' \
  'four deterministic idempotency keys' \
  'transaction DAG edges' \
  'version-two producer lineage'
do
  grep -F "$assertion" scripts/run-aws-e2e-test.sh >/dev/null
done
if rg -q 'cdc\.source\.baseline\.control\.v2|cdc\.tx\.baseline\.v2' user_data.sh.tpl local/source; then
  echo "Retired baseline topic remains in a deployment definition." >&2
  exit 1
fi

echo "shared local/AWS deployment configuration: ok"
