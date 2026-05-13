#!/usr/bin/env bash
set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for test_script in \
  test-source-postgres.sh \
  test-source-kafka.sh \
  test-debezium-connect.sh \
  test-flink.sh \
  test-mirrormaker2.sh \
  test-cloud-kafka.sh \
  test-client-postgres.sh \
  test-operational-postgres.sh \
  test-local-lambda.sh
do
  echo "==> $test_script"
  "$TEST_DIR/$test_script"
done
