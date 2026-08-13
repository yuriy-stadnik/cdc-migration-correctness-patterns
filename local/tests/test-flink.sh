#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-flink-jobmanager
require_container_running local-source-flink-taskmanager
grep -F "'properties.isolation.level' = 'read_committed'" local/source/flink/sql/init.sql >/dev/null
grep -F "'properties.enable.idempotence' = 'true'" local/source/flink/sql/init.sql >/dev/null
grep -F "'properties.acks' = 'all'" local/source/flink/sql/init.sql >/dev/null
grep -F "'properties.retries' = '2147483647'" local/source/flink/sql/init.sql >/dev/null
grep -F "'properties.max.in.flight.requests.per.connection' = '5'" local/source/flink/sql/init.sql >/dev/null
grep -F "'sink.delivery-guarantee' = 'exactly-once'" local/source/flink/sql/init.sql >/dev/null
grep -F "'sink.transactional-id-prefix' = 'local-client-customers'" local/source/flink/sql/init.sql >/dev/null
grep -F "CONCAT(COALESCE(\`transaction\`.id, 'no-tx')" local/source/flink/sql/init.sql >/dev/null

curl -fsS http://localhost:8081/jobs | grep -F '"jobs"' >/dev/null

curl -fsS http://localhost:8081/jobs
echo
echo "flink: ok"
