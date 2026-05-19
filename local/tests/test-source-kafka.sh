#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-kafka
docker exec local-source-kafka kafka-topics --bootstrap-server source-kafka:29092 --list >/tmp/local-source-kafka-topics.txt

grep -Fx "pg1.inventory.customers" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "pg1.inventory.accounts" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "pg1.inventory.orders_flat" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "client.customers" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "client.addresses" /tmp/local-source-kafka-topics.txt >/dev/null
grep -Fx "operational.orders" /tmp/local-source-kafka-topics.txt >/dev/null

cat /tmp/local-source-kafka-topics.txt
echo "source-kafka: ok"
