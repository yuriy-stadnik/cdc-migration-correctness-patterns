#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-cloud-kafka
docker exec local-cloud-kafka kafka-topics --bootstrap-server cloud-kafka:39092 --list >/tmp/local-cloud-kafka-topics.txt

grep -Fx "client.customers" /tmp/local-cloud-kafka-topics.txt >/dev/null
grep -Fx "client.addresses" /tmp/local-cloud-kafka-topics.txt >/dev/null
grep -Fx "pg1.transaction" /tmp/local-cloud-kafka-topics.txt >/dev/null
grep -Fx "operational.products" /tmp/local-cloud-kafka-topics.txt >/dev/null
grep -Fx "operational.orders" /tmp/local-cloud-kafka-topics.txt >/dev/null
grep -Fx "operational.order_items" /tmp/local-cloud-kafka-topics.txt >/dev/null
grep -Fx "operational.contact_numbers" /tmp/local-cloud-kafka-topics.txt >/dev/null

cat /tmp/local-cloud-kafka-topics.txt
echo "cloud-kafka: ok"
