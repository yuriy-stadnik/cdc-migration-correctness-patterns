#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_container_running local-cloud-destination-postgres
docker exec local-cloud-destination-postgres pg_isready -U appuser -d destinationdb >/dev/null
for relation in client.customers client.addresses cdc.v2_transaction cdc.v2_mutation cdc.v2_dependency cdc.v2_owner_state cdc.v2_receipt cdc.v2_outbox; do
  schema="${relation%%.*}"; table="${relation##*.}"
  require_sql_count local-cloud-destination-postgres destinationdb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema='${schema}' AND table_name='${table}'"
done
require_sql_count local-cloud-destination-postgres destinationdb 1 "SELECT count(*) FROM information_schema.columns WHERE table_schema='client' AND table_name='customers' AND column_name='version'"
require_sql_count local-cloud-destination-postgres destinationdb 0 "SELECT count(*) FROM information_schema.columns WHERE table_schema='client' AND column_name='cdc_version'"
echo "destination-postgres client/control schemas: ok"
