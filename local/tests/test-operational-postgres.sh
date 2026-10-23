#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
require_container_running local-cloud-destination-postgres
for table in products orders order_items contact_numbers; do
  require_sql_count local-cloud-destination-postgres destinationdb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema='operational' AND table_name='${table}'"
  require_sql_count local-cloud-destination-postgres destinationdb 1 "SELECT count(*) FROM information_schema.columns WHERE table_schema='operational' AND table_name='${table}' AND column_name='version'"
  require_sql_count local-cloud-destination-postgres destinationdb 0 "SELECT count(*) FROM information_schema.columns WHERE table_schema='operational' AND table_name='${table}' AND column_name='cdc_version'"
done
echo "destination-postgres operational schema: ok"
