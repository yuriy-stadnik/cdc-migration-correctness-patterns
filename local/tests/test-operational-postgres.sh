#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-cloud-operational-postgres
docker exec local-cloud-operational-postgres pg_isready -U appuser -d operationaldb >/dev/null

require_sql_count local-cloud-operational-postgres operationaldb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'operational' AND table_name = 'products'"
require_sql_count local-cloud-operational-postgres operationaldb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'operational' AND table_name = 'orders'"
require_sql_count local-cloud-operational-postgres operationaldb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'operational' AND table_name = 'order_items'"
require_sql_count local-cloud-operational-postgres operationaldb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'operational' AND table_name = 'contact_numbers'"
require_sql_count local-cloud-operational-postgres operationaldb 1 "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'operational' AND table_name = 'orders' AND column_name = 'source_tx_id'"
require_sql_count local-cloud-operational-postgres operationaldb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'cdc' AND table_name = 'transaction_metadata'"

docker exec -i local-cloud-operational-postgres psql -U appuser -d operationaldb <<'SQL'
SELECT 'products' AS table_name, count(*) FROM operational.products
UNION ALL SELECT 'orders', count(*) FROM operational.orders
UNION ALL SELECT 'order_items', count(*) FROM operational.order_items
UNION ALL SELECT 'contact_numbers', count(*) FROM operational.contact_numbers
UNION ALL SELECT 'transaction_metadata', count(*) FROM cdc.transaction_metadata
ORDER BY table_name;
SQL

echo "operational-postgres: ok"
