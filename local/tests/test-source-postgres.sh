#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-postgres
docker exec local-source-postgres pg_isready -U postgres -d appdb >/dev/null

require_source_count() {
  local expected="$1" sql="$2" actual
  actual="$(docker exec -i local-source-postgres psql -U postgres -d appdb -tAc "$sql" | tr -d '[:space:]')"
  if [ "$actual" != "$expected" ]; then
    echo "Expected SQL count $expected but got ${actual:-<empty>}: $sql" >&2
    return 1
  fi
}

docker exec -i local-source-postgres psql -U postgres -d appdb -v ON_ERROR_STOP=1 <<'SQL'
SELECT table_name, count
FROM (
  SELECT 'customers' AS table_name, count(*) FROM inventory.customers
  UNION ALL SELECT 'products', count(*) FROM inventory.products
  UNION ALL SELECT 'orders', count(*) FROM inventory.orders
  UNION ALL SELECT 'order_items', count(*) FROM inventory.order_items
) counts
ORDER BY table_name;
SQL

require_source_count 4 \
  "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='inventory' AND c.relname IN ('customers','products','orders','order_items') AND c.relreplident='f'"
require_source_count 3 \
  "SELECT count(*) FROM information_schema.table_constraints WHERE table_schema='inventory' AND constraint_type='FOREIGN KEY'"
require_source_count 1 \
  "SELECT count(*) FROM source_cdc.schema_version WHERE version=4"
require_source_count 6 \
  "SELECT count(*) FROM pg_publication_tables WHERE pubname='source_cdc_v2'"
require_source_count 0 \
  "SELECT count(*) FROM pg_namespace WHERE nspname='source_projection'"

echo "source-postgres: ok"
