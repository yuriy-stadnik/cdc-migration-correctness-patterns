#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-source-postgres
docker exec local-source-postgres pg_isready -U postgres -d appdb >/dev/null

docker exec -i local-source-postgres psql -U postgres -d appdb -v ON_ERROR_STOP=1 <<'SQL'
SELECT 'customers' AS table_name, count(*) FROM inventory.customers
UNION ALL SELECT 'accounts', count(*) FROM inventory.accounts
UNION ALL SELECT 'orders_flat', count(*) FROM inventory.orders_flat
ORDER BY table_name;

SELECT n.nspname || '.' || c.relname AS table_name, c.relreplident
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'inventory'
  AND c.relname IN ('customers', 'accounts', 'orders_flat')
  AND c.relreplident = 'f'
ORDER BY table_name;
SQL

actual_replica_identity_full="$(
  docker exec -i local-source-postgres psql -U postgres -d appdb -tAc \
    "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'inventory' AND c.relname IN ('customers', 'accounts', 'orders_flat') AND c.relreplident = 'f'" \
    | tr -d '[:space:]'
)"
if [ "$actual_replica_identity_full" != "3" ]; then
  echo "Expected REPLICA IDENTITY FULL on all inventory source tables" >&2
  exit 1
fi

source_fk_count="$(
  docker exec -i local-source-postgres psql -U postgres -d appdb -tAc \
    "SELECT count(*) FROM information_schema.table_constraints WHERE table_schema = 'inventory' AND constraint_type = 'FOREIGN KEY' AND constraint_name IN ('fk__inventory.accounts__inventory.customers', 'fk__inventory.orders_flat__inventory.customers')" \
    | tr -d '[:space:]'
)"
if [ "$source_fk_count" != "2" ]; then
  echo "Expected source foreign keys from accounts/orders_flat to customers" >&2
  exit 1
fi

echo "source-postgres: ok"
