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
SQL

echo "source-postgres: ok"
