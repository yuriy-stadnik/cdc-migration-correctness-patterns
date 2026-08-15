#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_container_running local-cloud-client-postgres
docker exec local-cloud-client-postgres pg_isready -U appuser -d clientdb >/dev/null

require_sql_count local-cloud-client-postgres clientdb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'client' AND table_name = 'customers'"
require_sql_count local-cloud-client-postgres clientdb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'client' AND table_name = 'addresses'"
require_sql_count local-cloud-client-postgres clientdb 1 "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'client' AND table_name = 'customers' AND column_name = 'source_tx_id'"
require_sql_count local-cloud-client-postgres clientdb 1 "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'client' AND table_name = 'customers' AND column_name = 'idempotency_key' AND data_type = 'character varying'"
require_sql_count local-cloud-client-postgres clientdb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'cdc' AND table_name = 'transaction_metadata'"
require_sql_count local-cloud-client-postgres clientdb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'cdc' AND table_name = 'processed_events'"
require_sql_count local-cloud-client-postgres clientdb 1 "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'cdc' AND table_name = 'postponed_fk_events'"
require_sql_count local-cloud-client-postgres clientdb 1 "SELECT count(*) FROM information_schema.table_constraints WHERE table_schema = 'cdc' AND table_name = 'processed_events' AND constraint_type = 'PRIMARY KEY'"
require_sql_count local-cloud-client-postgres clientdb 1 "SELECT count(*) FROM information_schema.table_constraints WHERE table_schema = 'client' AND table_name = 'addresses' AND constraint_name = 'fk__client.addresses__client.customers' AND constraint_type = 'FOREIGN KEY'"

docker exec -i local-cloud-client-postgres psql -U appuser -d clientdb <<'SQL'
SELECT 'customers' AS table_name, count(*) FROM client.customers
UNION ALL SELECT 'addresses', count(*) FROM client.addresses
UNION ALL SELECT 'processed_events', count(*) FROM cdc.processed_events
UNION ALL SELECT 'postponed_fk_events', count(*) FROM cdc.postponed_fk_events
UNION ALL SELECT 'transaction_metadata', count(*) FROM cdc.transaction_metadata
ORDER BY table_name;
SQL

echo "client-postgres: ok"
