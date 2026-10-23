#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_SQL="$ROOT_DIR/local/postgres/init/03-source-version-v2.sql"

if [ "${SOURCE_WRITERS_PAUSED:-0}" != "1" ]; then
  echo "Pause source writes and set SOURCE_WRITERS_PAUSED=1 before installing source version metadata." >&2
  exit 1
fi

# psql connection parameters may be passed as ordinary arguments or PG* variables.
has_schema="$(psql -X -v ON_ERROR_STOP=1 -Atqc \
  "SELECT to_regclass('source_cdc.schema_version') IS NOT NULL" "$@")"
legacy="$(psql -X -v ON_ERROR_STOP=1 -Atqc \
  "SELECT to_regnamespace('source_projection') IS NOT NULL OR to_regclass('inventory.accounts') IS NOT NULL OR to_regclass('inventory.orders_flat') IS NOT NULL" "$@")"
if [ "$legacy" = "t" ]; then
  echo "The flat/projection source model cannot be upgraded in place; recreate the source database with the normalized schema." >&2
  exit 1
fi

if [ "$has_schema" = "t" ]; then
  installed="$(psql -X -v ON_ERROR_STOP=1 -Atqc \
    "SELECT count(*) FROM source_cdc.schema_version WHERE version=3" "$@")"
else
  installed="0"
fi
if [ "$installed" = "1" ]; then
  echo "Normalized source version schema already installed; no changes made."
  exit 0
fi
if [ "$installed" != "0" ]; then
  echo "Could not determine the source schema version: $installed" >&2
  exit 1
fi

psql -X -v ON_ERROR_STOP=1 "$@" -f "$SOURCE_SQL"
