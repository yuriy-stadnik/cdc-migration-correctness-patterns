#!/usr/bin/env bash
set -euo pipefail

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_DIR="$(cd "$TEST_DIR/.." && pwd)"
REPO_ROOT="$(cd "$LOCAL_DIR/.." && pwd)"
cd "$REPO_ROOT"

NETWORK_NAME="${LOCAL_DOCKER_NETWORK_NAME:-cdc-migration-local}"
SOURCE_COMPOSE="${SOURCE_COMPOSE:-local/source/docker-compose.yml}"
CLOUD_COMPOSE="${CLOUD_COMPOSE:-local/cloud/docker-compose.yml}"

require_container_running() {
  local name="$1"
  local running

  running="$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || true)"
  if [ "$running" != "true" ]; then
    echo "Container is not running: $name" >&2
    return 1
  fi
}

require_topic() {
  local container="$1"
  local bootstrap="$2"
  local topic="$3"

  docker exec "$container" kafka-topics --bootstrap-server "$bootstrap" --list | grep -Fx "$topic" >/dev/null
}

require_sql_count() {
  local container="$1"
  local database="$2"
  local expected="$3"
  local sql="$4"
  local actual

  actual="$(docker exec -i "$container" psql -U appuser -d "$database" -tAc "$sql" | tr -d '[:space:]')"
  if [ "$actual" != "$expected" ]; then
    echo "Expected SQL count $expected but got ${actual:-<empty>}: $sql" >&2
    return 1
  fi
}
