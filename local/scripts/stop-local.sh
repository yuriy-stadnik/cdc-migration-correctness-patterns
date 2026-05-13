#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$LOCAL_DIR/.." && pwd)"
cd "$REPO_ROOT"

NETWORK_NAME="${LOCAL_DOCKER_NETWORK_NAME:-cdc-migration-local}"
SOURCE_COMPOSE="${SOURCE_COMPOSE:-local/source/docker-compose.yml}"
CLOUD_COMPOSE="${CLOUD_COMPOSE:-local/cloud/docker-compose.yml}"
DELETE_VOLUMES="${DELETE_VOLUMES:-0}"

down_args=(down --remove-orphans)
if [ "$DELETE_VOLUMES" = "1" ]; then
  down_args+=(--volumes)
fi

docker compose -f "$SOURCE_COMPOSE" "${down_args[@]}"
docker compose -f "$CLOUD_COMPOSE" "${down_args[@]}"

if [ "$DELETE_VOLUMES" = "1" ]; then
  docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
fi

echo "local split deployment stopped"
