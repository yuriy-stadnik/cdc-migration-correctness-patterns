#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_FILE="${CONNECTOR_FILE:-local/connectors/legacy-postgres-source.json}"
CONNECTOR_CONFIG_FILE="${CONNECTOR_CONFIG_FILE:-local/connectors/legacy-postgres-source-config.json}"
CONNECTOR_NAME="${CONNECTOR_NAME:-legacy-postgres-source}"

if [ -f "${CONNECTOR_CONFIG_FILE}" ]; then
  payload_file="${CONNECTOR_CONFIG_FILE}"
  url="${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config"
  method="PUT"
else
  payload_file="${CONNECTOR_FILE}"
  url="${CONNECT_URL}/connectors"
  method="POST"
fi

response_file="$(mktemp)"
status="$(curl -sS -o "${response_file}" -w "%{http_code}" -X "${method}" \
  -H "Content-Type: application/json" \
  --data @"${payload_file}" \
  "${url}")"

cat "${response_file}"
echo
rm -f "${response_file}"

if [ "${status}" -lt 200 ] || [ "${status}" -ge 300 ]; then
  echo "Connector registration failed with HTTP ${status}" >&2
  exit 1
fi

curl -fsS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status"
echo
