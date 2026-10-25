#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "==> AWS shell syntax"
bash -n \
  scripts/aws-common.sh \
  scripts/aws-up.sh \
  scripts/aws-down.sh \
  scripts/build-deployment-artifacts.sh \
  scripts/msk/msk-up.sh \
  scripts/msk/msk-down.sh \
  scripts/msk/msk-enable-mappings.sh \
  scripts/run-aws-e2e-test.sh

echo "==> AWS lifecycle retry/no-state tests"
./local/tests/test-aws-lifecycle-scripts.sh

echo "==> Shared local/AWS contract tests"
./local/tests/test-shared-deployment-config.sh

echo "==> Rendered EC2 user-data and Compose integration test"
./local/tests/test-aws-user-data-render.sh

echo "==> Local Compose validation"
docker compose -f local/source/docker-compose.yml config -q
docker compose -f local/cloud/docker-compose.yml config -q

echo "==> Shared Java and Lambda unit/integration tests plus artifacts"
./scripts/build-deployment-artifacts.sh

echo "==> Terraform formatting and offline validation"
terraform fmt -check -diff
terraform validate

echo "AWS offline verification: ok (no AWS API calls or deployment)"
