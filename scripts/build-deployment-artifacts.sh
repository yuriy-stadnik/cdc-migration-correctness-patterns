#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GRADLE_BIN="${GRADLE_BIN:-gradle}"

cd "$REPO_ROOT"

"$GRADLE_BIN" -p transaction-protocol --no-daemon clean test
"$GRADLE_BIN" -p transaction-apply --no-daemon clean test
"$GRADLE_BIN" -p transaction-projector --no-daemon clean test fatJar
"$GRADLE_BIN" -p lambda_java --no-daemon clean test shadowJar

test -f transaction-projector/build/libs/transaction-projector-all.jar
test -f lambda_java/build/libs/msk-to-aurora-lambda-1.0.0.jar

echo "deployment artifacts built after protocol, apply, projector, and Lambda tests"
