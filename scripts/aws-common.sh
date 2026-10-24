#!/usr/bin/env bash

AWS_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_REPO_ROOT="$(cd "$AWS_SCRIPT_DIR/.." && pwd)"
TERRAFORM_BIN="${TERRAFORM_BIN:-terraform}"
AUTO_APPROVE="${AUTO_APPROVE:-1}"
SKIP_BUILD="${SKIP_BUILD:-0}"
PROJECTOR_JAR="${PROJECTOR_JAR:-$AWS_REPO_ROOT/transaction-projector/build/libs/transaction-projector-all.jar}"
LAMBDA_JAR="${LAMBDA_JAR:-$AWS_REPO_ROOT/lambda_java/build/libs/msk-to-aurora-lambda-1.0.0.jar}"

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command not found: $command_name" >&2
    exit 1
  fi
}

require_binary_option() {
  local name="$1" value="$2"
  if [[ "$value" != "0" && "$value" != "1" ]]; then
    echo "$name must be 0 or 1, got: $value" >&2
    exit 1
  fi
}

require_positive_integer() {
  local name="$1" value="$2"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer, got: $value" >&2
    exit 1
  fi
}

terraform_run() {
  "$TERRAFORM_BIN" -chdir="$AWS_REPO_ROOT" "$@"
}

terraform_init() {
  terraform_run init -input=false
}

terraform_apply() {
  local arguments=(apply -input=false)
  if [[ "$AUTO_APPROVE" == "1" ]]; then
    arguments+=(-auto-approve)
  fi
  terraform_run "${arguments[@]}" "$@"
}

terraform_destroy() {
  local arguments=(destroy -input=false)
  if [[ "$AUTO_APPROVE" == "1" ]]; then
    arguments+=(-auto-approve)
  fi
  terraform_run "${arguments[@]}" "$@"
}

terraform_state_list() {
  local output status
  set +e
  output="$(terraform_run state list 2>&1)"
  status=$?
  set -e

  if [[ "$status" -eq 0 ]]; then
    printf '%s\n' "$output"
    return 0
  fi
  if grep -Eqi 'no state file|state contains no resources' <<<"$output"; then
    return 0
  fi

  printf '%s\n' "$output" >&2
  return "$status"
}

artifacts_exist() {
  [[ -f "$PROJECTOR_JAR" && -f "$LAMBDA_JAR" ]]
}

prepare_start_artifacts() {
  if [[ "$SKIP_BUILD" == "1" ]]; then
    if ! artifacts_exist; then
      echo "SKIP_BUILD=1 but required deployment artifacts are missing." >&2
      echo "Missing one or both: $PROJECTOR_JAR, $LAMBDA_JAR" >&2
      exit 1
    fi
    return
  fi
  "$AWS_REPO_ROOT/scripts/build-deployment-artifacts.sh"
}

prepare_destroy_artifacts() {
  if artifacts_exist; then
    return
  fi
  if [[ "$SKIP_BUILD" == "1" ]]; then
    echo "Terraform must evaluate artifact hashes before destroy, but artifacts are missing and SKIP_BUILD=1." >&2
    exit 1
  fi
  echo "Deployment artifacts are missing; building them so Terraform can evaluate the destroy plan."
  "$AWS_REPO_ROOT/scripts/build-deployment-artifacts.sh"
}

require_command "$TERRAFORM_BIN"
require_binary_option AUTO_APPROVE "$AUTO_APPROVE"
require_binary_option SKIP_BUILD "$SKIP_BUILD"
