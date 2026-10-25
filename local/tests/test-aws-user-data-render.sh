#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

command -v terraform >/dev/null 2>&1 || { echo "terraform is required" >&2; exit 1; }
docker compose version >/dev/null

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/aws-user-data-render.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

cat > "$TMP_DIR/main.tf" <<'TERRAFORM_CONFIG'
variable "repo_root" { type = string }

locals {
  rendered = templatefile("${var.repo_root}/user_data.sh.tpl", {
    msk_bootstrap_iam             = "b-1.example.kafka.us-east-1.amazonaws.com:9098"
    mm2_topic_allowlist           = "cdc[.]tx[.](control|mutations|dlq)[.]v2"
    mm2_group_allowlist           = "__no_groups__"
    region                        = "us-east-1"
    projector_bucket              = "example-artifact-bucket"
    projector_key                 = "transaction-projector-all.jar"
    source_roles_sql              = file("${var.repo_root}/local/postgres/init/01-role-and-grants.sql")
    source_inventory_sql          = file("${var.repo_root}/local/postgres/init/02-inventory.sql")
    source_version_sql            = file("${var.repo_root}/local/postgres/init/03-source-version-v2.sql")
    source_version_connector_json = replace(file("${var.repo_root}/local/source/connectors/source-version-v2-config.json"), "source-postgres", "legacy-postgres")
    transaction_topics            = file("${var.repo_root}/config/transaction-v2-topics.txt")
    e2e_source_insert_sql         = file("${var.repo_root}/config/e2e-source-insert.sql")
    e2e_source_update_sql         = file("${var.repo_root}/config/e2e-source-update.sql")
    mm2_common_properties         = file("${var.repo_root}/config/mm2-common.properties")
    ec2_cdc_test_script           = file("${var.repo_root}/scripts/ec2-cdc-to-kafka-test.sh")
  })
}

output "rendered" { value = local.rendered }
TERRAFORM_CONFIG

terraform -chdir="$TMP_DIR" init -backend=false -input=false >/dev/null
terraform -chdir="$TMP_DIR" apply -auto-approve -input=false -var "repo_root=$REPO_ROOT" >/dev/null
terraform -chdir="$TMP_DIR" output -raw rendered > "$TMP_DIR/user-data.sh"

bash -n "$TMP_DIR/user-data.sh"
awk '/^cat > docker-compose.yml <</ || /^cat >> docker-compose.yml <</ {emit=1; next}
     emit && /^EOF$/ {emit=0; next}
     emit {print}' "$TMP_DIR/user-data.sh" > "$TMP_DIR/docker-compose.yml"
docker compose -f "$TMP_DIR/docker-compose.yml" config -q
docker compose -f "$TMP_DIR/docker-compose.yml" config --format json |
  jq -e '.services["kafka-topics-init"].command as $cmd | ($cmd | length) == 1 and ($cmd[0] | contains("while IFS= read -r spec"))' >/dev/null
docker compose -f "$TMP_DIR/docker-compose.yml" config --format json |
  jq -e '.services["msk-topics-init"].command as $cmd | ($cmd | length) == 1 and ($cmd[0] | contains("while IFS= read -r spec"))' >/dev/null
docker compose -f "$TMP_DIR/docker-compose.yml" config --format json |
  jq -e '.services["flink-sql-init"].command as $cmd | ($cmd | length) == 3 and $cmd[0] == "bash" and $cmd[1] == "-lc" and ($cmd[2] | contains("flink run -d -m flink-jobmanager:8081 -c com.example.cdc.projector.TransactionProjectorJob /opt/flink/usrlib/transaction-projector-all.jar"))' >/dev/null

grep -F 'msk-topics-init:' "$TMP_DIR/docker-compose.yml" >/dev/null
grep -F 'mirrormaker2:' "$TMP_DIR/docker-compose.yml" >/dev/null
grep -F 'source_cdc.begin_write()' "$TMP_DIR/user-data.sh" >/dev/null
echo "rendered AWS user-data and Compose configuration: ok"
