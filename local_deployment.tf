variable "enable_local_deployment" {
  description = "When true, Terraform starts the local two-compose Docker deployment: source emulator plus cloud emulator."
  type        = bool
  default     = false
}

variable "local_docker_network_name" {
  description = "Shared Docker network used by the local source and cloud emulator compose stacks."
  type        = string
  default     = "cdc-migration-local"
}

locals {
  local_cloud_compose_file  = "${path.module}/local/cloud/docker-compose.yml"
  local_source_compose_file = "${path.module}/local/source/docker-compose.yml"
}

resource "terraform_data" "local_docker_network" {
  count = var.enable_local_deployment ? 1 : 0

  triggers_replace = [
    var.local_docker_network_name
  ]

  provisioner "local-exec" {
    command = "docker network inspect ${var.local_docker_network_name} >/dev/null 2>&1 || docker network create ${var.local_docker_network_name}"
  }
}

resource "terraform_data" "local_cloud_deployment" {
  count = var.enable_local_deployment ? 1 : 0

  triggers_replace = [
    filesha256(local.local_cloud_compose_file),
    filesha256("${path.module}/local/cloud/lambda-consumer/Dockerfile"),
    filesha256("${path.module}/local/cloud/lambda-consumer/build.gradle"),
    filesha256("${path.module}/local/cloud/lambda-consumer/settings.gradle"),
    filesha256("${path.module}/local/cloud/lambda-consumer/src/main/java/com/example/local/LocalLambdaConsumer.java"),
    filesha256("${path.module}/local/cloud/postgres/client-init/01-schema.sql"),
    filesha256("${path.module}/local/cloud/postgres/operational-init/01-schema.sql")
  ]

  depends_on = [terraform_data.local_docker_network]

  provisioner "local-exec" {
    command = "docker compose -f ${local.local_cloud_compose_file} up -d --build"
  }
}

resource "terraform_data" "local_source_deployment" {
  count = var.enable_local_deployment ? 1 : 0

  triggers_replace = [
    filesha256(local.local_source_compose_file),
    filesha256("${path.module}/local/source/mm2.properties"),
    filesha256("${path.module}/local/source/flink/sql/init.sql"),
    filesha256("${path.module}/local/source/connectors/legacy-postgres-source-config.json"),
    filesha256("${path.module}/local/postgres/init/01-role-and-grants.sql"),
    filesha256("${path.module}/local/postgres/init/02-inventory.sql")
  ]

  depends_on = [terraform_data.local_cloud_deployment]

  provisioner "local-exec" {
    command = "docker compose -f ${local.local_source_compose_file} up -d"
  }
}
