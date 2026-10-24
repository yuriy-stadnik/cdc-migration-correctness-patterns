# Kafka CDC Replication to MSK and Aurora

This project provisions source CDC and transaction projection on EC2, replication to MSK, and atomic transaction application into one Aurora PostgreSQL database containing `client` and `operational` schemas. See [`docs/transaction-protocol-operations.md`](docs/transaction-protocol-operations.md) for the authoritative path and guarantees.

This is a prototype built with an example inventory/customer/order dataset. Its main purpose is to demonstrate a real-time data flow for migrating a staging persistence layer from on-premises systems toward a service-oriented architecture. The pattern can support phased modernization workflows such as canary releases, blue/green deployments, reporting projections, audit stores, domain-specific read models, and other downstream systems that need live, consistent change streams during migration.

## Documentation

- [`SYSTEM_COMPONENTS_DESCRIPTION.md`](SYSTEM_COMPONENTS_DESCRIPTION.md): concise component and execution-flow overview.
- [`docs/java-transaction-protocol-and-projector.md`](docs/java-transaction-protocol-and-projector.md): Java protocol records, projector classes, dependencies, and execution sequence.
- [`docs/transaction-working-example.md`](docs/transaction-working-example.md): executable four-row source transaction traced through the local pipeline.
- [`docs/transaction-protocol-operations.md`](docs/transaction-protocol-operations.md): local protocol, dependency, recovery, and verification runbook.
- [`docs/CURRENT_STATUS.md`](docs/CURRENT_STATUS.md): current implementation and verification status.
- [`local/README.md`](local/README.md): local startup, test, and shutdown commands.
- [`scripts/ops/README.md`](scripts/ops/README.md): AWS inspection helpers.

The AWS environment uses the public EC2 instance as an on-premises infrastructure emulator. The main flow is:

1. The EC2 instance runs PostgreSQL, Debezium, Kafka, Flink, Kafka UI, and MirrorMaker2 in Docker.
2. The simplified local example uses four normalized tables. Each source row owns exactly one destination row with the same primary key.
3. Debezium routes row changes, source revision-journal rows, and transaction metadata into ordered `cdc.source.capture.v2`.
4. Flink assembles complete source transactions and emits immutable `cdc.tx.control.v2` and `cdc.tx.mutations.v2` records.
5. MirrorMaker2 replicates the `cdc.tx.*.v2` protocol topics to MSK Serverless using IAM authentication.
6. A Java Lambda function is triggered from those MSK topics.
7. Lambda stages fragments and atomically applies each complete source transaction to one Aurora database containing both business schemas.

## Architecture

The Terraform configuration creates:

- A VPC with two public subnets and two private subnets.
- An Internet Gateway and NAT Gateway.
- An MSK Serverless cluster with IAM client authentication.
- One Aurora PostgreSQL cluster in private subnets with `client`, `operational`, and `cdc` schemas.
- A public EC2 host that emulates on-prem infrastructure with Docker, PostgreSQL, Debezium, Kafka, Flink, Kafka UI, and MirrorMaker2.
- A private EC2 network-test host.
- A Java 17 Lambda function connected to the private subnets.
- MSK event source mappings for `cdc.tx.control.v2`, `cdc.tx.mutations.v2`, and `cdc.tx.dlq.v2`.
- IAM roles and security groups for EC2, MSK, Lambda, and Aurora.

The EC2 host is the source-side infrastructure emulator. AWS keeps MSK Serverless, Lambda, and the consolidated Aurora PostgreSQL cluster private, with Secrets Manager providing database credentials.

## Permissions, Topics, and Idempotency

The transaction protocol uses:

- `cdc.source.capture.v2` as the ordered source-planning lane.
- `cdc.tx.control.v2` for immutable manifests and lifecycle control.
- `cdc.tx.mutations.v2` for immutable numbered mutation payloads.
- `cdc.tx.dlq.v2` for quarantined source protocol failures.

Every mutation carries a canonical source transaction ID, source record sequence, typed object key, operation, idempotency key, source position, and expected/new object versions. PostgreSQL validates the canonical SHA-256 mutation-set digest and exact versions before applying any business row.

## Repository Layout

```text
.
├── aurora.tf                 # Aurora PostgreSQL cluster and instance
├── ec2.tf                    # EC2 hosts, IAM role, and user data wiring
├── iam-terraform-deployer-user.yaml # Optional IAM bootstrap stack for Terraform user setup
├── lambda.tf                 # Java Lambda function and MSK event source mapping
├── msk.tf                    # MSK Serverless cluster
├── network.tf                # VPC, subnets, routes, NAT, and Internet Gateway
├── outputs.tf                # Terraform outputs
├── providers.tf              # Terraform and AWS provider configuration
├── security.tf               # Security groups
├── scripts/
│   ├── connect-kafka-mm2.sh   # macOS/Linux helper for EC2 Instance Connect SSH
│   ├── connect-kafka-mm2.bat  # Windows helper for EC2 Instance Connect SSH
│   └── msk/
│       ├── msk-up.sh              # Enable MSK resources for cloud validation windows
│       ├── msk-down.sh            # Disable MSK resources to stop MSK Serverless cost
│       └── analyze-msk-cost-window.sh # Cost and lifecycle analysis for MSK
├── user_data.sh.tpl          # EC2 bootstrap script for Kafka and MirrorMaker2
├── variables.tf              # Input variables
├── local/                    # Local Docker integration stack, scripts, and component tests
│   ├── scripts/              # Local deployment and E2E helpers
│   └── tests/                # Component-level local stack checks
├── tmp/lambda_test.sh        # Manual Lambda invoke helper
└── lambda_java/
    ├── build.gradle          # Java Lambda build configuration
    ├── gradlew               # Gradle wrapper for macOS/Linux
    ├── gradlew.bat           # Gradle wrapper for Windows
    └── src/main/java/com/example/Handler.java
```

## Prerequisites

- Terraform `>= 1.5.0`
- AWS CLI configured with credentials for the target account
- Java 17
- Docker and Docker Compose v2 for local validation
- `jq` for the AWS E2E script
- Network access to AWS APIs
- Permissions to create VPC, EC2, MSK, RDS, Lambda, IAM, Secrets Manager, CloudWatch Logs, S3, KMS decrypt grants, and related resources

This project creates billable AWS resources, including NAT Gateway, EC2, MSK Serverless, Aurora, Lambda, and data transfer.

## Local Two-Compose Migration Emulation

For local development without MSK, Lambda, or Aurora, Terraform can start a split Docker deployment:

```text
source compose:
  PostgreSQL -> Debezium -> ordered capture -> Flink projector -> cdc.tx.*.v2 -> MirrorMaker2

cloud replacement compose:
  Kafka -> Java apply worker -> one PostgreSQL database (client + operational + cdc schemas)
```

The local flow reconstructs source boundaries, persists dependencies, and commits all projected effects and their receipt atomically.

Run it with Terraform:

```bash
terraform apply \
  -var enable_local_deployment=true \
  -target=terraform_data.local_docker_network \
  -target=terraform_data.local_cloud_deployment \
  -target=terraform_data.local_source_deployment
```

Run a delivery test:

```bash
RESET=1 ./local/scripts/run-local-e2e.sh
```

Start or stop the split local deployment without running the E2E assertions:

```bash
./local/scripts/start-local.sh
./local/scripts/stop-local.sh
DELETE_VOLUMES=1 ./local/scripts/stop-local.sh
```

Reset both local compose stacks and rerun from a clean slate:

```bash
RESET=1 ./local/scripts/run-local-e2e.sh
```

Run component checks separately after the local stack is up:

```bash
./local/tests/test-source-postgres.sh
./local/tests/test-debezium-connect.sh
./local/tests/test-source-kafka.sh
./local/tests/test-flink.sh
./local/tests/test-mirrormaker2.sh
./local/tests/test-cloud-kafka.sh
./local/tests/test-client-postgres.sh
./local/tests/test-operational-postgres.sh
./local/tests/test-local-lambda.sh
```

Run all component checks:

```bash
./local/tests/run-all-components.sh
```

The source compose lives in `local/source/docker-compose.yml`; the cloud replacement compose lives in `local/cloud/docker-compose.yml`. See `local/README.md` for verification commands.

## AWS Account Preparation

Use a dedicated AWS account or sandbox account when possible. At minimum, prepare these items before running Terraform:

1. Choose an AWS region. The default is `us-east-1`.
2. Confirm the AWS CLI identity that will run Terraform:

```bash
aws sts get-caller-identity
```

3. Bootstrap a deployer IAM user or role with permissions for this project. This repository includes `iam-terraform-deployer-user.yaml`, a CloudFormation template that creates a scoped IAM user named `terraform` by default and attaches permissions for the VPC, EC2, MSK, RDS/Aurora, Lambda, IAM roles, Secrets Manager, CloudWatch, Cost Explorer, CloudTrail lookup, and EC2 Instance Connect actions used by the project.

```bash
aws cloudformation deploy \
  --region us-east-1 \
  --stack-name kafka-mm2-msk-lab-terraform-user \
  --template-file iam-terraform-deployer-user.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    TerraformUserName=terraform \
    ProjectPrefix=kafka-mm2-msk-lab \
    CreateAccessKey=false
```

4. Configure AWS CLI credentials for the selected deployer identity. Prefer IAM Identity Center, role assumption, or manually managed short-lived credentials. If you intentionally need CloudFormation to create an access key for a disposable lab account, set `CreateAccessKey=true` and store the returned secret securely.
5. Make sure required service-linked roles can be created on first use. The bootstrap policy allows service-linked role creation for Auto Scaling, MSK, Lambda, and RDS.
6. Set your public IP as the SSH allowlist before applying EC2 resources:

```bash
export TF_VAR_ssh_cidr=<your-public-ip>/32
```

You can detect your current public IP with:

```bash
curl -fsS https://checkip.amazonaws.com
```

The helper scripts can also detect or accept your public IP and set `TF_VAR_ssh_cidr` automatically.

## Configuration

The main variables are defined in `variables.tf`:

- `aws_region`: AWS region, default `us-east-1`
- `enable_msk`: controls MSK Serverless + Lambda MSK event mappings, default `false`
- `project`: resource name prefix, default `kafka-mm2-msk-lab`
- `ec2_instance_type`: EC2 host type, default `t3.large`
- `ec2_root_volume_size`: root EBS volume size for the EC2 source emulator, default `80`
- `ec2_msk_bootstrap_iam`: optional MSK IAM bootstrap string for MirrorMaker2 on EC2
- `ssh_cidr`: allowed CIDR for SSH and optional Kafka test access, default `0.0.0.0/32`
- `ec2_instance_connect_user_name`: IAM user allowed to push temporary EC2 Instance Connect SSH keys, default `terraform`
- `db_name`: legacy variable retained for backward compatibility
- `db_username`: Aurora master username, default `appuser`
- `destination_db_name`: Aurora database containing the `client`, `operational`, and `cdc` schemas, default `destinationdb`
- `aurora_instance_class`: Aurora instance class, default `db.t3.medium`
- `mm2_topic_allowlist_regex`: MM2 replication allowlist, default `cdc[.]tx[.](control|mutations|dlq)[.]v2`
- `mm2_group_allowlist_regex`: MM2 group sync allowlist, default `__no_groups__`
- `enable_local_deployment`: starts the local two-compose Docker deployment, default `false`
- `local_docker_network_name`: shared Docker network for local source and cloud emulator stacks, default `cdc-migration-local`

Set `ssh_cidr` to your current public IP in CIDR form before applying. Terraform also reads this automatically from the `TF_VAR_ssh_cidr` OS environment variable.

```bash
export TF_VAR_ssh_cidr=<your-public-ip>/32
terraform apply
```

MSK is disabled by default to reduce cost. Enable it only for short validation windows:

```bash
./scripts/msk/msk-up.sh us-east-1
```

Disable MSK resources after validation:

```bash
./scripts/msk/msk-down.sh us-east-1
```

Analyze MSK cost and lifecycle events for a time window:

```bash
./scripts/msk/analyze-msk-cost-window.sh 2026-05-10 2026-05-14 us-east-1
```

Notes:
- Exact cluster create/delete timestamps require IAM permission `cloudtrail:LookupEvents`.
- Hour-level Cost Explorer granularity requires payer-account CE hourly opt-in; without it, charge windows are day-level.

The helper scripts below set `TF_VAR_ssh_cidr` before Terraform runs. They accept the IP as an argument, or read it from `MY_IP`, `PUBLIC_IP`, or `TF_VAR_ssh_cidr`.

## Build Lambda

Build the Java Lambda fat JAR before running Terraform apply:

```bash
cd lambda_java
./gradlew clean build
cd ..
```

Terraform expects the JAR at:

```text
lambda_java/build/libs/msk-to-aurora-lambda-1.0.0.jar
```

## Deploy

Initialize and apply Terraform:

```bash
terraform init
terraform plan
terraform apply
```

Important outputs include:

- `vpc_id`
- `ec2_public_ip`
- `ec2_instance_id`
- `ec2_private_ip`
- `lambda_net_test_private_ip`
- `msk_bootstrap_sasl_iam`
- `aurora_endpoint`
- `msk_cluster_arn`
- `aurora_master_secret_arn`
- `aurora_port`
- `sg_lambda_id`
- `sg_msk_id`
- `subnet_private_ids`

## Demo: Connect to EC2 with EC2 Instance Connect

The public EC2 host is `aws_instance.kafka_mm2`. SSH access is restricted by the `ssh_cidr` Terraform variable, so pass your public IP as a parameter when creating or updating the instance.

Run on macOS/Linux. If no IP is provided, the script uses `https://checkip.amazonaws.com` to detect your current public IPv4 address:

```bash
./scripts/connect-kafka-mm2.sh
```

You can also set an OS environment variable before running the script:

```bash
export MY_IP=<your-public-ip>
./scripts/connect-kafka-mm2.sh
```

Or pass the IP explicitly:

```bash
./scripts/connect-kafka-mm2.sh <your-public-ip> us-east-1 ~/.ssh/temp_ec2_key
```

Run on Windows. If no IP is provided, the script also attempts to detect it:

```bat
scripts\connect-kafka-mm2.bat
```

On Windows, you can set the OS environment variable before running Terraform:

```bat
set MY_IP=<your-public-ip>
scripts\apply-ec2-access.bat
```

The scripts:

1. Detect or accept your public IP and convert it to CIDR form, for example `<your-public-ip>/32`.
2. Create a temporary SSH key if it does not already exist.
3. Set `TF_VAR_ssh_cidr=<your-ip>/32` before Terraform runs.
4. Apply the targeted EC2 resources and EC2 Instance Connect IAM policy.
5. Read `ec2_instance_id` and `ec2_public_ip` from Terraform outputs.
6. Send the public key with `aws ec2-instance-connect send-ssh-public-key`.
7. Open SSH as `ec2-user`.

Equivalent manual commands:

```bash
export TF_VAR_ssh_cidr=<your-public-ip>/32
terraform apply \
  -target=aws_security_group.ec2 \
  -target=aws_instance.kafka_mm2 \
  -target=aws_iam_user_policy.ec2_instance_connect

aws ec2-instance-connect send-ssh-public-key \
  --region us-east-1 \
  --instance-id "$(terraform output -raw ec2_instance_id)" \
  --instance-os-user ec2-user \
  --ssh-public-key file://~/.ssh/temp_ec2_key.pub

ssh -i ~/.ssh/temp_ec2_key ec2-user@"$(terraform output -raw ec2_public_ip)"
```

## EC2 Bootstrap

The main EC2 host uses `user_data.sh.tpl` to emulate the on-premises side of the migration:

- Install Docker.
- Install Docker Compose v2.
- Download the AWS MSK IAM auth JAR.
- Download the transaction-projector artifact.
- Create normalized PostgreSQL init scripts.
- Start local PostgreSQL with logical replication enabled.
- Register a Debezium PostgreSQL CDC connector.
- Start local Kafka and Zookeeper.
- Start Flink JobManager, TaskManager, and the Java transaction projector that writes immutable v2 control and mutation topics.
- Start Kafka UI.
- Write Kafka client configuration for IAM auth.
- Write MirrorMaker2 configuration.
- Create `/opt/lab/docker-compose.yml`.
- Start MirrorMaker2 to replicate the v2 transaction topics to MSK Serverless.
- Install a `lab-up.service` systemd unit for restarting the Docker stack.

On the EC2 host, useful paths are:

```text
/opt/lab/docker-compose.yml
/opt/lab/mm2.properties
/opt/lab/client.properties
/opt/lab/postgres/init
/opt/lab/connectors
/opt/lab/integration-test-cdc-to-kafka.sh
/usr/local/bin/lab-up.sh
/var/log/user-data.log
```

Useful EC2 emulator ports are restricted by `ssh_cidr`:

- `9092`: local Kafka
- `8080`: Kafka UI
- `8081`: Flink UI
- `8083`: Debezium Connect REST API

## EC2 CDC to Kafka Test

The EC2 bootstrap creates `/opt/lab/integration-test-cdc-to-kafka.sh`. The test inserts one row into `inventory.customers` in the source PostgreSQL container and verifies that Debezium publishes the change to `cdc.source.capture.v2`.

After connecting to the EC2 host:

```bash
sudo /opt/lab/integration-test-cdc-to-kafka.sh
```

Optional parameters can be set as environment variables:

```bash
sudo env CUSTOMER_ID=10001 \
  EMAIL=cdc-test-10001@example.com \
  TIMEOUT_SECONDS=180 \
  /opt/lab/integration-test-cdc-to-kafka.sh
```

For an EC2 instance created before this script was added, copy and run the repository version:

```bash
./scripts/install-ec2-cdc-test.sh

ssh -i ~/.ssh/temp_ec2_key ec2-user@$(terraform output -raw ec2_public_ip) \
  'sudo /opt/lab/integration-test-cdc-to-kafka.sh'
```

If the test script is already installed, refresh the temporary EC2 Instance Connect key and run it:

```bash
./scripts/run-ec2-cdc-test.sh
```

EC2 Instance Connect SSH keys are temporary. If you run `scp` manually, first send the key again and copy the file immediately after:

```bash
aws ec2-instance-connect send-ssh-public-key \
  --region us-east-1 \
  --instance-id "$(terraform output -raw ec2_instance_id)" \
  --instance-os-user ec2-user \
  --ssh-public-key file://~/.ssh/temp_ec2_key.pub

scp -o StrictHostKeyChecking=accept-new \
  -i ~/.ssh/temp_ec2_key \
  scripts/ec2-cdc-to-kafka-test.sh \
  ec2-user@$(terraform output -raw ec2_public_ip):/tmp/
```

## Lambda Behavior

The Lambda handler:

- Receives immutable v2 control, mutation, and diagnostic records from MSK.
- Reads credentials from the RDS-managed Secrets Manager secret.
- Connects to one Aurora PostgreSQL database containing the `client`, `operational`, and `cdc` schemas.
- Durably stages fragments and applies only complete, verified source transactions.
- Commits business rows, numeric owner versions, and the immutable receipt atomically.

The Lambda environment variables are set by Terraform:

- `DESTINATION_DB_HOST`
- `DESTINATION_DB_PORT`
- `DESTINATION_DB_NAME`
- `DESTINATION_DB_USER`
- `DESTINATION_DB_SECRET_ARN`

## AWS E2E Test

After MSK is enabled and the EC2 host is configured with the current MSK bootstrap endpoint, run the full AWS path validation with:

```bash
./scripts/run-aws-e2e-test.sh us-east-1 ~/.ssh/temp_ec2_key
```

The existing AWS E2E script still targets the earlier six-table/seven-mutation deployment contract. It was intentionally not changed or run during the local simplification and must be reconciled before AWS validation.

Optional environment overrides:

```bash
AURORA_WAIT_SECONDS=300 \
CID=778802093 \
OID=978802093 \
EMAIL=aws-e2e-778802093@example.com \
./scripts/run-aws-e2e-test.sh us-east-1 ~/.ssh/temp_ec2_key
```

## Destroy

To remove the project resources:

```bash
terraform destroy
```

Because this project creates billable infrastructure, destroy the environment when it is no longer needed.
