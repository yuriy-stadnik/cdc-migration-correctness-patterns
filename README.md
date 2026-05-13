# Kafka MirrorMaker2 to MSK to Aurora

This project provisions an AWS environment that moves Kafka records through Amazon MSK Serverless into an Aurora PostgreSQL database using AWS Lambda.

The AWS environment uses the public EC2 instance as an on-premises infrastructure emulator. The main flow is:

1. The EC2 instance runs legacy PostgreSQL, Debezium, Kafka, Flink, Kafka UI, and MirrorMaker2 in Docker.
2. Debezium captures CDC from `inventory.customers` and writes Kafka topic `pg1.inventory.customers`.
3. Flink projects the local CDC stream into a local target PostgreSQL table for on-prem smoke testing.
4. MirrorMaker2 replicates local Kafka topics to MSK Serverless using IAM authentication.
5. A Java Lambda function is triggered from MSK topic `pg1.inventory.customers`.
6. The Lambda function writes Kafka event payloads into Aurora PostgreSQL.

## Architecture

The Terraform configuration creates:

- A VPC with two public subnets and two private subnets.
- An Internet Gateway and NAT Gateway.
- An MSK Serverless cluster with IAM client authentication.
- An Aurora PostgreSQL cluster in private subnets.
- A public EC2 host that emulates on-prem infrastructure with Docker, PostgreSQL, Debezium, Kafka, Flink, Kafka UI, and MirrorMaker2.
- A private EC2 network-test host.
- A Java 17 Lambda function connected to the private subnets.
- An MSK event source mapping that invokes Lambda from topic `pg1.inventory.customers`.
- IAM roles and security groups for EC2, MSK, Lambda, and Aurora.

## Repository Layout

```text
.
├── aurora.tf                 # Aurora PostgreSQL cluster and instance
├── ec2.tf                    # EC2 hosts, IAM role, and user data wiring
├── lambda.tf                 # Java Lambda function and MSK event source mapping
├── msk.tf                    # MSK Serverless cluster
├── network.tf                # VPC, subnets, routes, NAT, and Internet Gateway
├── outputs.tf                # Terraform outputs
├── providers.tf              # Terraform and AWS provider configuration
├── security.tf               # Security groups
├── scripts/
│   ├── connect-kafka-mm2.sh   # macOS/Linux helper for EC2 Instance Connect SSH
│   └── connect-kafka-mm2.bat  # Windows helper for EC2 Instance Connect SSH
├── user_data.sh.tpl          # EC2 bootstrap script for Kafka and MirrorMaker2
├── variables.tf              # Input variables
├── local/                    # Local Docker integration stack
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
- Network access to AWS APIs
- Permissions to create VPC, EC2, MSK, RDS, Lambda, IAM, Secrets Manager, and related resources

This project creates billable AWS resources, including NAT Gateway, EC2, MSK Serverless, Aurora, Lambda, and data transfer.

## Local Integration Stack

The `local/` directory contains a Docker Compose stack for validating the first part of the CDC pipeline before using AWS:

```text
local legacy PostgreSQL
  -> Debezium PostgreSQL CDC connector
  -> local Kafka CDC topic
  -> Flink SQL projection job
  -> local target PostgreSQL
```

Start it from the repository root:

```bash
docker compose -f local/docker-compose.yml up -d
```

The stack packages the local CDC components directly in this repository: one-shot PostgreSQL init, one-shot Debezium connector registration, Flink connector JAR mounting, Kafka UI, and a basic Flink SQL projection into target PostgreSQL. See `local/README.md` for topic checks, source-change commands, and cleanup steps.

## Local Two-Compose Migration Emulation

For local development without MSK, Lambda, or Aurora, Terraform can start a split Docker deployment:

```text
source compose:
  PostgreSQL -> Debezium CDC -> Kafka -> Flink -> operational.* Kafka topics -> MirrorMaker2

cloud replacement compose:
  Kafka -> local Lambda replacement -> PostgreSQL
```

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
./scripts/run-local-e2e.sh
```

Reset both local compose stacks and rerun from a clean slate:

```bash
RESET=1 ./scripts/run-local-e2e.sh
```

The source compose lives in `local/source/docker-compose.yml`; the cloud replacement compose lives in `local/cloud/docker-compose.yml`. See `local/README.md` for verification commands.

## Configuration

The main variables are defined in `variables.tf`:

- `aws_region`: AWS region, default `us-east-1`
- `project`: resource name prefix, default `kafka-mm2-msk-lab`
- `ec2_instance_type`: EC2 host type, default `t3.large`
- `ssh_cidr`: allowed CIDR for SSH and optional Kafka test access, default `0.0.0.0/32`
- `ec2_instance_connect_user_name`: IAM user allowed to push temporary EC2 Instance Connect SSH keys, default `terraform`
- `db_name`: Aurora database name, default `appdb`
- `db_username`: Aurora master username, default `appuser`

Set `ssh_cidr` to your current public IP in CIDR form before applying. Terraform also reads this automatically from the `TF_VAR_ssh_cidr` OS environment variable.

```bash
export TF_VAR_ssh_cidr=<your-public-ip>/32
terraform apply
```

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
- Download Flink Kafka, JSON, JDBC, and PostgreSQL connector JARs.
- Create local legacy PostgreSQL init scripts.
- Start local PostgreSQL with logical replication enabled.
- Register a Debezium PostgreSQL CDC connector.
- Start local Kafka and Zookeeper.
- Start Flink JobManager, TaskManager, and a Flink SQL projection job.
- Start Kafka UI.
- Write Kafka client configuration for IAM auth.
- Write MirrorMaker2 configuration.
- Create `/opt/lab/docker-compose.yml`.
- Start MirrorMaker2 to replicate local topics to MSK Serverless.
- Install a `lab-up.service` systemd unit for restarting the Docker stack.

On the EC2 host, useful paths are:

```text
/opt/lab/docker-compose.yml
/opt/lab/mm2.properties
/opt/lab/client.properties
/opt/lab/postgres/init
/opt/lab/connectors
/opt/lab/flink/sql/init.sql
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

The EC2 bootstrap creates `/opt/lab/integration-test-cdc-to-kafka.sh`. The test inserts one row into `inventory.customers` in the source PostgreSQL container and verifies that Debezium publishes the change to Kafka topic `pg1.inventory.customers`.

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

- Receives Kafka records from MSK.
- Reads Aurora credentials from the RDS-managed Secrets Manager secret.
- Connects to Aurora PostgreSQL using JDBC.
- Creates the target table if it does not exist.
- Stores each Kafka payload as JSONB.

The Lambda environment variables are set by Terraform:

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_SECRET_ARN`
- `TABLE_NAME`

## Manual Lambda Test Helper

`tmp/lambda_test.sh` creates a sample Kafka event payload and invokes the Lambda function through AWS CLI.

Before using it, review the script and make sure the function name and AWS region match your deployment. The current file also contains a stray `Key Highlights` line that should be removed or commented before executing it.

## Destroy

To remove the project resources:

```bash
terraform destroy
```

Because this project creates billable infrastructure, destroy the environment when it is no longer needed.
