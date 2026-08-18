# AWS Operational Access Scripts

These scripts inspect Kafka topics, MSK topics, Aurora tables, and source PostgreSQL data through the deployed EC2 instance.

They avoid direct local connections to MSK and Aurora. Local commands use Terraform outputs, EC2 Instance Connect, and SSH to run the actual database or Kafka clients inside the VPC from the EC2 host.

## Defaults

```text
AWS_REGION=us-east-1
KEY_PATH=$HOME/.ssh/temp_ec2_key
CLIENT_DB_NAME=clientdb
OPERATIONAL_DB_NAME=operationaldb
```

Override any value with environment variables:

```bash
AWS_REGION=us-east-1 KEY_PATH=~/.ssh/temp_ec2_key scripts/ops/msk/list-topics.sh
```

## Processes

- `source-postgres/`: commands for the source PostgreSQL container on EC2.
- `source-kafka/`: commands for the source Kafka container on EC2.
- `msk/`: commands for MSK topics through the MirrorMaker2 container on EC2.
- `aurora-client/`: commands for the client Aurora database through EC2.
- `aurora-operational/`: commands for the operational Aurora database through EC2.
- `containers/`: EC2 Docker process inspection.

## Examples

```bash
scripts/ops/source-postgres/list-tables.sh
scripts/ops/source-kafka/list-topics.sh
scripts/ops/msk/consume-topic.sh operational.contact_numbers 20
scripts/ops/aurora-client/list-tables.sh
scripts/ops/aurora-operational/query.sh "SELECT count(*) FROM operational.orders;"
scripts/ops/containers/status.sh
```

Aurora scripts retrieve the database password from AWS Secrets Manager locally and pass it to the remote shell through SSH stdin, not as a command-line argument.
