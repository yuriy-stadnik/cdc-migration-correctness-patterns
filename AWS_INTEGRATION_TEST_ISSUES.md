# AWS Integration Test Issues and Resolutions

Date: 2026-08-17

This file documents the issues found during the last AWS integration test run and the changes made to resolve them.

## Final Result

The AWS integration test completed successfully after deployment fixes:

```text
REMOTE_FULL_E2E_OK CID=787022811 OID=987022811 EMAIL=aws-e2e-787022811@example.com
```

Verified checks:

- Source PostgreSQL CDC events reached local Kafka.
- Events were mirrored from local Kafka to MSK.
- Lambda consumed MSK events and wrote destination Aurora rows.
- `client.customers`, `client.addresses`, `operational.orders`, and `operational.order_items` each reached the expected count.
- `operational.contact_numbers` reached the expected count of 2 rows.
- Source metadata columns were written to Aurora.
- Client and operational transaction metadata tables had expected rows.

## Issue 1: Terraform State Was Stale or Empty

### Symptom

The active `terraform.tfstate` did not describe the deployed AWS resources. The local state was effectively empty, while `terraform.tfstate.backup` still contained older resources.

### Impact

Terraform could not accurately reason about the existing AWS stack from the active state file. AWS discovery also showed that the resources referenced by the backup state were no longer present, so the backup state was stale.

### Resolution

The backup state was restored as the active state only as a starting point, then Terraform was allowed to reconcile the real AWS environment. Because the referenced resources no longer existed, Terraform recreated the stack.

The resulting active deployment produced current outputs, including:

```text
MSK bootstrap: boot-yuzwzvfx.c1.kafka-serverless.us-east-1.amazonaws.com:9098
EC2 instance: i-0e4e02670817d01a7
EC2 public IP: 54.173.168.236
```

## Issue 2: MirrorMaker2 Could Not Write Transactionally to MSK

### Symptom

MirrorMaker2 logs showed transactional authorization failures when writing to MSK:

```text
TransactionalIdAuthorizationException
```

### Impact

Topics and records were not mirrored reliably from the local source Kafka cluster into MSK. The integration test failed while waiting for mirrored MSK records.

### Root Cause

The EC2 IAM role policy allowed normal cluster/topic access, but it was missing permissions required for idempotent and transactional Kafka writes against MSK IAM auth.

### Resolution

The EC2 IAM policy in `ec2.tf` was updated to include the missing MSK permissions:

```text
kafka-cluster:WriteDataIdempotently
kafka-cluster:DescribeTransactionalId
kafka-cluster:AlterTransactionalId
```

Terraform was applied, and MirrorMaker2 was restarted through EC2 replacement so the updated policy and configuration were used.

## Issue 3: MSK Serverless Rejected Topic Config Sync

### Symptom

MirrorMaker2 attempted to synchronize topic configuration from source Kafka to MSK. MSK Serverless rejected some source topic configuration changes.

### Impact

MirrorMaker2 produced errors while trying to alter unsupported topic configuration on MSK Serverless.

### Root Cause

MSK Serverless does not allow all topic-level configuration changes that MirrorMaker2 may attempt to copy from the source cluster.

### Resolution

MirrorMaker2 cloud configuration in `user_data.sh.tpl` was changed to disable syncing topic configs and ACLs:

```text
source->target.sync.topic.configs.enabled = false
source->target.sync.topic.acls.enabled = false
```

## Issue 4: Only One Contact Number Was Committed

### Symptom

The integration test timed out waiting for destination Aurora contact numbers:

```text
operational.contact_numbers: timeout after 300s (last=1, expected=2)
```

Source Kafka with `read_uncommitted` showed both contact events, but `read_committed` showed only one event.

### Impact

Only the `HOME` contact number reached MSK and Aurora. The `MOBILE` contact number remained invisible to `read_committed` consumers, so the exactly-once pipeline did not deliver both expected destination rows.

### Root Cause

The cloud Flink SQL in `user_data.sh.tpl` used separate insert jobs for the contact-number outputs. One of the transactional outputs was not becoming visible to `read_committed` consumers in the integration flow.

The local pipeline had already solved this by using a single `INSERT ... UNION ALL` job for contact numbers.

### Resolution

The cloud Flink contact-number SQL was changed to match the local pipeline pattern: one committed insert job with `UNION ALL` branches for all contact-number records.

This made both contact records visible to `read_committed` consumers and allowed both rows to reach Aurora.

## Validation After Fixes

After the fixes, the EC2 instance was replaced so the updated `user_data.sh.tpl` configuration was applied from a clean boot.

The final AWS E2E command completed successfully:

```text
./scripts/run-aws-e2e-test.sh us-east-1 ~/.ssh/temp_ec2_key
```

Final test output:

```text
client.customers: ok (1)
client.addresses: ok (1)
operational.orders: ok (1)
operational.order_items: ok (1)
operational.contact_numbers: ok (2)
client.customers source metadata: ok (1)
operational.orders source metadata: ok (1)
client tx metadata rows: ok (6 >= 2)
operational tx metadata rows: ok (6 >= 2)
REMOTE_FULL_E2E_OK CID=787022811 OID=987022811 EMAIL=aws-e2e-787022811@example.com
Done.
```

Additional validation:

```text
terraform validate
git diff --check
```

Both checks passed.
