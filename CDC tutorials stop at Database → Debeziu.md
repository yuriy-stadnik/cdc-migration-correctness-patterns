From CDC to microservice-ready databases

Architecture overview: decomposing a monolith while it is still running

CDC tutorials often stop at:

Database → Debezium → Kafka

Real modernization starts there.

The architecture I built follows this path:

Legacy PostgreSQL
→ WAL / logical replication
→ Debezium
→ Ordered CDC capture in Kafka
→ Flink transaction validation and direct row mapping
→ Immutable transaction topics in Kafka
→ MirrorMaker2
→ Amazon MSK Serverless
→ AWS Lambda
→ Aurora PostgreSQL with client and operational schemas

The goal is not row-copying.

It's decomposing a monolith into domain-aligned schemas while the source system is still running. The normalized example gives every destination row one source owner with the same primary key.

That's a microservice-readiness pattern, not just a migration.

Concrete example:

inventory.addresses
→ client.addresses

inventory.contact_numbers
→ operational.contact_numbers

The same pipeline supports multiple modes:
• Migration – domain-shaped data into cloud databases
• Reporting – real-time read models, zero monolith load
• Integration – governed Kafka topics for third parties
• Canary – selected users read from new database first
• Blue/green – switch read path after validation

One design rule: every event preserves source metadata (operation type, timestamp, transaction ID, ordering). The destination is usable for audit and reconciliation, not just reads.

CDC does not replace governance. The pipeline is the transport layer. Governance is what makes it trustworthy.

Runs locally in Docker Compose. Deploys to AWS. Same code, same tests.

Link in first comment.

Question: What usually breaks first in production — schema changes, transaction consistency, reconciliation timeout, or something else?

https://github.com/yuriy-stadnik/cdc-migration-correctness-patterns

#Kafka #DataEngineering #CloudMigration #Debezium #Microservices


Comment #1
GitHub repo:
https://github.com/yuriy-stadnik/cdc-migration-correctness-patterns

Local Docker Compose deployment:
https://github.com/yuriy-stadnik/cdc-migration-correctness-patterns/tree/main/local

The repository includes both deployment paths:

• Docker Compose deployment for local development and repeatable testing
• AWS deployment using MSK Serverless, AWS Lambda, and Aurora PostgreSQL
• Component descriptions and deployment notes for both environments
• CDC metadata preservation for audit and reconciliation
• Flink-based validation and transaction-preserving direct row mapping

The goal is not just to demonstrate Debezium or Kafka.

The goal is to show a practical modernization pattern: keep the monolith running, transform legacy CDC events into service-aligned domains, validate correctness, and prepare for gradual migration to microservice-owned databases.

Follow-up posts will go deeper into transaction metadata, schema governance, reconciliation, and failure handling.
