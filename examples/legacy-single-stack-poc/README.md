# Legacy Single-Stack POC

This example contains the original one-file Docker Compose proof of concept for the local CDC path:

```text
legacy PostgreSQL -> Debezium -> Kafka -> Flink -> target PostgreSQL
```

Run it from the repository root:

```bash
docker compose -f examples/legacy-single-stack-poc/docker-compose.yml up -d
```

The compose file reuses shared assets from `local/`, including PostgreSQL init SQL, Debezium connector config, Flink SQL, and connector JARs.

Stop it with:

```bash
docker compose -f examples/legacy-single-stack-poc/docker-compose.yml down
```
