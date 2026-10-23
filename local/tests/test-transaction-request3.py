#!/usr/bin/env python3
"""Non-destructive local-stack probe. Creates uniquely named test rows; retains evidence.

Requires the two compose stacks already running. Does not reset volumes, offsets, or ledgers.
Uses only Python's standard library and Docker CLI. Every assertion observes the real DB.
"""
import json
import subprocess
import time


def run(*args):
    return subprocess.check_output(args, text=True, timeout=30).strip()


def destination(sql):
    return run("docker", "exec", "local-cloud-destination-postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1",
               "-U", "appuser", "-d", "destinationdb", "-tAc", sql)


def source(sql):
    return run("docker", "exec", "local-source-postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1",
               "-U", "postgres", "-d", "appdb", "-tAc", sql)


def wait_for(description, check, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.25)
    raise AssertionError("Timed out: " + description)


def main():
    row_id = time.time_ns() // 1_000_000 % 1_000_000_000
    product = "Request3-" + str(row_id)
    print(json.dumps({"fixture_id": row_id, "product": product}), flush=True)
    source(f"""BEGIN;
      INSERT INTO inventory.customers(id,first_name,last_name,email,status)
        VALUES({row_id},'Before','Request3','r3-{row_id}@example.test','ACTIVE');
      INSERT INTO inventory.accounts(account_id,customer_id,account_type,balance,street,city,state,zip_code,home_phone,mobile_phone)
        VALUES({row_id},{row_id},'CHECKING',10,'Before','Test','NJ','00000','111','333');
      COMMIT;""")
    wait_for("predecessor transaction", lambda: destination(
        f"SELECT count(*) FROM client.customers c JOIN cdc.applied_transaction a ON a.transaction_id=c.source_tx_id WHERE c.id={row_id}") == "1")
    source(f"""BEGIN;
      UPDATE inventory.customers SET first_name='After',updated_at=now() WHERE id={row_id};
      UPDATE inventory.accounts SET street='After' WHERE account_id={row_id};
      INSERT INTO inventory.orders_flat(order_id,customer_id,product_name,product_category,unit_price,quantity,total_price)
        VALUES({row_id},{row_id},'{product}','Request3',12.50,2,25); COMMIT;""")
    observations = set()

    def observe():
        # One statement, hence one PostgreSQL read snapshot across all seven mutation effects.
        value = destination(f"""SELECT
          (SELECT count(*) FROM client.customers WHERE id={row_id} AND first_name='After') +
          (SELECT count(*) FROM client.addresses WHERE customer_id={row_id} AND street='After') +
          (SELECT count(*) FROM operational.orders WHERE id={row_id}) +
          (SELECT count(*) FROM operational.products WHERE name='{product}') +
          (SELECT count(*) FROM operational.order_items WHERE order_id={row_id}) +
          (SELECT count(*) FROM operational.contact_numbers WHERE customer_id={row_id} AND source_tx_id=(SELECT source_tx_id FROM operational.orders WHERE id={row_id}))""")
        observations.add(int(value))
        assert value in ("0", "7"), f"Partially visible transaction: {value}/7"
        return value == "7"

    try:
        wait_for("atomic seven-mutation commit", observe)
    finally:
        print(json.dumps({"visibility_counts": sorted(observations)}), flush=True)
    tx = destination(f"SELECT source_tx_id FROM operational.orders WHERE id={row_id}")
    evidence = destination(f"""SELECT json_build_object('transaction_id',t.transaction_id,'state',t.state,
        'captured',t.captured_source_event_count,'expected',t.expected_mutation_count,'received',t.received_mutation_count,
        'receipt_mutations',a.mutation_count,'versions',(SELECT count(*) FROM cdc.version_history WHERE producer_transaction_id=t.transaction_id),
        'dependencies',(SELECT count(*) FROM cdc.dependency WHERE transaction_id=t.transaction_id))
        FROM cdc.tx_transaction t JOIN cdc.applied_transaction a USING(projection_generation,transaction_id)
        WHERE t.transaction_id='{tx}'""")
    result = json.loads(evidence)
    assert result["state"] == "APPLIED" and result["captured"] == 3
    assert all(result[k] == 7 for k in ("expected", "received", "receipt_mutations", "versions"))
    print(json.dumps(result), flush=True)
    print("PASS: source PostgreSQL -> Debezium -> Flink -> Kafka -> MM2 -> destination atomic commit", flush=True)


if __name__ == "__main__":
    main()
