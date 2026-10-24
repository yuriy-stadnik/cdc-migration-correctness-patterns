#!/usr/bin/env python3
"""Exercise the normalized source-owned version contract in disposable PostgreSQL."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import os
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
NAME = f"source-v2-it-{os.getpid()}"


def docker(*args, input_text=None, check=True):
    result = subprocess.run(
        ["docker", *args], input=input_text, text=True, capture_output=True, timeout=45
    )
    if check and result.returncode:
        raise AssertionError(
            f"docker {args[:3]} failed ({result.returncode}):\n{result.stdout}\n{result.stderr}"
        )
    return result


def psql(sql, *, app=False, expect_error=None):
    args = ["exec", "-i"]
    if app:
        args += ["-e", "PGPASSWORD=source_app"]
    args += [NAME, "psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
             "-U", "source_app" if app else "postgres", "-d", "appdb"]
    result = docker(*args, input_text=sql, check=False)
    if expect_error:
        assert result.returncode != 0 and expect_error in result.stderr, (
            f"expected {expect_error!r}, got {result.returncode}: {result.stderr}"
        )
    elif result.returncode:
        raise AssertionError(f"SQL failed:\n{sql}\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def query(sql):
    return psql(sql).splitlines()[-1]


def write(sql):
    return psql("BEGIN;\nSELECT source_cdc.begin_write();\n" + sql + "\nCOMMIT;", app=True)


def eq(sql, expected):
    actual = query(sql)
    assert actual == str(expected), f"{sql}: expected {expected!r}, got {actual!r}"


def initialize():
    docker("run", "-d", "--rm", "--name", NAME, "-e", "POSTGRES_PASSWORD=postgres",
           "-e", "POSTGRES_DB=appdb", "postgres:16-alpine", "-c", "wal_level=logical")
    for _ in range(60):
        ready = docker("exec", NAME, "psql", "-X", "-U", "postgres", "-d", "appdb",
                       "-Atqc", "SELECT 1", check=False)
        if ready.returncode == 0 and ready.stdout.strip() == "1":
            break
        time.sleep(0.2)
    else:
        raise AssertionError("isolated PostgreSQL did not start")
    for filename in ("01-role-and-grants.sql", "02-inventory.sql", "03-source-version-v2.sql"):
        psql((ROOT / "local" / "postgres" / "init" / filename).read_text())


def test_normalized_schema_and_capture_contract():
    tables = set(psql("SELECT table_name FROM information_schema.tables "
                      "WHERE table_schema='inventory' ORDER BY table_name").splitlines())
    assert tables == {"customers", "products", "orders", "order_items"}
    eq("SELECT count(*) FROM inventory.customers", 0)
    assert query("SELECT to_regnamespace('source_projection') IS NULL") == "t"
    assert query("SELECT to_regclass('source_cdc.dirty_owner') IS NULL") == "t"
    assert query("SELECT to_regclass('source_cdc.contribution') IS NULL") == "t"
    assert "pg_advisory" not in query(
        "SELECT pg_get_functiondef('source_cdc.begin_write()'::regprocedure)"
    )

    config = json.loads((ROOT / "local/source/connectors/source-version-v2-config.json").read_text())
    assert config["provide.transaction.metadata"] == "true"
    assert config["tombstones.on.delete"] == "false"
    assert config["transforms.route.replacement"] == "cdc.source.capture.v2"
    assert config["snapshot.mode"] == "no_data"
    configured = set(config["table.include.list"].split(","))
    published = set(psql("SELECT schemaname||'.'||tablename FROM pg_publication_tables "
                         "WHERE pubname='source_cdc_v2'").splitlines())
    assert configured == published, (configured - published, published - configured)
    eq("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
       "WHERE n.nspname='inventory' AND c.relname IN "
       "('customers','products','orders','order_items') "
       "AND c.relreplident='f'", 4)
    eq("SELECT count(*) FROM source_cdc.schema_version WHERE version=4", 1)


def test_permissions_and_rollback():
    psql("INSERT INTO inventory.customers(id,first_name,last_name) VALUES(901,'A','B')",
         app=True, expect_error="call source_cdc.begin_write()")
    psql("TRUNCATE inventory.customers", app=True, expect_error="permission denied")
    psql("BEGIN; SELECT source_cdc.begin_write(); "
         "INSERT INTO inventory.customers(id,first_name,last_name) VALUES(901,'A','B'); ROLLBACK;",
         app=True)
    eq("SELECT count(*) FROM inventory.customers WHERE id=901", 0)
    eq("SELECT count(*) FROM source_cdc.owner_state WHERE collection='inventory.customers' "
       "AND owner_key=jsonb_build_object('id',901)::text", 0)


def test_versions_and_lifecycle():
    write("INSERT INTO inventory.customers(id,first_name,last_name,version) VALUES(910,'A','B',999);\n"
          "UPDATE inventory.customers SET first_name='A',version=999 WHERE id=910;\n"
          "UPDATE inventory.customers SET first_name='C' WHERE id=910;")
    eq("SELECT version FROM inventory.customers WHERE id=910", 3)
    eq("SELECT string_agg(after_version::text,',' ORDER BY ordinal) FROM source_cdc.revision_journal "
       "WHERE collection='inventory.customers' AND owner_key=jsonb_build_object('id',910)::text", "1,2,3")
    eq("SELECT count(DISTINCT source_transaction_id) FROM source_cdc.revision_journal "
       "WHERE collection='inventory.customers' AND owner_key=jsonb_build_object('id',910)::text", 1)
    eq("SELECT count(*) FROM source_cdc.transaction_context c WHERE c.journal_count <> "
       "(SELECT count(*) FROM source_cdc.revision_journal j "
       "WHERE j.source_transaction_id=c.source_transaction_id)", 0)

    write("DELETE FROM inventory.customers WHERE id=910;")
    eq("SELECT version||':'||presence FROM source_cdc.owner_state "
       "WHERE collection='inventory.customers' AND owner_key=jsonb_build_object('id',910)::text", "4:DELETED")
    write("INSERT INTO inventory.customers(id,first_name,last_name) VALUES(910,'D','B');")
    eq("SELECT version FROM inventory.customers WHERE id=910", 5)
    psql("BEGIN; SELECT source_cdc.begin_write(); UPDATE inventory.customers SET id=911 WHERE id=910; COMMIT;",
         app=True, expect_error="primary-key changes are not supported")
    eq("SELECT version FROM inventory.customers WHERE id=910", 5)


def test_each_source_row_owns_one_destination_row():
    write("""
INSERT INTO inventory.customers(id,first_name,last_name) VALUES(940,'Parent','One');
INSERT INTO inventory.products(name,category,current_price) VALUES('v2-product','Tools',10);
INSERT INTO inventory.orders(id,customer_id,status) VALUES(950,940,'OPEN');
INSERT INTO inventory.order_items(order_id,product_name,quantity,price_at_purchase)
  VALUES(950,'v2-product',2,10);
""")
    tx = "(SELECT last_source_transaction_id FROM source_cdc.owner_state " \
         "WHERE collection='inventory.customers' AND owner_key=jsonb_build_object('id',940)::text)"
    eq(f"SELECT count(*) FROM source_cdc.revision_journal WHERE source_transaction_id={tx}", 4)
    eq(f"SELECT count(DISTINCT collection||'|'||owner_key) FROM source_cdc.revision_journal "
       f"WHERE source_transaction_id={tx}", 4)
    eq("SELECT version FROM inventory.order_items WHERE order_id=950 AND product_name='v2-product'", 1)
    write("UPDATE inventory.order_items SET quantity=3 WHERE order_id=950 AND product_name='v2-product';")
    eq("SELECT version FROM inventory.order_items WHERE order_id=950 AND product_name='v2-product'", 2)


def test_logical_capture_and_rollback():
    psql("SELECT slot_name FROM pg_create_logical_replication_slot('source_v2_it_slot','test_decoding')")
    write("INSERT INTO inventory.customers(id,first_name,last_name) VALUES(980,'Wal','Test');\n"
          "UPDATE inventory.customers SET first_name='Next' WHERE id=980;\n"
          "DELETE FROM inventory.customers WHERE id=980;")
    psql("BEGIN; SELECT source_cdc.begin_write(); "
         "INSERT INTO inventory.customers(id,first_name,last_name) VALUES(981,'Rollback','Only'); ROLLBACK;",
         app=True)
    decoded = psql("SELECT data FROM pg_logical_slot_get_changes('source_v2_it_slot',NULL,NULL)")
    row_events = [line for line in decoded.splitlines() if line.startswith("table inventory.customers:")]
    assert len(row_events) == 3, row_events
    assert "version[bigint]:1" in row_events[0]
    assert "version[bigint]:2" in row_events[1]
    assert "version[bigint]:2" in row_events[2]
    assert "after_presence[text]:'DELETED'" in decoded
    assert "981" not in decoded
    psql("SELECT pg_drop_replication_slot('source_v2_it_slot')")


def test_overflow_concurrency_and_restart():
    write("INSERT INTO inventory.customers(id,first_name,last_name) VALUES(960,'X','Y');")
    psql("ALTER TABLE inventory.customers DISABLE TRIGGER ALL; "
         "UPDATE inventory.customers SET version=9223372036854775807 WHERE id=960; "
         "ALTER TABLE inventory.customers ENABLE TRIGGER ALL; "
         "UPDATE source_cdc.owner_state SET version=9223372036854775807 "
         "WHERE collection='inventory.customers' AND owner_key=jsonb_build_object('id',960)::text;")
    psql("BEGIN; SELECT source_cdc.begin_write(); UPDATE inventory.customers SET first_name='Z' "
         "WHERE id=960; COMMIT;", app=True, expect_error="owner version overflow")

    write("INSERT INTO inventory.customers(id,first_name,last_name) VALUES(970,'X','Y'),(971,'X','Y');")
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(write, "UPDATE inventory.customers SET first_name='First' WHERE id=970;"),
            pool.submit(write, "UPDATE inventory.customers SET first_name='Second' WHERE id=971;"),
        ]
        for future in futures:
            future.result()
    eq("SELECT string_agg(version::text,',' ORDER BY id) FROM inventory.customers WHERE id IN (970,971)", "2,2")
    docker("restart", NAME)
    for _ in range(60):
        if docker("exec", NAME, "pg_isready", "-U", "postgres", "-d", "appdb", check=False).returncode == 0:
            break
        time.sleep(0.2)
    write("UPDATE inventory.customers SET first_name='After restart' WHERE id=970;")
    eq("SELECT version FROM inventory.customers WHERE id=970", 3)


def main():
    initialize()
    tests = (
        test_normalized_schema_and_capture_contract,
        test_permissions_and_rollback,
        test_versions_and_lifecycle,
        test_each_source_row_owns_one_destination_row,
        test_logical_capture_and_rollback,
        test_overflow_concurrency_and_restart,
    )
    for test in tests:
        test()
        print("PASS", test.__name__, flush=True)


if __name__ == "__main__":
    try:
        main()
    finally:
        docker("stop", NAME, check=False)
