#!/usr/bin/env python3
"""Generate real Debezium 2.7 capture in disposable containers; never uses shared services.
Run: python3 local/tests/test-projector-v2-capture.py /tmp/projector-v2-capture.jsonl
Then: V2_CAPTURE_FIXTURE=/tmp/projector-v2-capture.jsonl gradle test --tests '*V2CapturedEnvelopeTest'
"""
import json
import pathlib
import subprocess
import sys
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[2]
PREFIX = 'projector-v2-' + uuid.uuid4().hex[:10]
NAMES = []


def run(*args, input=None, timeout=90):
    p = subprocess.run(args, input=input, text=True, capture_output=True, timeout=timeout)
    if p.returncode:
        raise RuntimeError(f'{args[:4]}: {p.stderr[-3000:]} {p.stdout[-1000:]}')
    return p.stdout


def container(suffix, image, *args, env=()):
    name = PREFIX + '-' + suffix
    options = [x for e in env for x in ('-e', e)]
    NAMES.append(name)
    run('docker', 'run', '-d', '--name', name, '--network', PREFIX,
        '--network-alias', suffix, *options, image, *args)
    return name


def wait(action, seconds=100):
    end = time.monotonic() + seconds
    while True:
        try:
            return action()
        except Exception:
            if time.monotonic() >= end:
                raise
            time.sleep(2)


def sql(pg, statement):
    return run('docker', 'exec', '-i', pg, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
               '-U', 'postgres', '-d', 'appdb', input=statement)


def main():
    output = pathlib.Path(sys.argv[1]).resolve()
    if output.exists():
        raise ValueError('Refusing to overwrite existing fixture')
    run('docker', 'network', 'create', PREFIX)
    try:
        pg = container('pg', 'postgres:16-alpine', 'postgres', '-c', 'wal_level=logical',
                       env=('POSTGRES_PASSWORD=postgres', 'POSTGRES_DB=appdb'))
        kafka = container('kafka', 'apache/kafka:3.9.2', env=(
            'KAFKA_NODE_ID=1', 'KAFKA_PROCESS_ROLES=broker,controller',
            'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093',
            'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092',
            'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
            'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093',
            'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1',
            'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1', 'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0'))
        wait(lambda: sql(pg, 'SELECT 1;'))
        for name in ('01-role-and-grants.sql', '02-inventory.sql', '03-source-version-v2.sql'):
            sql(pg, (ROOT / 'local/postgres/init' / name).read_text())
        wait(lambda: run('docker','exec',kafka,'/opt/kafka/bin/kafka-topics.sh',
                         '--bootstrap-server','kafka:9092','--list',timeout=15))
        connect = container('connect', 'debezium/connect:2.7.3.Final', env=(
            'BOOTSTRAP_SERVERS=kafka:9092', 'GROUP_ID=fixture', 'CONFIG_STORAGE_TOPIC=fixture-config',
            'OFFSET_STORAGE_TOPIC=fixture-offset', 'STATUS_STORAGE_TOPIC=fixture-status',
            'CONFIG_STORAGE_REPLICATION_FACTOR=1', 'OFFSET_STORAGE_REPLICATION_FACTOR=1',
            'STATUS_STORAGE_REPLICATION_FACTOR=1', 'CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY=All'))
        wait(lambda: run('docker','exec',connect,'curl','-fsS','http://localhost:8083/connectors',timeout=10))
        config=json.loads((ROOT/'local/source/connectors/source-version-v2-config.json').read_text())
        config['database.hostname']='pg'
        run('docker','exec','-i',connect,'curl','-fsS','-X','POST','-H','Content-Type: application/json',
            '--data-binary','@-','http://localhost:8083/connectors',input=json.dumps({'name':'fixture','config':config}))
        def connector_ready():
            status=json.loads(run('docker','exec',connect,'curl','-fsS','http://localhost:8083/connectors/fixture/status',timeout=10))
            if status.get('connector',{}).get('state')!='RUNNING' or any(t.get('state')!='RUNNING' for t in status.get('tasks',[])):
                raise RuntimeError('connector not running')
        wait(connector_ready)
        sql(pg, """SET ROLE source_writer;
BEGIN; SELECT source_cdc.begin_write();
INSERT INTO inventory.customers(id,first_name,last_name) VALUES(1,'Initial','Customer');
INSERT INTO inventory.products(name,category,current_price) VALUES('USB-C Cable','Electronics',25);
INSERT INTO inventory.orders(id,customer_id,status) VALUES(5002,1,'OPEN');
INSERT INTO inventory.order_items(order_id,product_name,quantity,price_at_purchase) VALUES(5002,'USB-C Cable',2,25);
COMMIT;
BEGIN; SELECT source_cdc.begin_write();
UPDATE inventory.customers SET first_name='Updated' WHERE id=1;
UPDATE inventory.customers SET first_name='Twice' WHERE id=1;
UPDATE inventory.order_items SET quantity=3 WHERE order_id=5002 AND product_name='USB-C Cable';
COMMIT;
BEGIN; SELECT source_cdc.begin_write(); UPDATE inventory.customers SET first_name='ROLLBACK' WHERE id=1; ROLLBACK;
BEGIN; SELECT source_cdc.begin_write(); INSERT INTO inventory.customers(id,first_name,last_name) VALUES(99,'New','Customer'); COMMIT;
BEGIN; SELECT source_cdc.begin_write(); DELETE FROM inventory.customers WHERE id=99; COMMIT;
BEGIN; SELECT source_cdc.begin_write(); INSERT INTO inventory.customers(id,first_name,last_name) VALUES(99,'Returned','Customer'); COMMIT;
""")
        def captured():
            data=consume(kafka)
            rows=[json.loads(line) for line in data.splitlines() if line.startswith('{')]
            if sum(r.get('status')=='END' for r in rows)!=5:
                raise RuntimeError('waiting for five commits')
            return rows
        rows=wait(captured)
        output.write_text(''.join(json.dumps(r,separators=(',',':'))+'\n' for r in rows))
        print(f'PASS: captured {len(rows)} records, five committed transactions, fixture {output}')
    finally:
        for name in reversed(NAMES):
            subprocess.run(['docker','rm','-f','-v',name],capture_output=True)
        subprocess.run(['docker','network','rm',PREFIX],capture_output=True)


def consume(kafka):
    p=subprocess.run(['docker','exec',kafka,'/opt/kafka/bin/kafka-console-consumer.sh',
                      '--bootstrap-server','kafka:9092','--topic','cdc.source.capture.v2',
                      '--from-beginning','--timeout-ms','3000'],text=True,capture_output=True,timeout=20)
    # Kafka's timeout exit is expected; parse and assert content at the caller.
    return p.stdout


if __name__=='__main__':
    main()
