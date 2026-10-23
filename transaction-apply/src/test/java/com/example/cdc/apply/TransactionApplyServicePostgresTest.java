package com.example.cdc.apply;

import com.example.cdc.protocol.Protocol;
import com.example.cdc.protocol.ProtocolCodec;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.example.cdc.protocol.Protocol.*;
import static org.junit.jupiter.api.Assertions.*;

class TransactionApplyServicePostgresTest {
    static ConnectionProvider connections;
    private static EmbeddedPostgres postgres;

    @BeforeAll
    static void database() throws Exception {
        String url = System.getProperty("test.postgres.url");
        if (url == null) {
            postgres = EmbeddedPostgres.builder().start();
            connections = () -> postgres.getPostgresDatabase().getConnection();
        } else {
            String user = System.getProperty("test.postgres.user", "postgres");
            connections = () -> DriverManager.getConnection(url, user, "");
        }
        SchemaMigrator.migrate(connections);
    }

    @AfterAll static void closeDatabase() throws Exception { if (postgres != null) postgres.close(); }

    @Test
    void appliesWholeTransactionOnceAndRejectsStaleSuccessor() throws Exception {
        reset();
        TransactionApplyService service = new TransactionApplyService(connections, "test-worker");
        VersionToken absent = VersionToken.absentBaseline(GENERATION);
        Mutation customer = mutation("tx1", 1, "client.customers", Map.of("id", tv("int64", "42")),
                Operation.INSERT, absent, present("tx1", 1), json("""
                        {"id":42,"first_name":"Ada","last_name":"Lovelace","email":"a@x","status":"ACTIVE",
                         "created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}
                        """), List.of());
        Mutation address = mutation("tx1", 2, "client.addresses",
                Map.of("customer_id", tv("int64", "42"), "address_type", tv("text", "HOME")),
                Operation.INSERT, absent, present("tx1", 2),
                json("{\"customer_id\":42,\"address_type\":\"HOME\",\"street\":\"Main\",\"city\":\"X\",\"state\":\"NJ\",\"zip_code\":\"1\"}"), List.of());
        stage(service, manifest("tx1", 1, List.of(customer, address)), List.of(address, customer));

        assertTrue(service.applyNext().orElseThrow().applied());
        assertEquals(1, count("client.customers"));
        assertEquals(1, count("client.addresses"));
        assertEquals(1, count("cdc.applied_transaction"));
        assertEquals(TransactionApplyService.StageResult.DUPLICATE,
                service.stage(MUTATION_TOPIC, 0, 99, ProtocolCodec.json(customer)));
        assertTrue(service.applyNext().isEmpty());

        Mutation update = mutation("tx2", 1, "client.customers", Map.of("id", tv("int64", "42")),
                Operation.UPDATE, present("tx1", 1), present("tx2", 1),
                json("{\"id\":42,\"first_name\":\"Ada\",\"last_name\":\"Byron\",\"email\":\"a@x\",\"status\":\"ACTIVE\",\"created_at\":\"2026-01-01T00:00:00Z\",\"updated_at\":\"2026-01-02T00:00:00Z\"}"),
                List.of(new Dependency(DependencyType.WRITE_PREDECESSOR, canonical("tx1"),
                        object("client.customers", Map.of("id", tv("int64", "42"))), present("tx1", 1), "previous")));
        stage(service, manifest("tx2", 2, List.of(update)), List.of(update));
        assertTrue(service.applyNext().orElseThrow().applied());

        Mutation stale = mutation("tx3", 1, "client.customers", Map.of("id", tv("int64", "42")),
                Operation.UPDATE, present("tx1", 1), present("tx3", 1), update.after(), List.of());
        stage(service, manifest("tx3", 3, List.of(stale)), List.of(stale));
        var failed = service.applyNext().orElseThrow();
        assertFalse(failed.applied());
        assertTrue(failed.detail().contains("expected"));
        assertEquals("FAILED", scalar("SELECT state FROM cdc.tx_transaction WHERE transaction_id='" + canonical("tx3") + "'"));
        assertEquals("Byron", scalar("SELECT last_name FROM client.customers WHERE id=42"));
    }

    @Test
    void databaseFailureRollsBackEveryBusinessAndVersionEffect() throws Exception {
        reset();
        TransactionApplyService service = new TransactionApplyService(connections, "rollback-worker");
        VersionToken absent = VersionToken.absentBaseline(GENERATION);
        Mutation customer = mutation("rollback", 1, "client.customers", Map.of("id", tv("int64", "7")),
                Operation.INSERT, absent, present("rollback", 1),
                json("{\"id\":7,\"first_name\":\"A\",\"last_name\":\"B\"}"), List.of());
        Mutation invalidChild = mutation("rollback", 2, "client.addresses",
                Map.of("customer_id", tv("int64", "999"), "address_type", tv("text", "HOME")),
                Operation.INSERT, absent, present("rollback", 2),
                json("{\"customer_id\":999,\"address_type\":\"HOME\",\"street\":\"X\"}"), List.of());
        stage(service, manifest("rollback", 1, List.of(customer, invalidChild)), List.of(customer, invalidChild));

        assertThrows(java.sql.SQLException.class, service::applyNext);
        assertEquals(0, count("client.customers"));
        assertEquals(0, count("client.addresses"));
        assertEquals(0, count("cdc.applied_transaction"));
        assertEquals(0, count("cdc.version_history"));
    }

    @Test
    void independentReadyTransactionsCanBeClaimedByTwoWorkers() throws Exception {
        reset();
        TransactionApplyService staging = new TransactionApplyService(connections, "staging");
        VersionToken absent = VersionToken.absentBaseline(GENERATION);
        Mutation first = mutation("parallel-a", 1, "client.customers", Map.of("id", tv("int64", "1")),
                Operation.INSERT, absent, present("parallel-a", 1),
                json("{\"id\":1,\"first_name\":\"A\",\"last_name\":\"One\"}"), List.of());
        Mutation second = mutation("parallel-b", 1, "client.customers", Map.of("id", tv("int64", "2")),
                Operation.INSERT, absent, present("parallel-b", 1),
                json("{\"id\":2,\"first_name\":\"B\",\"last_name\":\"Two\"}"), List.of());
        stage(staging, manifest("parallel-a", 1, List.of(first)), List.of(first));
        stage(staging, manifest("parallel-b", 2, List.of(second)), List.of(second));

        var workers = Executors.newFixedThreadPool(2);
        try {
            var a = workers.submit(() -> new TransactionApplyService(connections, "worker-a").applyNext().orElseThrow());
            var b = workers.submit(() -> new TransactionApplyService(connections, "worker-b").applyNext().orElseThrow());
            assertTrue(a.get().applied());
            assertTrue(b.get().applied());
        } finally {
            workers.shutdownNow();
        }
        assertEquals(2, count("client.customers"));
        assertEquals(2, count("cdc.applied_transaction"));
    }

    static void stage(TransactionApplyService service, ControlMessage manifest, List<Mutation> mutations) throws Exception {
        long offset = 0;
        for (Mutation mutation : mutations) service.stage(MUTATION_TOPIC, 0, offset++, ProtocolCodec.json(mutation));
        service.stage(CONTROL_TOPIC, 0, offset, ProtocolCodec.json(manifest));
    }

    static Mutation mutation(String nativeTx, long sequence, String entity,
                                     Map<String, TypedValue> key, Operation operation,
                                     VersionToken expected, VersionToken next, JsonNode after,
                                     List<Dependency> dependencies) {
        String tx = canonical(nativeTx);
        ObjectIdentity object = object(entity, key);
        String payloadHash = ProtocolCodec.payloadHash(null, after);
        String id = ProtocolCodec.sha256(Map.of("tx", tx, "sequence", sequence, "object", object, "payload", payloadHash));
        return new Mutation(VERSION, tx, "db", "inc", "scope", GENERATION, 1, sequence, id, object,
                operation, List.of("event-" + sequence), List.of(sequence),
                List.of(new SourcePosition("POSTGRES_LSN", "0/" + sequence)), expected, expected, next,
                dependencies, id, payloadHash, null, operation == Operation.DELETE ? null : after);
    }

    static ControlMessage manifest(String nativeTx, long commit, List<Mutation> mutations) {
        String setHash = ProtocolCodec.mutationSetHash(mutations);
        return new ControlMessage(VERSION, ControlType.MANIFEST, canonical(nativeTx), "db", "inc", "scope",
                nativeTx, SourceStatus.COMMITTED, "POSTGRES_LSN", "0/" + commit, commit, GENERATION,
                1, 1, null, mutations.size(), Map.of("inventory.test", mutations.size()), mutations.size(),
                setHash, "manifest-" + nativeTx, 1, 600001, null);
    }

    static VersionToken present(String nativeTx, long sequence) {
        return new VersionToken(GENERATION, canonical(nativeTx), sequence, Presence.PRESENT);
    }
    static String canonical(String nativeTx) { return ProtocolCodec.canonicalTransactionId("db", "inc", "scope", nativeTx); }
    static TypedValue tv(String type, String value) { return new TypedValue(type, value); }
    static ObjectIdentity object(String entity, Map<String, TypedValue> key) { return new ObjectIdentity(entity, key); }
    static JsonNode json(String value) { try { return ProtocolCodec.mapper().readTree(value); } catch (Exception e) { throw new IllegalArgumentException(e); } }

    static void reset() throws Exception {
        try (var c = connections.open(); var s = c.createStatement()) {
            s.execute("TRUNCATE cdc.quarantine,cdc.outbox,cdc.apply_attempt,cdc.applied_transaction,cdc.version_history,cdc.object_state,cdc.dependency,cdc.mutation,cdc.tx_transaction,cdc.inbox_message,operational.order_items,operational.orders,operational.products,operational.contact_numbers,client.addresses,client.customers RESTART IDENTITY CASCADE");
        }
    }
    static int count(String table) throws Exception { return Integer.parseInt(scalar("SELECT count(*) FROM " + table)); }
    static String scalar(String sql) throws Exception { try (var c=connections.open();var s=c.createStatement();var rs=s.executeQuery(sql)){assertTrue(rs.next());return rs.getString(1);} }
}
