package com.example.cdc.apply;

import com.example.cdc.protocol.ProtocolCodec;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static com.example.cdc.protocol.Protocol.*;
import static com.example.cdc.apply.TransactionApplyServicePostgresTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real PostgreSQL tests. No production fault hooks or timing-only concurrency assertions. */
class AdversarialApplyPostgresTest {
    @BeforeAll static void start() throws Exception { database(); }
    @AfterAll static void stop() throws Exception { closeDatabase(); }
    @BeforeEach void clear() throws Exception { reset(); }
    private TransactionApplyService worker(String name) { return new TransactionApplyService(connections, name); }
    private static Mutation insert(String tx, long seq, long id) {
        return mutation(tx, seq, "client.customers", Map.of("id", tv("int64", "" + id)), Operation.INSERT,
                VersionToken.absentBaseline(GENERATION), present(tx, seq),
                json("{\"id\":" + id + ",\"first_name\":\"Test\",\"last_name\":\"" + tx + "\"}"), List.of());
    }
    private static Mutation depends(String tx, long id, List<Dependency> deps) {
        Mutation m = insert(tx, 1, id);
        return mutation(tx, 1, m.object().entity(), m.object().primaryKey(), m.operation(),
                m.expectedDestinationVersion(), m.newObjectVersion(), m.after(), deps);
    }
    private static Dependency after(String tx) { return new Dependency(DependencyType.AFTER_TRANSACTION, canonical(tx), null, null, "test"); }
    private static String state(String tx) throws Exception { return scalar("SELECT state FROM cdc.tx_transaction WHERE transaction_id='" + canonical(tx) + "'"); }
    private static String marker(ControlMessage manifest, ControlType type) {
        ObjectNode n = ProtocolCodec.mapper().valueToTree(manifest);
        n.put("messageType", type.name()); n.put("reason", "fault injection");
        return n.toString();
    }

    @Test void interleavedFragmentsNeverApplyPartialTransactionsAndReplayIsNoOp() throws Exception {
        var s = worker("interleaved");
        var a = List.of(insert("a",1,1), insert("a",2,2), insert("a",3,3));
        var b = List.of(insert("b",1,4), insert("b",2,5));
        var c = List.of(insert("c",1,6));
        for (var tx : List.of(a,b,c)) s.stage(CONTROL_TOPIC,0,0,ProtocolCodec.json(manifest(tx.get(0)==a.get(0)?"a":tx.get(0)==b.get(0)?"b":"c",1,tx)));
        for (Mutation m : List.of(a.get(0),b.get(0),c.get(0),a.get(1),b.get(1),a.get(2))) {
            s.stage(MUTATION_TOPIC,3,m.mutationSequence(),ProtocolCodec.json(m));
            s.drain(10);
            int ac = Integer.parseInt(scalar("SELECT count(*) FROM client.customers WHERE id IN (1,2,3)"));
            int bc = Integer.parseInt(scalar("SELECT count(*) FROM client.customers WHERE id IN (4,5)"));
            assertTrue(ac==0 || ac==3); assertTrue(bc==0 || bc==2);
        }
        assertEquals(6,count("client.customers")); assertEquals(3,count("cdc.applied_transaction"));
        stage(s,manifest("a",1,a),a); stage(s,manifest("b",1,b),b); stage(s,manifest("c",1,c),c);
        assertEquals(0,s.drain(10)); assertEquals(6,count("cdc.version_history"));
    }

    @Test void futureVersionWaitsForPredecessor() throws Exception {
        var s=worker("out-of-order"); var first=insert("v8",1,10);
        var next=mutation("v9",1,first.object().entity(),first.object().primaryKey(),Operation.UPDATE,
                first.newObjectVersion(),present("v9",1),json("{\"id\":10,\"first_name\":\"Nine\",\"last_name\":\"X\"}"),
                List.of(new Dependency(DependencyType.WRITE_PREDECESSOR,canonical("v8"),first.object(),first.newObjectVersion(),"v8")));
        stage(s,manifest("v9",2,List.of(next)),List.of(next));
        assertEquals("WAITING_DEPENDENCIES",state("v9")); assertTrue(s.applyNext().isEmpty());
        stage(s,manifest("v8",1,List.of(first)),List.of(first)); assertEquals(2,s.drain(10));
        assertEquals("Nine",scalar("SELECT first_name FROM client.customers WHERE id=10"));
    }

    @Test void fanInWaitsForEveryPredecessor() throws Exception {
        var s=worker("fan-in"); var fourth=depends("four",4,List.of(after("one"),after("two"),after("three")));
        stage(s,manifest("four",4,List.of(fourth)),List.of(fourth));
        int id=0;
        for(String tx:List.of("one","two","three")) {
            var m=insert(tx,1,++id); stage(s,manifest(tx,id,List.of(m)),List.of(m));
            assertTrue(s.applyNext().orElseThrow().applied());
            assertEquals(id<3?"WAITING_DEPENDENCIES":"READY",state("four"));
        }
        assertTrue(s.applyNext().orElseThrow().applied()); assertEquals(4,count("cdc.applied_transaction"));
    }

    @Test void missingFragmentWaitsAndValidLateFragmentRecoversBeforeDlq() throws Exception {
        var s=worker("late"); var a=insert("late",1,1); var b=insert("late",2,2); var h=manifest("late",1,List.of(a,b));
        stage(s,h,List.of(a)); assertTrue(s.applyNext().isEmpty()); assertEquals("COLLECTING",state("late"));
        s.stage(CONTROL_TOPIC,0,1,marker(h,ControlType.INCOMPLETE)); assertEquals("INCOMPLETE",state("late"));
        assertEquals(0,count("client.customers")); s.stage(MUTATION_TOPIC,1,2,ProtocolCodec.json(b));
        assertEquals("READY",state("late")); assertTrue(s.applyNext().orElseThrow().applied());
    }

    @Test void lateIncompleteMarkerMustNotReopenDlq() throws Exception {
        var s=worker("dlq"); var m=insert("dlq",1,1); var h=manifest("dlq",1,List.of(m));
        stage(s,h,List.of()); s.stage(DLQ_TOPIC,0,1,marker(h,ControlType.DLQ));
        s.stage(MUTATION_TOPIC,0,2,ProtocolCodec.json(m)); assertEquals("DLQ",state("dlq"));
        s.stage(CONTROL_TOPIC,0,3,marker(h,ControlType.INCOMPLETE));
        assertEquals("DLQ",state("dlq")); assertTrue(s.applyNext().isEmpty()); assertEquals(0,count("client.customers"));
    }

    @Test void jsonFormattingIsNotAConflictingDuplicate() throws Exception {
        var s=worker("replay"); var m=insert("format",1,1); stage(s,manifest("format",1,List.of(m)),List.of(m));
        String pretty=ProtocolCodec.mapper().writerWithDefaultPrettyPrinter().writeValueAsString(m);
        assertEquals(TransactionApplyService.StageResult.DUPLICATE,s.stage(MUTATION_TOPIC,11,999,pretty));
        assertEquals(0,count("cdc.quarantine"));
    }

    @Test void conflictingFragmentMustBlockUnappliedTransaction() throws Exception {
        var s=worker("conflict"); var m=insert("conflict",1,1); stage(s,manifest("conflict",1,List.of(m)),List.of(m));
        ObjectNode changed=ProtocolCodec.mapper().valueToTree(m);
        ((ObjectNode)changed.get("after")).put("first_name","tampered");
        assertEquals(TransactionApplyService.StageResult.QUARANTINED,s.stage(MUTATION_TOPIC,0,2,changed.toString()));
        assertEquals("FAILED",state("conflict")); assertTrue(s.applyNext().isEmpty());
    }

    @Test void lostCommitAcknowledgementCannotDemoteAppliedReceipt() throws Exception {
        var s=worker("stage"); var m=insert("uncertain",1,1); stage(s,manifest("uncertain",1,List.of(m)),List.of(m));
        AtomicBoolean injected=new AtomicBoolean();
        ConnectionProvider lostAck=()->{
            Connection c=connections.open();
            return (Connection)Proxy.newProxyInstance(Connection.class.getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args)->{
                try {
                    Object result=method.invoke(c,args);
                    if(method.getName().equals("commit") && injected.compareAndSet(false,true)) throw new SQLException("commit ack lost","08006");
                    return result;
                } catch(InvocationTargetException e) { throw e.getCause(); }
            });
        };
        try { new TransactionApplyService(lostAck,"lost-ack").applyNext(); } catch(SQLException expected) { assertEquals("08006",expected.getSQLState()); }
        assertTrue(injected.get()); assertEquals(1,count("client.customers")); assertEquals(1,count("cdc.applied_transaction"));
        assertEquals("APPLIED",state("uncertain")); assertEquals(0,s.drain(10));
    }

    @Test void cycleMustNotQuarantineAnUnrelatedAncestor() throws Exception {
        var s=worker("cycle"); var ancestor=insert("ancestor",1,1); stage(s,manifest("ancestor",1,List.of(ancestor)),List.of(ancestor));
        var a=depends("cycle-a",2,List.of(after("cycle-b"),after("ancestor")));
        var b=depends("cycle-b",3,List.of(after("cycle-a")));
        stage(s,manifest("cycle-a",2,List.of(a)),List.of(a)); stage(s,manifest("cycle-b",3,List.of(b)),List.of(b));
        assertEquals("FAILED",state("cycle-a")); assertEquals("FAILED",state("cycle-b"));
        assertEquals("READY",state("ancestor")); assertTrue(s.applyNext().orElseThrow().applied());
    }

    @Test void readinessSweepDetectsCycleMissedByConcurrentInitialChecks() throws Exception {
        var s=worker("cycle-recovery");
        var a=depends("stale-cycle-a",1,List.of(after("stale-cycle-b")));
        var b=depends("stale-cycle-b",2,List.of());
        stage(s,manifest("stale-cycle-a",1,List.of(a)),List.of(a));
        stage(s,manifest("stale-cycle-b",2,List.of(b)),List.of(b));
        // Model the committed DB state possible when two staging transactions each checked its
        // own edge before the other transaction's edge became visible.
        try(var c=connections.open();var ps=c.prepareStatement("""
                INSERT INTO cdc.dependency(projection_generation,transaction_id,mutation_sequence,
                  dependency_index,dependency_type,producer_transaction_id,reason)
                VALUES(?,?,1,99,'AFTER_TRANSACTION',?,?)
                """)) {
            ps.setString(1,GENERATION);ps.setString(2,canonical("stale-cycle-b"));
            ps.setString(3,canonical("stale-cycle-a"));ps.setString(4,"concurrent edge");ps.executeUpdate();
        }
        try(var c=connections.open()) { execute(c,"UPDATE cdc.tx_transaction SET state='WAITING_DEPENDENCIES'"); }
        assertEquals(0,s.drain(1));
        assertEquals("FAILED",state("stale-cycle-a"));assertEquals("FAILED",state("stale-cycle-b"));
    }

    @Test void independentWorkersOverlapWhileDuplicateClaimSkipsLockedTransaction() throws Exception {
        var s=worker("staging"); var a=insert("parallel-a",1,1); var b=insert("parallel-b",1,2);
        stage(s,manifest("parallel-a",1,List.of(a)),List.of(a)); stage(s,manifest("parallel-b",2,List.of(b)),List.of(b));
        try(var gate=connections.open()) {
            execute(gate,"SELECT pg_advisory_lock(88310)"); installGate();
            var pool=Executors.newFixedThreadPool(2);
            try {
                var first=pool.submit(()->worker("first").applyNext()); var second=pool.submit(()->worker("second").applyNext());
                await(()->blocked()==2);
                assertEquals(0,count("client.customers")); assertEquals(0,count("cdc.applied_transaction"));
                assertTrue(worker("duplicate-claim").applyNext().isEmpty());
                execute(gate,"SELECT pg_advisory_unlock(88310)");
                assertTrue(first.get(10,TimeUnit.SECONDS).orElseThrow().applied());
                assertTrue(second.get(10,TimeUnit.SECONDS).orElseThrow().applied());
            } finally { execute(gate,"SELECT pg_advisory_unlock_all()"); pool.shutdownNow(); removeGate(); }
        }
        assertEquals(2,count("cdc.applied_transaction")); assertEquals(2,count("cdc.apply_attempt"));
    }

    @Test void backendDeathDuringBusinessWriteRollsBackAndNewWorkerRecovers() throws Exception {
        var s=worker("stage"); var m=insert("crash",1,1); stage(s,manifest("crash",1,List.of(m)),List.of(m));
        try(var gate=connections.open()) {
            execute(gate,"SELECT pg_advisory_lock(88310)"); installGate(); var pool=Executors.newSingleThreadExecutor();
            try {
                var attempt=pool.submit(()->worker("crashed").applyNext()); await(()->blocked()==1);
                assertEquals(0,count("client.customers"));
                execute(gate,"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE 'INSERT INTO client.customers%'");
                assertThrows(ExecutionException.class,()->attempt.get(10,TimeUnit.SECONDS));
                assertEquals(0,count("client.customers")); assertEquals(0,count("cdc.version_history")); assertEquals(0,count("cdc.applied_transaction"));
            } finally { execute(gate,"SELECT pg_advisory_unlock_all()"); pool.shutdownNow(); removeGate(); }
        }
        // Transient failure reporting may schedule a retry; advance only this isolated test's eligibility.
        try(var c=connections.open()){execute(c,"UPDATE cdc.tx_transaction SET next_attempt_at=clock_timestamp()");}
        assertEquals(1,worker("replacement").drain(10)); assertEquals("APPLIED",state("crash"));
    }

    @Test void twoWritersCannotConsumeTheSameObjectVersion() throws Exception {
        var s=worker("staging"); var a=insert("writer-a",1,1); var b=insert("writer-b",1,1);
        stage(s,manifest("writer-a",1,List.of(a)),List.of(a)); stage(s,manifest("writer-b",2,List.of(b)),List.of(b));
        var pool=Executors.newFixedThreadPool(2);
        try {
            var first=pool.submit(()->worker("first").applyNext().orElseThrow());
            var second=pool.submit(()->worker("second").applyNext().orElseThrow());
            assertNotEquals(first.get(10,TimeUnit.SECONDS).applied(),second.get(10,TimeUnit.SECONDS).applied());
        } finally { pool.shutdownNow(); }
        assertEquals(1,count("client.customers")); assertEquals(1,count("cdc.applied_transaction"));
        assertEquals(1,count("cdc.version_history"));
    }

    @Test void deletingThenRecreatingPreservesTombstoneVersionChain() throws Exception {
        var s=worker("lifecycle"); var created=insert("created",1,1);
        stage(s,manifest("created",1,List.of(created)),List.of(created));assertEquals(1,s.drain(10));
        var tombstone=new VersionToken(GENERATION,canonical("deleted"),1,Presence.DELETED);
        var deleted=mutation("deleted",1,created.object().entity(),created.object().primaryKey(),Operation.DELETE,
                created.newObjectVersion(),tombstone,null,List.of(after("created")));
        stage(s,manifest("deleted",2,List.of(deleted)),List.of(deleted));assertEquals(1,s.drain(10));
        assertEquals(0,count("client.customers"));assertEquals("DELETED",scalar("SELECT presence FROM cdc.object_state"));
        var recreated=mutation("recreated",1,created.object().entity(),created.object().primaryKey(),Operation.INSERT,
                tombstone,present("recreated",1),created.after(),List.of(after("deleted")));
        stage(s,manifest("recreated",3,List.of(recreated)),List.of(recreated));assertEquals(1,s.drain(10));
        stage(s,manifest("deleted",2,List.of(deleted)),List.of(deleted));assertEquals(0,s.drain(10));
        assertEquals(1,count("client.customers"));assertEquals(3,count("cdc.version_history"));
    }

    @Test void unknownObjectIsNotProofOfAbsence() throws Exception {
        var s=worker("unknown"); var missing=object("operational.products",Map.of("name",tv("text","unknown")));
        var m=depends("absence",1,List.of(new Dependency(DependencyType.NOT_EXISTS,null,missing,null,"must be known absent")));
        stage(s,manifest("absence",1,List.of(m)),List.of(m));
        assertEquals("WAITING_DEPENDENCIES",state("absence"));assertTrue(s.applyNext().isEmpty());
    }

    @Test void predicateObjectMustStayLockedUntilCommit() throws Exception {
        var s=worker("staging"); var obj=object("operational.products",Map.of("name",tv("text","predicate")));
        String absent=ProtocolCodec.json(VersionToken.absentBaseline(GENERATION));
        try(var c=connections.open();var p=c.prepareStatement("INSERT INTO cdc.object_state(projection_generation,entity_name,object_key,presence,current_version,current_version_text) VALUES(?,?,?,'ABSENT',?::jsonb,?)")) {
            p.setString(1,GENERATION);p.setString(2,obj.entity());p.setString(3,ProtocolCodec.canonicalObjectKey(obj));p.setString(4,absent);p.setString(5,absent);p.executeUpdate();
        }
        var reader=depends("reader",1,List.of(new Dependency(DependencyType.NOT_EXISTS,null,obj,null,"atomic precondition")));
        stage(s,manifest("reader",1,List.of(reader)),List.of(reader));
        try(var gate=connections.open()) {
            execute(gate,"SELECT pg_advisory_lock(88310)");installGate();var pool=Executors.newSingleThreadExecutor();
            try {
                var attempt=pool.submit(()->worker("reader").applyNext());await(()->blocked()==1);
                // NOWAIT is a deterministic lock assertion, not a timing-based guess about worker progress.
                try(var observer=connections.open()) {
                    observer.setAutoCommit(false);
                    SQLException blocked=assertThrows(SQLException.class,()->execute(observer,"SELECT 1 FROM cdc.object_state WHERE entity_name='operational.products' FOR UPDATE NOWAIT"));
                    assertEquals("55P03",blocked.getSQLState());observer.rollback();
                }
                execute(gate,"SELECT pg_advisory_unlock(88310)");assertTrue(attempt.get(10,TimeUnit.SECONDS).orElseThrow().applied());
            } finally {execute(gate,"SELECT pg_advisory_unlock_all()");pool.shutdownNow();removeGate();}
        }
    }

    @Test void writePredecessorRequiresTheDeclaredObjectVersion() throws Exception {
        var s=worker("explicit-version");var produced=insert("producer",1,1);
        stage(s,manifest("producer",1,List.of(produced)),List.of(produced));assertEquals(1,s.drain(10));
        var different=insert("different",1,2);
        stage(s,manifest("different",2,List.of(different)),List.of(different));assertEquals(1,s.drain(10));
        var dependent=depends("dependent",3,List.of(new Dependency(DependencyType.WRITE_PREDECESSOR,
                canonical("producer"),different.object(),produced.newObjectVersion(),"explicit wrong object version")));
        stage(s,manifest("dependent",3,List.of(dependent)),List.of(dependent));
        assertEquals("WAITING_DEPENDENCIES",state("dependent"));
        assertTrue(s.applyNext().isEmpty());assertEquals(2,count("client.customers"));
    }

    @Test void malformedResultingVersionIsQuarantinedBeforeReadiness() throws Exception {
        var s=worker("semantic-validation");var m=insert("bad-version",1,1);
        ObjectNode changed=ProtocolCodec.mapper().valueToTree(m);
        ((ObjectNode)changed.get("newObjectVersion")).put("producerTransactionId",canonical("somebody-else"));
        assertEquals(TransactionApplyService.StageResult.QUARANTINED,
                s.stage(MUTATION_TOPIC,0,0,changed.toString()));
        assertEquals("FAILED",state("bad-version"));assertEquals(0,count("client.customers"));
    }

    @Test void writePredecessorWithoutVersionIsQuarantinedBeforeReadiness() throws Exception {
        var s=worker("dependency-shape");var produced=insert("shape-producer",1,1);
        var dependent=depends("bad-dependency",2,List.of(new Dependency(DependencyType.WRITE_PREDECESSOR,
                canonical("shape-producer"),produced.object(),produced.newObjectVersion(),"must name exact version")));
        ObjectNode changed=ProtocolCodec.mapper().valueToTree(dependent);
        ((ObjectNode)changed.withArray("dependencies").get(0)).remove("requiredVersion");
        assertEquals(TransactionApplyService.StageResult.QUARANTINED,
                s.stage(MUTATION_TOPIC,0,0,changed.toString()));
        assertEquals("FAILED",state("bad-dependency"));assertEquals(0,count("client.customers"));
    }

    @Test void killedWorkerBeforeClaimAfterClaimAndAfterCommitRecovers() throws Exception {
        for(String phase:List.of("before-claim","after-claim","after-commit")) {
            reset();var m=insert("process-crash",1,1);var h=manifest("process-crash",1,List.of(m));
            stage(worker("stage"),h,List.of(m));
            Process child=child(phase);var reader=Executors.newSingleThreadExecutor();
            try {
                var line=reader.submit(()->{
                    var output=new BufferedReader(new InputStreamReader(child.getInputStream()));String text;
                    while((text=output.readLine())!=null)if(text.startsWith("FAULT_POINT:"))return text;
                    throw new AssertionError("worker exited before fault point: "+child.waitFor());
                });
                assertEquals("FAULT_POINT:"+phase,line.get(15,TimeUnit.SECONDS));
                assertEquals(phase.equals("after-commit")?1:0,count("client.customers"));
                if(phase.equals("after-claim"))assertTrue(worker("competitor").applyNext().isEmpty());
                child.destroyForcibly();assertTrue(child.waitFor(10,TimeUnit.SECONDS));
                // Replay the durable inbox after process death (Kafka acknowledgment is a separate ingestion step).
                stage(worker("replay"),h,List.of(m));worker("replacement").drain(10);
                assertEquals("APPLIED",state("process-crash"));assertEquals(1,count("client.customers"));
                assertEquals(1,count("cdc.applied_transaction"));assertEquals(1,count("cdc.version_history"));
            } finally {child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);reader.shutdownNow();}
        }
    }

    @Test void killedWorkerDuringSqlRollsBackAndReplays() throws Exception {
        var m=insert("process-sql",1,1);stage(worker("stage"),manifest("process-sql",1,List.of(m)),List.of(m));
        try(var gate=connections.open()) {
            execute(gate,"SELECT pg_advisory_lock(88310)");installGate();Process child=child("during-sql");
            try {
                await(()->blocked()==1);assertEquals(0,count("client.customers"));assertTrue(worker("competitor").applyNext().isEmpty());
                child.destroyForcibly();assertTrue(child.waitFor(10,TimeUnit.SECONDS));
                execute(gate,"SELECT pg_advisory_unlock(88310)");await(()->blocked()==0);
            } finally {child.destroyForcibly();execute(gate,"SELECT pg_advisory_unlock_all()");removeGate();}
        }
        assertEquals(0,count("cdc.applied_transaction"));assertEquals(0,count("client.customers"));
        assertEquals(1,worker("replacement").drain(10));assertEquals(1,count("cdc.applied_transaction"));
    }

    private static Process child(String phase)throws Exception {
        String url;try(var c=connections.open()){url=c.getMetaData().getURL();}
        return new ProcessBuilder(System.getProperty("java.home")+"/bin/java","-cp",System.getProperty("test.runtime.classpath"),
                CrashWorker.class.getName(),url,phase).redirectErrorStream(true).start();
    }

    private static void installGate() throws Exception {
        try(var c=connections.open()) {
            execute(c,"CREATE OR REPLACE FUNCTION cdc.test_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock_shared(88310); RETURN NEW; END $$");
            execute(c,"CREATE TRIGGER test_gate AFTER INSERT ON client.customers FOR EACH ROW EXECUTE FUNCTION cdc.test_gate()");
        }
    }
    private static void removeGate() throws Exception { try(var c=connections.open()){execute(c,"DROP TRIGGER IF EXISTS test_gate ON client.customers"); execute(c,"DROP FUNCTION IF EXISTS cdc.test_gate()");} }
    private static void execute(Connection c,String sql)throws Exception {try(var s=c.createStatement()){s.execute(sql);}}
    private static int blocked() {try{return Integer.parseInt(scalar("SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE 'INSERT INTO client.customers%'"));}catch(Exception e){throw new RuntimeException(e);}}
    private static void await(BooleanSupplier condition)throws Exception {long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10); while(!condition.getAsBoolean()){if(System.nanoTime()>deadline)fail("DB barrier was not reached");Thread.sleep(20);}}
}
