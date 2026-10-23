package com.example.cdc.apply;

import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import com.fasterxml.jackson.databind.JsonNode;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.*;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

import static com.example.cdc.apply.V2Support.scalar;
import static com.example.cdc.protocol.ProtocolV2.*;
import static com.example.cdc.protocol.ProtocolV2Codec.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real PostgreSQL coverage for the normalized direct-row protocol. */
class V2DestinationApplyPostgresTest {
    static EmbeddedPostgres pg; static ConnectionProvider destination,source; static Baseline baseline; static String boundary;
    static TransactionApplyService service; static long sequence;

    @BeforeAll static void setup() throws Exception {
        pg=EmbeddedPostgres.builder().setServerConfig("wal_level","logical").setServerConfig("max_replication_slots","4").start();
        destination=()->pg.getPostgresDatabase().getConnection();SchemaMigrator.migrate(destination);
        try(Connection c=destination.open();Statement s=c.createStatement()){s.execute("CREATE DATABASE appdb");s.execute("CREATE ROLE dbz LOGIN REPLICATION PASSWORD 'dbz'");}
        source=()->pg.getDatabase("postgres","appdb").getConnection();
        try(Connection c=source.open();Statement s=c.createStatement()) {
            s.execute("CREATE SCHEMA inventory AUTHORIZATION postgres");s.execute("GRANT USAGE,CREATE ON SCHEMA inventory TO dbz");
            s.execute(Files.readString(Path.of("../local/postgres/init/02-inventory.sql")));
            s.execute(Files.readString(Path.of("../local/postgres/init/03-source-version-v2.sql")));
        }
        Path bundle=Path.of(System.getProperty("java.io.tmpdir"),"direct-baseline-"+UUID.randomUUID()+".jsonl");
        baseline=DestinationV2Reconciler.reconcile(source,destination,"test-db","test-inc","test-scope",GENERATION,
                DestinationV2Reconciler.Mode.POPULATE_EMPTY,bundle);
        boundary=baseline.boundary().position();service=new TransactionApplyService(destination,"test");
    }
    @AfterAll static void close() throws Exception {if(pg!=null)pg.close();}

    record Transition(OwnerIdentity owner,OwnerState before,JsonNode beforeImage,
                      OwnerState after,JsonNode afterImage,ProducerReference producer) {}
    record Draft(Manifest manifest,List<Mutation> mutations) {}

    static OwnerIdentity owner(String collection,Map<String,TypedValue> key) {
        return new OwnerIdentity("test-db","test-inc",collection,key);
    }
    static OwnerIdentity customer(long id) {return owner("inventory.customers",Map.of("id",new TypedValue("int64",Long.toString(id))));}
    static JsonNode row(String json) {try{return mapper().readTree(json);}catch(Exception e){throw new AssertionError(e);}}
    static JsonNode customerRow(long id,String first,long version) {return row("{\"id\":"+id+",\"first_name\":\""+first+"\",\"last_name\":\"User\",\"version\":"+version+"}");}
    static ProducerReference baselineProducer() {return new ProducerReference(ProducerType.BASELINE,baseline.baselineId());}

    static String nextLsn(long increment) {
        BigInteger value=V2TransactionApplyService.position(boundary).add(BigInteger.valueOf(increment));
        return value.shiftRight(32).toString(16).toUpperCase()+"/"+String.format("%08X",value.and(BigInteger.valueOf(0xffffffffL)).longValue());
    }
    static Draft draft(List<Transition> changes) {
        String uuid=UUID.randomUUID().toString(),tx=transactionId("test-db","test-inc",uuid);
        List<OwnerTransition> transitions=new ArrayList<>();List<Mutation> mutations=new ArrayList<>();long ordinal=0,mutationSequence=0;
        for(Transition change:changes) {
            Predecessor predecessor=new Predecessor(change.before,change.producer);
            OwnerTransition transition=new OwnerTransition(tx,change.owner,predecessor,change.after,
                    List.of(new JournalRevision(++ordinal,change.before,change.after)),change.beforeImage,change.afterImage);
            transitions.add(transition);ObjectIdentity target=V2BusinessRows.target(change.owner);assertNotNull(target);
            Operation operation=change.after.presence()==Presence.DELETED?Operation.DELETE:
                    change.before.presence()==Presence.PRESENT?Operation.UPDATE:Operation.INSERT;
            mutations.add(signMutation(new Mutation(VERSION,SCHEMA_VERSION,RecordType.MUTATION,tx,"test-db","test-inc",
                    "test-scope",GENERATION,baseline.baselineId(),++mutationSequence,"pending","pending",
                    change.owner,target,operation,predecessor,change.after,List.of(),change.beforeImage,change.afterImage)));
        }
        long lsn=++sequence*10;
        SourceBoundary sourceBoundary=new SourceBoundary(uuid,Long.toString(90000+sequence),nextLsn(lsn),nextLsn(lsn+1),
                transitions.size(),transitions.size(),Map.of("source_cdc.revision_journal",transitions.size()));
        Manifest manifest=signManifest(new Manifest(VERSION,SCHEMA_VERSION,RecordType.MANIFEST,tx,"test-db","test-inc",
                "test-scope",GENERATION,baseline.baselineId(),sourceBoundary,transitions.size(),mutations.size(),transitions,"pending"),mutations);
        return new Draft(manifest,mutations);
    }
    static TransactionApplyService.StageResult stage(Draft draft) throws Exception {
        long offset=0;for(Mutation mutation:draft.mutations)service.stage(MUTATION_TOPIC,0,offset++,json(mutation));
        return service.stage(CONTROL_TOPIC,0,offset,json(draft.manifest));
    }
    static String query(String sql) throws Exception {try(Connection c=destination.open();Statement s=c.createStatement();ResultSet r=s.executeQuery(sql)){assertTrue(r.next());return r.getString(1);}}

    @Test void schemaAndBaselineContainOnlyDirectVersionedRows() throws Exception {
        assertEquals("1",query("SELECT max(version)::text FROM cdc.schema_migration"));
        assertEquals("0",query("SELECT count(*) FROM information_schema.columns WHERE table_schema IN ('client','operational') AND column_name='cdc_version'"));
        try(Connection c=source.open();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT to_regnamespace('source_projection') IS NULL")){assertTrue(r.next());assertTrue(r.getBoolean(1));}
        assertEquals("6",query("SELECT count(*) FROM information_schema.tables WHERE table_schema IN ('client','operational')"));
    }

    @Test void insertUpdateDeleteAndReinsertUseOneRowLineage() throws Exception {
        long id=8_000_001;OwnerState absent=new OwnerState(Presence.ABSENT,null),present1=new OwnerState(Presence.PRESENT,1L);
        Draft insert=draft(List.of(new Transition(customer(id),absent,null,present1,customerRow(id,"First",1),baselineProducer())));
        assertEquals(TransactionApplyService.StageResult.STAGED,stage(insert));assertTrue(service.applyNext().orElseThrow().applied());
        assertEquals("1",query("SELECT version::text FROM client.customers WHERE id="+id));
        ProducerReference first=new ProducerReference(ProducerType.TRANSACTION,insert.manifest.transactionId());
        OwnerState v2=new OwnerState(Presence.PRESENT,2L);
        Draft update=draft(List.of(new Transition(customer(id),present1,customerRow(id,"First",1),v2,customerRow(id,"Second",2),first)));
        stage(update);assertTrue(service.applyNext().orElseThrow().applied());
        ProducerReference second=new ProducerReference(ProducerType.TRANSACTION,update.manifest.transactionId());
        OwnerState deleted=new OwnerState(Presence.DELETED,3L);
        Draft remove=draft(List.of(new Transition(customer(id),v2,customerRow(id,"Second",2),deleted,null,second)));
        stage(remove);assertTrue(service.applyNext().orElseThrow().applied());assertEquals("0",query("SELECT count(*) FROM client.customers WHERE id="+id));
        Draft revive=draft(List.of(new Transition(customer(id),deleted,null,new OwnerState(Presence.PRESENT,4L),
                customerRow(id,"Again",4),new ProducerReference(ProducerType.TRANSACTION,remove.manifest.transactionId()))));
        stage(revive);assertTrue(service.applyNext().orElseThrow().applied());assertEquals("4",query("SELECT version::text FROM client.customers WHERE id="+id));
    }

    @Test void allSixSourceTablesMapDirectlyByTheSamePrimaryKey() throws Exception {
        long id=8_000_100;String product="direct-"+id;OwnerState absent=new OwnerState(Presence.ABSENT,null),present=new OwnerState(Presence.PRESENT,1L);
        OwnerIdentity address=owner("inventory.addresses",Map.of("customer_id",new TypedValue("int64",Long.toString(id)),"address_type",new TypedValue("text","HOME")));
        OwnerIdentity contact=owner("inventory.contact_numbers",Map.of("customer_id",new TypedValue("int64",Long.toString(id)),"phone_type",new TypedValue("text","HOME")));
        OwnerIdentity productOwner=owner("inventory.products",Map.of("name",new TypedValue("text",product)));
        OwnerIdentity order=owner("inventory.orders",Map.of("id",new TypedValue("int64",Long.toString(id))));
        OwnerIdentity item=owner("inventory.order_items",Map.of("order_id",new TypedValue("int64",Long.toString(id)),"product_name",new TypedValue("text",product)));
        List<Transition> changes=List.of(
                new Transition(customer(id),absent,null,present,customerRow(id,"Direct",1),baselineProducer()),
                new Transition(address,absent,null,present,row("{\"customer_id\":"+id+",\"address_type\":\"HOME\",\"street\":\"Road\",\"version\":1}"),baselineProducer()),
                new Transition(contact,absent,null,present,row("{\"customer_id\":"+id+",\"phone_type\":\"HOME\",\"phone_number\":\"555\",\"version\":1}"),baselineProducer()),
                new Transition(productOwner,absent,null,present,row("{\"name\":\""+product+"\",\"category\":\"Tools\",\"current_price\":10,\"version\":1}"),baselineProducer()),
                new Transition(order,absent,null,present,row("{\"id\":"+id+",\"customer_id\":"+id+",\"status\":\"OPEN\",\"version\":1}"),baselineProducer()),
                new Transition(item,absent,null,present,row("{\"order_id\":"+id+",\"product_name\":\""+product+"\",\"quantity\":1,\"price_at_purchase\":10,\"version\":1}"),baselineProducer()));
        Draft all=draft(changes);stage(all);var result=service.applyNext().orElseThrow();assertTrue(result.applied(),result.toString());
        assertEquals("6",query("SELECT (SELECT count(*) FROM client.customers WHERE id="+id+")+(SELECT count(*) FROM client.addresses WHERE customer_id="+id+")+(SELECT count(*) FROM operational.contact_numbers WHERE customer_id="+id+")+(SELECT count(*) FROM operational.products WHERE name='"+product+"')+(SELECT count(*) FROM operational.orders WHERE id="+id+")+(SELECT count(*) FROM operational.order_items WHERE order_id="+id+")"));
    }

    @Test void conflictingDuplicateDescriptorIsQuarantined() throws Exception {
        long id=8_000_200;OwnerState absent=new OwnerState(Presence.ABSENT,null),present=new OwnerState(Presence.PRESENT,1L);
        Draft original=draft(List.of(new Transition(customer(id),absent,null,present,customerRow(id,"Original",1),baselineProducer())));
        assertEquals(TransactionApplyService.StageResult.STAGED,service.stage(MUTATION_TOPIC,0,0,json(original.mutations.get(0))));
        Mutation base=original.mutations.get(0);
        Mutation changed=signMutation(new Mutation(VERSION,SCHEMA_VERSION,RecordType.MUTATION,base.transactionId(),base.sourceDatabaseId(),
                base.sourceIncarnation(),base.captureScopeId(),base.projectionGeneration(),base.baselineId(),base.mutationSequence(),
                "pending","pending",base.owner(),base.object(),base.operation(),base.expected(),base.result(),base.dependencies(),
                base.before(),customerRow(id,"Forged",1)));
        assertEquals(TransactionApplyService.StageResult.QUARANTINED,service.stage(MUTATION_TOPIC,0,1,json(changed)));
    }
}
