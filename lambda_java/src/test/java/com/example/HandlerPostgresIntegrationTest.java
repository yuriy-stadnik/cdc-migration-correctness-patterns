package com.example;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.example.cdc.apply.ConnectionProvider;
import com.example.cdc.apply.SchemaMigrator;
import com.example.cdc.apply.TransactionApplyService;
import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import com.fasterxml.jackson.databind.JsonNode;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.example.cdc.protocol.ProtocolV2.*;
import static com.example.cdc.protocol.ProtocolV2Codec.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** AWS adapter integration: AWS-shaped MSK records -> Handler -> real PostgreSQL transaction apply. */
class HandlerPostgresIntegrationTest {
    record TransactionFixture(Manifest manifest, Mutation mutation) {}

    @Test void awsMskBatchAppliesAtomicallyAndReplayIsIdempotent() throws Exception {
        try (EmbeddedPostgres postgres = EmbeddedPostgres.builder().start()) {
            ConnectionProvider connections = () -> postgres.getPostgresDatabase().getConnection();
            SchemaMigrator.migrate(connections);
            Handler handler = new Handler(new TransactionApplyService(connections,"lambda-integration"));
            long customerId = 9_100_001L;
            TransactionFixture fixture = customerInsert(customerId);
            KafkaEvent event = event(fixture);

            assertEquals(Map.of("status","ok","staged",2,"applied",1),handler.handleRequest(event,null));
            assertEquals("1",query(connections,"SELECT count(*)::text FROM client.customers WHERE id="+customerId));
            assertEquals("1",query(connections,"SELECT mutation_count::text FROM cdc.v2_receipt WHERE transaction_id='"+
                    fixture.manifest().transactionId()+"'"));
            assertEquals(fixture.mutation().idempotencyKey(),query(connections,
                    "SELECT idempotency_key FROM client.customers WHERE id="+customerId));

            assertEquals(Map.of("status","ok","staged",2,"applied",0),handler.handleRequest(event,null));
            assertEquals("1",query(connections,"SELECT count(*)::text FROM client.customers WHERE id="+customerId));
            assertEquals("1",query(connections,"SELECT count(*)::text FROM cdc.v2_receipt WHERE transaction_id='"+
                    fixture.manifest().transactionId()+"'"));
        }
    }

    private static TransactionFixture customerInsert(long id) throws Exception {
        String sourceUuid = UUID.randomUUID().toString();
        String transactionId = transactionId("test-db","test-inc",sourceUuid);
        OwnerIdentity owner = new OwnerIdentity("test-db","test-inc","inventory.customers",
                Map.of("id",new TypedValue("int64",Long.toString(id))));
        ObjectIdentity object = new ObjectIdentity("client.customers",owner.primaryKey());
        OwnerState absent = new OwnerState(Presence.ABSENT,null);
        OwnerState present = new OwnerState(Presence.PRESENT,1L);
        Predecessor predecessor = new Predecessor(absent,new ProducerReference(ProducerType.BASELINE,EMPTY_BASELINE_ID));
        JsonNode after = ProtocolV2Codec.mapper().readTree("""
                {"id":%d,"first_name":"Aws","last_name":"Integration","email":"aws-%d@example.com","status":"ACTIVE","version":1}
                """.formatted(id,id));
        OwnerTransition transition = new OwnerTransition(transactionId,owner,predecessor,present,
                List.of(new JournalRevision(1,absent,present)),null,after);
        Mutation mutation = signMutation(new Mutation(VERSION,SCHEMA_VERSION,RecordType.MUTATION,
                transactionId,"test-db","test-inc","test-scope",GENERATION,EMPTY_BASELINE_ID,
                1,"pending","pending",owner,object,Operation.INSERT,predecessor,present,List.of(),null,after));
        SourceBoundary boundary = new SourceBoundary(sourceUuid,"100","10","11",1,1,
                Map.of("source_cdc.revision_journal",1));
        Manifest manifest = signManifest(new Manifest(VERSION,SCHEMA_VERSION,RecordType.MANIFEST,
                transactionId,"test-db","test-inc","test-scope",GENERATION,EMPTY_BASELINE_ID,
                boundary,1,1,List.of(transition),"pending"),List.of(mutation));
        return new TransactionFixture(manifest,mutation);
    }

    private static KafkaEvent event(TransactionFixture fixture) {
        Map<String,List<KafkaEvent.KafkaEventRecord>> records = new LinkedHashMap<>();
        // Manifest first deliberately exercises cross-topic arrival before the mutation is complete.
        records.put(CONTROL_TOPIC+"-0",List.of(record(null,0,70,json(fixture.manifest()))));
        records.put(MUTATION_TOPIC+"-0",List.of(record(MUTATION_TOPIC,0,71,json(fixture.mutation()))));
        KafkaEvent event = new KafkaEvent(); event.setRecords(records); return event;
    }

    private static KafkaEvent.KafkaEventRecord record(String topic,int partition,long offset,String json) {
        KafkaEvent.KafkaEventRecord record = new KafkaEvent.KafkaEventRecord();
        record.setTopic(topic); record.setPartition(partition); record.setOffset(offset);
        record.setValue(Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)));
        return record;
    }

    private static String query(ConnectionProvider connections,String sql) throws Exception {
        try (Connection connection = connections.open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next(); return result.getString(1);
        }
    }
}
