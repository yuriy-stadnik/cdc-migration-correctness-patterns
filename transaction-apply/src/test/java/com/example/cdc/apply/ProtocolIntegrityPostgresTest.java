package com.example.cdc.apply;

import com.example.cdc.protocol.ProtocolCodec;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.*;
import java.util.List;
import java.util.Map;
import static com.example.cdc.protocol.Protocol.*;
import static com.example.cdc.apply.TransactionApplyServicePostgresTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** Release-blocking approved invariants. Keep failing assertions visible until protocol repair is approved. */
class ProtocolIntegrityPostgresTest {
    @BeforeAll static void start()throws Exception{database();}
    @AfterAll static void stop()throws Exception{closeDatabase();}
    @BeforeEach void clear()throws Exception{reset();}
    private static Mutation valid() {
        return mutation("integrity",1,"client.customers",Map.of("id",tv("int64","1")),Operation.INSERT,
                VersionToken.absentBaseline(GENERATION),present("integrity",1),
                json("{\"id\":1,\"first_name\":\"Original\",\"last_name\":\"Value\"}"),List.of());
    }
    @Test void firstDeliveryWithTamperedPayloadCannotSatisfyOriginalManifest()throws Exception {
        var s=new TransactionApplyService(connections,"integrity");var m=valid();
        ObjectNode forged=ProtocolCodec.mapper().valueToTree(m);
        ((ObjectNode)forged.get("after")).put("first_name","Changed without updating payload hash");
        s.stage(MUTATION_TOPIC,0,0,forged.toString());s.stage(CONTROL_TOPIC,0,0,ProtocolCodec.json(manifest("integrity",1,List.of(m))));
        assertEquals(0,s.drain(10),"unverified payload must not commit");assertEquals(0,count("client.customers"));
    }
    @Test void manifestMustBindResultingVersionAndDependencies()throws Exception {
        var m=valid();ObjectNode forged=ProtocolCodec.mapper().valueToTree(m);
        ((ObjectNode)forged.get("newObjectVersion")).put("producerTransactionId","not-this-transaction");
        Mutation changed=ProtocolCodec.read(forged.toString(),Mutation.class);
        assertNotEquals(ProtocolCodec.mutationSetHash(List.of(m)),ProtocolCodec.mutationSetHash(List.of(changed)),
                "a manifest must bind the mutation descriptor, not only its claimed payload hash");
    }
    @Test void unknownPayloadSchemaCannotApply()throws Exception {
        var s=new TransactionApplyService(connections,"schema");var m=valid();
        ObjectNode changed=ProtocolCodec.mapper().valueToTree(m);changed.put("schemaVersion",999);
        s.stage(MUTATION_TOPIC,0,0,changed.toString());s.stage(CONTROL_TOPIC,0,0,ProtocolCodec.json(manifest("integrity",1,List.of(m))));
        assertEquals(0,s.drain(10),"unknown schema must block application");
    }
}
