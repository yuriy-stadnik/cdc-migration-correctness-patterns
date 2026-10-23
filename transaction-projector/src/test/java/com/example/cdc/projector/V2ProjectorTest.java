package com.example.cdc.projector;

import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.example.cdc.protocol.ProtocolV2.*;
import static org.junit.jupiter.api.Assertions.*;

class V2ProjectorTest {
    static ObjectNode json(String value) { return (ObjectNode) V2Capture.parse(value); }
    static ObjectNode customer(long id,long version) {
        return json("{\"id\":"+id+",\"version\":"+version+",\"first_name\":\"A\",\"last_name\":\"B\"}");
    }
    static ObjectNode address(long customer,long version,String street) {
        return json("{\"customer_id\":"+customer+",\"address_type\":\"HOME\",\"street\":\""+street+"\",\"version\":"+version+"}");
    }
    static ObjectNode order(long id,long customer,long version) {
        return json("{\"id\":"+id+",\"customer_id\":"+customer+",\"status\":\"OPEN\",\"version\":"+version+"}");
    }
    static CaptureRecord record(String topic,JsonNode value) { return new CaptureRecord(topic,0,0,0,value.toString()); }

    static final class Tx {
        final String uuid; final long xid,start; final List<JsonNode> rows=new ArrayList<>(); long journalCount;
        Tx(long occurrence,long xid,long start) {
            uuid=new UUID(0,occurrence).toString();this.xid=xid;this.start=start;
            add("source_cdc.transaction_context",null,context(0),"c");
        }
        ObjectNode context(long count) {
            return json("{\"full_xid\":\""+xid+"\",\"source_transaction_id\":\""+uuid
                    +"\",\"journal_count\":"+count+"}");
        }
        void add(String collection,JsonNode before,JsonNode after,String op) {
            ObjectNode root=json("{\"op\":\""+op+"\"}");root.set("before",before);root.set("after",after);
            root.set("source",json("{\"schema\":\""+collection.split("\\.")[0]+"\",\"table\":\""
                    +collection.split("\\.")[1]+"\",\"txId\":"+xid+",\"lsn\":"+(start+rows.size()+1)+"}"));
            root.set("transaction",json("{\"id\":\""+xid+":"+(start+rows.size()+1)
                    +"\",\"total_order\":"+(rows.size()+1)+"}"));rows.add(root);
        }
        Tx business(String collection,ObjectNode before,ObjectNode after) {
            ObjectNode image=after==null?before:after;ObjectNode key=json("{}");
            switch(collection) {
                case "inventory.customers","inventory.orders" -> key.set("id",image.get("id"));
                case "inventory.addresses" -> {key.set("customer_id",image.get("customer_id"));key.set("address_type",image.get("address_type"));}
                case "inventory.contact_numbers" -> {key.set("customer_id",image.get("customer_id"));key.set("phone_type",image.get("phone_type"));}
                case "inventory.products" -> key.set("name",image.get("name"));
                case "inventory.order_items" -> {key.set("order_id",image.get("order_id"));key.set("product_name",image.get("product_name"));}
                default -> throw new IllegalArgumentException(collection);
            }
            add(collection,before,after,before==null?"c":after==null?"d":"u");
            long next=after==null?before.get("version").longValue()+1:after.get("version").longValue();
            ObjectNode j=json("{\"source_transaction_id\":\""+uuid+"\",\"ordinal\":"+(++journalCount)
                    +",\"collection\":\""+collection+"\",\"before_presence\":\""
                    +(before==null?"ABSENT":"PRESENT")+"\",\"after_presence\":\""
                    +(after==null?"DELETED":"PRESENT")+"\",\"after_version\":"+next+"}");
            j.put("owner_key",key.toString());
            if(before==null){j.putNull("before_version");j.putNull("before_image");}
            else {j.put("before_version",before.get("version").longValue());j.put("before_image",before.toString());}
            if(after==null)j.putNull("after_image");else j.put("after_image",after.toString());
            add("source_cdc.revision_journal",null,j,"c");return this;
        }
        List<CaptureRecord> records() {
            add("source_cdc.transaction_context",context(0),context(journalCount),"u");
            ObjectNode begin=json("{\"id\":\""+xid+":"+start+"\",\"status\":\"BEGIN\"}");
            ObjectNode end=json("{\"id\":\""+xid+":"+(start+rows.size()+1)
                    +"\",\"status\":\"END\",\"event_count\":"+rows.size()+"}");
            Map<String,Integer> counts=new TreeMap<>();rows.forEach(r->counts.merge(V2Capture.collection(r),1,Integer::sum));
            var collections=end.putArray("data_collections");counts.forEach((c,n)->collections.addObject().put("data_collection",c).put("event_count",n));
            List<CaptureRecord> result=new ArrayList<>();result.add(record(CAPTURE_TOPIC,begin));
            rows.forEach(r->result.add(record(CAPTURE_TOPIC,r)));result.add(record(CAPTURE_TOPIC,end));return result;
        }
    }

    static BaselineOwner baselineOwner(String collection,ObjectNode payload) {
        V2Capture capture=new V2Capture("db","inc");OwnerIdentity owner=capture.ownerFromRow(collection,payload);
        return new BaselineOwner(VERSION,SCHEMA_VERSION,RecordType.BASELINE_OWNER,"baseline","db","inc","scope",GENERATION,
                owner,new OwnerState(Presence.PRESENT,payload.get("version").longValue()),payload,"0");
    }
    static List<CaptureRecord> baseline(BaselineOwner...owners) {
        List<BaselineOwner> rows=List.of(owners);
        Baseline baseline=new Baseline(VERSION,SCHEMA_VERSION,RecordType.BASELINE,"baseline",BaselineStatus.ACTIVE,
                "db","inc","scope",GENERATION,new SnapshotBoundary("POSTGRES_LSN","0",true,rows.size(),0),rows.size());
        List<CaptureRecord> input=new ArrayList<>();
        input.add(new CaptureRecord(V2Projector.BASELINE_CONTROL_INPUT,0,0,0,ProtocolV2Codec.json(baseline)));
        rows.forEach(owner->input.add(new CaptureRecord(BASELINE_TOPIC,0,0,0,ProtocolV2Codec.json(owner))));return input;
    }

    final V2Projector planner=new V2Projector("db","inc","scope");
    final V2ProjectorState state=new V2ProjectorState();
    List<OutputRecord> send(List<CaptureRecord> records) {List<OutputRecord> out=new ArrayList<>();records.forEach(r->out.addAll(planner.accept(state,r)));return out;}
    static List<Manifest> manifests(List<OutputRecord> out) {return out.stream().filter(r->r.topic().equals(CONTROL_TOPIC)).map(r->ProtocolV2Codec.readManifest(CONTROL_TOPIC,r.value())).toList();}
    static List<Mutation> mutations(List<OutputRecord> out) {return out.stream().filter(r->r.topic().equals(MUTATION_TOPIC)).map(r->ProtocolV2Codec.readMutation(MUTATION_TOPIC,r.value())).toList();}

    @Test void multipleUpdatesReduceToOneDirectMutation() {
        send(baseline(baselineOwner("inventory.customers",customer(1,1))));
        List<OutputRecord> out=send(new Tx(1,7,100).business("inventory.customers",customer(1,1),customer(1,2))
                .business("inventory.customers",customer(1,2),customer(1,3)).records());
        assertTrue(state.quarantine.isEmpty(),state.quarantine.toString());assertEquals(1,mutations(out).size());
        Mutation mutation=mutations(out).get(0);assertEquals(1L,mutation.expected().state().version());assertEquals(3L,mutation.result().version());
        ProtocolV2Codec.verifyManifest(manifests(out).get(0),mutations(out));
    }

    @Test void normalizedAddressOwnsExactlyOneDestinationRow() {
        send(baseline(baselineOwner("inventory.customers",customer(1,1)),baselineOwner("inventory.addresses",address(1,7,"Old"))));
        List<OutputRecord> out=send(new Tx(1,7,100).business("inventory.addresses",address(1,7,"Old"),address(1,8,"New")).records());
        assertTrue(state.quarantine.isEmpty(),state.quarantine.toString());assertEquals(1,mutations(out).size());
        Mutation mutation=mutations(out).get(0);assertEquals("client.addresses",mutation.object().entity());
        assertEquals("inventory.addresses",mutation.owner().collection());assertEquals(mutation.owner().primaryKey(),mutation.object().primaryKey());
        assertTrue(mutation.dependencies().stream().anyMatch(d->d.type()==DependencyType.EXISTS));
    }

    @Test void outOfOrderTransactionsWaitForTheActualRowPredecessor() {
        send(baseline(baselineOwner("inventory.customers",customer(1,1))));
        assertTrue(manifests(send(new Tx(2,8,200).business("inventory.customers",customer(1,2),customer(1,3)).records())).isEmpty());
        List<OutputRecord> out=send(new Tx(1,7,100).business("inventory.customers",customer(1,1),customer(1,2)).records());
        assertEquals(2,manifests(out).size());assertTrue(state.quarantine.isEmpty(),state.quarantine.toString());
    }

    @Test void sourceForeignKeyShapeCreatesParentDependencyWithoutFences() {
        send(baseline(baselineOwner("inventory.customers",customer(1,1))));
        List<OutputRecord> out=send(new Tx(1,7,100).business("inventory.orders",null,order(10,1,1)).records());
        assertTrue(state.quarantine.isEmpty(),state.quarantine.toString());
        assertTrue(mutations(out).get(0).dependencies().stream().anyMatch(d->d.type()==DependencyType.EXISTS
                &&d.owner().collection().equals("inventory.customers")));
    }

    @Test void duplicateEvidenceMustBeByteIndependentButSemanticallyEqual() {
        send(baseline(baselineOwner("inventory.customers",customer(1,1))));
        List<CaptureRecord> records=new Tx(1,7,100).business("inventory.customers",customer(1,1),customer(1,2)).records();
        assertEquals(1,manifests(send(records)).size());assertTrue(send(records).isEmpty());
        ObjectNode changed=json(records.get(2).value());((ObjectNode)changed.get("after")).put("first_name","forged");
        send(List.of(record(CAPTURE_TOPIC,changed)));assertNotNull(state.haltedReason);
    }
}
