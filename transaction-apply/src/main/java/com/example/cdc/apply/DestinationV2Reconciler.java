package com.example.cdc.apply;

import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import static com.example.cdc.protocol.ProtocolV2.*;
import static com.example.cdc.protocol.ProtocolV2Codec.*;
import static com.example.cdc.apply.V2Support.*;

/** Controlled offline boundary: lock source writes, prove source registry completeness, then reconcile target. */
public final class DestinationV2Reconciler {
    public enum Mode { VERIFY_EXISTING, POPULATE_EMPTY }
    private static final SortedSet<String> TABLES=new TreeSet<>(Set.of("inventory.customers","inventory.addresses",
        "inventory.contact_numbers","inventory.products","inventory.orders","inventory.order_items",
        "source_cdc.transaction_context","source_cdc.revision_journal"));
    private static final List<String> TARGETS=List.of("client.customers","client.addresses","operational.contact_numbers","operational.products","operational.orders","operational.order_items");
    private record Row(OwnerIdentity owner,OwnerState state,JsonNode payload,BaselineOwner record){}
    private DestinationV2Reconciler(){}

    /** Writes an immutable JSONL bundle before committing destination activation while source tables are locked. */
    public static Baseline reconcile(ConnectionProvider source,ConnectionProvider destination,String db,String incarnation,
                                     String scope,String generation,Mode mode,Path bundle)throws Exception {
        if(bundle==null||Files.exists(bundle))throw new IllegalArgumentException("baseline bundle path must not already exist");
        try(Connection src=source.open();Connection dst=destination.open()) {
            src.setAutoCommit(false);dst.setAutoCommit(false);
            try {
                src.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                execute(src,"LOCK TABLE inventory.customers,inventory.addresses,inventory.contact_numbers,inventory.products,inventory.orders,inventory.order_items,source_cdc.owner_state IN SHARE MODE");
                if(!truthy(scalar(src,"SELECT EXISTS(SELECT 1 FROM source_cdc.schema_version WHERE version=3)")))throw new Conflict("normalized source schema is not installed");
                String position=scalar(src,"SELECT pg_current_wal_lsn()::text");
                List<Row> sourceRows=sourceRows(src,db,incarnation,scope,generation);
                long present=sourceRows.stream().filter(r->r.state.presence()==Presence.PRESENT).count();
                long tombstones=sourceRows.size()-present;
                String baselineId="baseline-v2:"+db+":"+incarnation+":"+position.replace('/','-');
                List<Row> rows=new ArrayList<>();
                for(Row r:sourceRows) {
                    var ownerRecord=signBaselineOwner(new BaselineOwner(VERSION,SCHEMA_VERSION,RecordType.BASELINE_OWNER,baselineId,db,incarnation,scope,generation,r.owner,r.state,r.payload,position));
                    rows.add(new Row(r.owner,r.state,r.payload,ownerRecord));
                }
                List<BaselineOwner> records=rows.stream().map(Row::record).toList();
                String lane=V2Support.lane(db,incarnation,scope,generation);
                lease(dst,true);
                String existing=scalar(dst,"SELECT baseline_id FROM cdc.v2_namespace WHERE lane=?",lane);
                if(existing!=null)throw new Conflict("source lane already activated; baseline replacement is forbidden");
                if(scalar(dst,"SELECT lane FROM cdc.v2_namespace LIMIT 1")!=null)throw new Conflict("another source lane owns the target namespace");
                reconcileBusinessRows(dst,rows,mode,baselineId);
                SnapshotBoundary boundary=new SnapshotBoundary("POSTGRES_LSN",position,true,present,tombstones);
                Baseline baseline=signBaseline(new Baseline(VERSION,SCHEMA_VERSION,RecordType.BASELINE,baselineId,BaselineStatus.ACTIVE,
                    db,incarnation,scope,generation,boundary,rows.size()),records);
                activate(dst,lane,baseline,rows);
                // The file is prepared atomically. A failed DB COMMIT leaves a harmless, unactivated bundle.
                Path temp=bundle.resolveSibling(bundle.getFileName()+"."+UUID.randomUUID()+".tmp");
                List<String> lines=new ArrayList<>();lines.add(json(baseline));for(var record:records)lines.add(json(record));
                Files.write(temp,lines,StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW);Files.move(temp,bundle,StandardCopyOption.ATOMIC_MOVE);
                dst.commit();src.commit();return baseline;
            }catch(Exception e){try{dst.rollback();}catch(Exception ignored){}try{src.rollback();}catch(Exception ignored){}throw e;}
        }
    }
    private static List<Row> sourceRows(Connection c,String db,String inc,String scope,String gen)throws SQLException {
        List<Row> rows=new ArrayList<>();Set<String> seen=new HashSet<>();
        try(var p=c.prepareStatement("SELECT collection,owner_key,version,presence FROM source_cdc.owner_state ORDER BY collection,owner_key");var r=p.executeQuery()) {
            while(r.next()) {
                String collection=r.getString(1),ownerText=r.getString(2);long version=r.getLong(3);Presence presence=Presence.valueOf(r.getString(4));
                if(version<=0||presence==Presence.ABSENT)throw new Conflict("invalid source owner registry row");
                JsonNode key;try{key=mapper().readTree(ownerText);}catch(Exception e){throw new Conflict("invalid canonical source owner key");}
                OwnerIdentity owner=owner(collection,key,db,inc);String ownerKey=json(owner);if(!seen.add(ownerKey))throw new Conflict("duplicate source owner registry row");
                OwnerState state=new OwnerState(presence,version);JsonNode payload=sourcePayload(c,owner,state);
                rows.add(new Row(owner,state,payload,null));
            }
        }
        Map<String,String> businessTables=Map.of("inventory.customers","inventory.customers",
            "inventory.addresses","inventory.addresses","inventory.contact_numbers","inventory.contact_numbers",
            "inventory.products","inventory.products","inventory.orders","inventory.orders",
            "inventory.order_items","inventory.order_items");
        for(var entry:businessTables.entrySet()) {
            long registry=Long.parseLong(scalar(c,"SELECT count(*) FROM source_cdc.owner_state WHERE collection=?",entry.getKey()));
            long live=Long.parseLong(scalar(c,"SELECT count(*) FROM "+entry.getValue()));
            long present=Long.parseLong(scalar(c,"SELECT count(*) FROM source_cdc.owner_state WHERE collection=? AND presence='PRESENT'",entry.getKey()));
            if(registry<present||present!=live)throw new Conflict("source owner registry/table coverage disagrees for "+entry.getKey());
            Set<String> registered=stringSet(c,"SELECT owner_key FROM source_cdc.owner_state WHERE collection=? AND presence='PRESENT'",entry.getKey());
            String rowKeys=switch(entry.getKey()) {
                case "inventory.customers" -> "SELECT jsonb_build_object('id',id)::text FROM inventory.customers";
                case "inventory.addresses" -> "SELECT jsonb_build_object('customer_id',customer_id,'address_type',address_type)::text FROM inventory.addresses";
                case "inventory.contact_numbers" -> "SELECT jsonb_build_object('customer_id',customer_id,'phone_type',phone_type)::text FROM inventory.contact_numbers";
                case "inventory.products" -> "SELECT jsonb_build_object('name',name)::text FROM inventory.products";
                case "inventory.orders" -> "SELECT jsonb_build_object('id',id)::text FROM inventory.orders";
                case "inventory.order_items" -> "SELECT jsonb_build_object('order_id',order_id,'product_name',product_name)::text FROM inventory.order_items";
                default -> throw new Conflict("unrecognized source table in baseline inventory");
            };
            if(!registered.equals(stringSet(c,rowKeys)))throw new Conflict("source owner keys do not exactly cover table rows for "+entry.getKey());
        }
        Set<String> published=new TreeSet<>();
        try(var p=c.prepareStatement("SELECT schemaname||'.'||tablename FROM pg_publication_tables WHERE pubname='source_cdc_v2'");var r=p.executeQuery()){while(r.next())published.add(r.getString(1));}
        if(!published.equals(TABLES))throw new Conflict("source v2 publication inventory mismatch: "+published);
        // Only registered owner records are considered; direct table keys and owner registry were committed together.
        if(!truthy(scalar(c,"SELECT EXISTS(SELECT 1 FROM pg_publication_tables WHERE schemaname='inventory' AND tablename='customers')")))
            throw new Conflict("source capture inventory/publication is incomplete");
        return rows;
    }
    private static Set<String> stringSet(Connection c,String sql,Object...args)throws SQLException {
        Set<String> result=new TreeSet<>();try(var p=c.prepareStatement(sql)) {
            for(int i=0;i<args.length;i++)p.setObject(i+1,args[i]);
            try(var r=p.executeQuery()){while(r.next())result.add(r.getString(1));}
        }return result;
    }
    private static OwnerIdentity owner(String collection,JsonNode key,String db,String inc) {
        Map<String,TypedValue> values=new TreeMap<>();
        if(!key.isObject())throw new Conflict("source owner key object expected");
        key.fields().forEachRemaining(e->values.put(e.getKey(),new TypedValue(Set.of("id","customer_id","order_id").contains(e.getKey())?"int64":"text",e.getValue().asText())));
        return new OwnerIdentity(db,inc,collection,values);
    }
    private static JsonNode sourcePayload(Connection c,OwnerIdentity owner,OwnerState state)throws SQLException {
        String collection=owner.collection();String value;
        if(state.presence()==Presence.DELETED) {
            String table=collection.substring("inventory.".length());String where=String.join(" AND ",new TreeMap<>(owner.primaryKey()).keySet().stream().map(k->k+"=?").toList());
            if(scalar(c,"SELECT to_jsonb(t)::text FROM inventory."+table+" t WHERE "+where,V2BusinessRows.keys(new ObjectIdentity(V2BusinessRows.target(owner).entity(),owner.primaryKey())).toArray())!=null)
                throw new Conflict("deleted owner still has a source business row");
            return null;
        }
        String table=collection.substring("inventory.".length());String where=String.join(" AND ",new TreeMap<>(owner.primaryKey()).keySet().stream().map(k->k+"=?").toList());
        value=scalar(c,"SELECT to_jsonb(t)::text FROM inventory."+table+" t WHERE "+where,V2BusinessRows.keys(new ObjectIdentity(V2BusinessRows.target(owner).entity(),owner.primaryKey())).toArray());
        if(value==null)throw new Conflict("PRESENT source owner has no matching row: "+json(owner));
        JsonNode payload=V2BusinessRows.parse(value);
        if(!payload.path("version").isIntegralNumber()||payload.path("version").longValue()!=state.version())throw new Conflict("source row and owner registry version disagree");
        return payload;
    }
    private static void reconcileBusinessRows(Connection c,List<Row> rows,Mode mode,String baselineId)throws SQLException {
        List<Row> mapped=rows.stream().filter(r->V2BusinessRows.target(r.owner)!=null).toList();Map<String,Row> expected=new TreeMap<>();
        for(Row r:mapped)if(expected.put(json(V2BusinessRows.target(r.owner)),r)!=null)throw new Conflict("two source owners map to one destination key");
        long total=0;for(String table:TARGETS)total+=Long.parseLong(scalar(c,"SELECT count(*) FROM "+table));
        if(mode==Mode.POPULATE_EMPTY&&total!=0)throw new Conflict("POPULATE_EMPTY requires all six destination tables empty");
        if(mode==Mode.VERIFY_EXISTING) {
            for(var e:expected.entrySet()) {
                Row source= e.getValue(); ObjectIdentity object=V2BusinessRows.target(source.owner);
                JsonNode actual=V2BusinessRows.read(c,object,true);
                if(source.state.presence()==Presence.PRESENT) {
                    if(actual==null||!json(V2BusinessRows.business(object,actual,false)).equals(json(V2BusinessRows.business(object,source.payload,false))))throw new Conflict("destination row does not match source baseline: "+e.getKey());
                    long prior=actual.path("version").isIntegralNumber()?actual.path("version").longValue():0;
                    if(prior!=0&&prior!=source.state.version())throw new Conflict("destination has a conflicting numeric version");
                }else if(actual!=null)throw new Conflict("destination row exists for source tombstone");
            }
            for(String table:TARGETS) {
                try(var p=c.prepareStatement("SELECT to_jsonb(t)::text FROM "+table+" t");var r=p.executeQuery()) {
                    while(r.next()) {
                        JsonNode row=V2BusinessRows.parse(r.getString(1));String key=keyFor(table,row);if(!expected.containsKey(key))throw new Conflict("destination contains key absent from complete source baseline: "+key);
                    }
                }
            }
        }
        if(mode==Mode.POPULATE_EMPTY) for(Row r:mapped)if(r.state.presence()==Presence.PRESENT) {
            ObjectIdentity o=V2BusinessRows.target(r.owner);String id="baseline:"+baselineId+":"+json(r.owner);
            V2BusinessRows.write(c,o,r.payload,r.state.version(),"BASELINE:"+baselineId,id,true,null);
        }
        // Stamp direct row versions only after every target key/payload passed reconciliation.
        for(Row r:mapped)if(r.state.presence()==Presence.PRESENT) {
            ObjectIdentity o=V2BusinessRows.target(r.owner);var args=new ArrayList<Object>();args.add(r.state.version());args.addAll(V2BusinessRows.keys(o));
            one(execute(c,"UPDATE "+o.entity()+" SET version=? WHERE "+V2BusinessRows.predicate(o)+" AND (version IS NULL OR version=?)",versionArgs(r.state.version(),V2BusinessRows.keys(o)).toArray()),"baseline version backfill "+json(o));
        }
    }
    private static List<Object> versionArgs(long version,List<Object> keys) {List<Object> a=new ArrayList<>();a.add(version);a.addAll(keys);a.add(version);return a;}
    private static JsonNode wantedWithVersion(ObjectIdentity o,Row r) {ObjectNode n=(ObjectNode)V2BusinessRows.business(o,r.payload,false);n.put("version",r.state.version());return n;}
    private static String keyFor(String table,JsonNode row) {
        ObjectNode key=mapper().createObjectNode();switch(table){case "client.customers"->key.set("id",row.get("id"));case "client.addresses"->{key.set("customer_id",row.get("customer_id"));key.set("address_type",row.get("address_type"));}case "operational.contact_numbers"->{key.set("customer_id",row.get("customer_id"));key.set("phone_type",row.get("phone_type"));}case "operational.products"->key.set("name",row.get("name"));case "operational.orders"->key.set("id",row.get("id"));case "operational.order_items"->{key.set("order_id",row.get("order_id"));key.set("product_name",row.get("product_name"));}default->throw new Conflict("unknown target");}
        Map<String,TypedValue> values=new TreeMap<>();key.fields().forEachRemaining(e->values.put(e.getKey(),new TypedValue(Set.of("id","customer_id","order_id").contains(e.getKey())?"int64":"text",e.getValue().asText())));return json(new ObjectIdentity(table,values));
    }
    private static void activate(Connection c,String lane,Baseline baseline,List<Row> sourceRows)throws SQLException {
        Baseline signed=baseline;List<BaselineOwner> owners=new ArrayList<>();
        for(Row r:sourceRows)owners.add(signBaselineOwner(new BaselineOwner(VERSION,SCHEMA_VERSION,RecordType.BASELINE_OWNER,signed.baselineId(),signed.sourceDatabaseId(),signed.sourceIncarnation(),signed.captureScopeId(),signed.projectionGeneration(),r.owner,r.state,r.payload,signed.boundary().position())));
        verifyBaseline(signed,owners);
        execute(c,"INSERT INTO cdc.v2_baseline(lane,baseline_id,payload) VALUES(?,?,?::jsonb)",lane,signed.baselineId(),json(signed));
        for(BaselineOwner o:owners) {
            String key=json(o.owner());execute(c,"INSERT INTO cdc.v2_baseline_owner(lane,baseline_id,owner_key,version,presence,payload) VALUES(?,?,?,?,?,?::jsonb)",lane,o.baselineId(),key,o.state().version(),o.state().presence().name(),json(o));
            execute(c,"INSERT INTO cdc.v2_owner_state(lane,owner_key,owner,version,presence,producer_type,producer_id,payload) VALUES(?,?,?::jsonb,?,?, 'BASELINE',?,?::jsonb)",lane,key,json(o.owner()),o.state().version(),o.state().presence().name(),o.baselineId(),json(V2BusinessRows.normalize(o.payload())));
            execute(c,"INSERT INTO cdc.v2_producer(lane,owner_key,version,presence,producer_type,producer_id,final_revision) VALUES(?,?,?,?, 'BASELINE',?,true)",lane,key,o.state().version(),o.state().presence().name(),o.baselineId());
        }
        execute(c,"INSERT INTO cdc.v2_namespace(singleton,lane,source_database_id,source_incarnation,capture_scope_id,projection_generation,baseline_id) VALUES(true,?,?,?,?,?,?)",lane,signed.sourceDatabaseId(),signed.sourceIncarnation(),signed.captureScopeId(),signed.projectionGeneration(),signed.baselineId());
    }
}
