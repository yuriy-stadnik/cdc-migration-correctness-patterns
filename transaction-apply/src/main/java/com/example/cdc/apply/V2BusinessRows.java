package com.example.cdc.apply;

import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;
import static com.example.cdc.apply.V2Support.*;
import static com.example.cdc.protocol.ProtocolV2Codec.*;

/** Whitelisted table/column SQL; numeric versions are read from the physical business rows. */
final class V2BusinessRows {
    static final Map<String,List<String>> COLUMNS=Map.of(
        "client.customers",List.of("id","first_name","last_name","email","status","created_at","updated_at"),
        "operational.products",List.of("name","category","current_price"),
        "operational.orders",List.of("id","customer_id","order_date","status"),
        "operational.order_items",List.of("order_id","product_name","quantity","price_at_purchase"));
    static ObjectIdentity target(OwnerIdentity owner) {
        String table=switch(owner.collection()) {
            case "inventory.customers"->"client.customers";case "inventory.products"->"operational.products";
            case "inventory.orders"->"operational.orders";case "inventory.order_items"->"operational.order_items";default->null;
        };return table==null?null:new ObjectIdentity(table,owner.primaryKey());
    }
    static JsonNode parse(String json) {try{return mapper().readTree(json);}catch(Exception e){throw new Conflict("invalid stored JSON");}}
    static JsonNode normalize(JsonNode payload) {
        if(payload==null||payload.isNull())return null;
        ObjectNode n=payload.deepCopy();
        for(String f:List.of("created_at","updated_at","order_date")) if(n.hasNonNull(f))n.put(f,OffsetDateTime.parse(n.get(f).asText()).toInstant().toString());
        for(String f:List.of("balance","unit_price","total_price","current_price","price_at_purchase"))if(n.hasNonNull(f))n.put(f,new BigDecimal(n.get(f).asText()));
        return n;
    }
    static JsonNode business(ObjectIdentity object,JsonNode image,boolean includeVersion) {
        if(image==null||image.isNull())return null;
        JsonNode normalized=normalize(image);ObjectNode result=mapper().createObjectNode();
        for(String column:COLUMNS.get(object.entity()))result.set(column,normalized.has(column)?normalized.get(column):mapper().nullNode());
        if(includeVersion)result.set("version",normalized.has("version")?normalized.get("version"):mapper().nullNode());
        return result;
    }
    static List<Object> keys(ObjectIdentity object) {
        List<Object> args=new ArrayList<>();new TreeMap<>(object.primaryKey()).values().forEach(v->args.add(v.type().equals("int64")?Long.valueOf(v.value()):v.value()));return args;
    }
    static String predicate(ObjectIdentity object) {return String.join(" AND ",new TreeMap<>(object.primaryKey()).keySet().stream().map(k->k+"=?").toList());}
    static JsonNode read(Connection c,ObjectIdentity object,boolean lock)throws SQLException {
        String value=scalar(c,"SELECT to_jsonb(t)::text FROM "+object.entity()+" t WHERE "+predicate(object)+(lock?" FOR UPDATE":""),keys(object).toArray());
        return value==null?null:business(object,parse(value),true);
    }
    static void validate(Connection c,Mutation m)throws SQLException {
        JsonNode row=read(c,m.object(),true);
        if(m.expected().state().presence()==Presence.PRESENT) {
            if(row==null || !row.path("version").isIntegralNumber() || row.path("version").longValue()!=m.expected().state().version())
                throw new Conflict("business version disagrees with expected owner state: "+json(m.object()));
            if(!json(row).equals(json(business(m.object(),m.before(),true))))throw new Conflict("business payload disagrees with source before image");
        } else if(row!=null)throw new Conflict("expected absent business key already exists");
    }
    static void apply(Connection c,Mutation m)throws SQLException {
        if(m.operation()==Operation.DELETE) {
            List<Object> args=keys(m.object());args.add(m.expected().state().version());
            one(execute(c,"DELETE FROM "+m.object().entity()+" WHERE "+predicate(m.object())+" AND version=?",args.toArray()),"delete "+json(m.object()));return;
        }
        write(c,m.object(),m.after(),m.result().version(),m.transactionId(),m.idempotencyKey(),m.operation()==Operation.INSERT,m.expected().state().version());
    }
    static void write(Connection c,ObjectIdentity object,JsonNode payload,long version,String tx,String id,boolean insert,Long expected)throws SQLException {
        // jsonb_populate_record supplies PostgreSQL's exact numeric/timestamp column conversions.
        ObjectNode row=(ObjectNode)business(object,payload,false);row.put("version",version).put("source_tx_id",tx).put("idempotency_key",id);
        List<String> columns=new ArrayList<>(COLUMNS.get(object.entity()));columns.addAll(List.of("version","source_tx_id","idempotency_key"));
        if(insert) {
            String names=String.join(",",columns);
            one(execute(c,"INSERT INTO "+object.entity()+" ("+names+") SELECT "+names+" FROM jsonb_populate_record(NULL::"+object.entity()+",?::jsonb)",json(row)),"insert "+json(object));
        } else {
            String names=String.join(",",columns);
            List<Object> args=new ArrayList<>();args.add(json(row));args.addAll(keys(object));args.add(expected);
            one(execute(c,"UPDATE "+object.entity()+" SET ("+names+")=(SELECT "+names+" FROM jsonb_populate_record(NULL::"+object.entity()+",?::jsonb)) WHERE "+predicate(object)+" AND version=?",args.toArray()),"update "+json(object));
        }
    }
    static List<OwnerIdentity> parents(OwnerIdentity owner,JsonNode image) {
        if(image==null||image.isNull())return List.of();List<OwnerIdentity> result=new ArrayList<>();
        if(owner.collection().equals("inventory.orders") && image.hasNonNull("customer_id"))
            result.add(new OwnerIdentity(owner.sourceDatabaseId(),owner.sourceIncarnation(),"inventory.customers",Map.of("id",new TypedValue("int64",image.get("customer_id").asText()))));
        if(owner.collection().equals("inventory.order_items")) {
            result.add(new OwnerIdentity(owner.sourceDatabaseId(),owner.sourceIncarnation(),"inventory.orders",Map.of("id",new TypedValue("int64",image.get("order_id").asText()))));
            result.add(new OwnerIdentity(owner.sourceDatabaseId(),owner.sourceIncarnation(),"inventory.products",Map.of("name",new TypedValue("text",image.get("product_name").asText()))));
        }return result;
    }
}
