package com.example.cdc.apply;

import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import com.fasterxml.jackson.databind.JsonNode;
import java.sql.*;
import java.util.*;
import static com.example.cdc.protocol.ProtocolV2.*;
import static com.example.cdc.protocol.ProtocolV2Codec.*;
import static com.example.cdc.apply.V2Support.*;
import static com.example.cdc.apply.TransactionApplyService.*;

/** Source-owned numeric versions, durable staging, and a single atomic destination commit. */
final class V2TransactionApplyService {
    private final ConnectionProvider connections;
    V2TransactionApplyService(ConnectionProvider connections){this.connections=connections;}
    record Work(String lane,String tx) {}
    record State(OwnerState state,ProducerReference producer,JsonNode payload) {}

    StageResult stage(String topic,String value)throws SQLException {
        if(DLQ_TOPIC.equals(topic)) {
            JsonNode diagnostic=V2BusinessRows.parse(value);
            if(!diagnostic.path("protocolVersion").isInt()||diagnostic.path("protocolVersion").intValue()!=2
                    ||!diagnostic.path("recordType").asText().equals("CAPTURE_FAILURE"))throw new Conflict("unsupported v2 diagnostic");
            try(Connection c=connections.open()){quarantine(c,null,null,"source capture failure",value);}return StageResult.QUARANTINED;
        }
        // Baseline activation is an administrator/source reconciliation operation, never a Kafka assertion.
        Mutation mutation=null;Manifest manifest=null;
        try {
            mutation=MUTATION_TOPIC.equals(topic)?readMutation(topic,value):null;
            manifest=CONTROL_TOPIC.equals(topic)?readManifest(topic,value):null;
            if(mutation==null&&manifest==null)throw new Conflict("unsupported v2 input; baseline requires controlled reconciliation");
        } catch(IllegalArgumentException|Conflict malformed) {
            try(Connection c=connections.open()){quarantine(c,null,null,malformed.getMessage(),value);}
            return StageResult.QUARANTINED;
        }
        Work work=mutation!=null?new Work(lane(mutation),mutation.transactionId()):new Work(lane(manifest),manifest.transactionId());
        try(Connection c=connections.open()) {
            c.setAutoCommit(false);
            try {
                execute(c,"INSERT INTO cdc.v2_transaction(lane,transaction_id) VALUES(?,?) ON CONFLICT DO NOTHING",work.lane,work.tx);
                String status=scalar(c,"SELECT state FROM cdc.v2_transaction WHERE lane=? AND transaction_id=? FOR UPDATE",work.lane,work.tx);
                boolean duplicate=false;
                try {
                    if("FAILED".equals(status))throw new Conflict("transaction is quarantined");
                    if(mutation!=null) {
                        String prior=scalar(c,"SELECT payload::text FROM cdc.v2_mutation WHERE lane=? AND transaction_id=? AND sequence=?",work.lane,work.tx,mutation.mutationSequence());
                        if(prior!=null) {
                            Mutation stored=readMutation(MUTATION_TOPIC,prior);
                            if(!json(stored).equals(json(mutation)))throw new Conflict("immutable mutation sequence conflict");
                            duplicate=true;
                        } else {
                            if("APPLIED".equals(status))throw new Conflict("new descriptor after applied receipt");
                            execute(c,"INSERT INTO cdc.v2_mutation(lane,transaction_id,sequence,mutation_id,payload) VALUES(?,?,?,?,?::jsonb)",work.lane,work.tx,mutation.mutationSequence(),mutation.mutationId(),json(mutation));
                        }
                    } else {
                        String prior=scalar(c,"SELECT manifest::text FROM cdc.v2_transaction WHERE lane=? AND transaction_id=?",work.lane,work.tx);
                        if(prior!=null) {
                            if(!json(readManifest(CONTROL_TOPIC,prior)).equals(json(manifest)))throw new Conflict("immutable manifest conflict");
                            duplicate=true;
                        } else execute(c,"UPDATE cdc.v2_transaction SET manifest=?::jsonb,mutation_set_hash=? WHERE lane=? AND transaction_id=?",json(manifest),manifest.mutationSetHash(),work.lane,work.tx);
                    }
                    Manifest complete=manifest(c,work);
                    if(complete!=null) {
                        List<Mutation> mutations=mutations(c,work);
                        if(mutations.size()>complete.destinationDescriptorCount())throw new Conflict("excess transaction mutations");
                        if(mutations.size()==complete.destinationDescriptorCount()) {
                            verify(complete,mutations);
                            if("APPLIED".equals(status)) {
                                if(!receipt(c,work,complete))throw new Conflict("APPLIED transaction has no matching receipt");
                            } else {
                                for(Dependency d:dependencies(complete,mutations))execute(c,"""
                                    INSERT INTO cdc.v2_dependency(lane,transaction_id,dependency_key,dependency_type,owner_key,required_version,required_presence,producer_type,producer_id)
                                    VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING
                                    """,work.lane,work.tx,json(d),d.type().name(),json(d.owner()),d.requiredState().version(),d.requiredState().presence().name(),d.producer().type().name(),d.producer().id());
                                if(cycle(c,work))throw new Conflict("dependency cycle");
                                execute(c,"UPDATE cdc.v2_transaction SET state='READY',last_error=NULL WHERE lane=? AND transaction_id=?",work.lane,work.tx);
                            }
                        }
                    }
                    c.commit();return duplicate?StageResult.DUPLICATE:StageResult.STAGED;
                } catch(IllegalArgumentException|Conflict corrupt) {
                    execute(c,"UPDATE cdc.v2_transaction SET state='FAILED',last_error=? WHERE lane=? AND transaction_id=? AND state<>'APPLIED'",corrupt.getMessage(),work.lane,work.tx);
                    quarantine(c,work.lane,work.tx,corrupt.getMessage(),value);c.commit();return StageResult.QUARANTINED;
                }
            }catch(Exception e){c.rollback();if(e instanceof SQLException se)throw se;throw e;}
        }
    }
    Optional<ApplyResult> applyNext()throws SQLException {
        Work work=null;Manifest manifest=null;
        try(Connection c=connections.open()) {
            c.setAutoCommit(false);
            try {
                lease(c,false);
                try(var p=prepare(c,"SELECT lane,transaction_id FROM cdc.v2_transaction WHERE state IN ('READY','WAITING') ORDER BY attempts,transaction_id FOR UPDATE SKIP LOCKED LIMIT 1");var r=p.executeQuery()) {
                    if(!r.next()){c.commit();return Optional.empty();}work=new Work(r.getString(1),r.getString(2));
                }
                manifest=manifest(c,work);List<Mutation> mutations=mutations(c,work);verify(manifest,mutations);
                activeBaseline(c,manifest);
                if(receipt(c,work,manifest)){mark(c,work,"APPLIED",null);c.commit();return Optional.of(new ApplyResult(work.tx,true,"matching immutable receipt"));}
                List<OwnerIdentity> locks=new ArrayList<>();manifest.sourceTransitions().forEach(t->locks.add(t.owner()));
                for(Dependency d:dependencies(manifest,mutations))locks.add(d.owner());
                for(var t:manifest.sourceTransitions()) {
                    locks.addAll(V2BusinessRows.parents(t.owner(),t.before()));locks.addAll(V2BusinessRows.parents(t.owner(),t.after()));
                }
                lockOwners(c,work.lane,locks);
                // Staging uses the same transaction-row lock; verify persisted content again after all key locks.
                manifest=manifest(c,work);mutations=mutations(c,work);verify(manifest,mutations);activeBaseline(c,manifest);
                if(cycle(c,work))throw new Conflict("dependency cycle");
                for(Dependency d:dependencies(manifest,mutations))verifyProducer(c,manifest,d.owner(),d.requiredState(),d.producer());
                for(OwnerTransition transition:manifest.sourceTransitions())validateState(c,manifest,transition);
                for(Mutation mutation:mutations)V2BusinessRows.validate(c,mutation);
                validateParents(c,manifest,mutations);
                for(Dependency d:dependencies(manifest,mutations))if(d.type()==DependencyType.EXISTS||d.type()==DependencyType.NOT_EXISTS)validatePresence(c,manifest,d);
                // Deferred FK constraints see the complete final transaction. Logical locks already cover every parent.
                for(Mutation mutation:mutations)V2BusinessRows.apply(c,mutation);
                for(OwnerTransition transition:manifest.sourceTransitions())advance(c,manifest,transition);
                execute(c,"INSERT INTO cdc.v2_receipt(lane,transaction_id,mutation_set_hash,mutation_count) VALUES(?,?,?,?)",work.lane,work.tx,manifest.mutationSetHash(),mutations.size());
                execute(c,"INSERT INTO cdc.v2_outbox(event_id,lane,transaction_id,payload) VALUES(?,?,?,?::jsonb)",work.lane+"|"+work.tx,work.lane,work.tx,json(Map.of("transactionId",work.tx,"mutationSetHash",manifest.mutationSetHash(),"mutationCount",mutations.size())));
                mark(c,work,"APPLIED",null);c.commit();return Optional.of(new ApplyResult(work.tx,true,"committed atomically"));
            } catch(Missing missing) {
                // No business/owner writes precede the dependency and presence checks.
                if(work!=null){mark(c,work,"WAITING",missing.getMessage());c.commit();return Optional.of(new ApplyResult(work.tx,false,"WAITING: "+missing.getMessage()));}
                c.rollback();throw missing;
            } catch(Exception e) {
                try{c.rollback();}catch(SQLException secondary){e.addSuppressed(secondary);}
                if(work==null){if(e instanceof SQLException se)throw se;throw e;}
                return Optional.of(failure(work,manifest,e));
            }
        }
    }
    private ApplyResult failure(Work work,Manifest manifest,Exception failure)throws SQLException {
        try(Connection c=connections.open()) {
            c.setAutoCommit(false);
            try {
                scalar(c,"SELECT state FROM cdc.v2_transaction WHERE lane=? AND transaction_id=? FOR UPDATE",work.lane,work.tx);
                if(manifest!=null&&receipt(c,work,manifest)){c.commit();return new ApplyResult(work.tx,true,"matching receipt after uncertain commit");}
                String sql=failure instanceof SQLException s?s.getSQLState():null;
                boolean retry=sql!=null&&(sql.startsWith("08")||sql.startsWith("40")||sql.startsWith("55")||sql.equals("57P01"));
                mark(c,work,retry?"WAITING":"FAILED",failure.getMessage());
                if(!retry)quarantine(c,work.lane,work.tx,failure.getMessage()==null?failure.toString():failure.getMessage(),null);
                c.commit();return new ApplyResult(work.tx,false,(retry?"RETRY: ":"CONFLICT: ")+failure.getMessage());
            }catch(Exception e){c.rollback();if(e instanceof SQLException se)throw se;throw e;}
        }
    }
    private static void mark(Connection c,Work work,String status,String error)throws SQLException {
        execute(c,"UPDATE cdc.v2_transaction SET state=?,last_error=?,attempts=attempts+1 WHERE lane=? AND transaction_id=?",status,error,work.lane,work.tx);
    }
    private static Manifest manifest(Connection c,Work work)throws SQLException {
        String value=scalar(c,"SELECT manifest::text FROM cdc.v2_transaction WHERE lane=? AND transaction_id=?",work.lane,work.tx);
        return value==null?null:readManifest(CONTROL_TOPIC,value);
    }
    private static List<Mutation> mutations(Connection c,Work work)throws SQLException {
        List<Mutation> result=new ArrayList<>();try(var p=prepare(c,"SELECT payload::text FROM cdc.v2_mutation WHERE lane=? AND transaction_id=? ORDER BY sequence",work.lane,work.tx);var r=p.executeQuery()) {while(r.next())result.add(readMutation(MUTATION_TOPIC,r.getString(1)));}return result;
    }
    private static void verify(Manifest manifest,List<Mutation> mutations) {
        if(manifest==null)throw new Conflict("missing immutable manifest");verifyManifest(manifest,mutations);
        for(Mutation m:mutations) {
            var t=manifest.sourceTransitions().stream().filter(x->x.owner().equals(m.owner())).findFirst().orElseThrow();
            if(!json(V2BusinessRows.normalize(t.before())).equals(json(V2BusinessRows.normalize(m.before())))
                    ||!json(V2BusinessRows.normalize(t.after())).equals(json(V2BusinessRows.normalize(m.after()))))throw new Conflict("mutation payload differs from source owner transition");
        }
    }
    private static List<Dependency> dependencies(Manifest m,List<Mutation> mutations) {
        TreeMap<String,Dependency> all=new TreeMap<>();
        for(var t:m.sourceTransitions()) {
            var d=new Dependency(DependencyType.WRITE_PREDECESSOR,t.owner(),t.expected().state(),t.expected().producer(),"source row predecessor");all.put(json(d),d);
        }
        for(var mutation:mutations)for(var d:mutation.dependencies())all.put(json(d),d);return new ArrayList<>(all.values());
    }
    private static boolean cycle(Connection c,Work work)throws SQLException {
        return "true".equals(scalar(c,"""
            WITH RECURSIVE edges(id) AS (
              SELECT producer_id FROM cdc.v2_dependency WHERE lane=? AND transaction_id=? AND producer_type='TRANSACTION'
              UNION SELECT d.producer_id FROM cdc.v2_dependency d JOIN edges e ON d.transaction_id=e.id WHERE d.lane=? AND d.producer_type='TRANSACTION')
            SELECT EXISTS(SELECT 1 FROM edges WHERE id=?)
            """,work.lane,work.tx,work.lane,work.tx));
    }
    private static Baseline activeBaseline(Connection c,Manifest m)throws SQLException {
        String lane=lane(m);
        String active=scalar(c,"SELECT baseline_id FROM cdc.v2_namespace WHERE lane=?",lane);
        if(active==null)throw new Missing("no locally reconciled active baseline for source/scope/generation");
        if(!active.equals(m.baselineId()))throw new Conflict("wrong active baseline");
        String value=scalar(c,"SELECT payload::text FROM cdc.v2_baseline WHERE lane=? AND baseline_id=?",lane,active);
        if(value==null)throw new Conflict("active baseline evidence missing");Baseline b=readBaseline(CONTROL_TOPIC,value);
        if(position(m.boundary().commitPosition()).compareTo(position(b.boundary().position()))<=0)throw new Conflict("source transaction is not after reconciled baseline boundary");
        return b;
    }
    static java.math.BigInteger position(String s) {
        if(s.contains("/")){var a=s.split("/",-1);if(a.length!=2)throw new Conflict("invalid source position");return new java.math.BigInteger(a[0],16).shiftLeft(32).add(new java.math.BigInteger(a[1],16));}
        return new java.math.BigInteger(s);
    }
    private static boolean receipt(Connection c,Work w,Manifest m)throws SQLException {
        try(var p=prepare(c,"SELECT mutation_set_hash,mutation_count FROM cdc.v2_receipt WHERE lane=? AND transaction_id=?",w.lane,w.tx);var r=p.executeQuery()) {
            if(!r.next())return false;
            if(!r.getString(1).equals(m.mutationSetHash())||r.getInt(2)!=m.destinationDescriptorCount())throw new Conflict("immutable applied receipt mismatch");return true;
        }
    }
    private static void verifyProducer(Connection c,Manifest m,OwnerIdentity owner,OwnerState required,ProducerReference ref)throws SQLException {
        String lane=lane(m),key=json(owner);
        if(ref.type()==ProducerType.BASELINE) {
            if(!ref.id().equals(m.baselineId()))throw new Conflict("foreign baseline predecessor");
            String payload=scalar(c,"SELECT payload::text FROM cdc.v2_baseline_owner WHERE lane=? AND baseline_id=? AND owner_key=?",lane,ref.id(),key);
            if(required.presence()==Presence.ABSENT) {
                if(payload!=null)throw new Conflict("absence contradicts reconciled baseline");return;
            }
            if(payload==null||!readBaselineOwner(BASELINE_TOPIC,payload).state().equals(required))throw new Conflict("baseline predecessor state mismatch");
        } else {
            String receiptHash=scalar(c,"SELECT mutation_set_hash FROM cdc.v2_receipt WHERE lane=? AND transaction_id=?",lane,ref.id());
            if(receiptHash==null) {
                String status=scalar(c,"SELECT state FROM cdc.v2_transaction WHERE lane=? AND transaction_id=?",lane,ref.id());
                if("FAILED".equals(status)||"APPLIED".equals(status))throw new Conflict("predecessor failed or missing applied receipt");
                throw new Missing("unapplied predecessor "+ref.id());
            }
            Manifest producer=manifest(c,new Work(lane,ref.id()));
            if(producer==null||!receiptHash.equals(producer.mutationSetHash()))throw new Conflict("predecessor immutable receipt mismatch");
            if(position(producer.boundary().commitPosition()).compareTo(position(m.boundary().commitPosition()))>=0)throw new Conflict("predecessor source commit is not earlier");
            OwnerTransition produced=producer.sourceTransitions().stream().filter(t->t.owner().equals(owner)
                    &&t.result().equals(required)).findFirst().orElse(null);
            if(produced==null)throw new Conflict("predecessor receipt does not produce required source revision");
        }
        try(var p=prepare(c,"SELECT presence,producer_type,producer_id,final_revision FROM cdc.v2_producer WHERE lane=? AND owner_key=? AND version=?",lane,key,required.version());var r=p.executeQuery()) {
            if(!r.next()||!r.getString(1).equals(required.presence().name())||!r.getString(2).equals(ref.type().name())||!r.getString(3).equals(ref.id())||!r.getBoolean(4))throw new Conflict("missing or mismatched final source producer version");
        }
    }
    static State state(Connection c,String lane,OwnerIdentity owner)throws SQLException {
        try(var p=prepare(c,"SELECT version,presence,producer_type,producer_id,payload::text FROM cdc.v2_owner_state WHERE lane=? AND owner_key=?",lane,json(owner));var r=p.executeQuery()) {
            if(!r.next())return null;return new State(new OwnerState(Presence.valueOf(r.getString(2)),r.getLong(1)),new ProducerReference(ProducerType.valueOf(r.getString(3)),r.getString(4)),r.getString(5)==null?null:V2BusinessRows.parse(r.getString(5)));
        }
    }
    private static void validateState(Connection c,Manifest m,OwnerTransition t)throws SQLException {
        State current=state(c,lane(m),t.owner());
        if(t.expected().state().presence()==Presence.ABSENT) {
            if(current!=null||scalar(c,"SELECT owner_key FROM cdc.v2_producer WHERE lane=? AND owner_key=? LIMIT 1",lane(m),json(t.owner()))!=null)throw new Conflict("first creation contradicts known owner lineage");
        } else if(current==null||!current.state.equals(t.expected().state())||!current.producer.equals(t.expected().producer()))throw new Conflict("current owner version/presence/producer disagrees with expected");
        if(current!=null&&!json(current.payload).equals(json(V2BusinessRows.normalize(t.before()))))throw new Conflict("owner payload disagrees with source before image");
    }
    private static void validatePresence(Connection c,Manifest m,Dependency d)throws SQLException {
        State current=state(c,lane(m),d.owner());boolean exists=d.type()==DependencyType.EXISTS;
        if(d.requiredState().presence()==Presence.ABSENT) {
            if(exists||current!=null)throw new Conflict("absence predicate contradicts lineage");
        } else {
            if(current==null||current.state.presence()!=(exists?Presence.PRESENT:Presence.DELETED)
                    ||d.requiredState().presence()!=current.state.presence()||current.state.version()<d.requiredState().version())throw new Conflict("parent presence predicate mismatch");
            String changed=scalar(c,"SELECT version FROM cdc.v2_producer WHERE lane=? AND owner_key=? AND version>? AND version<=? AND presence<>? LIMIT 1",lane(m),json(d.owner()),d.requiredState().version(),current.state.version(),current.state.presence().name());
            if(changed!=null)throw new Conflict("presence belongs to a different lifecycle");
        }
        ObjectIdentity target=V2BusinessRows.target(d.owner());
        if(target!=null) {
            JsonNode business=V2BusinessRows.read(c,target,true);
            if(exists?(business==null||!business.path("version").isIntegralNumber()||business.path("version").longValue()!=current.state.version()):business!=null)throw new Conflict("parent business/metadata presence disagreement");
        }
    }
    private static void validateParents(Connection c,Manifest m,List<Mutation> mutations)throws SQLException {
        Map<String,OwnerTransition> nets=new TreeMap<>();for(var t:m.sourceTransitions())nets.put(json(t.owner()),t);
        List<Dependency> deps=dependencies(m,mutations);
        for(var t:m.sourceTransitions()) {
            for(var parent:V2BusinessRows.parents(t.owner(),t.after())) {
                var same=nets.get(json(parent));
                if(same!=null){if(same.result().presence()!=Presence.PRESENT)throw new Conflict("final parent is deleted");}
                else if(deps.stream().noneMatch(d->d.type()==DependencyType.EXISTS&&d.owner().equals(parent)))throw new Conflict("missing explicit parent presence predicate");
            }
        }
        // Source and destination deferred foreign keys reject surviving children at commit.
    }
    private static void advance(Connection c,Manifest m,OwnerTransition t)throws SQLException {
        String lane=lane(m),key=json(t.owner());
        for(var r:t.revisions())execute(c,"INSERT INTO cdc.v2_producer(lane,owner_key,version,presence,producer_type,producer_id,final_revision) VALUES(?,?,?,?,?,?,?)",lane,key,r.after().version(),r.after().presence().name(),"TRANSACTION",m.transactionId(),r.after().equals(t.result()));
        if(t.expected().state().presence()==Presence.ABSENT)execute(c,"INSERT INTO cdc.v2_owner_state(lane,owner_key,owner,version,presence,producer_type,producer_id,payload) VALUES(?,?,?::jsonb,?,?, 'TRANSACTION',?,?::jsonb)",lane,key,json(t.owner()),t.result().version(),t.result().presence().name(),m.transactionId(),json(V2BusinessRows.normalize(t.after())));
        else one(execute(c,"UPDATE cdc.v2_owner_state SET version=?,presence=?,producer_type='TRANSACTION',producer_id=?,payload=?::jsonb WHERE lane=? AND owner_key=? AND version=? AND presence=? AND producer_type=? AND producer_id=?",t.result().version(),t.result().presence().name(),m.transactionId(),json(V2BusinessRows.normalize(t.after())),lane,key,t.expected().state().version(),t.expected().state().presence().name(),t.expected().producer().type().name(),t.expected().producer().id()),"advance source owner");
    }
}
