package com.example.cdc.apply;

import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import java.sql.*;
import java.util.*;
import static com.example.cdc.protocol.ProtocolV2Codec.*;

final class V2Support {
    private V2Support() {}
    static String lane(String db,String inc,String scope,String generation) {return json(List.of(db,inc,scope,generation));}
    static String lane(Manifest m) {return lane(m.sourceDatabaseId(),m.sourceIncarnation(),m.captureScopeId(),m.projectionGeneration());}
    static String lane(Mutation m) {return lane(m.sourceDatabaseId(),m.sourceIncarnation(),m.captureScopeId(),m.projectionGeneration());}
    static String lane(Baseline b) {return lane(b.sourceDatabaseId(),b.sourceIncarnation(),b.captureScopeId(),b.projectionGeneration());}
    static PreparedStatement prepare(Connection c,String sql,Object...args)throws SQLException {
        var p=c.prepareStatement(sql);for(int i=0;i<args.length;i++)p.setObject(i+1,args[i]);return p;
    }
    static int execute(Connection c,String sql,Object...args)throws SQLException {try(var p=prepare(c,sql,args)){return p.executeUpdate();}}
    static String scalar(Connection c,String sql,Object...args)throws SQLException {try(var p=prepare(c,sql,args);var r=p.executeQuery()){return r.next()?r.getString(1):null;}}
    static boolean truthy(String value){return "t".equals(value)||"true".equalsIgnoreCase(value);}
    static void lease(Connection c,boolean exclusive)throws SQLException {
        try(var p=c.prepareStatement("SELECT "+(exclusive?"pg_advisory_xact_lock":"pg_advisory_xact_lock_shared")+"(772402,2)")) {p.execute();}
    }
    static void one(int count,String detail) {if(count!=1)throw new Conflict("expected exactly one row: "+detail+", actual="+count);}
    static class Conflict extends RuntimeException {Conflict(String text){super(text);}}
    static class Missing extends RuntimeException {Missing(String text){super(text);}}
    static void quarantine(Connection c,String lane,String tx,String reason,String evidence)throws SQLException {
        execute(c,"INSERT INTO cdc.v2_quarantine(lane,transaction_id,reason,evidence) VALUES(?,?,?,?::jsonb)",lane,tx,reason,evidence);
    }
    static void lockOwners(Connection c,String lane,Collection<OwnerIdentity> owners)throws SQLException {
        for(String key:lockKeys(lane,owners)) {
            execute(c,"INSERT INTO cdc.v2_logical_lock(lock_key) VALUES(?) ON CONFLICT DO NOTHING",key);
            scalar(c,"SELECT lock_key FROM cdc.v2_logical_lock WHERE lock_key=? FOR UPDATE",key);
        }
    }
    static List<String> lockKeys(String lane,Collection<OwnerIdentity> owners) {
        TreeSet<String> keys=new TreeSet<>();for(var o:owners)keys.add(lane+"|"+json(o));return List.copyOf(keys);
    }
}
