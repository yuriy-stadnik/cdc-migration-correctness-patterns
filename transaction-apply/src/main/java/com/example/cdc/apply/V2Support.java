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
    static PreparedStatement prepare(Connection c,String sql,Object...args)throws SQLException {
        var p=c.prepareStatement(sql);for(int i=0;i<args.length;i++)p.setObject(i+1,args[i]);return p;
    }
    static int execute(Connection c,String sql,Object...args)throws SQLException {try(var p=prepare(c,sql,args)){return p.executeUpdate();}}
    static String scalar(Connection c,String sql,Object...args)throws SQLException {try(var p=prepare(c,sql,args);var r=p.executeQuery()){return r.next()?r.getString(1):null;}}
    static void one(int count,String detail) {if(count!=1)throw new Conflict("expected exactly one row: "+detail+", actual="+count);}
    static class Conflict extends RuntimeException {Conflict(String text){super(text);}}
    static class Missing extends RuntimeException {Missing(String text){super(text);}}
}
