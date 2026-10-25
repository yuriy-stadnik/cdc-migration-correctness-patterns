package com.example.cdc.apply;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.security.MessageDigest;
import java.util.*;

/** Atomic schema installation with exact catalog validation. */
public final class SchemaMigrator {
    private static final List<String> FILES = List.of("V001__transaction_protocol.sql");
    private SchemaMigrator() {}
    static String script(int version) {
        try (var stream = SchemaMigrator.class.getResourceAsStream("/migration/" + FILES.get(version-1))) {
            if (stream == null) throw new IllegalStateException("migration not packaged");
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) { throw new IllegalStateException("cannot read migration",e); }
    }
    private static String checksum(String sql) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(sql.getBytes(StandardCharsets.UTF_8))); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }
    public static void migrate(ConnectionProvider connections) throws SQLException {
        try (Connection c=connections.open()) {
            c.setAutoCommit(false);
            try (Statement s=c.createStatement()) {
                s.execute("SELECT pg_advisory_xact_lock(hashtext('cdc-transaction-schema-v2'))");
                int current=0;
                if (exists(c,"cdc.schema_migration")) {
                    try (ResultSet rs=s.executeQuery("SELECT version,name,checksum FROM cdc.schema_migration ORDER BY version")) {
                        while(rs.next()) {
                            int v=rs.getInt(1);
                            if (v != ++current || v>FILES.size() || !FILES.get(v-1).equals(rs.getString(2))
                                    || !checksum(script(v)).equals(rs.getString(3))) throw new SQLException("migration history/checksum drift");
                        }
                    }
                    if(current==0) throw new SQLException("empty recorded migration history");
                    validate(c,current);
                } else {
                    boolean existing=false;
                    try(ResultSet rs=s.executeQuery("SELECT EXISTS(SELECT 1 FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname IN ('client','operational','cdc') AND r.relkind IN ('r','v'))")) {rs.next();existing=rs.getBoolean(1);}
                    if(existing) validate(c,1); else s.execute(script(1));
                    s.execute("CREATE TABLE cdc.schema_migration(version INTEGER PRIMARY KEY,name TEXT NOT NULL,checksum TEXT NOT NULL,installed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())");
                    record(c,1); current=1;
                }
                for(int v=current+1;v<=FILES.size();v++) {s.execute(script(v));record(c,v);}
                validate(c,FILES.size());
                c.commit();
            } catch(Exception e) {c.rollback();if(e instanceof SQLException se)throw se;throw e;}
        }
    }
    private static boolean exists(Connection c,String table) throws SQLException {
        try(var p=c.prepareStatement("SELECT to_regclass(?) IS NOT NULL")){p.setString(1,table);try(var r=p.executeQuery()){r.next();return r.getBoolean(1);}}
    }
    private static void record(Connection c,int version) throws SQLException {
        try(var p=c.prepareStatement("INSERT INTO cdc.schema_migration(version,name,checksum) VALUES(?,?,?)")) {
            p.setInt(1,version);p.setString(2,FILES.get(version-1));p.setString(3,checksum(script(version)));p.executeUpdate();
        }
    }
    /** Construct the expected catalog in transaction-local scratch schemas, then roll it back. */
    private static void validate(Connection c,int version) throws SQLException {
        Set<String> actual=catalog(c,"client","operational","cdc");
        Savepoint save=c.setSavepoint();
        String prefix="check_"+UUID.randomUUID().toString().replace("-","")+"_";
        Set<String> expected;
        try(Statement s=c.createStatement()) {
            for(int v=1;v<=version;v++) {
                String sql=script(v);
                for(String schema:List.of("client","operational","cdc")) sql=sql.replaceAll("\\b"+schema+"\\b",prefix+schema);
                s.execute(sql);
            }
            expected=catalog(c,prefix+"client",prefix+"operational",prefix+"cdc");
        } finally {c.rollback(save);c.releaseSavepoint(save);}
        Set<String> normalized=new TreeSet<>();for(String item:expected)normalized.add(item.replace(prefix,""));
        if(!actual.equals(normalized)) {
            Set<String> missing=new TreeSet<>(normalized);missing.removeAll(actual);
            Set<String> extra=new TreeSet<>(actual);extra.removeAll(normalized);
            throw new SQLException("schema drift; missing="+missing.stream().limit(3).toList()+" unexpected="+extra.stream().limit(3).toList());
        }
    }
    private static Set<String> catalog(Connection c,String... schemas) throws SQLException {
        // Includes columns/defaults, constraints (including deferred FKs), indexes, triggers, views and functions.
        String sql="""
          WITH objects AS (SELECT r.oid,r.relname,r.relkind,n.nspname FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
            WHERE n.nspname=ANY(?) AND r.relname NOT IN ('schema_migration','schema_migration_pkey') AND r.relkind IN ('r','v'))
          SELECT 'relation|'||nspname||'.'||relname||'|'||relkind::text FROM objects
          UNION ALL SELECT 'column|'||o.nspname||'.'||o.relname||'|'||a.attname||'|'||format_type(a.atttypid,a.atttypmod)||'|'||a.attnotnull||'|'||coalesce(pg_get_expr(d.adbin,d.adrelid),'')
            FROM objects o JOIN pg_attribute a ON a.attrelid=o.oid AND a.attnum>0 AND NOT a.attisdropped
            LEFT JOIN pg_attrdef d ON d.adrelid=o.oid AND d.adnum=a.attnum
          UNION ALL SELECT 'constraint|'||o.nspname||'.'||o.relname||'|'||x.conname||'|'||pg_get_constraintdef(x.oid)
            FROM objects o JOIN pg_constraint x ON x.conrelid=o.oid
          UNION ALL SELECT 'index|'||pg_get_indexdef(i.indexrelid) FROM objects o JOIN pg_index i ON i.indrelid=o.oid
          UNION ALL SELECT 'trigger|'||pg_get_triggerdef(t.oid) FROM objects o JOIN pg_trigger t ON t.tgrelid=o.oid WHERE NOT t.tgisinternal
          UNION ALL SELECT 'view|'||nspname||'.'||relname||'|'||pg_get_viewdef(oid) FROM objects WHERE relkind='v'
          UNION ALL SELECT 'function|'||pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=ANY(?)
          """;
        Set<String> result=new TreeSet<>();try(var p=c.prepareStatement(sql)) {
            var array=c.createArrayOf("text",schemas);p.setArray(1,array);p.setArray(2,array);
            try(var r=p.executeQuery()){while(r.next())result.add(r.getString(1));}finally{array.free();}
        }return result;
    }
}
