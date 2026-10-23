package com.example.cdc.apply;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class SchemaMigratorPostgresTest {
    @Test void concurrentFreshInstallIsIdempotentAndDriftIsRejected() throws Exception {
        try(var pg=EmbeddedPostgres.builder().start()) {
            ConnectionProvider connections=()->pg.getPostgresDatabase().getConnection();
            var workers=Executors.newFixedThreadPool(3);
            try {
                var installs=new ArrayList<Future<?>>();
                for(int i=0;i<3;i++)installs.add(workers.submit(()->{SchemaMigrator.migrate(connections);return null;}));
                for(var install:installs)install.get();
            } finally {workers.shutdownNow();}
            try(Connection c=connections.open();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT count(*),max(version) FROM cdc.schema_migration")) {
                assertTrue(r.next());assertEquals(1,r.getInt(1));assertEquals(1,r.getInt(2));
            }
            try(Connection c=connections.open();Statement s=c.createStatement()){s.execute("ALTER TABLE client.customers ADD COLUMN drift_marker text");}
            assertThrows(SQLException.class,()->SchemaMigrator.migrate(connections));
        }
    }
}
