package com.example.cdc.apply;

import java.nio.file.Path;
import java.sql.DriverManager;

/** Offline reconciler CLI; activate destination metadata and write a projector baseline JSONL bundle. */
public final class SourceVersionReconcileMain {
    private SourceVersionReconcileMain(){}
    public static void main(String[] args)throws Exception {
        if(args.length!=6)throw new IllegalArgumentException("usage: SourceVersionReconcileMain <db-id> <incarnation> <scope> <generation> <VERIFY_EXISTING|POPULATE_EMPTY> <new-bundle.jsonl>");
        ConnectionProvider source=provider("SOURCE"),destination=provider("DESTINATION");
        var baseline=DestinationV2Reconciler.reconcile(source,destination,args[0],args[1],args[2],args[3],
                DestinationV2Reconciler.Mode.valueOf(args[4]),Path.of(args[5]));
        System.out.println("Activated baseline "+baseline.baselineId()+" at "+baseline.boundary().position()+"; bundle="+args[5]);
        System.out.println("Publish line 1 to cdc.source.baseline.control.v2 and remaining lines to cdc.tx.baseline.v2 before enabling v2 capture.");
    }
    private static ConnectionProvider provider(String prefix) {
        String url=required(prefix+"_JDBC_URL"),user=required(prefix+"_DB_USER"),password=required(prefix+"_DB_PASSWORD");
        return ()->DriverManager.getConnection(url,user,password);
    }
    private static String required(String key){String value=System.getenv(key);if(value==null||value.isBlank())throw new IllegalStateException("Missing "+key);return value;}
}
