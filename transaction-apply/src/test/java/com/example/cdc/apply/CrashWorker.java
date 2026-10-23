package com.example.cdc.apply;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/** Isolated test JVM. Fault pauses wrap JDBC, never production locks or production fault switches. */
public final class CrashWorker {
    public static void main(String[] args)throws Exception {
        String url=args[0],phase=args[1];
        if(phase.equals("before-claim")) pause(phase);
        ConnectionProvider provider=()-> {
            Connection c=DriverManager.getConnection(url,"postgres","");
            return (Connection)Proxy.newProxyInstance(Connection.class.getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,values)->{
                try {
                    Object result=method.invoke(c,values);
                    if(phase.equals("after-claim") && method.getName().equals("prepareStatement")
                            && ((String)values[0]).contains("ORDER BY source_commit_sequence NULLS LAST")) {
                        PreparedStatement statement=(PreparedStatement)result;
                        return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),new Class<?>[]{PreparedStatement.class},(p,m,v)->{
                            try {Object answer=m.invoke(statement,v);if(m.getName().equals("executeQuery"))pause(phase);return answer;}
                            catch(InvocationTargetException e){throw e.getCause();}
                        });
                    }
                    return result;
                } catch(InvocationTargetException e){throw e.getCause();}
            });
        };
        var result=new TransactionApplyService(provider,"subprocess-"+phase).applyNext().orElseThrow();
        if(!result.applied())throw new AssertionError(result);
        pause("after-commit");
    }
    private static void pause(String marker)throws Exception {System.out.println("FAULT_POINT:"+marker);System.out.flush();if(System.in.read()<0)throw new IllegalStateException("test controller disconnected");}
}
