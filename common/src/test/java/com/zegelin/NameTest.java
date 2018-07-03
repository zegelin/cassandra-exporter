package com.zegelin;

import javax.management.MalformedObjectNameException;

public class NameTest {

    public static void main(String[] args) throws MalformedObjectNameException {
//        final ObjectName name = ObjectName.getInstance("org.apache.cassandra.metrics:type=ColumnFamily,keyspace=stress,scope=table6,name=MeanRowSize");
//        final ObjectName pattern = ObjectName.getInstance("org.apache.cassandra.metrics:type=\"ColumnFamily\",*");
//
//        pattern.apply(name);

        final String scope = "ChunkCache".replaceAll("Cache", "").toLowerCase();



    }
}
