package com.zegelin;
//
//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.Session;
//import org.slf4j.LoggerFactory;
//
//public class LotsOTables {
//
//    public static void main(String[] args) {
//        LoggerFactory.getLogger("hello");
//
//        final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
//
//        final Session session = cluster.connect();
//
//        session.execute("CREATE KEYSPACE lotsotables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
//
//        for (int i = 0; i < 1000; i++) {
//            session.execute(String.format("CREATE TABLE lotsotables.table%d (name text PRIMARY KEY);", i));
//        }
//
//    }
//}
