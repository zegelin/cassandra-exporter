package com.zegelin.prometheus.cassandra.collector.dynamic;

import java.util.Map;


// TODO: better name
public interface GroupThing<X> {
    String name();
    String help();
    Map<? extends Map<String, String>, X> labeledMBeans();
}
