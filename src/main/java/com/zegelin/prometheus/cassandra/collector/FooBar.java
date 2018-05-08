package com.zegelin.prometheus.cassandra.collector;

import com.zegelin.prometheus.cassandra.MBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.NamedObject;

import java.util.Map;
import java.util.function.Function;

@FunctionalInterface
// TODO: better name!
public interface FooBar {
    MBeanMetricFamilyCollector forMBean(final String name, final String help, final Map<String, String> labels, final NamedObject<?> mBean);
}
