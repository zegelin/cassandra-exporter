package com.zegelin.prometheus.cassandra.collector;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.zegelin.prometheus.cassandra.MBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.NamedObject;

import javax.management.ObjectName;
import java.util.Map;
import java.util.function.Function;

public abstract class NamedObjectCollector<T> extends SimpleCollector<NamedObject<T>> {
    public NamedObjectCollector(final String name, final String help,
                         final Constructor<NamedObject<T>> constructor,
                         final Map<ImmutableMap<String, String>, NamedObject<T>> metrics) {
        super(name, help, constructor, metrics);
    }

    @Override
    public MBeanMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        @SuppressWarnings("ConstantConditions") // ImmutableMap values cannot be null
        final Map<ImmutableMap<String, String>, NamedObject<T>> metrics = ImmutableMap.copyOf(Maps.filterValues(this.metrics, m -> !m.name.equals(mBeanName)));

        if (metrics.isEmpty())
            return null;

        return this.constructor.instantiate(this.name, this.help, metrics);
    }
}
