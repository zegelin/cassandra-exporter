package com.zegelin.prometheus.cassandra.collector.dynamic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.zegelin.prometheus.cassandra.MBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.NamedObject;
import com.zegelin.prometheus.domain.MetricFamily;

import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class FunctionCollector<T> implements MBeanMetricFamilyCollector, GroupThing<T> {
    private final String name, help;

    public interface CollectorFunction<T> extends Function<GroupThing<T>, Stream<MetricFamily<?>>> { }

    private final CollectorFunction<T> collectFunction;

    private final Map<? extends Map<String, String>, NamedObject<T>> labeledObjects;

    public FunctionCollector(final String name, final String help,
                             final Map<? extends Map<String, String>, NamedObject<T>> labeledObjects,
                             final CollectorFunction<T> collectFunction) {
        this.name = name;
        this.help = help;
        this.labeledObjects = ImmutableMap.copyOf(labeledObjects);
        this.collectFunction = collectFunction;
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    public String help() {
        return help;
    }

    @Override
    public Map<? extends Map<String, String>, T> labeledMBeans() {
        return Maps.transformValues(labeledObjects, o -> o.object);
    }

    @Override
    public MBeanMetricFamilyCollector merge(final MBeanMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof FunctionCollector)) {
            throw new IllegalStateException();
        }

        final FunctionCollector<T> other = (FunctionCollector<T>) rawOther;



        final HashMap<Map<String, String>, NamedObject<T>> labeledObjects = new HashMap<>(this.labeledObjects);
        for (final Map.Entry<? extends Map<String, String>, NamedObject<T>> entry : (other).labeledObjects.entrySet()) {
            labeledObjects.merge(entry.getKey(), entry.getValue(), (o1, o2) -> {throw new IllegalStateException();});
        }

        return new FunctionCollector<>(name, help, labeledObjects, collectFunction);
    }

    @Override
    public MBeanMetricFamilyCollector removeMBean(final ObjectName objectName) {
        @SuppressWarnings("ConstantConditions") // ImmutableMap values cannot be null
        final Map<Map<String, String>, NamedObject<T>> metrics = ImmutableMap.copyOf(Maps.filterValues(this.labeledObjects, m -> !m.name.equals(objectName)));

        if (metrics.isEmpty())
            return null;

        return new FunctionCollector<>(this.name, this.help, metrics, collectFunction);
    }

    @Override
    public Stream<MetricFamily<?>> collect() {
        return collectFunction.apply(this);
    }
}
