package com.zegelin.cassandra.exporter.collector.dynamic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.zegelin.jmx.NamedObject;
import com.zegelin.cassandra.exporter.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;

import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class FunctionalMetricFamilyCollector<T> extends MBeanGroupMetricFamilyCollector {
    private final String name, help;

    public interface LabeledObjectGroup<T> {
        String name();
        String help();
        Map<Labels, T> labeledObjects();
    }

    public interface CollectorFunction<T> extends Function<LabeledObjectGroup<T>, Stream<MetricFamily>> {}


    private final CollectorFunction<T> collectorFunction;

    private final Map<Labels, NamedObject<T>> labeledObjects;

    private final LabeledObjectGroup<T> objectGroup = new LabeledObjectGroup<T>() {
        @Override
        public String name() {
            return FunctionalMetricFamilyCollector.this.name;
        }

        @Override
        public String help() {
            return FunctionalMetricFamilyCollector.this.help;
        }

        @Override
        public Map<Labels, T> labeledObjects() {
            return Maps.transformValues(FunctionalMetricFamilyCollector.this.labeledObjects, o -> o.object);
        }
    };

    public FunctionalMetricFamilyCollector(final String name, final String help,
                                           final Map<Labels, NamedObject<T>> labeledObjects,
                                           final CollectorFunction<T> collectorFunction) {
        this.name = name;
        this.help = help;
        this.labeledObjects = ImmutableMap.copyOf(labeledObjects);
        this.collectorFunction = collectorFunction;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof FunctionalMetricFamilyCollector)) {
            throw new IllegalStateException();
        }

        final FunctionalMetricFamilyCollector<T> other = (FunctionalMetricFamilyCollector<T>) rawOther;

        final Map<Labels, NamedObject<T>> newLabeledObjects = new HashMap<>(labeledObjects);
        for (final Map.Entry<Labels, NamedObject<T>> entry : other.labeledObjects.entrySet()) {
            newLabeledObjects.merge(entry.getKey(), entry.getValue(), (o1, o2) -> {throw new IllegalStateException(String.format("Object %s and %s cannot be merged, yet their labels are the same.", o1, o2));});
        }

        return new FunctionalMetricFamilyCollector<>(name, help, newLabeledObjects, collectorFunction);
    }

    @Override
    public MBeanGroupMetricFamilyCollector removeMBean(final ObjectName objectName) {
        @SuppressWarnings("ConstantConditions") // ImmutableMap values cannot be null
        final Map<Labels, NamedObject<T>> metrics = ImmutableMap.copyOf(Maps.filterValues(this.labeledObjects, m -> !m.name.equals(objectName)));

        if (metrics.isEmpty())
            return null;

        return new FunctionalMetricFamilyCollector<>(this.name, this.help, metrics, collectorFunction);
    }

    @Override
    public Stream<MetricFamily> collect() {
        return collectorFunction.apply(objectGroup);
    }
}
