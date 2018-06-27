package com.zegelin.prometheus.cassandra.collector.dynamic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.jmx.NamedObject;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;

import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class FunctionCollector<T> implements MBeanGroupMetricFamilyCollector {
    private final String name, help;

    public interface LabeledMBeanGroup<T> {
        String name();
        String help();
        Map<Labels, T> labeledMBeans();
    }

    public interface CollectorFunction<T> extends Function<LabeledMBeanGroup<T>, Stream<? extends MetricFamily<?>>> {
        default Factory.Builder.CollectorConstructor asCollector() {
            return ((name, help, labels, mBean) -> new FunctionCollector<>(name, help, ImmutableMap.of(labels, mBean.cast()), this));
        }


//        default <V> CollectorFunction<V> compose(CollectorFunction<? super V> before) {
//            return null;
//        }
//
//        default <V> CollectorFunction<V> andThen(CollectorFunction<? extends V> after) {
//            return null;
//        }
    }

    private final CollectorFunction<T> collectFunction;

    private final Map<Labels, NamedObject<T>> labeledObjects;

    private final LabeledMBeanGroup<T> objectGroup = new LabeledMBeanGroup<T>() {
        @Override
        public String name() {
            return FunctionCollector.this.name;
        }

        @Override
        public String help() {
            return FunctionCollector.this.help;
        }

        @Override
        public Map<Labels, T> labeledMBeans() {
            return Maps.transformValues(FunctionCollector.this.labeledObjects, o -> o.object);
        }
    };

    public FunctionCollector(final String name, final String help,
                             final Map<Labels, NamedObject<T>> labeledObjects,
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
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof FunctionCollector)) {
            throw new IllegalStateException();
        }

        final FunctionCollector<T> other = (FunctionCollector<T>) rawOther;

        final HashMap<Labels, NamedObject<T>> labeledObjects = new HashMap<>(this.labeledObjects);
        for (final Map.Entry<Labels, NamedObject<T>> entry : (other).labeledObjects.entrySet()) {
            labeledObjects.merge(entry.getKey(), entry.getValue(), (o1, o2) -> {throw new IllegalStateException(String.format("Object %s and %s cannot be merged, and their labels are the same.", o1, o2));});
        }

        return new FunctionCollector<>(name, help, labeledObjects, collectFunction);
    }

    @Override
    public MBeanGroupMetricFamilyCollector removeMBean(final ObjectName objectName) {
        @SuppressWarnings("ConstantConditions") // ImmutableMap values cannot be null
        final Map<Labels, NamedObject<T>> metrics = ImmutableMap.copyOf(Maps.filterValues(this.labeledObjects, m -> !m.name.equals(objectName)));

        if (metrics.isEmpty())
            return null;

        return new FunctionCollector<>(this.name, this.help, metrics, collectFunction);
    }

    @Override
    public Stream<? extends MetricFamily<?>> collect() {
        return collectFunction.apply(objectGroup);
    }


}
