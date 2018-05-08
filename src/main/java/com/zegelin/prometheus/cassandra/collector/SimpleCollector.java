package com.zegelin.prometheus.cassandra.collector;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.zegelin.prometheus.cassandra.MBeanMetricFamilyCollector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class SimpleCollector<T> implements MBeanMetricFamilyCollector {
    protected final String name, help;

    protected final Constructor<T> constructor;

    // labels -> MBeans
    protected final Map<ImmutableMap<String, String>, T> metrics;

    @FunctionalInterface
    public interface Constructor<T> {
        MBeanMetricFamilyCollector instantiate(final String name, final String help, final Map<ImmutableMap<String, String>, T> metrics);
    }

    // TODO: come up with a better name for this
    public static <T> Map<ImmutableMap<String, String>, T> yyy(final Map<String, String> labels, final T metric) {
        return ImmutableMap.of(ImmutableMap.copyOf(labels), metric);
    }

    SimpleCollector(final String name, final String help,
                    final Constructor<T> constructor,
                    final Map<ImmutableMap<String, String>, T> metrics) {
        this.name = name;
        this.help = help;
        this.constructor = constructor;
        this.metrics = ImmutableMap.copyOf(metrics);
    }

    @Override
    public final String name() {
        return this.name;
    }

    @SuppressWarnings("unchecked")
    public MBeanMetricFamilyCollector merge(final MBeanMetricFamilyCollector other) {
        // TODO: check that `other` is the same sub-class

        final HashMap<ImmutableMap<String, String>, T> metrics = new HashMap<>(this.metrics);
        for (final Map.Entry<ImmutableMap<String, String>, T> entry : ((SimpleCollector<T>) other).metrics.entrySet()) {
            metrics.merge(entry.getKey(), entry.getValue(), this::mergeMetric);
        }

        return this.constructor.instantiate(this.name, this.help, metrics);
    }


    protected T mergeMetric(final T existingValue, final T newValue) {
        throw new IllegalStateException(); // TODO: better exception message
    }

    protected <MetricT> Set<MetricT> transformMetrics(final Maps.EntryTransformer<Map<String, String>, T, MetricT> entryTransformer) {
        // TODO: avoid a copy here
        return ImmutableSet.copyOf(Maps.transformEntries(metrics, entryTransformer).values().stream().filter(Objects::nonNull).iterator());
    }



    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .toString();
    }
}
