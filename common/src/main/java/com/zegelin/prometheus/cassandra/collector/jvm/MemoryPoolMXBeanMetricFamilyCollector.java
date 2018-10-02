package com.zegelin.prometheus.cassandra.collector.jvm;

import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.zegelin.prometheus.cassandra.MetricValueConversionFunctions.neg1ToNaN;

public class MemoryPoolMXBeanMetricFamilyCollector implements MBeanGroupMetricFamilyCollector {
    private static final ObjectName MEMORY_POOL_MXBEAN_NAME_PATTERN = ObjectNames.create(ManagementFactory.MEMORY_POOL_MXBEAN_DOMAIN_TYPE + ",*");

    public static final Factory FACTORY = mBean -> {
        if (!MEMORY_POOL_MXBEAN_NAME_PATTERN.apply(mBean.name))
            return null;

        final MemoryPoolMXBean memoryPoolMXBean = (MemoryPoolMXBean) mBean.object;

        final Labels poolLabels = new Labels(ImmutableMap.of(
                "pool", memoryPoolMXBean.getName(),
                "type", memoryPoolMXBean.getType().name()
        ));

        return new MemoryPoolMXBeanMetricFamilyCollector(ImmutableMap.of(poolLabels, memoryPoolMXBean));
    };

    private final Map<Labels, MemoryPoolMXBean> labeledMemoryPoolMXBeans;

    private MemoryPoolMXBeanMetricFamilyCollector(final Map<Labels, MemoryPoolMXBean> labeledMemoryPoolMXBeans) {
        this.labeledMemoryPoolMXBeans = labeledMemoryPoolMXBeans;
    }

    @Override
    public String name() {
        return ManagementFactory.MEMORY_POOL_MXBEAN_DOMAIN_TYPE;
    }

    @Override
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof MemoryPoolMXBeanMetricFamilyCollector)) {
            throw new IllegalStateException();
        }

        final MemoryPoolMXBeanMetricFamilyCollector other = (MemoryPoolMXBeanMetricFamilyCollector) rawOther;

        final Map<Labels, MemoryPoolMXBean> labeledMemoryPoolMXBeans = new HashMap<>(this.labeledMemoryPoolMXBeans);
        for (final Map.Entry<Labels, MemoryPoolMXBean> entry : other.labeledMemoryPoolMXBeans.entrySet()) {
            labeledMemoryPoolMXBeans.merge(entry.getKey(), entry.getValue(), (o1, o2) -> {
                throw new IllegalStateException(String.format("Object %s and %s cannot be merged, yet their labels are the same.", o1, o2));
            });
        }

        return new MemoryPoolMXBeanMetricFamilyCollector(labeledMemoryPoolMXBeans);
    }

    @Override
    public MBeanGroupMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        return null;
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<NumericMetric> initialBytesMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> usedBytesMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> committedBytesMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> maximumBytesMetrics = Stream.builder();

        for (final Map.Entry<Labels, MemoryPoolMXBean> entry : labeledMemoryPoolMXBeans.entrySet()) {
            final Labels labels = entry.getKey();
            final MemoryPoolMXBean memoryPoolMXBean = entry.getValue();

            final MemoryUsage usage = memoryPoolMXBean.getUsage();

            initialBytesMetrics.add(new NumericMetric(labels, neg1ToNaN(usage.getInit())));
            usedBytesMetrics.add(new NumericMetric(labels, usage.getUsed()));
            committedBytesMetrics.add(new NumericMetric(labels, usage.getCommitted()));
            maximumBytesMetrics.add(new NumericMetric(labels, neg1ToNaN(usage.getMax())));
        }

        return Stream.of(
                new GaugeMetricFamily("cassandra_jvm_memory_pool_initial_bytes", "Initial size of the memory pool.", initialBytesMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_memory_pool_used_bytes", null, usedBytesMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_memory_pool_committed_bytes", null, committedBytesMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_memory_pool_maximum_bytes", "Maximum size of the memory pool.", maximumBytesMetrics.build())
        );
    }
}
