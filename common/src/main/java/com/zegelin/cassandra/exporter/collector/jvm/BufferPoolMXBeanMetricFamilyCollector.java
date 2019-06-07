package com.zegelin.cassandra.exporter.collector.jvm;

import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.cassandra.exporter.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;

import javax.management.ObjectName;
import java.lang.management.BufferPoolMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.zegelin.cassandra.exporter.MetricValueConversionFunctions.neg1ToNaN;

public class BufferPoolMXBeanMetricFamilyCollector extends MBeanGroupMetricFamilyCollector {
    private static final ObjectName BUFFER_POOL_MXBEAN_NAME_PATTERN = ObjectNames.create("java.nio:type=BufferPool,name=*");

    public static final Factory FACTORY = mBean -> {
        if (!BUFFER_POOL_MXBEAN_NAME_PATTERN.apply(mBean.name))
            return null;

        final BufferPoolMXBean bufferPoolMXBean = (BufferPoolMXBean) mBean.object;

        final Labels poolLabels = Labels.of("pool", bufferPoolMXBean.getName());

        return new BufferPoolMXBeanMetricFamilyCollector(ImmutableMap.of(poolLabels, bufferPoolMXBean));
    };

    private final Map<Labels, BufferPoolMXBean> labeledBufferPoolMXBeans;

    private BufferPoolMXBeanMetricFamilyCollector(final Map<Labels, BufferPoolMXBean> labeledBufferPoolMXBeans) {
        this.labeledBufferPoolMXBeans = labeledBufferPoolMXBeans;
    }

    @Override
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof BufferPoolMXBeanMetricFamilyCollector)) {
            throw new IllegalStateException();
        }

        final BufferPoolMXBeanMetricFamilyCollector other = (BufferPoolMXBeanMetricFamilyCollector) rawOther;

        final Map<Labels, BufferPoolMXBean> labeledBufferPoolMXBeans = new HashMap<>(this.labeledBufferPoolMXBeans);
        for (final Map.Entry<Labels, BufferPoolMXBean> entry : other.labeledBufferPoolMXBeans.entrySet()) {
            labeledBufferPoolMXBeans.merge(entry.getKey(), entry.getValue(), (o1, o2) -> {
                throw new IllegalStateException(String.format("Object %s and %s cannot be merged, yet their labels are the same.", o1, o2));
            });
        }

        return new BufferPoolMXBeanMetricFamilyCollector(labeledBufferPoolMXBeans);
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<NumericMetric> estimatedBuffersMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> totalCapacityBytesMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> usedBytesMetrics = Stream.builder();

        for (final Map.Entry<Labels, BufferPoolMXBean> entry : labeledBufferPoolMXBeans.entrySet()) {
            final Labels labels = entry.getKey();
            final BufferPoolMXBean bufferPoolMXBean = entry.getValue();

            estimatedBuffersMetrics.add(new NumericMetric(labels, bufferPoolMXBean.getCount()));
            totalCapacityBytesMetrics.add(new NumericMetric(labels, bufferPoolMXBean.getTotalCapacity()));
            usedBytesMetrics.add(new NumericMetric(labels, neg1ToNaN(bufferPoolMXBean.getMemoryUsed())));
        }

        return Stream.of(
                new GaugeMetricFamily("cassandra_jvm_nio_buffer_pool_estimated_buffers", "Estimated current number of buffers in the pool.", estimatedBuffersMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_nio_buffer_pool_estimated_capacity_bytes_total", "Estimated total capacity of the buffers in the pool.", totalCapacityBytesMetrics.build()),
                new GaugeMetricFamily("cassandra_jvm_nio_buffer_pool_estimated_used_bytes", "Estimated memory usage by the JVM for the pool.", usedBytesMetrics.build())
        );
    }
}
