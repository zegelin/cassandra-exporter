package com.zegelin.prometheus.cassandra;

import com.zegelin.prometheus.domain.MetricFamily;

import javax.management.ObjectName;
import java.util.stream.Stream;

public interface MBeanMetricFamilyCollector {
    /**
     * @return the name of the collector. Collectors with the same name will be merged together {@see merge}.
     */
    String name();

    /**
     * Merge two {@link MBeanMetricFamilyCollector}s together.
     *
     * @param other The other {@link MBeanMetricFamilyCollector} to merge with.
     * @return a new {@link MBeanMetricFamilyCollector} that is the combinator of this {@link MBeanMetricFamilyCollector} and {@param other}
     */
    default MBeanMetricFamilyCollector merge(final MBeanMetricFamilyCollector other) {
        throw new IllegalStateException(String.format("Merging of %s and %s not implemented.", this, other));
    }

    /**
     * @return a new MBeanMetricFamilyCollector with the named MBean removed, or null if the collector is empty.
     */
    default MBeanMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        return null;
    }

    /**
     * @return a {@link Stream} of {@link MetricFamily}s that contain the metrics collected by this collector.
     */
    Stream<MetricFamily<?>> collect();
}
