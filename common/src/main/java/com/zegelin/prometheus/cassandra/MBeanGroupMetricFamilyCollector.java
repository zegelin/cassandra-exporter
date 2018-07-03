package com.zegelin.prometheus.cassandra;

import com.zegelin.jmx.NamedObject;
import com.zegelin.prometheus.domain.MetricFamily;

import javax.management.ObjectName;
import java.util.stream.Stream;

public interface MBeanGroupMetricFamilyCollector {
    /**
     * @return the name of the collector. Collectors with the same name will be merged together {@see merge}.
     */
    String name();

    /**
     * Merge two {@link MBeanGroupMetricFamilyCollector}s together.
     *
     * @param other The other {@link MBeanGroupMetricFamilyCollector} to merge with.
     * @return a new {@link MBeanGroupMetricFamilyCollector} that is the combinator of this {@link MBeanGroupMetricFamilyCollector} and {@param other}
     */
    default MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector other) {
        throw new IllegalStateException(String.format("Merging of %s and %s not implemented.", this, other));
    }

    /**
     * @return a new MBeanGroupMetricFamilyCollector with the named MBean removed, or null if the collector is empty.
     */
    default MBeanGroupMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        return null;
    }

    /**
     * @return a {@link Stream} of {@link MetricFamily}s that contain the metrics collected by this collector.
     */
    Stream<? extends MetricFamily<?>> collect();


    interface Factory {
        /**
         * Create a {@link MBeanGroupMetricFamilyCollector} for the given MBean, or null if this factory
         * doesn't support the given MBean.
         *
         * @return the MBeanGroupMetricFamilyCollector for the given MBean, or null
         */
        MBeanGroupMetricFamilyCollector createCollector(final NamedObject<?> mBean);
    }
}
