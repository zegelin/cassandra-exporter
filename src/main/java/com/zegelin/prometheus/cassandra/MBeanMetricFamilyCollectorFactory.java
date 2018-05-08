package com.zegelin.prometheus.cassandra;

public interface MBeanMetricFamilyCollectorFactory {
    /**
     * Create a {@link MBeanMetricFamilyCollector} for the given MBean, or null if this factory
     * doesn't support the given MBean.
     *
     * @return the MBeanMetricFamilyCollector for the given MBean, or null
     */
    MBeanMetricFamilyCollector createCollector(final NamedObject<?> mBean);
}
