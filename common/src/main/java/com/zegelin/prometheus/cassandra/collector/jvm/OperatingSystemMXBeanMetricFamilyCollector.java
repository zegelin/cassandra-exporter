package com.zegelin.prometheus.cassandra.collector.jvm;


import com.sun.management.UnixOperatingSystemMXBean;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.stream.Stream;

import static com.zegelin.prometheus.cassandra.MetricValueConversionFunctions.nanosecondsToSeconds;
import static com.zegelin.prometheus.cassandra.MetricValueConversionFunctions.neg1ToNaN;

public class OperatingSystemMXBeanMetricFamilyCollector extends MBeanGroupMetricFamilyCollector {
    private static final ObjectName OPERATING_SYSTEM_MXBEAN_NAME = ObjectNames.create(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);

    public static final Factory FACTORY = mBean -> {
        if (!OPERATING_SYSTEM_MXBEAN_NAME.apply(mBean.name))
            return null;

        return new OperatingSystemMXBeanMetricFamilyCollector((OperatingSystemMXBean) mBean.object);
    };

    private final OperatingSystemMXBean operatingSystemMXBean;

    private OperatingSystemMXBeanMetricFamilyCollector(final OperatingSystemMXBean operatingSystemMXBean) {
        this.operatingSystemMXBean = operatingSystemMXBean;
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<MetricFamily> metricFamilies = Stream.builder();

        metricFamilies.add(new GaugeMetricFamily("cassandra_os_1m_load_average", "1 minute system load average.", Stream.of(new NumericMetric(Labels.of(), neg1ToNaN((float) operatingSystemMXBean.getSystemLoadAverage())))));

        if (operatingSystemMXBean instanceof UnixOperatingSystemMXBean) {
            final UnixOperatingSystemMXBean unixOperatingSystemMXBean = (UnixOperatingSystemMXBean) operatingSystemMXBean;

            metricFamilies.add(new GaugeMetricFamily("cassandra_process_maximum_file_descriptors", "Maximum number of file descriptors.", Stream.of(new NumericMetric(Labels.of(), (float) unixOperatingSystemMXBean.getMaxFileDescriptorCount()))));
            metricFamilies.add(new GaugeMetricFamily("cassandra_process_open_file_descriptors", "Current number of in-use file descriptors.", Stream.of(new NumericMetric(Labels.of(), (float) unixOperatingSystemMXBean.getOpenFileDescriptorCount()))));

            metricFamilies.add(new GaugeMetricFamily("cassandra_process_vm_committed_bytes", null, Stream.of(new NumericMetric(Labels.of(), neg1ToNaN((float) unixOperatingSystemMXBean.getCommittedVirtualMemorySize())))));

            metricFamilies.add(new GaugeMetricFamily("cassandra_process_cpu_load_ratio", null, Stream.of(new NumericMetric(Labels.of(), neg1ToNaN((float) unixOperatingSystemMXBean.getProcessCpuLoad())))));
            metricFamilies.add(new GaugeMetricFamily("cassandra_process_cpu_time_seconds", null, Stream.of(new NumericMetric(Labels.of(), nanosecondsToSeconds(neg1ToNaN((float) unixOperatingSystemMXBean.getProcessCpuTime()))))));

            metricFamilies.add(new GaugeMetricFamily("cassandra_os_free_memory_bytes", null, Stream.of(new NumericMetric(Labels.of(), (float) unixOperatingSystemMXBean.getFreePhysicalMemorySize()))));
            metricFamilies.add(new GaugeMetricFamily("cassandra_os_free_swap_bytes", null, Stream.of(new NumericMetric(Labels.of(), (float) unixOperatingSystemMXBean.getFreeSwapSpaceSize()))));

            metricFamilies.add(new GaugeMetricFamily("cassandra_os_cpu_load_ratio", null, Stream.of(new NumericMetric(Labels.of(), neg1ToNaN((float) unixOperatingSystemMXBean.getSystemCpuLoad())))));
        }

        return metricFamilies.build();
    }
}
