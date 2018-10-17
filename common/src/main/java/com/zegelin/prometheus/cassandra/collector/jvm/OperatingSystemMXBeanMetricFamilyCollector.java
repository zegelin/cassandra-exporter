package com.zegelin.prometheus.cassandra.collector.jvm;


import com.sun.management.UnixOperatingSystemMXBean;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
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

        metricFamilies.add(new GaugeMetricFamily("cassandra_os_1m_load_average", "1 minute system load average (as seen by the Cassandra JVM process).", Stream.of(new NumericMetric(null, neg1ToNaN((float) operatingSystemMXBean.getSystemLoadAverage())))));

        if (operatingSystemMXBean instanceof UnixOperatingSystemMXBean) {
            final UnixOperatingSystemMXBean unixOperatingSystemMXBean = (UnixOperatingSystemMXBean) operatingSystemMXBean;

            metricFamilies.add(new GaugeMetricFamily("cassandra_process_maximum_file_descriptors", "Maximum number of file descriptors that can be opened by the Cassandra JVM process.", Stream.of(new NumericMetric(null, (float) unixOperatingSystemMXBean.getMaxFileDescriptorCount()))));
            metricFamilies.add(new GaugeMetricFamily("cassandra_process_open_file_descriptors", "Current number of open file descriptors in the Cassandra JVM process.", Stream.of(new NumericMetric(null, (float) unixOperatingSystemMXBean.getOpenFileDescriptorCount()))));

            metricFamilies.add(new GaugeMetricFamily("cassandra_process_vm_committed_bytes", "Amount of virtual memory that is guaranteed to be available to the Cassandra JVM process.", Stream.of(new NumericMetric(null, neg1ToNaN((float) unixOperatingSystemMXBean.getCommittedVirtualMemorySize())))));

            metricFamilies.add(new GaugeMetricFamily("cassandra_process_recent_cpu_load_ratio", "\"Recent\" (as defined by the JVM) CPU usage for the Cassandra JVM process.", Stream.of(new NumericMetric(null, neg1ToNaN((float) unixOperatingSystemMXBean.getProcessCpuLoad())))));
            metricFamilies.add(new GaugeMetricFamily("cassandra_process_cpu_seconds_total", "Cumulative CPU time used by the Cassandra JVM process.", Stream.of(new NumericMetric(null, nanosecondsToSeconds(neg1ToNaN((float) unixOperatingSystemMXBean.getProcessCpuTime()))))));

            metricFamilies.add(new GaugeMetricFamily("cassandra_os_memory_bytes_total", "Total physical memory available (as seen by the Cassandra JVM process).", Stream.of(new NumericMetric(null, (float) unixOperatingSystemMXBean.getTotalPhysicalMemorySize()))));
            metricFamilies.add(new GaugeMetricFamily("cassandra_os_free_memory_bytes", "Amount of free physical memory available (as seen by the Cassandra JVM process).", Stream.of(new NumericMetric(null, (float) unixOperatingSystemMXBean.getFreePhysicalMemorySize()))));

            metricFamilies.add(new GaugeMetricFamily("cassandra_os_swap_bytes_total", "Total swap space available (as seen by the Cassandra JVM process).", Stream.of(new NumericMetric(null, (float) unixOperatingSystemMXBean.getTotalSwapSpaceSize()))));
            metricFamilies.add(new GaugeMetricFamily("cassandra_os_free_swap_bytes", "Amount of free swap space available (as seen by the Cassandra JVM process).", Stream.of(new NumericMetric(null, (float) unixOperatingSystemMXBean.getFreeSwapSpaceSize()))));

            metricFamilies.add(new GaugeMetricFamily("cassandra_os_recent_cpu_load_ratio", "\"Recent\" (as defined by the JVM) CPU usage for the system (as seen by the Cassandra JVM process).", Stream.of(new NumericMetric(null, neg1ToNaN((float) unixOperatingSystemMXBean.getSystemCpuLoad())))));
        }

        return metricFamilies.build();
    }
}
