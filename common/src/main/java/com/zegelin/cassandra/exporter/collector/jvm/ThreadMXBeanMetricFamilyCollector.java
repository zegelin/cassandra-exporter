package com.zegelin.cassandra.exporter.collector.jvm;

import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.cassandra.exporter.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.*;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.stream.Stream;

import static com.zegelin.cassandra.exporter.MetricValueConversionFunctions.nanosecondsToSeconds;

public class ThreadMXBeanMetricFamilyCollector extends  MBeanGroupMetricFamilyCollector {
    private static final ObjectName THREAD_MXBEAN_NAME = ObjectNames.create(ManagementFactory.THREAD_MXBEAN_NAME);

    private static final Labels USER_THREAD_COUNT_LABELS = Labels.of("type", "user");
    private static final Labels DAEMON_THREAD_COUNT_LABELS = Labels.of("type", "daemon");

    public static Factory factory(final boolean perThreadTimingEnabled) {
        return mBean -> {
            if (!THREAD_MXBEAN_NAME.apply(mBean.name))
                return null;

            return new ThreadMXBeanMetricFamilyCollector((ThreadMXBean) mBean.object, perThreadTimingEnabled);
        };
    }

    private final ThreadMXBean threadMXBean;
    private final boolean perThreadTimingEnabled;

    private ThreadMXBeanMetricFamilyCollector(final ThreadMXBean threadMXBean, final boolean perThreadTimingEnabled) {
        this.threadMXBean = threadMXBean;
        this.perThreadTimingEnabled = perThreadTimingEnabled;
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<MetricFamily> metricFamilies = Stream.builder();

        {
            final int threadCount = threadMXBean.getThreadCount();
            final int daemonThreadCount = threadMXBean.getDaemonThreadCount();
            final int userThreadCount = threadCount - daemonThreadCount;

            metricFamilies.add(new GaugeMetricFamily("cassandra_jvm_thread_count", "Current number of live threads.", Stream.of(
                    new NumericMetric(USER_THREAD_COUNT_LABELS, userThreadCount),
                    new NumericMetric(DAEMON_THREAD_COUNT_LABELS, daemonThreadCount)
            )));
        }

        metricFamilies.add(new GaugeMetricFamily("cassandra_jvm_threads_started_total", "Cumulative number of started threads (since JVM start).", Stream.of(new NumericMetric(null, threadMXBean.getTotalStartedThreadCount()))));

        if (perThreadTimingEnabled && threadMXBean instanceof com.sun.management.ThreadMXBean && threadMXBean.isThreadCpuTimeEnabled()) {
            final com.sun.management.ThreadMXBean threadMXBeanEx = (com.sun.management.ThreadMXBean) threadMXBean;

            final long[] threadIds = threadMXBeanEx.getAllThreadIds();
            final ThreadInfo[] threadInfos = threadMXBeanEx.getThreadInfo(threadIds);
            final long[] threadCpuTimes = threadMXBeanEx.getThreadCpuTime(threadIds);
            final long[] threadUserTimes = threadMXBeanEx.getThreadUserTime(threadIds);

            final Stream.Builder<NumericMetric> threadCpuTimeMetrics = Stream.builder();

            for (int i = 0; i < threadIds.length; i++) {
                final long threadCpuTime = threadCpuTimes[i];
                final long threadUserTime = threadUserTimes[i];

                if (threadCpuTime == -1 || threadUserTime == -1) {
                    continue;
                }

                final long threadSystemTime = threadCpuTime - threadUserTime;

                final Labels systemModeLabels = new Labels(ImmutableMap.of(
                        "id", String.valueOf(threadIds[i]),
                        "name", threadInfos[i].getThreadName(),
                        "mode", "system"
                ));

                final Labels userModeLabels = new Labels(ImmutableMap.of(
                        "id", String.valueOf(threadIds[i]),
                        "name", threadInfos[i].getThreadName(),
                        "mode", "user"
                        ));

                threadCpuTimeMetrics.add(new NumericMetric(systemModeLabels, nanosecondsToSeconds(threadSystemTime)));
                threadCpuTimeMetrics.add(new NumericMetric(userModeLabels, nanosecondsToSeconds(threadUserTime)));
            }

            metricFamilies.add(new CounterMetricFamily("cassandra_jvm_thread_cpu_time_seconds_total", "Cumulative thread CPU time (since JVM start).", threadCpuTimeMetrics.build()));
        }

        return metricFamilies.build();
    }
}
