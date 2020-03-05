package com.zegelin.cassandra.exporter.collector;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.zegelin.jmx.NamedObject;
import com.zegelin.cassandra.exporter.MBeanGroupMetricFamilyCollector;
import com.zegelin.cassandra.exporter.MetricValueConversionFunctions;
import com.zegelin.cassandra.exporter.SamplingCounting;
import com.zegelin.prometheus.domain.Interval;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.SummaryMetricFamily;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxCounterMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxTimerMBean;

import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.zegelin.cassandra.exporter.CassandraMetricsUtilities.jmxTimerMBeanAsSamplingCounting;
import static com.zegelin.cassandra.exporter.MetricValueConversionFunctions.microsecondsToSeconds;

/*
    Certain latency metrics in Cassandra are exposed via two separate MBeans -- a `Latency` bean
    and a `TotalLatency` bean.

    Currently this assumes that these metrics are always in microseconds.

    This collector combines both into a single Prometheus Summary metric.
 */
public class LatencyMetricGroupSummaryCollector extends MBeanGroupMetricFamilyCollector {
    static class LatencyMetricGroup {
        final NamedObject<SamplingCounting> latencyTimer;
        final NamedObject<JmxCounterMBean> totalLatencyCounter; // totalLatency is a sum of all latency values, represented as a Counter

        LatencyMetricGroup(final NamedObject<SamplingCounting> latencyTimer, final NamedObject<JmxCounterMBean> totalLatencyCounter) {
            this.latencyTimer = latencyTimer;
            this.totalLatencyCounter = totalLatencyCounter;
        }

        static LatencyMetricGroup merge(final LatencyMetricGroup a, final LatencyMetricGroup b) {
            if (a.latencyTimer != null && a.totalLatencyCounter != null) {
                throw new IllegalStateException(); // TODO: exception message (this group is already full "merged")
            }

            return new LatencyMetricGroup(
                    a.latencyTimer == null ? b.latencyTimer : a.latencyTimer,
                    a.totalLatencyCounter == null ? b.totalLatencyCounter : a.totalLatencyCounter
            );
        }

        boolean incomplete() {
            return latencyTimer == null || totalLatencyCounter == null;
        }

        LatencyMetricGroup removeMBean(final ObjectName objectName) {
            final NamedObject<SamplingCounting> newLatencyTimer = latencyTimer != null && !latencyTimer.name.equals(objectName) ? latencyTimer : null;
            final NamedObject<JmxCounterMBean> newTotalLatencyCounter = totalLatencyCounter != null && !totalLatencyCounter.name.equals(objectName) ? totalLatencyCounter : null;

            if (newLatencyTimer == null && newTotalLatencyCounter == null) {
                return null;
            }

            if (newLatencyTimer == latencyTimer && newTotalLatencyCounter == totalLatencyCounter) {
                return this;
            }

            return new LatencyMetricGroup(newLatencyTimer, newTotalLatencyCounter);
        }
    }

    private final String name;
    private final String help;
    private final Map<Labels, LatencyMetricGroup> latencyMetricGroups;

    private LatencyMetricGroupSummaryCollector(final String name, final String help, final Map<Labels, LatencyMetricGroup> latencyMetricGroups) {
        this.name = name;
        this.help = help;
        this.latencyMetricGroups = ImmutableMap.copyOf(latencyMetricGroups);
    }


    public static LatencyMetricGroupSummaryCollector collectorForMBean(final String name, final String help, final Labels labels, final NamedObject<?> mBean) {
        final NamedObject<SamplingCounting> timer = (mBean.object instanceof JmxTimerMBean) ? jmxTimerMBeanAsSamplingCounting(mBean) : null;
        final NamedObject<JmxCounterMBean> counter = mBean.map((n, o) -> (o instanceof JmxCounterMBean) ? (JmxCounterMBean) o : null);

        final LatencyMetricGroup latencyMetricGroup = new LatencyMetricGroup(timer, counter);

        return new LatencyMetricGroupSummaryCollector(name, help, ImmutableMap.of(labels, latencyMetricGroup));
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof LatencyMetricGroupSummaryCollector)) {
            throw new IllegalStateException();
        }

        final LatencyMetricGroupSummaryCollector other = (LatencyMetricGroupSummaryCollector) rawOther;

        final HashMap<Labels, LatencyMetricGroup> newLatencyMetricGroups = new HashMap<>(latencyMetricGroups);
        for (final Map.Entry<Labels, LatencyMetricGroup> group : other.latencyMetricGroups.entrySet()) {
            newLatencyMetricGroups.merge(group.getKey(), group.getValue(), LatencyMetricGroup::merge);
        }

        return new LatencyMetricGroupSummaryCollector(name, help, newLatencyMetricGroups);
    }

    @Override
    public MBeanGroupMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        final HashMap<Labels, LatencyMetricGroup> newLatencyMetricGroups = new HashMap<>(latencyMetricGroups.size());

        for (final Map.Entry<Labels, LatencyMetricGroup> entry : latencyMetricGroups.entrySet()) {
            final LatencyMetricGroup latencyMetricGroup = entry.getValue().removeMBean(mBeanName);

            if (latencyMetricGroup == null) {
                continue;
            }

            newLatencyMetricGroups.put(entry.getKey(), latencyMetricGroup);
        }

        if (newLatencyMetricGroups.size() == 0) {
            return null;
        }

        return new LatencyMetricGroupSummaryCollector(name, help, newLatencyMetricGroups);
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream<SummaryMetricFamily.Summary> summaryStream = latencyMetricGroups.entrySet().stream()
                .map(e -> new Object() {
                    final Labels labels = e.getKey();
                    final LatencyMetricGroup latencyMetricGroup = e.getValue();
                })
                .filter(e -> !e.latencyMetricGroup.incomplete())
                .map(e -> {
                    final float count = e.latencyMetricGroup.latencyTimer.object.getCount();
                    final float sum = microsecondsToSeconds(e.latencyMetricGroup.totalLatencyCounter.object.getCount());

                    final Iterable<Interval> quantiles = Iterables.transform(e.latencyMetricGroup.latencyTimer.object.getIntervals(),
                            i -> i.transform(MetricValueConversionFunctions::nanosecondsToSeconds)
                    );

                    return new SummaryMetricFamily.Summary(e.labels, sum, count, quantiles);
                });


        return Stream.of(new SummaryMetricFamily(this.name, this.help, summaryStream));
    }
}
