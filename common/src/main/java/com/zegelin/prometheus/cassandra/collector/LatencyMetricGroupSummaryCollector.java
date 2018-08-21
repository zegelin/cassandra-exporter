package com.zegelin.prometheus.cassandra.collector;

import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.NamedObject;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.SamplingCounting;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.Quantile;
import com.zegelin.prometheus.domain.SummaryMetricFamily;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxCounterMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxTimerMBean;

import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.zegelin.prometheus.cassandra.CassandraMetricsUtilities.jmxTimerMBeanAsSamplingCounting;

/*
    Certain latency metrics in Cassandra are exposed via two separate MBeans -- a `Latency` bean
    and a `TotalLatency` bean.

    This collector combines both into a single Prometheus Summary metric.
 */
public class LatencyMetricGroupSummaryCollector implements MBeanGroupMetricFamilyCollector {
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
    }

    private final String name;
    private final String help;
    private final Map<Labels, LatencyMetricGroup> latencyMetricGroups;

    private LatencyMetricGroupSummaryCollector(final String name, final String help, final Map<Labels, LatencyMetricGroup> latencyMetricGroups) {
        this.name = name;
        this.help = help;
        this.latencyMetricGroups = latencyMetricGroups;
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

        final HashMap<Labels, LatencyMetricGroup> latencyMetricGroups = new HashMap<>(this.latencyMetricGroups);
        for (final Map.Entry<Labels, LatencyMetricGroup> group : other.latencyMetricGroups.entrySet()) {
            latencyMetricGroups.merge(group.getKey(), group.getValue(), LatencyMetricGroup::merge);
        }

        return new LatencyMetricGroupSummaryCollector(name, help, latencyMetricGroups);
    }

    @Override
    public MBeanGroupMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        throw new IllegalStateException(); // TODO: implement
    }

    @Override
    public Stream<? extends MetricFamily<?>> collect() {
        final Stream<SummaryMetricFamily.Summary> summaryStream = latencyMetricGroups.entrySet().stream()
                .map(e -> new Object() {
                    final Labels labels = e.getKey();
                    final LatencyMetricGroup latencyMetricGroup = e.getValue();
                })
                .filter(e -> !e.latencyMetricGroup.incomplete())
                .map(e -> {
                    final float count = e.latencyMetricGroup.latencyTimer.object.getCount();
                    final float sum = e.latencyMetricGroup.totalLatencyCounter.object.getCount();

                    final Map<Quantile, Float> quantiles = e.latencyMetricGroup.latencyTimer.object.getQuantiles();

                    return new SummaryMetricFamily.Summary(e.labels, sum, count, quantiles);
                });


        return Stream.of(new SummaryMetricFamily(this.name, this.help, summaryStream));
    }
}
