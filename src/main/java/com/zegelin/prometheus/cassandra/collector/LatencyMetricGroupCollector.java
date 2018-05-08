package com.zegelin.prometheus.cassandra.collector;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.zegelin.prometheus.cassandra.MBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.NamedObject;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.SummaryMetricFamily;

import javax.management.ObjectName;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.zegelin.prometheus.cassandra.CassandraMetricsUtilities.metricForMBean;


/*
    Certain latency metrics in Cassandra are exposed via two separate MBeans -- a `Latency` bean
    and a `TotalLatency` bean.

    This collector combines both into a single Prometheus Summary metric.
 */
// TODO: optimize
public class LatencyMetricGroupCollector extends SimpleCollector<LatencyMetricGroupCollector.LatencyMetricGroup> {
    static final Set<Double> QUANTILES = ImmutableSet.of(.5, .75, .95, .98, .99, .999);

    static class LatencyMetricGroup {
        final NamedObject<Timer> latencyTimer;
        final NamedObject<Counter> totalLatencyCounter;

        LatencyMetricGroup(final NamedObject<Timer> latencyTimer, final NamedObject<Counter> totalLatencyCounter) {
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
     }

    private LatencyMetricGroupCollector(final String name, final String help, final Map<ImmutableMap<String, String>, LatencyMetricGroup> latencyMetricGroups) {
        super(name, help, LatencyMetricGroupCollector::new, latencyMetricGroups);
    }

    public static LatencyMetricGroupCollector asSummary(final String name, final String help, final Map<String, String> labels, final NamedObject<?> mBean) {
        final NamedObject<Metric> metric = metricForMBean(mBean);

        final NamedObject<Timer> timer = metric.map((n, m) -> (m instanceof Timer) ? (Timer) m : null);
        final NamedObject<Counter> counter = metric.map((n, m) -> (m instanceof Counter) ? (Counter) m : null);

        final LatencyMetricGroup latencyMetricGroup = new LatencyMetricGroup(timer, counter);

        return new LatencyMetricGroupCollector(name, help, ImmutableMap.of(ImmutableMap.copyOf(labels), latencyMetricGroup));
    }

    @Override
    public MBeanMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
        throw new IllegalStateException(); // TODO: implement
    }

    @Override
    protected LatencyMetricGroup mergeMetric(final LatencyMetricGroup existingValue, final LatencyMetricGroup newValue) {
        return LatencyMetricGroup.merge(existingValue, newValue);
    }

    private static SummaryMetricFamily.Summary collectSummary(final Map<String, String> labels, final LatencyMetricGroup group) {
        final double count = group.latencyTimer.object.getCount();
        final double sum = group.totalLatencyCounter.object.getCount(); // totalLatency is a sum of all latency values, represented as a Codahale Counter


        final Snapshot snapshot = group.latencyTimer.object.getSnapshot();
        final ImmutableMap<Double, Number> quantiles = Maps.toMap(QUANTILES, snapshot::getValue);

        return new SummaryMetricFamily.Summary(labels, sum, count, quantiles);
    }

    @Override
    public Stream<MetricFamily<?>> collect() {
        return Stream.of(new SummaryMetricFamily(this.name, this.help, transformMetrics(LatencyMetricGroupCollector::collectSummary)));
    }
}
