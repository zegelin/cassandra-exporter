//package com.zegelin.prometheus.cassandra.collector;
//
//import com.codahale.metrics.Counter;
//import com.codahale.metrics.Metric;
//import com.codahale.metrics.Snapshot;
//import com.codahale.metrics.Timer;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Maps;
//import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
//import com.zegelin.jmx.NamedObject;
//import com.zegelin.prometheus.domain.MetricFamily;
//import com.zegelin.prometheus.domain.Quantile;
//import com.zegelin.prometheus.domain.SummaryMetricFamily;
//import org.apache.cassandra.metrics.CassandraMetricsRegistry;
//
//import javax.management.ObjectName;
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Stream;
//
//import static com.zegelin.prometheus.cassandra.CassandraMetricsUtilities.metricForMBean;
//
//
///*
//    Certain latency metrics in Cassandra are exposed via two separate MBeans -- a `Latency` bean
//    and a `TotalLatency` bean.
//
//    This collector combines both into a single Prometheus Summary metric.
// */
//// TODO: optimize
//public class JMXLatencyMetricGroupCollector extends SimpleCollector<JMXLatencyMetricGroupCollector.LatencyMetricGroup> {
//    static final Set<Double> QUANTILES = ImmutableSet.of(.5, .75, .95, .98, .99, .999);
//
//    static class LatencyMetricGroup {
//        final NamedObject<CassandraMetricsRegistry.JmxTimerMBean> latencyTimer;
//        final NamedObject<CassandraMetricsRegistry.JmxCounterMBean> totalLatencyCounter;
//
//        LatencyMetricGroup(final NamedObject<CassandraMetricsRegistry.JmxTimerMBean> latencyTimer, final NamedObject<CassandraMetricsRegistry.JmxCounterMBean> totalLatencyCounter) {
//            this.latencyTimer = latencyTimer;
//            this.totalLatencyCounter = totalLatencyCounter;
//        }
//
//        static LatencyMetricGroup merge(final LatencyMetricGroup a, final LatencyMetricGroup b) {
//            if (a.latencyTimer != null && a.totalLatencyCounter != null) {
//                throw new IllegalStateException(); // TODO: exception message (this group is already full "merged")
//            }
//
//            return new LatencyMetricGroup(
//                    a.latencyTimer == null ? b.latencyTimer : a.latencyTimer,
//                    a.totalLatencyCounter == null ? b.totalLatencyCounter : a.totalLatencyCounter
//            );
//        }
//     }
//
//    private JMXLatencyMetricGroupCollector(final String name, final String help, final Map<ImmutableMap<String, String>, LatencyMetricGroup> latencyMetricGroups) {
//        super(name, help, JMXLatencyMetricGroupCollector::new, latencyMetricGroups);
//    }
//
//    public static JMXLatencyMetricGroupCollector asSummary(final String name, final String help, final Map<String, String> labels, final NamedObject<?> mBean) {
//        final NamedObject<CassandraMetricsRegistry.JmxTimerMBean> timer = mBean.map((n, m) -> (m instanceof CassandraMetricsRegistry.JmxTimerMBean) ? (CassandraMetricsRegistry.JmxTimerMBean) m : null);
//        final NamedObject<CassandraMetricsRegistry.JmxCounterMBean> counter = mBean.map((n, m) -> (m instanceof CassandraMetricsRegistry.JmxCounterMBean) ? (CassandraMetricsRegistry.JmxCounterMBean) m : null);
//
//        final LatencyMetricGroup latencyMetricGroup = new LatencyMetricGroup(timer, counter);
//
//        return new JMXLatencyMetricGroupCollector(name, help, ImmutableMap.of(ImmutableMap.copyOf(labels), latencyMetricGroup));
//    }
//
//    @Override
//    public MBeanGroupMetricFamilyCollector removeMBean(final ObjectName mBeanName) {
//        throw new IllegalStateException(); // TODO: implement
//    }
//
//    @Override
//    protected LatencyMetricGroup mergeMetric(final LatencyMetricGroup existingValue, final LatencyMetricGroup newValue) {
//        return LatencyMetricGroup.merge(existingValue, newValue);
//    }
//
//    private static SummaryMetricFamily.Summary collectSummary(final Map<String, String> labels, final LatencyMetricGroup group) {
//        final double count = group.latencyTimer.object.getCount();
//        final double sum = group.totalLatencyCounter.object.getCount(); // totalLatency is a sum of all latency values, represented as a Codahale Counter
//
//
//        final Map<Quantile, Number> quantiles = ImmutableMap.<Quantile, Number>builder()
////                .put(.5, group.latencyTimer.object.get50thPercentile())
////                .put(.75, group.latencyTimer.object.get75thPercentile())
////                .put(.95, group.latencyTimer.object.get95thPercentile())
////                .put(.98, group.latencyTimer.object.get98thPercentile())
////                .put(.99, group.latencyTimer.object.get99thPercentile())
////                .put(.999, group.latencyTimer.object.get999thPercentile())
//                .build();
//
//
//        return new SummaryMetricFamily.Summary(labels, sum, count, quantiles);
//    }
//
//    @Override
//    public Stream<MetricFamily<?>> collect() {
//        return Stream.of(new SummaryMetricFamily(this.name, this.help, transformMetrics(JMXLatencyMetricGroupCollector::collectSummary)));
//    }
//}
