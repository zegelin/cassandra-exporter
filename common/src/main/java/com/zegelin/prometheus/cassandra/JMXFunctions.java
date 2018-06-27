//package com.zegelin.prometheus.cassandra;
//
//import com.codahale.metrics.*;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Maps;
//import com.zegelin.prometheus.cassandra.harvester.dynamic.LabeledMBeanGroup;
//import com.zegelin.prometheus.domain.*;
//import org.apache.cassandra.metrics.CassandraMetricsRegistry;
//import org.apache.cassandra.utils.EstimatedHistogram;
//
//import java.util.Collection;
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//public class JMXFunctions {
//
//    /**
//     * Collect a {@link Counter} as a Prometheus counter
//     */
//    public static Stream<MetricFamily<?>> counterAsCounter(final LabeledMBeanGroup<CassandraMetricsRegistry.JmxCounterMBean> group) {
//        final Set<NumericMetric> collect = group.labeledMBeans().entrySet().stream()
//                .map(e -> new NumericMetric(e.getKey(), e.getValue().getCount()))
//                .collect(Collectors.toSet());
//
//        return Stream.of(new CounterMetricFamily(group.name(), group.help(), collect));
//    }
//
//    /**
//     * Collect a {@link Counter} as a Prometheus gauge
//     */
//    public static Stream<MetricFamily<?>> counterAsGauge(final LabeledMBeanGroup<CassandraMetricsRegistry.JmxCounterMBean> group) {
//        final Set<NumericMetric> collect = group.labeledMBeans().entrySet().stream()
//                .map(e -> new NumericMetric(e.getKey(), e.getValue().getCount()))
//                .collect(Collectors.toSet());
//
//        return Stream.of(new GaugeMetricFamily(group.name(), group.help(), collect));
//    }
//
//
//    /**
//     * Collect a {@link Meter} as a Prometheus counter
//     */
//    public static Stream<MetricFamily<?>> meterAsCounter(final LabeledMBeanGroup<CassandraMetricsRegistry.JmxMeterMBean> group) {
//        final Set<NumericMetric> collect = group.labeledMBeans().entrySet().stream()
//                .map(e -> new NumericMetric(e.getKey(), e.getValue().getCount()))
//                .collect(Collectors.toSet());
//
//        return Stream.of(new CounterMetricFamily(group.name(), group.help(), collect));
//    }
//
//
//    /**
//     * Collect a {@link Gauge} with a {@see Number} value as a Prometheus gauge
//     */
//    public static Stream<MetricFamily<?>> numericGaugeAsGauge(final LabeledMBeanGroup<CassandraMetricsRegistry.JmxGaugeMBean> group) {
//        final Collection<NumericMetric> numericMetrics = Maps.transformEntries(group.labeledMBeans(), (k, v) -> new NumericMetric(k, (Number) v.getValue()))
//                .values();
//
//        return Stream.of(new GaugeMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(numericMetrics)));
//    }
//
//    /**
//     * Collect a {@link Gauge} with a {@see Number} value as a Prometheus counter
//     */
//    public static Stream<MetricFamily<?>> numericGaugeAsCounter(final LabeledMBeanGroup<CassandraMetricsRegistry.JmxGaugeMBean> group) {
//        final Collection<NumericMetric> numericMetrics = Maps.transformEntries(group.labeledMBeans(), (k, v) -> new NumericMetric(k, (Number) v.getValue()))
//                .values();
//
//        return Stream.of(new CounterMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(numericMetrics)));
//    }
//
//    /**
//     * Collect a {@link Gauge} with a Cassandra {@see EstimatedHistogram} value as a Prometheus summary
//     */
//    public static Stream<MetricFamily<?>> histogramGaugeAsSummary(final LabeledMBeanGroup<CassandraMetricsRegistry.JmxGaugeMBean> group) {
//        final Collection<SummaryMetricFamily.Summary> summaries =  Maps.transformEntries(group.labeledMBeans(), (labels, gauge) -> {
//            final long[] bucketData = (long []) gauge.getValue();
//
//            if (bucketData.length == 0) {
//                return new SummaryMetricFamily.Summary(labels, Double.NaN, Double.NaN, Maps.toMap(Quantile.STANDARD_QUANTILES, q -> Double.NaN));
//            }
//
//            final EstimatedHistogram histogram = new EstimatedHistogram(bucketData);
//
//            final Map<Quantile, Number> quantiles = Maps.toMap(Quantile.STANDARD_QUANTILES, q -> histogram.percentile(q.value));
//
//            return new SummaryMetricFamily.Summary(labels, Double.NaN, histogram.count(), quantiles);
//        }).values();
//
//        return Stream.of(new SummaryMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(summaries)));
//    }
//
//
//
//    public static Stream<MetricFamily<?>> timerAsSummary(final LabeledMBeanGroup<CassandraMetricsRegistry.JmxTimerMBean> group) {
//        final Collection<SummaryMetricFamily.Summary> values = Maps.transformEntries(group.labeledMBeans(), (labels, metric) -> {
//            final long count = metric.getCount();
//
//            final Map<Quantile, Number> quantiles = ImmutableMap.<Quantile, Number>builder()
////                    .put(.5, metric.get50thPercentile())
////                    .put(.75, metric.get75thPercentile())
////                    .put(.95, metric.get95thPercentile())
////                    .put(.98, metric.get98thPercentile())
////                    .put(.99, metric.get99thPercentile())
////                    .put(.999, metric.get999thPercentile())
//                    .build();
//
//            return new SummaryMetricFamily.Summary(labels, Double.NaN, count, quantiles);
//        }).values();
//
//        return Stream.of(new SummaryMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(values)));
//    }
//
//    public static Stream<MetricFamily<?>> histogramAsSummary(final LabeledMBeanGroup<CassandraMetricsRegistry.JmxHistogramMBean> group) {
//        final Collection<SummaryMetricFamily.Summary> values = Maps.transformEntries(group.labeledMBeans(), (labels, metric) -> {
//            final long count = metric.getCount();
//
//            final Map<Quantile, Number> quantiles = ImmutableMap.<Quantile, Number>builder()
////                    .put(.5, metric.get50thPercentile())
////                    .put(.75, metric.get75thPercentile())
////                    .put(.95, metric.get95thPercentile())
////                    .put(.98, metric.get98thPercentile())
////                    .put(.99, metric.get99thPercentile())
////                    .put(.999, metric.get999thPercentile())
//                    .build();
//
//            return new SummaryMetricFamily.Summary(labels, Double.NaN, count, quantiles);
//        }).values();
//
//        return Stream.of(new SummaryMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(values)));
//    }
//}
