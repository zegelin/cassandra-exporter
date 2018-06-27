package com.zegelin.prometheus.cassandra;

import com.codahale.metrics.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector.Factory.Builder.CollectorConstructor;
import com.zegelin.prometheus.cassandra.collector.dynamic.FunctionCollector;
import com.zegelin.prometheus.cassandra.collector.dynamic.FunctionCollector.CollectorFunction;
import com.zegelin.prometheus.domain.*;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.utils.EstimatedHistogram;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectorFunctions {

    public static <T> CollectorConstructor asCollector(final CollectorFunction<T> fn) {
        return ((name, help, labels, mBean) -> new FunctionCollector<>(name, help, ImmutableMap.of(labels, mBean.cast()), fn));
    }

    /**
     * Collect a {@link CassandraMetricsRegistry.JmxCounterMBean} as a Prometheus counter
     */
    public static CollectorFunction<CassandraMetricsRegistry.JmxCounterMBean> counterAsCounter(final LongFunction<Long> scaleFunction) {
        return group -> {
            final Set<NumericMetric> collect = group.labeledMBeans().entrySet().stream()
                    .map(e -> new NumericMetric(e.getKey(), scaleFunction.apply(e.getValue().getCount())))
                    .collect(Collectors.toSet());

            return Stream.of(new CounterMetricFamily(group.name(), group.help(), collect));
        };
    }

    public static CollectorFunction<CassandraMetricsRegistry.JmxCounterMBean> counterAsCounter() {
        return counterAsCounter(l -> l);
    }


    /**
     * Collect a {@link CassandraMetricsRegistry.JmxCounterMBean} as a Prometheus gauge
     */
    public static CollectorFunction<CassandraMetricsRegistry.JmxCounterMBean> counterAsGauge(final LongFunction<Long> scaleFunction) {
        return group -> {
            final Set<NumericMetric> collect = group.labeledMBeans().entrySet().stream()
                    .map(e -> new NumericMetric(e.getKey(), scaleFunction.apply(e.getValue().getCount())))
                    .collect(Collectors.toSet());

            return Stream.of(new GaugeMetricFamily(group.name(), group.help(), collect));
        };
    }

    public static CollectorFunction<CassandraMetricsRegistry.JmxCounterMBean> counterAsGauge() {
        return counterAsGauge(l -> l);
    }



    /**
     * Collect a {@link CassandraMetricsRegistry.JmxMeterMBean} as a Prometheus counter
     */
    public static CollectorFunction<CassandraMetricsRegistry.JmxMeterMBean> meterAsCounter(final LongFunction<Long> scaleFunction) {
        return group -> {
            final Set<NumericMetric> collect = group.labeledMBeans().entrySet().stream()
                    .map(entry -> new NumericMetric(entry.getKey(), scaleFunction.apply(entry.getValue().getCount())))
                    .collect(Collectors.toSet());

            return Stream.of(new CounterMetricFamily(group.name(), group.help(), collect));
        };
    }

    public static CollectorFunction<CassandraMetricsRegistry.JmxMeterMBean> meterAsCounter() {
        return meterAsCounter(l -> l);
    }


    /**
     * Collect a {@link CassandraMetricsRegistry.JmxGaugeMBean} with a {@link Number} value as a Prometheus gauge
     */
    public static CollectorFunction<CassandraMetricsRegistry.JmxGaugeMBean> numericGaugeAsGauge(final Function<Number, Number> scaleFunction) {
        return group -> {
            final Collection<NumericMetric> numericMetrics = Maps.transformEntries(group.labeledMBeans(), (k, v) -> new NumericMetric(k, scaleFunction.apply((Number) v.getValue())))
                    .values();

            return Stream.of(new GaugeMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(numericMetrics)));
        };
    }

    public static CollectorFunction<CassandraMetricsRegistry.JmxGaugeMBean> numericGaugeAsGauge() {
        return numericGaugeAsGauge(Function.identity());
    }


    /**
     * Collect a {@link CassandraMetricsRegistry.JmxGaugeMBean} with a {@see Number} value as a Prometheus counter
     */
    public static CollectorFunction<CassandraMetricsRegistry.JmxGaugeMBean> numericGaugeAsCounter() {
        return group -> {
            final Collection<NumericMetric> numericMetrics = Maps.transformEntries(group.labeledMBeans(), (k, v) -> new NumericMetric(k, (Number) v.getValue()))
                    .values();

            return Stream.of(new CounterMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(numericMetrics)));
        };
    }

    /**
     * Collect a {@link CassandraMetricsRegistry.JmxGaugeMBean} with a Cassandra {@link EstimatedHistogram} value as a Prometheus summary
     */
    public static CollectorFunction<CassandraMetricsRegistry.JmxGaugeMBean> histogramGaugeAsSummary(final Function<Number, Number> quantileScaleFunction) {
        return group -> {
            final Collection<SummaryMetricFamily.Summary> summaries =  Maps.transformEntries(group.labeledMBeans(), (labels, gauge) -> {
                final long[] bucketData = (long[]) gauge.getValue();

                if (bucketData.length == 0) {
                    return new SummaryMetricFamily.Summary(labels, Double.NaN, Double.NaN, Maps.toMap(Quantile.STANDARD_QUANTILES, q -> Double.NaN));
                }

                final EstimatedHistogram histogram = new EstimatedHistogram(bucketData);

                final Map<Quantile, Number> quantiles = Maps.toMap(Quantile.STANDARD_QUANTILES, q -> quantileScaleFunction.apply(histogram.percentile(q.value)));

                return new SummaryMetricFamily.Summary(labels, Double.NaN, histogram.count(), quantiles);
            }).values();

            return Stream.of(new SummaryMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(summaries)));
        };
    }

    public static CollectorFunction<CassandraMetricsRegistry.JmxGaugeMBean> histogramGaugeAsSummary() {
        return histogramGaugeAsSummary(l -> l);
    }


//    public static <X extends Sampling & Counting> Stream<MetricFamily<?>> samplingAndCountingAsSummary(final FunctionCollector.LabeledMBeanGroup<X> group) {
//        final Collection<SummaryMetricFamily.Summary> values = Maps.transformEntries(group.labeledMBeans(), (labels, metric) -> {
//            final long count = metric.getCount();
//            final Snapshot snapshot = metric.getSnapshot();
//
//            final Map<Quantile, Number> quantiles = Maps.toMap(Quantile.STANDARD_QUANTILES, q -> snapshot.getValue(q.value));
//
//            return new SummaryMetricFamily.Summary(labels, Double.NaN, count, quantiles);
//        }).values();
//
//        return Stream.of(new SummaryMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(values)));
//    }
//
//    public static Stream<MetricFamily<?>> timerAsSummary(final FunctionCollector.LabeledMBeanGroup<Timer> group) {
//        return samplingAndCountingAsSummary(group);
//    }
//
//    public static Stream<MetricFamily<?>> histogramAsSummary(final FunctionCollector.LabeledMBeanGroup<Histogram> group) {
//        return samplingAndCountingAsSummary(group);
//    }



    public static <X extends Sampling & Counting> CollectorFunction<X> samplingAndCountingAsSummary() {
        return group -> {
            final Collection<SummaryMetricFamily.Summary> values = Maps.transformEntries(group.labeledMBeans(), (labels, metric) -> {
                final long count = metric.getCount();
                final Snapshot snapshot = metric.getSnapshot();

                final Map<Quantile, Number> quantiles = Maps.toMap(Quantile.STANDARD_QUANTILES, q -> snapshot.getValue(q.value));

                return new SummaryMetricFamily.Summary(labels, Double.NaN, count, quantiles);
            }).values();

            return Stream.of(new SummaryMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(values)));
        };
    }


    public static CollectorFunction<CassandraMetricsRegistry.JmxTimerMBean> timerAsSummary(final Function<Number, Number> quantileScaleFunction) {
        return group -> {
//            Maps.transformEntries(group.labeledMBeans(), (labels, timer) -> {
//                return null;
//            }).values();
//
//            return Stream.of(new SummaryMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(values)));

            return Stream.of();
        };
    }

    public static CollectorFunction<CassandraMetricsRegistry.JmxTimerMBean> timerAsSummary() {
        return timerAsSummary(Function.identity());
    }


    public static CollectorFunction<CassandraMetricsRegistry.JmxHistogramMBean> histogramAsSummary(final Function<Number, Number> quantileScaleFunction) {
        return group -> {
            return Stream.of();
        };
    }

    public static CollectorFunction<CassandraMetricsRegistry.JmxHistogramMBean> histogramAsSummary() {
        return histogramAsSummary(Function.identity());
    }


    public static Stream<MetricFamily<?>> histogramAsSummary(final FunctionCollector.LabeledMBeanGroup<CassandraMetricsRegistry.JmxHistogramMBean> group) {
        return Stream.of();//samplingAndCountingAsSummary(group);
    }
}
