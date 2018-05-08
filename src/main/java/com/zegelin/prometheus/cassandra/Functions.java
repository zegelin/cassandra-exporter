package com.zegelin.prometheus.cassandra;

import com.codahale.metrics.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.zegelin.prometheus.cassandra.collector.dynamic.GroupThing;
import com.zegelin.prometheus.domain.*;
import org.apache.cassandra.utils.EstimatedHistogram;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Functions {
    static final Set<Double> QUANTILES = ImmutableSet.of(.5, .75, .95, .98, .99, .999);

    /**
     * Collect a {@link com.codahale.metrics.Counter} as a Prometheus counter
     */
    public static Stream<MetricFamily<?>> counterAsCounter(final GroupThing<Counter> group) {
        final Set<NumericMetric> collect = group.labeledMBeans().entrySet().stream()
                .map(e -> new NumericMetric(e.getKey(), e.getValue().getCount()))
                .collect(Collectors.toSet());

        return Stream.of(new CounterMetricFamily(group.name(), group.help(), collect));
    }

    /**
     * Collect a {@link com.codahale.metrics.Counter} as a Prometheus gauge
     */
    public static Stream<MetricFamily<?>> counterAsGauge(final GroupThing<Counter> group) {
        final Set<NumericMetric> collect = group.labeledMBeans().entrySet().stream()
                .map(e -> new NumericMetric(e.getKey(), e.getValue().getCount()))
                .collect(Collectors.toSet());

        return Stream.of(new GaugeMetricFamily(group.name(), group.help(), collect));
    }


    /**
     * Collect a {@link com.codahale.metrics.Meter} as a Prometheus counter
     */
    public static Stream<MetricFamily<?>> meterAsCounter(final GroupThing<Meter> group) {
        final Set<NumericMetric> collect = group.labeledMBeans().entrySet().stream()
                .map(e -> new NumericMetric(e.getKey(), e.getValue().getCount()))
                .collect(Collectors.toSet());

        return Stream.of(new CounterMetricFamily(group.name(), group.help(), collect));
    }


    /**
     * Collect a {@link com.codahale.metrics.Gauge} with a {@see Number} value as a Prometheus gauge
     */
    public static Stream<MetricFamily<?>> numericGaugeAsGauge(final GroupThing<Gauge<Number>> group) {
        final Collection<NumericMetric> numericMetrics = Maps.transformEntries(group.labeledMBeans(), (k, v) -> new NumericMetric(k, v.getValue()))
                .values();

        return Stream.of(new GaugeMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(numericMetrics)));
    }

    /**
     * Collect a {@link com.codahale.metrics.Gauge} with a {@see Number} value as a Prometheus counter
     */
    public static Stream<MetricFamily<?>> numericGaugeAsCounter(final GroupThing<Gauge<Number>> group) {
        final Collection<NumericMetric> numericMetrics = Maps.transformEntries(group.labeledMBeans(), (k, v) -> new NumericMetric(k, v.getValue()))
                .values();

        return Stream.of(new CounterMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(numericMetrics)));
    }

    /**
     * Collect a {@link com.codahale.metrics.Gauge} with a Cassandra {@see EstimatedHistogram} value as a Prometheus summary
     */
    public static Stream<MetricFamily<?>> histogramGaugeAsSummary(final GroupThing<Gauge<long[]>> group) {
        final Collection<SummaryMetricFamily.Summary> summaries =  Maps.transformEntries(group.labeledMBeans(), (labels, gauge) -> {
            final long[] bucketData = gauge.getValue();

            if (bucketData.length == 0) {
                return new SummaryMetricFamily.Summary(labels, Double.NaN, Double.NaN, Maps.toMap(QUANTILES, q -> Double.NaN));
            }

            final EstimatedHistogram histogram = new EstimatedHistogram(bucketData);

            final Map<Double, Number> quantiles = Maps.toMap(QUANTILES, histogram::percentile);

            return new SummaryMetricFamily.Summary(labels, Double.NaN, histogram.count(), quantiles);
        }).values();

        return Stream.of(new SummaryMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(summaries)));
    }


    public static <X extends Sampling & Counting> Stream<MetricFamily<?>> samplingAndCountingAsSummary(final GroupThing<X> group) {
        final Collection<SummaryMetricFamily.Summary> values = Maps.transformEntries(group.labeledMBeans(), (labels, metric) -> {
            final long count = metric.getCount();
            final Snapshot snapshot = metric.getSnapshot();

            final Map<Double, Number> quantiles = Maps.toMap(QUANTILES, snapshot::getValue);

            return new SummaryMetricFamily.Summary(labels, Double.NaN, count, quantiles);
        }).values();

        return Stream.of(new SummaryMetricFamily(group.name(), group.help(), ImmutableSet.copyOf(values)));
    }

    public static Stream<MetricFamily<?>> timerAsSummary(final GroupThing<Timer> group) {
        return samplingAndCountingAsSummary(group);
    }

    public static Stream<MetricFamily<?>> histogramAsSummary(final GroupThing<Histogram> group) {
        return samplingAndCountingAsSummary(group);
    }
}
