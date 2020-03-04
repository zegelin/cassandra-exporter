package com.zegelin.cassandra.exporter;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zegelin.function.FloatFloatFunction;
import com.zegelin.cassandra.exporter.collector.dynamic.FunctionalMetricFamilyCollector.CollectorFunction;
import com.zegelin.cassandra.exporter.collector.dynamic.FunctionalMetricFamilyCollector.LabeledObjectGroup;
import com.zegelin.prometheus.domain.*;
import com.zegelin.prometheus.domain.Interval.Quantile;

import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxCounterMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxGaugeMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.JmxMeterMBean;
import org.apache.cassandra.utils.EstimatedHistogram;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CollectorFunctions {
    private CollectorFunctions() {}

    private static Stream<NumericMetric> counterMetricsStream(final LabeledObjectGroup<JmxCounterMBean> group, final FloatFloatFunction scaleFunction) {
        return group.labeledObjects().entrySet().stream()
                .map(e -> new Object() {
                    final Labels labels = e.getKey();
                    final JmxCounterMBean counter = e.getValue();
                })
                .map(e -> new NumericMetric(e.labels, scaleFunction.apply((float) e.counter.getCount())));
    }

    /**
     * Collect a {@link JmxCounterMBean} as a Prometheus counter
     */
    public static CollectorFunction<JmxCounterMBean> counterAsCounter(final FloatFloatFunction scaleFunction) {
        return group -> {
            final Stream<NumericMetric> metricStream = counterMetricsStream(group, scaleFunction);

            return Stream.of(new CounterMetricFamily(group.name(), group.help(), metricStream));
        };
    }

    public static CollectorFunction<JmxCounterMBean> counterAsCounter() {
        return counterAsCounter(l -> l);
    }


    /**
     * Collect a {@link JmxCounterMBean} as a Prometheus gauge
     */
    public static CollectorFunction<JmxCounterMBean> counterAsGauge(final FloatFloatFunction scaleFunction) {
        return group -> {
            final Stream<NumericMetric> metricStream = counterMetricsStream(group, scaleFunction);

            return Stream.of(new GaugeMetricFamily(group.name(), group.help(), metricStream));
        };
    }

    public static CollectorFunction<JmxCounterMBean> counterAsGauge() {
        return counterAsGauge(FloatFloatFunction.identity());
    }



    /**
     * Collect a {@link JmxMeterMBean} as a Prometheus counter
     */
    public static CollectorFunction<JmxMeterMBean> meterAsCounter(final FloatFloatFunction scaleFunction) {
        return group -> {
            final Stream<NumericMetric> metricStream = group.labeledObjects().entrySet().stream()
                    .map(e -> new Object() {
                        final Labels labels = e.getKey();
                        final JmxMeterMBean meter = e.getValue();
                    })
                    .map(e -> new NumericMetric(e.labels, scaleFunction.apply((float) e.meter.getCount())));


            return Stream.of(new CounterMetricFamily(group.name(), group.help(), metricStream));
        };
    }

    public static CollectorFunction<JmxMeterMBean> meterAsCounter() {
        return meterAsCounter(FloatFloatFunction.identity());
    }


    private static Stream<NumericMetric> numericGaugeMetricsStream(final LabeledObjectGroup<JmxGaugeMBean> group, final FloatFloatFunction scaleFunction) {
        return group.labeledObjects().entrySet().stream()
                .map(e -> new Object() {
                    final Labels labels = e.getKey();
                    final JmxGaugeMBean gauge = e.getValue();
                })
                .map(e -> new NumericMetric(e.labels, scaleFunction.apply(((Number) e.gauge.getValue()).floatValue())));
    }

    /**
     * Collect a {@link JmxGaugeMBean} with a {@link Number} value as a Prometheus gauge
     */
    public static CollectorFunction<JmxGaugeMBean> numericGaugeAsGauge(final FloatFloatFunction scaleFunction) {
        return group -> {
            final Stream<NumericMetric> metricStream = numericGaugeMetricsStream(group, scaleFunction);

            return Stream.of(new GaugeMetricFamily(group.name(), group.help(), metricStream));
        };
    }

    public static CollectorFunction<JmxGaugeMBean> numericGaugeAsGauge() {
        return numericGaugeAsGauge(FloatFloatFunction.identity());
    }


    /**
     * Collect a {@link JmxGaugeMBean} with a {@link Number} value as a Prometheus counter
     */
    public static CollectorFunction<JmxGaugeMBean> numericGaugeAsCounter(final FloatFloatFunction scaleFunction) {
        return group -> {
            final Stream<NumericMetric> metricStream = numericGaugeMetricsStream(group, scaleFunction);

            return Stream.of(new CounterMetricFamily(group.name(), group.help(), metricStream));
        };
    }

    public static CollectorFunction<JmxGaugeMBean> numericGaugeAsCounter() {
        return numericGaugeAsCounter(FloatFloatFunction.identity());
    }



    /**
     * Collect a {@link JmxGaugeMBean} with a Cassandra {@link EstimatedHistogram} value as a Prometheus summary
     */
    public static CollectorFunction<JmxGaugeMBean> histogramGaugeAsSummary(final FloatFloatFunction bucketScaleFunction,Set<Quantile> excludeQuantiles) {
        Set<Quantile> includedQuantiles =excludeQuantiles==null?Interval.Quantile.STANDARD_PERCENTILES:Sets.difference(Interval.Quantile.STANDARD_PERCENTILES,excludeQuantiles);
        return group -> {
            final Stream<SummaryMetricFamily.Summary> summaryStream = group.labeledObjects().entrySet().stream()
                    .map(e -> new Object() {
                        final Labels labels = e.getKey();
                        final JmxGaugeMBean gauge = e.getValue();
                    })
                    .map(e -> {
                        final long[] bucketData = (long[]) e.gauge.getValue();

                        if (bucketData.length == 0) {
                            return new SummaryMetricFamily.Summary(e.labels, Float.NaN, Float.NaN, Interval.asIntervals(includedQuantiles, q -> Float.NaN));
                        }

                        final EstimatedHistogram histogram = new EstimatedHistogram(bucketData);
                        final Iterable<Interval> quantiles = Interval.asIntervals(includedQuantiles, q -> bucketScaleFunction.apply((float) histogram.percentile(q.value)));

                        return new SummaryMetricFamily.Summary(e.labels, Float.NaN, histogram.count(), quantiles);
                    });

            return Stream.of(new SummaryMetricFamily(group.name(), group.help(), summaryStream));
        };
    }

    public static CollectorFunction<JmxGaugeMBean> histogramGaugeAsSummary(Set<Quantile> excludeQuantiles) {
        return histogramGaugeAsSummary((l -> l),excludeQuantiles);
    }

    /**
     * Collect a {@link SamplingCounting} as a Prometheus summary
     */
    protected static CollectorFunction<SamplingCounting> samplingAndCountingAsSummary(final FloatFloatFunction quantileScaleFunction,Set<Quantile> excludeQuantiles) {
        return group -> {
            final Stream<SummaryMetricFamily.Summary> summaryStream = group.labeledObjects().entrySet().stream()
                    .map(e -> new Object() {
                        final Labels labels = e.getKey();
                        final SamplingCounting samplingCounting = e.getValue();
                    })
                    .map(e -> {
                        
                        Iterator<Interval> itr = e.samplingCounting.getIntervals().iterator();
                        ArrayList<Interval> filtered = Lists.newArrayList();
                        while(itr.hasNext()) {
                            Interval interval = itr.next();
                            if(excludeQuantiles.contains(interval.quantile)) {
                                continue;
                            }
                            filtered.add(interval);
                        }
                        
                        final Iterable<Interval> quantiles = Iterables.transform(filtered, i -> i.transform(quantileScaleFunction));
                        return new SummaryMetricFamily.Summary(e.labels, Float.NaN, e.samplingCounting.getCount(), quantiles);
                    });

            return Stream.of(new SummaryMetricFamily(group.name(), group.help(), summaryStream));
        };
    }

    public static CollectorFunction<SamplingCounting> samplingAndCountingAsSummary(Set<Quantile> excludeQuantiles) {
        return samplingAndCountingAsSummary(FloatFloatFunction.identity(),excludeQuantiles);
    }
}
