package com.zegelin.prometheus.cassandra.collector;

import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.CounterMetricFamily;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;

import java.util.stream.Stream;

public abstract class GossiperMBeanMetricFamilyCollector extends MBeanGroupMetricFamilyCollector {
    protected abstract void collect(final Stream.Builder<NumericMetric> generationNumberMetrics,
                                    final Stream.Builder<NumericMetric> downtimeMetrics,
                                    final Stream.Builder<NumericMetric> activeMetrics);

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<NumericMetric> generationNumberMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> downtimeMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> activeMetrics = Stream.builder();

        collect(generationNumberMetrics, downtimeMetrics, activeMetrics);

        return Stream.of(
                new GaugeMetricFamily("cassandra_endpoint_generation", "Current endpoint generation number.", generationNumberMetrics.build()),
                new CounterMetricFamily("cassandra_endpoint_downtime_seconds_total", "Endpoint downtime (since server start).", downtimeMetrics.build()),
                new GaugeMetricFamily("cassandra_endpoint_active", "Endpoint activeness (0 = down, 1 = up).", activeMetrics.build())
        );
    }
}
