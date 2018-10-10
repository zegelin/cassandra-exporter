package com.zegelin.prometheus.domain;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GaugeMetricFamily extends MetricFamily<NumericMetric> {
    public GaugeMetricFamily(final String name, final String help, final Stream<NumericMetric> metrics) {
        super(name, help, metrics);
    }

    private GaugeMetricFamily(final String name, final String help, final Supplier<Stream<NumericMetric>> metricsStreamSupplier) {
        super(name, help, metricsStreamSupplier);
    }


    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public MetricFamily<NumericMetric> cache() {
        final List<NumericMetric> metrics = metrics().collect(Collectors.toList());

        return new GaugeMetricFamily(name, help, metrics::stream);
    }
}
