package com.zegelin.prometheus.domain;

import java.util.stream.Stream;

public class CounterMetricFamily extends MetricFamily<NumericMetric> {
    public CounterMetricFamily(final String name, final String help, final Stream<NumericMetric> metrics) {
        super(name, help, metrics);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
