package com.zegelin.prometheus.domain;

import java.util.Set;

public class GaugeMetricFamily extends MetricFamily<NumericMetric> {
    public GaugeMetricFamily(final String name, final String help, final Set<NumericMetric> metrics) {
        super(name, help, metrics);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
