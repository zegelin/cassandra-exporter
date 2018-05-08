package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

public class SummaryMetricFamily extends MetricFamily<SummaryMetricFamily.Summary> {
    public SummaryMetricFamily(final String name, final String help, final Set<Summary> metrics) {
        super(name, help, metrics);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public static class Summary extends Metric {
        public final Number sum, count;
        public final Map<Double, Number> quantiles;

        public Summary(final Map<String, String> labels, final double sum, final double count, final Map<Double, Number> quantiles) {
            super(labels);

            this.sum = sum;
            this.count = count;
            this.quantiles = ImmutableMap.copyOf(quantiles);
        }
    }
}
