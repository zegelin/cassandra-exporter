package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;

public class HistogramMetricFamily extends MetricFamily<HistogramMetricFamily.Histogram> {
    protected HistogramMetricFamily(final String name, final String help, final Set<Histogram> metrics) {
        super(name, help, metrics);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public static class Histogram extends Metric {
        public final Number sum, count;
        public final Map<Double, Number> quantiles;

        public Histogram(final Map<String, String> labels, final Number sum, final Number count, final Map<Double, Number> quantiles) {
            super(labels);

            this.sum = sum;
            this.count = count;
            this.quantiles = ImmutableMap.<Double, Number>builder()
                    .putAll(quantiles)
                    .put(Double.POSITIVE_INFINITY, count)
                    .build();
        }
    }
}
