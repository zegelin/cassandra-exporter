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
        public final Map<Quantile, Number> quantiles;

        public Histogram(final Labels labels, final Number sum, final Number count, final Map<Quantile, Number> quantiles) {
            super(labels);

            this.sum = sum;
            this.count = count;
            this.quantiles = ImmutableMap.<Quantile, Number>builder()
                    .putAll(quantiles)
                    .put(Quantile.POSITIVE_INFINITY, count)
                    .build();
        }
    }
}
