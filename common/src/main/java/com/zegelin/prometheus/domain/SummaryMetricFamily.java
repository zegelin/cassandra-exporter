package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.stream.Stream;

public class SummaryMetricFamily extends MetricFamily<SummaryMetricFamily.Summary> {
    public SummaryMetricFamily(final String name, final String help, final Stream<Summary> metrics) {
        super(name, help, metrics);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public static class Summary extends Metric {
        public final float sum;
        public final float count;
        public final Map<Quantile, Float> quantiles;

        public Summary(final Labels labels, final float sum, final float count, final Map<Quantile, Float> quantiles) {
            super(labels);

            this.sum = sum;
            this.count = count;
            this.quantiles = ImmutableMap.copyOf(quantiles);
        }
    }
}
