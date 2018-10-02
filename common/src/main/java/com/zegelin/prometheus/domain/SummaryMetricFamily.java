package com.zegelin.prometheus.domain;

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
        public final Stream<Interval> quantiles;

        public Summary(final Labels labels, final float sum, final float count, final Stream<Interval> quantiles) {
            super(labels);

            this.sum = sum;
            this.count = count;
            this.quantiles = quantiles;
        }
    }
}
