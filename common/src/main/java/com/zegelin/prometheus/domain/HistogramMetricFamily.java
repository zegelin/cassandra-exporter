package com.zegelin.prometheus.domain;

import java.util.stream.Stream;

public class HistogramMetricFamily extends MetricFamily<HistogramMetricFamily.Histogram> {
    public HistogramMetricFamily(final String name, final String help, final Stream<Histogram> metrics) {
        super(name, help, metrics);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public static class Histogram extends Metric {
        public final float sum;
        public final float count;
        public final Stream<Interval> buckets;

        public Histogram(final Labels labels, final float sum, final float count, final Stream<Interval> buckets) {
            super(labels);

            this.sum = sum;
            this.count = count;
            this.buckets = buckets;
        }
    }
}
