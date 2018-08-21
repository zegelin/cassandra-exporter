package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.stream.Stream;

public class HistogramMetricFamily extends MetricFamily<HistogramMetricFamily.Histogram> {
    protected HistogramMetricFamily(final String name, final String help, final Stream<Histogram> metrics) {
        super(name, help, metrics);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public static class Histogram extends Metric {
        public final float sum;
        public final float count;
        public final Map<Quantile, Float> buckets;

        public Histogram(final Labels labels, final float sum, final float count, final Map<Quantile, Float> buckets) {
            super(labels);

            this.sum = sum;
            this.count = count;
            this.buckets = ImmutableMap.<Quantile, Float>builder()
                    .putAll(buckets)
                    .put(Quantile.POSITIVE_INFINITY, count)
                    .build();
        }
    }
}
