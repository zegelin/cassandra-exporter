package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HistogramMetricFamily extends MetricFamily<HistogramMetricFamily.Histogram> {
    public HistogramMetricFamily(final String name, final String help, final Stream<Histogram> metrics) {
        this(name, help, () -> metrics);
    }

    HistogramMetricFamily(final String name, final String help, final Supplier<Stream<Histogram>> metricsStreamSupplier) {
        super(name, help, metricsStreamSupplier);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public HistogramMetricFamily cachedCopy() {
        final List<Histogram> metrics = metrics().collect(Collectors.toList());

        return new HistogramMetricFamily(name, help, metrics::stream);
    }

    public static class Histogram extends Metric {
        public final float sum;
        public final float count;
        public final Iterable<Interval> buckets;

        public Histogram(final Labels labels, final float sum, final float count, final Iterable<Interval> buckets) {
            super(labels);

            this.sum = sum;
            this.count = count;
            this.buckets = ImmutableList.copyOf(buckets);
        }
    }
}
