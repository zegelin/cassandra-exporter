package com.zegelin.prometheus.domain;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SummaryMetricFamily extends MetricFamily<SummaryMetricFamily.Summary> {
    public SummaryMetricFamily(final String name, final String help, final Stream<Summary> metrics) {
        super(name, help, metrics);
    }

    private SummaryMetricFamily(final String name, final String help, final Supplier<Stream<Summary>> metricsStreamSupplier) {
        super(name, help, metricsStreamSupplier);
    }

    @Override
    public <R> R accept(final MetricFamilyVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public MetricFamily<Summary> cache() {
        final List<Summary> metrics = metrics().collect(Collectors.toList());

        return new SummaryMetricFamily(name, help, metrics::stream);
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
