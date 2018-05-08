package com.zegelin.prometheus.domain;

public interface MetricFamilyVisitor<T> {
    T visit(final CounterMetricFamily metricFamily);
    T visit(final GaugeMetricFamily metricFamily);
    T visit(final SummaryMetricFamily metricFamily);
    T visit(final HistogramMetricFamily metricFamily);
    T visit(final UntypedMetricFamily metricFamily);
}
