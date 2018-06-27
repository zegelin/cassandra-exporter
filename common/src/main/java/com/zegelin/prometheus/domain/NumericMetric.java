package com.zegelin.prometheus.domain;

public class NumericMetric extends Metric {
    public final Number value;

    public NumericMetric(final Labels labels, final Number value) {
        super(labels);
        this.value = value;
    }
}
