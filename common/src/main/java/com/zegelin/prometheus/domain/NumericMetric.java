package com.zegelin.prometheus.domain;

public class NumericMetric extends Metric {
    public final float value;

    public NumericMetric(final Labels labels, final float value) {
        super(labels);
        this.value = value;
    }
}
