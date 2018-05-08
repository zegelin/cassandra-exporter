package com.zegelin.prometheus.domain;

import java.util.Map;

public class NumericMetric extends Metric {
    public final Number value;

    public NumericMetric(final Map<String, String> labels, final Number value) {
        super(labels);
        this.value = value;
    }
}
