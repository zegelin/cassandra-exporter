package com.zegelin.prometheus.domain;

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableMap;
import com.zegelin.prometheus.exposition.PrometheusTextFormatWriter;

import java.util.Map;

public final class Labels extends ForwardingMap<String, String> {
    private final ImmutableMap<String, String> labels;
    private final String plainTextRepr;

    public Labels(final Map<String, String> labels) {
        this.labels = ImmutableMap.copyOf(labels);
        this.plainTextRepr = PrometheusTextFormatWriter.formatLabels(labels);
    }

    @Override
    protected Map<String, String> delegate() {
        return labels;
    }

    public String asPlainTextFormatString() {
        return this.plainTextRepr;
    }
}
