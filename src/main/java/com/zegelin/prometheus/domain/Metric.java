package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

public abstract class Metric {
    public final Map<String, String> labels;

    protected Metric(final Map<String, String> labels) {
        this.labels = ImmutableMap.copyOf(labels);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof Metric)) return false;

        final Metric metric = (Metric) o;
        return Objects.equals(labels, metric.labels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labels);
    }
}
