package com.zegelin.prometheus.domain;

import java.util.Objects;

public abstract class Metric {
    public final Labels labels;

    protected Metric(final Labels labels) {
        this.labels = labels;
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
