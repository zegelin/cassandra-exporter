package com.zegelin.prometheus.domain;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

public abstract class MetricFamily<T extends Metric> {
    public final String name, help;
    public final Set<T> metrics;

    protected MetricFamily(final String name, final String help, final Set<T> metrics) {
        this.name = name;
        this.help = help;
        this.metrics = ImmutableSet.copyOf(metrics);
    }

    public abstract <R> R accept(final MetricFamilyVisitor<R> visitor);

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricFamily)) return false;

        final MetricFamily<?> that = (MetricFamily<?>) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("help", help)
                .add("metrics", metrics)
                .toString();
    }
}
