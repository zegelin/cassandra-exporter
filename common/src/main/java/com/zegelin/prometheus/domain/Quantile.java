package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

public final class Quantile {
    public static final Set<Quantile> STANDARD_QUANTILES = ImmutableSet.of(q(.5), q(.75), q(.95), q(.98), q(.99), q(.999));

    public static final Quantile POSITIVE_INFINITY = q(Double.POSITIVE_INFINITY);

    public final double value;

    private final String cachedStringRepr;
    private final Labels summaryLabel, histogramLabel;

    public Quantile(final double value) {
        this.value = value;

        this.cachedStringRepr = Double.toString(value);
        this.summaryLabel = new Labels(ImmutableMap.of("quantile", cachedStringRepr));
        this.histogramLabel = new Labels(ImmutableMap.of("le", cachedStringRepr));
    }

    private static Quantile q(final double value) {
        return new Quantile(value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Quantile quantile = (Quantile) o;
        return Double.compare(quantile.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return cachedStringRepr;
    }

    public Labels asSummaryLabels() {
        return summaryLabel;
    }

    public Labels asHistogramLabels() {
        return histogramLabel;
    }
}
