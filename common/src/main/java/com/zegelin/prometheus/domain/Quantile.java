package com.zegelin.prometheus.domain;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

public final class Quantile {
    public static final Quantile Q_50 = q(.5f);
    public static final Quantile Q_75 = q(.75f);
    public static final Quantile Q_95 = q(.95f);
    public static final Quantile Q_98 = q(.98f);
    public static final Quantile Q_99 = q(.99f);
    public static final Quantile Q_999 = q(.999f);

    public static final Set<Quantile> STANDARD_QUANTILES = ImmutableSet.of(Q_50, Q_75, Q_95, Q_98, Q_99, Q_999);

    static final Quantile POSITIVE_INFINITY = q(Float.POSITIVE_INFINITY);

    public final float value;

    private final String cachedStringRepr;
    private final Labels summaryLabels, histogramLabels;

    public Quantile(final float value) {
        this.value = value;

        this.cachedStringRepr = Double.toString(value);
        this.summaryLabels = new Labels(ImmutableMap.of("quantile", cachedStringRepr));
        this.histogramLabels = new Labels(ImmutableMap.of("le", cachedStringRepr));
    }

    private static Quantile q(final float value) {
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
        return summaryLabels;
    }

    public Labels asHistogramLabels() {
        return histogramLabels;
    }
}
