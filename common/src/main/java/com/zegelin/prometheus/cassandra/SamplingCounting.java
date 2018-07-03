package com.zegelin.prometheus.cassandra;

import com.zegelin.prometheus.domain.Quantile;

import java.util.Map;

/**
 * Similar to {@link com.codahale.metrics.Sampling} and {@link com.codahale.metrics.Counting}
 * but as a concrete interface and also deals with quantiles directly, rather than {@link com.codahale.metrics.Snapshot}s.
 * This makes it adaptable to JMX MBeans that only expose known quantiles.
 */
public interface SamplingCounting {
    long getCount();

    Map<Quantile, Number> getQuantiles();
}
