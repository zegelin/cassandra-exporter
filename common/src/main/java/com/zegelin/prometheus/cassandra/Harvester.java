package com.zegelin.prometheus.cassandra;

import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;

import java.util.stream.Stream;

public interface Harvester {
    public Stream<MetricFamily> collect();

    public Labels globalLabels();
}
