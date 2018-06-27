package com.zegelin.prometheus.cassandra;

import com.zegelin.prometheus.domain.MetricFamily;

import java.util.Map;
import java.util.stream.Stream;

public interface Harvester {

    public Stream<MetricFamily> collect();

    public Map<String, String> globalLabels();
}
