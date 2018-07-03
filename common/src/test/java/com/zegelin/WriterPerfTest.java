package com.zegelin;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.Quantile;
import com.zegelin.prometheus.domain.SummaryMetricFamily;
import com.zegelin.prometheus.exposition.PrometheusTextFormatWriter;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class WriterPerfTest {
    static final Set<Double> QUANTILES = ImmutableSet.of(.5, .75, .95, .98, .99, .999);

    static final Map<String, String> labels = ImmutableMap.of(
            "table", "sometable",
            "keyspace", "somekeyspace"
    );

    public static void main(String[] args) throws IOException {

        final Stream<MetricFamily> metricsStream = IntStream.range(0, 30).mapToObj(i -> {

            final Stream<SummaryMetricFamily.Summary> summariesStream = IntStream.range(0, 300).mapToObj(j -> {
                final Map<Quantile, Number> quantileValues = ImmutableMap.copyOf(Maps.asMap(Quantile.STANDARD_QUANTILES, q -> 28));

                final Labels labels = new Labels(ImmutableMap.of(
                        "somelabel", Float.toString(j),
                        "someotherlabel", Float.toString(j)
                ));

                return new SummaryMetricFamily.Summary(labels, 56, 77, quantileValues);
            });

            return new SummaryMetricFamily(String.format("some_summary%d", i), "some help string", summariesStream);
        });


        final List<MetricFamily> metrics = metricsStream.collect(Collectors.toList());

        System.out.println("Ready.");
        System.in.read();

        final PrometheusTextFormatWriter writer = new PrometheusTextFormatWriter(ByteStreams.nullOutputStream(), Instant.now(), new Labels(ImmutableMap.of("global", "label")));

        int iterations = 100;

        final Stopwatch stopwatch = Stopwatch.createStarted();

        for (int i = 0; i < iterations; i++) {
            for (final MetricFamily metric : metrics) {
                metric.accept(writer);

            }

            if (i % 10 == 0)
                System.out.printf("%d done.%n", i);
        }

        final long elapsed = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);

        System.out.printf("%s ms total, %s ms per iteration%n", elapsed, elapsed / (float) iterations);
    }
}
