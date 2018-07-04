package com.zegelin.prometheus.exposition;

import com.google.common.base.Stopwatch;
import com.google.common.escape.CharEscaper;
import com.google.common.escape.Escaper;
import com.google.common.io.CharStreams;
import com.zegelin.prometheus.domain.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

public class PrometheusTextFormatWriter implements Closeable, MetricFamilyVisitor {
    private final String timestamp;
    private final Labels globalLabels;

    private static final String BANNER = loadBanner();
    private final OutputStream stream;

    private final StringBuilder stringBuilder = new StringBuilder(1024*1024*10);

    static class Statistics {
        int metricFamilyCount = 0;
        int metricCount = 0;

        private final Stopwatch stopwatch = Stopwatch.createStarted();

        void stopAndAppendStatistics(final StringBuilder stringBuilder) {
            stopwatch.stop();

            stringBuilder.append("\n\n");
            stringBuilder.append("# Thanks and come again!\n\n");
            stringBuilder.append(String.format("# Wrote %s metrics for %s metric families in %s\n", metricCount, metricFamilyCount, stopwatch.toString()));
        }
    }

    private final Statistics statistics = new Statistics();

    private static String loadBanner() {
        // TODO: add git commit hash, version info, etc.

        try (final InputStream bannerStream = PrometheusTextFormatWriter.class.getResourceAsStream("/banner.txt")) {
            final StringBuilder stringBuilder = new StringBuilder();

            CharStreams.copy(new InputStreamReader(bannerStream), stringBuilder);

            stringBuilder.append("# prometheus-cassandra <version> <git sha>"); // TODO!
            stringBuilder.append("\n\n");

            return stringBuilder.toString();

        } catch (final IOException e) {
            // that's a shame
        }

        return "# prometheus-cassandra\n\n";
    }

    private enum MetricFamilyType {
        GAUGE,
        COUNTER,
        HISTOGRAM,
        SUMMARY,
        UNTYPED;

        public final String id;

        MetricFamilyType() {
            id = this.name().toLowerCase();
        }
    }

    private static class Escapers {
        private static char[] ESCAPED_SLASH = new char[]{'\\', '\\'};
        private static char[] ESCAPED_NEW_LINE = new char[]{'\\', 'n'};
        private static char[] ESCAPED_DOUBLE_QUOTE = new char[]{'\\', '"'};

        private static Escaper HELP_STRING_ESCAPER = new CharEscaper() {
            @Override
            protected char[] escape(final char c) {
                switch (c) {
                    case '\\': return ESCAPED_SLASH;
                    case '\n': return ESCAPED_NEW_LINE;
                    default: return null;
                }
            }
        };

        private static Escaper LABEL_VALUE_ESCAPER = new CharEscaper() {
            @Override
            protected char[] escape(final char c) {
                switch (c) {
                    case '\\': return ESCAPED_SLASH;
                    case '\n': return ESCAPED_NEW_LINE;
                    case '"': return ESCAPED_DOUBLE_QUOTE;
                    default: return null;
                }
            }
        };
    }

    private static void writeLabel(final StringBuilder stringBuilder, final Map.Entry<String, String> label) {
        stringBuilder.append(label.getKey())
                .append("=\"")
                .append(Escapers.LABEL_VALUE_ESCAPER.escape(label.getValue()))
                .append('"');
    }

    private static void writeLabels(final StringBuilder stringBuilder, final Map<String, String> labels) {
        if (labels.isEmpty())
            return;

        final Iterator<Map.Entry<String, String>> labelsIterator = labels.entrySet().iterator();

        writeLabel(stringBuilder, labelsIterator.next());

        while (labelsIterator.hasNext()) {
            stringBuilder.append(',');
            writeLabel(stringBuilder, labelsIterator.next());
        }
    }

    public static String formatLabels(final Map<String, String> labels) {
        final StringBuilder stringBuilder = new StringBuilder();

        writeLabels(stringBuilder, labels);

        return stringBuilder.toString();
    }


    public PrometheusTextFormatWriter(final OutputStream stream, final Instant timestamp, final Labels globalLabels) {
        this.stream = stream;
        this.timestamp = " " + Long.toString(timestamp.toEpochMilli());
        this.globalLabels = globalLabels;

        write(sb -> sb.append(BANNER));
    }

    private void write(final Consumer<StringBuilder> consumer) {
        stringBuilder.setLength(0);
        consumer.accept(stringBuilder);

        try {
            stream.write(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));

        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeFamilyHeader(final StringBuilder stringBuilder, final MetricFamily<?> metricFamily, final MetricFamilyType type) {
        statistics.metricFamilyCount ++;

        if (metricFamily.help != null) {
            stringBuilder.append("# HELP ")
                    .append(metricFamily.name)
                    .append(' ')
                    .append(Escapers.HELP_STRING_ESCAPER.escape(metricFamily.help))
                    .append('\n');
        }

        stringBuilder.append("# TYPE ")
                .append(metricFamily.name)
                .append(' ')
                .append(type.id)
                .append('\n');
    }

    private  void writeLabels(final StringBuilder stringBuilder, final Labels... labelSets) {
        stringBuilder.append('{');

        for (int i = 0; i < labelSets.length; i++) {
            final Labels labels = labelSets[i];

            if (labels.isEmpty())
                continue;

            stringBuilder.append(labels.asPlainTextFormatString());

            if (i != labelSets.length - 1)
                stringBuilder.append(',');
        }

        stringBuilder.append('}');
    }

    private void writeMetric(final StringBuilder stringBuilder, final String familyName, final String suffix, final float value, final Labels... labelSets) {
        statistics.metricCount ++;

        // family + optional suffix
        stringBuilder.append(familyName);
        if (suffix != null) {
            stringBuilder.append('_')
                    .append(suffix);
        }

        writeLabels(stringBuilder, labelSets);

        stringBuilder.append(' ')
                .append(value)
                .append(timestamp);

        stringBuilder.append('\n');
    }

    private void writeNumericFamily(final MetricFamily<NumericMetric> metricFamily, final MetricFamilyType type) {
        write(stringBuilder -> {
            writeFamilyHeader(stringBuilder, metricFamily, type);

            metricFamily.metrics.forEach(metric -> {
                writeMetric(stringBuilder, metricFamily.name, null, metric.value.floatValue(), metric.labels, globalLabels);
            });
        });
    }

    @Override
    public Void visit(final CounterMetricFamily metricFamily) {
        writeNumericFamily(metricFamily, MetricFamilyType.COUNTER);
        return null;
    }

    @Override
    public Void visit(final GaugeMetricFamily metricFamily) {
        writeNumericFamily(metricFamily, MetricFamilyType.GAUGE);
        return null;
    }



    @Override
    public Void visit(final SummaryMetricFamily metricFamily) {
        write(stringBuilder -> {
            writeFamilyHeader(stringBuilder, metricFamily, MetricFamilyType.SUMMARY);

            for (final Metric metric : metricFamily.metrics) {
                final SummaryMetricFamily.Summary summary = (SummaryMetricFamily.Summary) metric;

                writeMetric(stringBuilder, metricFamily.name, "sum", summary.sum.floatValue(), summary.labels);
                writeMetric(stringBuilder, metricFamily.name, "count", summary.count.floatValue(), summary.labels);

                summary.quantiles.forEach((quantile, value) -> {
                    writeMetric(stringBuilder, metricFamily.name, null, value.floatValue(), summary.labels, quantile.asSummaryLabels(), globalLabels);
                });
            }
        });

        return null;
    }

    @Override
    public Void visit(final HistogramMetricFamily metricFamily) {
        write(stringBuilder -> {
            writeFamilyHeader(stringBuilder, metricFamily, MetricFamilyType.HISTOGRAM);

            metricFamily.metrics.forEach(histogram -> {
                writeMetric(stringBuilder, metricFamily.name, "sum", histogram.sum.floatValue(), histogram.labels);
                writeMetric(stringBuilder, metricFamily.name, "count", histogram.count.floatValue(), histogram.labels);

                histogram.quantiles.forEach((quantile, value) -> {
                    writeMetric(stringBuilder, metricFamily.name, "bucket", value.floatValue(), histogram.labels, quantile.asHistogramLabels(), globalLabels);
                });
            });
        });

        return null;
    }

    @Override
    public Void visit(final UntypedMetricFamily metricFamily) {
        write(stringBuilder -> {
            writeFamilyHeader(stringBuilder, metricFamily, MetricFamilyType.UNTYPED);

            metricFamily.metrics.forEach(metric -> {
                writeMetric(stringBuilder, metricFamily.name, metric.name, metric.value.floatValue(), metric.labels, globalLabels);
            });
        });

        return null;
    }

    @Override
    public void close() throws IOException {
        write(statistics::stopAndAppendStatistics);

        stream.close();
    }
}
