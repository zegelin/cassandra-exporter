package com.zegelin.prometheus.exposition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.escape.CharEscaper;
import com.google.common.escape.Escaper;
import com.google.common.io.CharStreams;
import com.zegelin.prometheus.domain.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class PrometheusTextFormatWriter implements Closeable, MetricFamilyVisitor {
    private final BufferedWriter writer;
    private final String timestamp;
    private final Map<String, String> globalLabels;

    private static final String BANNER = loadBanner();

    private static String loadBanner() {
        // TODO: add git commit hash, version info, etc.

        try (final InputStream bannerStream = PrometheusTextFormatWriter.class.getResourceAsStream("/banner.txt")) {
            final StringBuilder stringBuilder = new StringBuilder();

            CharStreams.copy(new InputStreamReader(bannerStream), stringBuilder);

            stringBuilder.append("# prometheus-cassandra <version> <git sha>");

            return stringBuilder.toString();

        } catch (final IOException e) {
            // that's a shame
        }

        return "# prometheus-cassandra";
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


    public PrometheusTextFormatWriter(final OutputStream stream, final Instant timestamp, final Map<String, String> globalLabels) {
        this.writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8));
        this.timestamp = Long.toString(timestamp.toEpochMilli());
        this.globalLabels = ImmutableMap.copyOf(globalLabels);

        try {
            this.writer.append(BANNER);

        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeFamilyHeader(final MetricFamily<?> metricFamily, final MetricFamilyType type) {
        try {
            writer.newLine();

            if (metricFamily.help != null) {
                writer.append("# HELP ")
                        .append(metricFamily.name)
                        .append(' ')
                        .append(Escapers.HELP_STRING_ESCAPER.escape(metricFamily.help));

                writer.newLine();
            }

            writer.append("# TYPE ")
                    .append(metricFamily.name)
                    .append(' ')
                    .append(type.id);

            writer.newLine();

        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeLabel(final Map.Entry<String, String> label) throws IOException {
        writer.append(label.getKey())
                .append("=\"")
                .append(Escapers.LABEL_VALUE_ESCAPER.escape(label.getValue()))
                .append('"');
    }

    @SafeVarargs
    private final void writeLabels(final Iterable<Map.Entry<String, String>>... labels) throws IOException {
        final Iterable<Map.Entry<String, String>> allLabels = Iterables.concat(
                Iterables.concat(labels),
                globalLabels.entrySet()
        );

        final Iterator<Map.Entry<String, String>> labelsIterator = allLabels.iterator();

        if (labelsIterator.hasNext()) {
            writer.append('{');

            writeLabel(labelsIterator.next());

            while (labelsIterator.hasNext()) {
                writer.append(',');
                writeLabel(labelsIterator.next());
            }

            writer.append('}');
        }
    }

    @SafeVarargs
    private final void writeMetric(final String familyName, final String suffix, final float value, final Iterable<Map.Entry<String, String>>... labels) {
        try {
            // family + optional suffix
            writer.append(familyName);
            if (suffix != null) {
                writer.append('_').append(suffix);
            }

            writeLabels(labels);

            writer.append(' ')
                    .append(Float.toString(value))
                    .append(' ')
                    .append(timestamp);

            writer.newLine();

        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeNumericFamily(final MetricFamily<NumericMetric> metricFamily, final MetricFamilyType type) {
        writeFamilyHeader(metricFamily, type);

        metricFamily.metrics.forEach(metric -> {
            writeMetric(metricFamily.name, null, metric.value.floatValue(), metric.labels.entrySet());
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
        writeFamilyHeader(metricFamily, MetricFamilyType.SUMMARY);

        metricFamily.metrics.forEach(summary -> {
            final Set<Map.Entry<String, String>> summaryLabels = summary.labels.entrySet();

            writeMetric(metricFamily.name, "sum", summary.sum.floatValue(), summaryLabels);
            writeMetric(metricFamily.name, "count", summary.count.floatValue(), summaryLabels);

            summary.quantiles.forEach((quantile, value) -> {
                final Set<Map.Entry<String, String>> quantileLabels = ImmutableSet.of(Maps.immutableEntry("quantile", Double.toString(quantile)));

                writeMetric(metricFamily.name, null, value.floatValue(), summaryLabels, quantileLabels);
            });
        });

        return null;
    }

    @Override
    public Void visit(final HistogramMetricFamily metricFamily) {
        writeFamilyHeader(metricFamily, MetricFamilyType.HISTOGRAM);

        metricFamily.metrics.forEach(histogram -> {
            final Set<Map.Entry<String, String>> histogramLabels = histogram.labels.entrySet();

            writeMetric(metricFamily.name, "sum", histogram.sum.floatValue(), histogramLabels);
            writeMetric(metricFamily.name, "count", histogram.count.floatValue(), histogramLabels);

            histogram.quantiles.forEach((quantile, value) -> {
                final Set<Map.Entry<String, String>> quantileLabels = ImmutableSet.of(Maps.immutableEntry("le", Double.toString(quantile)));

                writeMetric(metricFamily.name, "bucket", value.floatValue(), histogramLabels, quantileLabels);
            });
        });

        return null;
    }

    @Override
    public Void visit(final UntypedMetricFamily metricFamily) {
        writeFamilyHeader(metricFamily, MetricFamilyType.UNTYPED);

        metricFamily.metrics.forEach(metric -> {
            writeMetric(metricFamily.name, metric.name, metric.value.floatValue(), metric.labels.entrySet());
        });

        return null;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
