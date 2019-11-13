package com.zegelin.prometheus.exposition.text;

import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import com.zegelin.netty.Floats;
import com.zegelin.prometheus.domain.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.time.Instant;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

class TextFormatMetricFamilyWriter {
    private enum MetricFamilyType {
        GAUGE,
        COUNTER,
        HISTOGRAM,
        SUMMARY,
        UNTYPED;

        private final String encoded;

        MetricFamilyType() {
            encoded = this.name().toLowerCase();
        }

        void write(final ByteBuf buffer) {
            ByteBufUtil.writeAscii(buffer, encoded);
        }
    }

    private static Escaper HELP_STRING_ESCAPER = new CharEscaperBuilder()
            .addEscape('\\', "\\\\")
            .addEscape('\n', "\\n")
            .toEscaper();

    private final String timestamp;
    private final Labels globalLabels;
    private final boolean includeHelp;

    private final Consumer<ByteBuf> headerWriter;
    private final Function<ByteBuf, Boolean> metricWriter;

    TextFormatMetricFamilyWriter(final Instant timestamp, final Labels globalLabels, final boolean includeHelp, final MetricFamily<?> metricFamily) {
        this.timestamp = " " + timestamp.toEpochMilli();
        this.globalLabels = globalLabels;
        this.includeHelp = includeHelp;

        this.headerWriter = metricFamily.accept(new HeaderVisitor());
        this.metricWriter = metricFamily.accept(new MetricVisitor());
    }

    class HeaderVisitor implements MetricFamilyVisitor<Consumer<ByteBuf>> {
        private void writeFamilyHeader(final MetricFamily metricFamily, final ByteBuf buffer, final MetricFamilyType type) {
            // # HELP <family name> <help>\n
            if (includeHelp && metricFamily.help != null) {
                ByteBufUtil.writeAscii(buffer, "# HELP ");
                ByteBufUtil.writeAscii(buffer, metricFamily.name);
                buffer.writeByte(' ');
                ByteBufUtil.writeUtf8(buffer, HELP_STRING_ESCAPER.escape(metricFamily.help));
                buffer.writeByte('\n');
            }

            // # TYPE <family name> <type>\n
            ByteBufUtil.writeAscii(buffer, "# TYPE ");
            ByteBufUtil.writeAscii(buffer, metricFamily.name);
            buffer.writeByte(' ');
            type.write(buffer);
            buffer.writeByte('\n');
        }



        private Consumer<ByteBuf> forType(final MetricFamily metricFamily, final MetricFamilyType type) {
            return (buffer) -> writeFamilyHeader(metricFamily, buffer, type);
        }

        @Override
        public Consumer<ByteBuf> visit(final CounterMetricFamily metricFamily) {
            return forType(metricFamily, MetricFamilyType.COUNTER);
        }

        @Override
        public Consumer<ByteBuf> visit(final GaugeMetricFamily metricFamily) {
            return forType(metricFamily, MetricFamilyType.GAUGE);
        }

        @Override
        public Consumer<ByteBuf> visit(final SummaryMetricFamily metricFamily) {
            return forType(metricFamily, MetricFamilyType.SUMMARY);
        }

        @Override
        public Consumer<ByteBuf> visit(final HistogramMetricFamily metricFamily) {
            return forType(metricFamily, MetricFamilyType.HISTOGRAM);
        }

        @Override
        public Consumer<ByteBuf> visit(final UntypedMetricFamily metricFamily) {
            return forType(metricFamily, MetricFamilyType.UNTYPED);
        }
    }

    class MetricVisitor implements MetricFamilyVisitor<Function<ByteBuf, Boolean>> {
        private void writeLabels(final ByteBuf buffer, final Labels labels, final boolean commaPrefix) {
            if (commaPrefix) {
                buffer.writeByte(',');
            }

            buffer.writeBytes(labels.asPlainTextFormatUTF8EncodedByteBuf().slice());
        }

        private void writeLabelSets(final ByteBuf buffer, final Labels... labelSets) {
            buffer.writeByte('{');

            boolean needsComma = false;

            for (final Labels labels : labelSets) {
                if (labels == null || labels.isEmpty())
                    continue;

                writeLabels(buffer, labels, needsComma);

                needsComma = true;
            }

            if (!globalLabels.isEmpty()) {
                writeLabels(buffer, globalLabels, needsComma);
            }

            buffer.writeByte('}');
        }

        private void writeMetric(final ByteBuf buffer, final MetricFamily metricFamily, final String suffix, final float value, final Labels... labelSets) {
            ByteBufUtil.writeAscii(buffer, metricFamily.name);
            if (suffix != null) {
                ByteBufUtil.writeAscii(buffer, suffix);
            }

            writeLabelSets(buffer, labelSets);

            buffer.writeByte(' ');

            Floats.writeFloatString(buffer, value);
            ByteBufUtil.writeAscii(buffer, timestamp); // timestamp already has a leading space
            buffer.writeByte('\n');
        }

        private <T extends Metric> Function<ByteBuf, Boolean> metricWriter(final MetricFamily<T> metricFamily, final BiConsumer<T, ByteBuf> writer) {
            final Iterator<T> metricIterator = metricFamily.metrics().iterator();

            return (buffer) -> {
                if (metricIterator.hasNext()) {
                    writer.accept(metricIterator.next(), buffer);

                    return true;
                }

                return false;
            };
        }

        @Override
        public Function<ByteBuf, Boolean> visit(final CounterMetricFamily metricFamily) {
            return metricWriter(metricFamily, (counter, buffer) -> {
                writeMetric(buffer, metricFamily, null, counter.value, counter.labels);
            });
        }

        @Override
        public Function<ByteBuf, Boolean> visit(final GaugeMetricFamily metricFamily) {
            return metricWriter(metricFamily, (gauge, buffer) -> {
                writeMetric(buffer, metricFamily, null, gauge.value, gauge.labels);
            });
        }

        @Override
        public Function<ByteBuf, Boolean> visit(final SummaryMetricFamily metricFamily) {
            return metricWriter(metricFamily, (summary, buffer) -> {
                writeMetric(buffer, metricFamily, "_sum", summary.sum, summary.labels);
                writeMetric(buffer, metricFamily, "_count", summary.count, summary.labels);

                summary.quantiles.forEach(interval -> {
                    writeMetric(buffer, metricFamily, null, interval.value, summary.labels, interval.quantile.asSummaryLabel());
                });
            });
        }

        @Override
        public Function<ByteBuf, Boolean> visit(final HistogramMetricFamily metricFamily) {
            return metricWriter(metricFamily, (histogram, buffer) -> {
                writeMetric(buffer, metricFamily, "_sum", histogram.sum, histogram.labels);
                writeMetric(buffer, metricFamily, "_count", histogram.count, histogram.labels);

                histogram.buckets.forEach(interval -> {
                    writeMetric(buffer, metricFamily, "_bucket", interval.value, histogram.labels, interval.quantile.asHistogramLabel());
                });

                writeMetric(buffer, metricFamily, "_bucket", histogram.count, histogram.labels, Interval.Quantile.POSITIVE_INFINITY.asHistogramLabel());
            });
        }

        @Override
        public Function<ByteBuf, Boolean> visit(final UntypedMetricFamily metricFamily) {
            return metricWriter(metricFamily, (untyped, buffer) -> {
                writeMetric(buffer, metricFamily, null, untyped.value, untyped.labels);
            });
        }
    }



    /***
     * Write the header fields (TYPE, HELP) for the MetricFamily to the provided ByteBuf.
     */
    void writeFamilyHeader(final ByteBuf buffer) {
        this.headerWriter.accept(buffer);
    }

    /***
     * Write the next Metric from the MetricFamily to the provided ByteBuf.
     *
     * @return true if there are more Metrics to write, false if not.
     */
    boolean writeMetric(final ByteBuf buffer) {
        return this.metricWriter.apply(buffer);
    }
}
