package com.zegelin.prometheus.exposition.json;

import com.google.common.base.Stopwatch;
import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import com.zegelin.prometheus.domain.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.zegelin.prometheus.exposition.json.JsonFragment.*;

public class JsonFormatChunkedInput implements ChunkedInput<ByteBuf> {
    private enum State {
        HEADER,
        METRIC_FAMILY,
        METRIC,
        FOOTER,
        EOF
    }

    private enum MetricFamilyType {
        GAUGE,
        COUNTER,
        HISTOGRAM,
        SUMMARY,
        UNTYPED;

        void write(final ByteBuf buffer) {
            JsonFragment.writeAsciiString(buffer, name());
        }
    }

    private static final Escaper JSON_STRING_ESCAPER = new CharEscaperBuilder()
            .addEscape('"', "\\\"")
            .addEscape('\\', "\\\\")
            .addEscape('/', "\\/")
            .addEscape('\b', "\\b")
            .addEscape('\f', "\\f")
            .addEscape('\n', "\\n")
            .addEscape('\r', "\\r")
            .addEscape('\t', "\\t")
            .toEscaper();


    private final Iterator<MetricFamily> metricFamilyIterator;

    private final Instant timestamp;
    private final Labels globalLabels;
    private final boolean includeHelp;

    private State state = State.HEADER;
    private MetricFamilyWriter metricFamilyWriter;

    private int metricFamilyCount = 0;
    private int metricCount = 0;

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();


    public JsonFormatChunkedInput(final Stream<MetricFamily> metricFamilies, final Instant timestamp, final Labels globalLabels, final boolean includeHelp) {
        this.metricFamilyIterator = metricFamilies.iterator();
        this.timestamp = timestamp;
        this.globalLabels = globalLabels;
        this.includeHelp = includeHelp;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return state == State.EOF;
    }

    @Override
    public void close() throws Exception {
    }


    public static ByteBuf formatLabels(final Map<String, String> labels) {
        final ByteBuf buffer = Unpooled.buffer();

        JsonToken.OBJECT_START.write(buffer);

        final Iterator<Map.Entry<String, String>> labelsIterator = labels.entrySet().iterator();

        while (labelsIterator.hasNext()) {
            final Map.Entry<String, String> label = labelsIterator.next();

            writeObjectKey(buffer, label.getKey());
            writeUtf8String(buffer, JSON_STRING_ESCAPER.escape(label.getValue()));

            if (labelsIterator.hasNext()) {
                JsonToken.COMMA.write(buffer);
            }
        }

        JsonToken.OBJECT_END.write(buffer);

        return buffer;
    }


    class MetricFamilyWriter {
        private final Consumer<ByteBuf> headerWriter;
        private final Function<ByteBuf, Boolean> metricWriter;

        class HeaderVisitor implements MetricFamilyVisitor<Consumer<ByteBuf>> {
            private void writeFamilyHeader(final MetricFamily<?> metricFamily, final ByteBuf buffer, final MetricFamilyType type) {
                writeObjectKey(buffer, metricFamily.name);

                JsonToken.OBJECT_START.write(buffer);

                writeObjectKey(buffer, "type");
                type.write(buffer);

                if (includeHelp && metricFamily.help != null) {
                    JsonToken.COMMA.write(buffer);

                    writeObjectKey(buffer, "help");
                    writeUtf8String(buffer, JSON_STRING_ESCAPER.escape(metricFamily.help));
                }

                JsonToken.COMMA.write(buffer);

                writeObjectKey(buffer, "metrics");
                JsonToken.ARRAY_START.write(buffer);
            }

            private Consumer<ByteBuf> forType(final MetricFamily<?> metricFamily, final MetricFamilyType type) {
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
            private <T extends Metric> Function<ByteBuf, Boolean> metricWriter(final MetricFamily<T> metricFamily, final BiConsumer<T, ByteBuf> valueWriter) {
                final Iterator<T> metricIterator = metricFamily.metrics().iterator();

                return (buffer) -> {
                    if (metricIterator.hasNext()) {
                        final T metric = metricIterator.next();

                        JsonToken.OBJECT_START.write(buffer);
                        writeObjectKey(buffer, "labels");
                        if (metric.labels != null) {
                            buffer.writeBytes(metric.labels.asJSONFormatUTF8EncodedByteBuf().slice());
                        } else {
                            writeNull(buffer);
                        }

                        JsonToken.COMMA.write(buffer);

                        writeObjectKey(buffer, "value");
                        valueWriter.accept(metric, buffer);

                        JsonToken.OBJECT_END.write(buffer);

                        if (metricIterator.hasNext()) {
                            JsonToken.COMMA.write(buffer);
                        }

                        return true;
                    }

                    return false;
                };
            }

            @Override
            public Function<ByteBuf, Boolean> visit(final CounterMetricFamily metricFamily) {
                return metricWriter(metricFamily, (counter, buffer) -> {
                    writeFloat(buffer, counter.value);
                });
            }

            @Override
            public Function<ByteBuf, Boolean> visit(final GaugeMetricFamily metricFamily) {
                return metricWriter(metricFamily, (gauge, buffer) -> {
                    writeFloat(buffer, gauge.value);
                });
            }

            private void writeSumAndCount(final ByteBuf buffer, final float sum, final float count) {
                writeObjectKey(buffer, "sum");
                writeFloat(buffer, sum);

                JsonToken.COMMA.write(buffer);

                writeObjectKey(buffer, "count");
                writeFloat(buffer, count);
            }

            private void writeIntervals(final ByteBuf buffer, final Iterable<Interval> intervals) {
                JsonToken.OBJECT_START.write(buffer);

                final Iterator<Interval> iterator = intervals.iterator();

                while (iterator.hasNext()) {
                    final Interval interval = iterator.next();

                    writeObjectKey(buffer, interval.quantile.toString());
                    writeFloat(buffer, interval.value);

                    if (iterator.hasNext()) {
                        JsonToken.COMMA.write(buffer);
                    }
                }

                JsonToken.OBJECT_END.write(buffer);
            }

            @Override
            public Function<ByteBuf, Boolean> visit(final SummaryMetricFamily metricFamily) {
                return metricWriter(metricFamily, (summary, buffer) -> {
                    JsonToken.OBJECT_START.write(buffer);

                    writeSumAndCount(buffer, summary.sum, summary.count);

                    JsonToken.COMMA.write(buffer);

                    writeObjectKey(buffer, "quantiles");
                    writeIntervals(buffer, summary.quantiles);

                    JsonToken.OBJECT_END.write(buffer);
                });
            }

            @Override
            public Function<ByteBuf, Boolean> visit(final HistogramMetricFamily metricFamily) {
                return metricWriter(metricFamily, (histogram, buffer) -> {
                    JsonToken.OBJECT_START.write(buffer);

                    writeSumAndCount(buffer, histogram.sum, histogram.count);

                    JsonToken.COMMA.write(buffer);

                    writeObjectKey(buffer, "buckets");
                    writeIntervals(buffer, histogram.buckets);

                    JsonToken.OBJECT_END.write(buffer);
                });
            }

            @Override
            public Function<ByteBuf, Boolean> visit(final UntypedMetricFamily metricFamily) {
                return metricWriter(metricFamily, (untyped, buffer) -> {
                    writeFloat(buffer, untyped.value);
                });
            }
        }

        MetricFamilyWriter(final MetricFamily<?> metricFamily) {
            this.headerWriter = metricFamily.accept(new HeaderVisitor());
            this.metricWriter = metricFamily.accept(new MetricVisitor());
        }

        void writeFamilyHeader(final ByteBuf buffer) {
            this.headerWriter.accept(buffer);
        }

        void writeFamilyFooter(final ByteBuf buffer) {
            JsonToken.ARRAY_END.write(buffer);
            JsonToken.OBJECT_END.write(buffer);
        }

        boolean writeMetric(final ByteBuf buffer) {
            return this.metricWriter.apply(buffer);
        }
    }

    private void writeStatistics(final ByteBuf chunkBuffer) {
        JsonToken.OBJECT_START.write(chunkBuffer);

        writeObjectKey(chunkBuffer, "expositionTime");
        writeLong(chunkBuffer, stopwatch.elapsed(TimeUnit.MILLISECONDS));

        JsonToken.COMMA.write(chunkBuffer);

        writeObjectKey(chunkBuffer, "metricFamilyCount");
        writeLong(chunkBuffer, metricFamilyCount);

        JsonToken.COMMA.write(chunkBuffer);

        writeObjectKey(chunkBuffer, "metricCount");
        writeLong(chunkBuffer, metricCount);

        JsonToken.OBJECT_END.write(chunkBuffer);
    }

    private void nextSlice(final ByteBuf chunkBuffer) {
        switch (state) {
            case HEADER:
                stopwatch.start();

                JsonToken.OBJECT_START.write(chunkBuffer);

                writeObjectKey(chunkBuffer, "timestamp");
                writeLong(chunkBuffer, timestamp.toEpochMilli());

                JsonToken.COMMA.write(chunkBuffer);

                writeObjectKey(chunkBuffer, "globalLabels");
                chunkBuffer.writeBytes(globalLabels.asJSONFormatUTF8EncodedByteBuf().slice());

                JsonToken.COMMA.write(chunkBuffer);

                writeObjectKey(chunkBuffer, "metricFamilies");
                JsonToken.OBJECT_START.write(chunkBuffer);

                state = State.METRIC_FAMILY;
                return;

            case METRIC_FAMILY:
                if (!metricFamilyIterator.hasNext()) {
                    state = State.FOOTER;
                    return;
                }

                metricFamilyCount++;

                final MetricFamily<?> metricFamily = metricFamilyIterator.next();

                metricFamilyWriter = new MetricFamilyWriter(metricFamily);

                metricFamilyWriter.writeFamilyHeader(chunkBuffer);

                state = State.METRIC;
                return;

            case METRIC:
                if (!metricFamilyWriter.writeMetric(chunkBuffer)) {
                    metricFamilyWriter.writeFamilyFooter(chunkBuffer);

                    if (metricFamilyIterator.hasNext()) {
                        JsonToken.COMMA.write(chunkBuffer);
                    }

                    state = State.METRIC_FAMILY;
                    return;
                }

                metricCount++;

                return;

            case FOOTER:
                stopwatch.stop();

                JsonToken.OBJECT_END.write(chunkBuffer); // end of "metricFamilies"

                JsonToken.COMMA.write(chunkBuffer);

                writeObjectKey(chunkBuffer, "statistics");
                writeStatistics(chunkBuffer);

                JsonToken.OBJECT_END.write(chunkBuffer); // end of main object

                state = State.EOF;
                return;

            case EOF:
                return;

            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public ByteBuf readChunk(final ChannelHandlerContext ctx) throws Exception {
        final ByteBuf chunkBuffer = ctx.alloc().buffer(1024 * 1024 * 5);

        // add slices till we hit the chunk size (or slightly over it), or hit EOF
        while (chunkBuffer.readableBytes() < 1024 * 1024 && state != State.EOF) {
            try {
                nextSlice(chunkBuffer);

            } catch (final Exception e) {
                chunkBuffer.release();
                throw e;
            }
        }

        return chunkBuffer;
    }
}
