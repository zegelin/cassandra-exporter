package com.zegelin.prometheus.exposition;

import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.Iterators;
import com.google.common.escape.CharEscaper;
import com.google.common.escape.Escaper;
import com.zegelin.prometheus.domain.*;
import io.netty.buffer.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.internal.PlatformDependent;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

public class TextFormatChunkedInput implements ChunkedInput<HttpContent> {
    // A UnpooledByteBufAllocator that returns ByteBufs that don't participate in leak detection and instead
    // depend on the GC to release them. Used for constant and/or long-lived ByteBufs.
    private static final UnpooledByteBufAllocator GC_UNPOOLED = new UnpooledByteBufAllocator(PlatformDependent.directBufferPreferred(), true);

    private static ByteBuf constantUtf8Buffer(final CharSequence seq) {
        return Unpooled.unmodifiableBuffer(
                Unpooled.unreleasableBuffer(
                        ByteBufUtil.writeUtf8(GC_UNPOOLED, seq)
                )
        );
    }

    private static final ByteBuf COMMA = constantUtf8Buffer(",");
    private static final ByteBuf LEFT_BRACE = constantUtf8Buffer("{");
    private static final ByteBuf RIGHT_BRACE = constantUtf8Buffer("}");
    private static final ByteBuf SPACE = constantUtf8Buffer(" ");
    private static final ByteBuf NEW_LINE = constantUtf8Buffer("\n");
    private static final ByteBuf FAMILY_HEADER_HELP = constantUtf8Buffer("# HELP ");
    private static final ByteBuf FAMILY_HEADER_TYPE = constantUtf8Buffer("# TYPE ");
    private static final ByteBuf SUFFIX_SUM = constantUtf8Buffer("_sum");
    private static final ByteBuf SUFFIX_COUNT = constantUtf8Buffer("_count");
    private static final ByteBuf SUFFIX_BUCKET = constantUtf8Buffer("_bucket");

    enum State {
        BANNER,
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

        public final ByteBuf utf8Encoded;

        MetricFamilyType() {
            utf8Encoded = constantUtf8Buffer(this.name().toLowerCase());
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

    private final Iterator<MetricFamily<?>> metricFamilyIterator;

    private final ByteBuf timestamp;
    private final Labels globalLabels;

    private State state = State.BANNER;
    private FooBar fooBar;

    public TextFormatChunkedInput(final Iterable<MetricFamily<?>> metricFamilies, final Instant timestamp, final Labels globalLabels) {
        this.metricFamilyIterator = metricFamilies.iterator();
        this.timestamp = Unpooled.unmodifiableBuffer(ByteBufUtil.writeUtf8(UnpooledByteBufAllocator.DEFAULT, " " + Long.toString(timestamp.toEpochMilli())));
        this.globalLabels = globalLabels;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return state == State.EOF;
    }

    @Override
    public void close() throws Exception {
        if (this.timestamp.refCnt() > 0) {
            this.timestamp.release();
        }

        // TODO: can be null
        this.fooBar.close();

        return;
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

        while (labelsIterator.hasNext()) {
            writeLabel(stringBuilder, labelsIterator.next());

            if (labelsIterator.hasNext()) {
                stringBuilder.append(',');
            }
        }
    }

    public static ByteBuf formatLabels(final Map<String, String> labels) {
        final StringBuilder stringBuilder = new StringBuilder();

        writeLabels(stringBuilder, labels);

        return constantUtf8Buffer(stringBuilder);
    }


    class FooBar extends ForwardingIterator<ByteBuf> implements MetricFamilyVisitor<ByteBuf>, Closeable {
        private final ChannelHandlerContext ctx;

        private ByteBuf encodedFamilyName;
        private Iterator<ByteBuf> metricIterator;

        FooBar(final ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }


        @Override
        protected Iterator<ByteBuf> delegate() {
            return metricIterator;
        }

        private ByteBuf familyHeader(final MetricFamily metricFamily, final MetricFamilyType type) {
            encodedFamilyName = ByteBufUtil.writeUtf8(ctx.alloc(), metricFamily.name);

            final CompositeByteBuf header = ctx.alloc().compositeBuffer();

            // # HELP <family name> <help>\n
            if (metricFamily.help != null) {
                header.addComponents(true,
                        FAMILY_HEADER_HELP,
                        encodedFamilyName.retain(),
                        SPACE,
                        ByteBufUtil.writeUtf8(ctx.alloc(), Escapers.HELP_STRING_ESCAPER.escape(metricFamily.help)),
                        NEW_LINE);
            }

            // # TYPE <family name> <type>\n
            header.addComponents(true,
                    FAMILY_HEADER_TYPE,
                    encodedFamilyName.retain(),
                    SPACE,
                    type.utf8Encoded,
                    NEW_LINE);

            return header;
        }

        private void addLabels(final CompositeByteBuf buffer, final Labels labels) {
            if (labels.isEmpty())
                return;

            buffer.addComponent(true, labels.asPlainTextFormatUTF8EncodedByteBuf().retain());
        }

        private void addLabelSets(final CompositeByteBuf buffer, final Labels... labelSets) {
            buffer.addComponent(true, LEFT_BRACE);

            final Iterator<Labels> labelSetsIterator = Iterators.concat(
                    Iterators.forArray(labelSets),
                    Iterators.singletonIterator(globalLabels)
            );

            while (labelSetsIterator.hasNext()) {
                addLabels(buffer, labelSetsIterator.next());

                if (labelSetsIterator.hasNext()) {
                    buffer.addComponent(true, COMMA);
                }
            }

            buffer.addComponent(true, RIGHT_BRACE);
        }

        private void addMetric(final CompositeByteBuf buffer, final ByteBuf suffix, final float value, final Labels... labelSets) {
            buffer.addComponent(true, encodedFamilyName.retain());
            if (suffix != null) {
                buffer.addComponent(true, suffix);
            }

            addLabelSets(buffer, labelSets);

            buffer.addComponents(true,
                    SPACE,
                    ByteBufUtil.writeUtf8(ctx.alloc(), Float.toString(value)),
                    timestamp.retain(),
                    NEW_LINE
            );
        }

        private <T extends Metric> Iterator<ByteBuf> encodeMetrics(final MetricFamily<T> metricFamily, final BiConsumer<T, CompositeByteBuf> encoder) {
            return Iterators.transform(metricFamily.metrics.iterator(), m -> {
                final CompositeByteBuf buffer = ctx.alloc().compositeBuffer(50);

                encoder.accept(m, buffer);

                return buffer;
            });
        }

        @Override
        public ByteBuf visit(final CounterMetricFamily metricFamily) {
            metricIterator = encodeMetrics(metricFamily, (counter, buffer) -> {
                addMetric(buffer, null, counter.value.floatValue(), counter.labels);
            });

            return familyHeader(metricFamily, MetricFamilyType.COUNTER);
        }

        @Override
        public ByteBuf visit(final GaugeMetricFamily metricFamily) {
            metricIterator = encodeMetrics(metricFamily, (gauge, buffer) -> {
                addMetric(buffer, null, gauge.value.floatValue(), gauge.labels);
            });

            return familyHeader(metricFamily, MetricFamilyType.GAUGE);
        }

        @Override
        public ByteBuf visit(final SummaryMetricFamily metricFamily) {
            metricIterator = encodeMetrics(metricFamily, (summary, buffer) -> {
                addMetric(buffer, SUFFIX_SUM, summary.sum.floatValue(), summary.labels);
                addMetric(buffer, SUFFIX_COUNT, summary.count.floatValue(), summary.labels);

                summary.quantiles.forEach((quantile, value) -> {
                    addMetric(buffer, null, value.floatValue(), quantile.asSummaryLabels(), summary.labels);
                });
            });

            return familyHeader(metricFamily, MetricFamilyType.SUMMARY);
        }

        @Override
        public ByteBuf visit(final HistogramMetricFamily metricFamily) {
            metricIterator = encodeMetrics(metricFamily, (histogram, buffer) -> {
                addMetric(buffer, SUFFIX_SUM, histogram.sum.floatValue(), histogram.labels);
                addMetric(buffer, SUFFIX_COUNT, histogram.count.floatValue(), histogram.labels);

                histogram.quantiles.forEach((quantile, value) -> {
                    addMetric(buffer, SUFFIX_BUCKET, value.floatValue(), quantile.asSummaryLabels(), histogram.labels);
                });
            });

            return familyHeader(metricFamily, MetricFamilyType.HISTOGRAM);
        }

        @Override
        public ByteBuf visit(final UntypedMetricFamily metricFamily) {
            metricIterator = encodeMetrics(metricFamily, (untyped, buffer) -> {
                addMetric(buffer, null, untyped.value.floatValue(), untyped.labels);
            });

            return familyHeader(metricFamily, MetricFamilyType.UNTYPED);
        }

        @Override
        public void close() throws IOException {
            // close should have no effect when called multiple times
            if (encodedFamilyName.refCnt() > 0)
                encodedFamilyName.release();
        }
    }

    // this would be
    private ByteBuf nextSlice(final ChannelHandlerContext ctx) throws Exception {
        while (true) {
            switch (state) {
                case BANNER:
                    // TODO: banner
                    state = State.METRIC_FAMILY;
                    continue;

                case METRIC_FAMILY:
                    if (!metricFamilyIterator.hasNext()) {
                        state = State.FOOTER;
                        continue;
                    }

                    state = State.METRIC;
                    fooBar = new FooBar(ctx);

                    return metricFamilyIterator.next().accept(fooBar);

                case METRIC:
                    if (!fooBar.hasNext()) {
                        state = State.METRIC_FAMILY;
                        fooBar.close();
                        continue;
                    }

                    return fooBar.next();

                case FOOTER:
                    // TODO: implement stats + footer
                    state = State.EOF;
                    continue;

                case EOF:
                    return null;

                default:
                    throw new IllegalStateException();
            }
        }
    }

    @Override
    public HttpContent readChunk(final ChannelHandlerContext ctx) throws Exception {
        final CompositeByteBuf chunkBuffer = ctx.alloc().compositeBuffer(100);

        // add slices till we hit the chunk size (or slightly over it), or hit EOF
        while (chunkBuffer.capacity() < 1024 * 1024) {
            final ByteBuf slice = nextSlice(ctx);

            if (slice == null) {
                break;
            }

            chunkBuffer.addComponent(true, slice);
        }

        return new DefaultHttpContent(chunkBuffer);
    }
}
