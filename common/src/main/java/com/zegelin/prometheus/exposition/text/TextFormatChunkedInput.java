package com.zegelin.prometheus.exposition.text;

import com.google.common.base.Stopwatch;
import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import com.zegelin.netty.Resources;
import com.zegelin.prometheus.domain.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class TextFormatChunkedInput implements ChunkedInput<ByteBuf> {
    private enum State {
        BANNER,
        METRIC_FAMILY,
        METRIC,
        FOOTER,
        EOF
    }

    private static final ByteBuf BANNER = Resources.asByteBuf(TextFormatChunkedInput.class, "banner.txt");

    private final Iterator<MetricFamily> metricFamiliesIterator;

    private final Instant timestamp;
    private final Labels globalLabels;
    private final boolean includeHelp;

    private State state = State.BANNER;
    private TextFormatMetricFamilyWriter metricFamilyWriter;

    private int metricFamilyCount = 0;
    private int metricCount = 0;

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();


    public TextFormatChunkedInput(final Stream<MetricFamily> metricFamilies, final Instant timestamp, final Labels globalLabels, final boolean includeHelp) {
        this.metricFamiliesIterator = metricFamilies.iterator();
        this.timestamp = timestamp;
        this.globalLabels = globalLabels;
        this.includeHelp = includeHelp;
    }

    @Override
    public boolean isEndOfInput() {
        return state == State.EOF;
    }

    @Override
    public void close() {}


    private void nextSlice(final ByteBuf chunkBuffer) {
        switch (state) {
            case BANNER:
                stopwatch.start();

                chunkBuffer.writeBytes(BANNER.slice());

                state = State.METRIC_FAMILY;
                return;

            case METRIC_FAMILY:
                if (!metricFamiliesIterator.hasNext()) {
                    state = State.FOOTER;
                    return;
                }

                metricFamilyCount++;

                final MetricFamily<?> metricFamily = metricFamiliesIterator.next();

                metricFamilyWriter = new TextFormatMetricFamilyWriter(timestamp, globalLabels, includeHelp, metricFamily);

                metricFamilyWriter.writeFamilyHeader(chunkBuffer);

                state = State.METRIC;
                return;

            case METRIC:
                final boolean hasMoreMetrics = metricFamilyWriter.writeMetric(chunkBuffer);

                metricCount ++;

                if (!hasMoreMetrics) {
                    chunkBuffer.writeByte('\n'); // separate from next family
                    state = State.METRIC_FAMILY;
                    return;
                }

                return;

            case FOOTER:
                stopwatch.stop();
                ByteBufUtil.writeAscii(chunkBuffer, "\n\n# Thanks and come again!\n\n");
                ByteBufUtil.writeAscii(chunkBuffer, String.format("# Wrote %s metrics for %s metric families in %s\n", metricCount, metricFamilyCount, stopwatch.toString()));

                state = State.EOF;
                return;

            case EOF:
                return;

            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public ByteBuf readChunk(final ChannelHandlerContext ctx) {
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
