package com.zegelin.prometheus.exposition;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

public class FormattedChunkedInput implements ChunkedInput<ByteBuf> {
    private final FormattedExposition formattedExposition;

    public FormattedChunkedInput(FormattedExposition formattedExposition) {
        this.formattedExposition = formattedExposition;
    }

    @Override
    public ByteBuf readChunk(final ChannelHandlerContext ctx) throws Exception {
        final ByteBuf chunkBuffer = ctx.alloc().buffer(1024 * 1024 * 5);

        // add slices till we hit the chunk size (or slightly over it), or hit EOF
        while (chunkBuffer.readableBytes() < 1024 * 1024 && !isEndOfInput()) {
            formattedExposition.nextSlice(new NettyExpositionSink(chunkBuffer));
        }

        return chunkBuffer;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        return formattedExposition.isEndOfInput();
    }

    @Override
    public void close() throws Exception {
    }
}
