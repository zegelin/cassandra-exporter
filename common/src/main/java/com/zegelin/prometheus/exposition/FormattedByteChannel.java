package com.zegelin.prometheus.exposition;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class FormattedByteChannel implements ReadableByteChannel {
    public static final int MIN_CHUNK_SIZE = 1024 * 1024;
    public static final int MAX_CHUNK_SIZE = MIN_CHUNK_SIZE * 5;

    private final FormattedExposition formattedExposition;

    public FormattedByteChannel(FormattedExposition formattedExposition) {
        this.formattedExposition = formattedExposition;
    }

    @Override
    public int read(ByteBuffer dst) {
        if (!isOpen()) {
            return -1;
        }

        NioExpositionSink sink = new NioExpositionSink(dst);
        while (sink.getIngestedByteCount() < MIN_CHUNK_SIZE && isOpen()) {
            formattedExposition.nextSlice(sink);
        }

        return sink.getIngestedByteCount();
    }

    @Override
    public boolean isOpen() {
        return !formattedExposition.isEndOfInput();
    }

    @Override
    public void close() {
    }
}
