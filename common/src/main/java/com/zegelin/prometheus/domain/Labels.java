package com.zegelin.prometheus.domain;

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableMap;
import com.zegelin.prometheus.exposition.PrometheusTextFormatWriter;
import com.zegelin.prometheus.exposition.TextFormatChunkedInput;
import io.netty.buffer.ByteBuf;

import java.util.Map;

public final class Labels extends ForwardingMap<String, String> {
    private final ImmutableMap<String, String> labels;
    private final String plainTextRepr;
    private final ByteBuf encodedPlaintextFormat;

    public Labels(final Map<String, String> labels) {
        this.labels = ImmutableMap.copyOf(labels);
        this.plainTextRepr = PrometheusTextFormatWriter.formatLabels(labels);
        this.encodedPlaintextFormat = TextFormatChunkedInput.formatLabels(labels);
    }

    @Override
    protected Map<String, String> delegate() {
        return labels;
    }

    public String asPlainTextFormatString() {
        return this.plainTextRepr;
    }

    public ByteBuf asPlainTextFormatUTF8EncodedByteBuf() {
        return encodedPlaintextFormat;
    }

    @Override
    protected void finalize() throws Throwable {
        this.encodedPlaintextFormat.release();

        super.finalize();
    }
}
