package com.zegelin.prometheus.domain;

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableMap;
import com.zegelin.prometheus.exposition.JsonFormatChunkedInput;
import com.zegelin.prometheus.exposition.TextFormatChunkedInput;
import io.netty.buffer.ByteBuf;

import java.util.Map;

public final class Labels extends ForwardingMap<String, String> {
    private final ImmutableMap<String, String> labels;
    private final ByteBuf plainTextFormatUTF8EncodedByteBuf, jsonFormatUTF8EncodedByteBuf;
    private final boolean isEmpty;

    public Labels(final Map<String, String> labels) {
        this.labels = ImmutableMap.copyOf(labels);
        this.isEmpty = this.labels.isEmpty();
        this.plainTextFormatUTF8EncodedByteBuf = TextFormatChunkedInput.formatLabels(labels);
        this.jsonFormatUTF8EncodedByteBuf = JsonFormatChunkedInput.formatLabels(labels);
    }

    public static Labels of(final String key, final String value) {
        return new Labels(ImmutableMap.of(key, value));
    }

    public static Labels of() {
        return new Labels(ImmutableMap.of());
    }

    @Override
    protected Map<String, String> delegate() {
        return labels;
    }

    @Override
    public boolean isEmpty() {
        return isEmpty;
    }

    public ByteBuf asPlainTextFormatUTF8EncodedByteBuf() {
        return plainTextFormatUTF8EncodedByteBuf;
    }

    public ByteBuf asJSONFormatUTF8EncodedByteBuf() {
        return jsonFormatUTF8EncodedByteBuf;
    }

    @Override
    protected void finalize() throws Throwable {
        this.plainTextFormatUTF8EncodedByteBuf.release();
        this.jsonFormatUTF8EncodedByteBuf.release();

        super.finalize();
    }
}
