package com.zegelin.prometheus.domain;

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableMap;
import com.zegelin.prometheus.exposition.json.JsonFormatChunkedInput;
import com.zegelin.prometheus.exposition.text.TextFormatLabels;
import io.netty.buffer.ByteBuf;

import java.util.Map;

public final class Labels extends ForwardingMap<String, String> {
    private final ImmutableMap<String, String> labels;
    private final boolean isEmpty;

    private ByteBuf plainTextFormatUTF8EncodedByteBuf, jsonFormatUTF8EncodedByteBuf;

    public Labels(final Map<String, String> labels) {
        this.labels = ImmutableMap.copyOf(labels);
        this.isEmpty = this.labels.isEmpty();
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
        if (plainTextFormatUTF8EncodedByteBuf == null) {
            this.plainTextFormatUTF8EncodedByteBuf = TextFormatLabels.formatLabels(labels);
        }

        return plainTextFormatUTF8EncodedByteBuf;
    }

    public ByteBuf asJSONFormatUTF8EncodedByteBuf() {
        if (jsonFormatUTF8EncodedByteBuf == null) {
            this.jsonFormatUTF8EncodedByteBuf = JsonFormatChunkedInput.formatLabels(labels);
        }

        return jsonFormatUTF8EncodedByteBuf;
    }

    @Override
    protected void finalize() throws Throwable {
        this.plainTextFormatUTF8EncodedByteBuf.release();
        this.jsonFormatUTF8EncodedByteBuf.release();

        super.finalize();
    }
}
