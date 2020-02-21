package com.zegelin.prometheus.exposition.json;

import com.zegelin.prometheus.exposition.ExpositionSink;

enum JsonToken {
    OBJECT_START('{'),
    OBJECT_END('}'),
    ARRAY_START('['),
    ARRAY_END(']'),
    DOUBLE_QUOTE('"'),
    COMMA(','),
    COLON(':');

    final byte encoded;

    JsonToken(final char c) {
        this.encoded = (byte) c;
    }

    void write(final ExpositionSink<?> buffer) {
        buffer.writeByte(encoded);
    }
}
