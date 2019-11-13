package com.zegelin.prometheus.exposition.json;

import io.netty.buffer.ByteBuf;

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

    void write(final ByteBuf buffer) {
        buffer.writeByte(encoded);
    }
}
