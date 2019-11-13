package com.zegelin.prometheus.exposition.json;

import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import com.zegelin.netty.Floats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

final class JsonFragment {
    private JsonFragment() {}

    private static Escaper STRING_ESCAPER = new CharEscaperBuilder()
            .addEscape('\b', "\\b")
            .addEscape('\f', "\\f")
            .addEscape('\n', "\\n")
            .addEscape('\r', "\\r")
            .addEscape('\t', "\\t")
            .addEscape('"', "\\\"")
            .addEscape('\\', "\\\\")
            .toEscaper();

    static void writeNull(final ByteBuf buffer) {
        ByteBufUtil.writeAscii(buffer, "null");
    }

    static void writeAsciiString(final ByteBuf buffer, final String string) {
        JsonToken.DOUBLE_QUOTE.write(buffer);
        ByteBufUtil.writeAscii(buffer, STRING_ESCAPER.escape(string));
        JsonToken.DOUBLE_QUOTE.write(buffer);
    }

    static void writeUtf8String(final ByteBuf buffer, final String string) {
        JsonToken.DOUBLE_QUOTE.write(buffer);
        ByteBufUtil.writeUtf8(buffer, STRING_ESCAPER.escape(string));
        JsonToken.DOUBLE_QUOTE.write(buffer);
    }

    static void writeObjectKey(final ByteBuf buffer, final String key) {
        writeAsciiString(buffer, key);
        JsonToken.COLON.write(buffer);
    }

    static void writeFloat(final ByteBuf buffer, final float f) {
        if (Float.isNaN(f)) {
            ByteBufUtil.writeAscii(buffer, "\"NaN\"");
            return;
        }

        if (Float.isInfinite(f)) {
            ByteBufUtil.writeAscii(buffer, (f < 0 ? "\"-Inf\"" : "\"+Inf\""));
            return;
        }

        Floats.writeFloatString(buffer, f);
    }

    static void writeLong(final ByteBuf buffer, final long l) {
        ByteBufUtil.writeAscii(buffer, Long.toString(l));
    }
}
