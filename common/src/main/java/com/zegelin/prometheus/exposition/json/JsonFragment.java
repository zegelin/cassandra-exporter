package com.zegelin.prometheus.exposition.json;

import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import com.zegelin.prometheus.exposition.ExpositionSink;

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

    static void writeNull(final ExpositionSink<?> buffer) {
        buffer.writeAscii("null");
    }

    static void writeAsciiString(final ExpositionSink<?> buffer, final String string) {
        JsonToken.DOUBLE_QUOTE.write(buffer);
        buffer.writeAscii(STRING_ESCAPER.escape(string));
        JsonToken.DOUBLE_QUOTE.write(buffer);
    }

    static void writeUtf8String(final ExpositionSink<?> buffer, final String string) {
        JsonToken.DOUBLE_QUOTE.write(buffer);
        buffer.writeUtf8(STRING_ESCAPER.escape(string));
        JsonToken.DOUBLE_QUOTE.write(buffer);
    }

    static void writeObjectKey(final ExpositionSink<?> buffer, final String key) {
        buffer.writeAscii(key);
        JsonToken.COLON.write(buffer);
    }

    static void writeFloat(final ExpositionSink<?> buffer, final float f) {
        if (Float.isNaN(f)) {
            buffer.writeAscii("\"NaN\"");
            return;
        }

        if (Float.isInfinite(f)) {
            buffer.writeAscii((f < 0 ? "\"-Inf\"" : "\"+Inf\""));
            return;
        }

        buffer.writeFloat(f);
    }

    static void writeLong(final ExpositionSink<?> buffer, final long l) {
        buffer.writeAscii(Long.toString(l));
    }
}
