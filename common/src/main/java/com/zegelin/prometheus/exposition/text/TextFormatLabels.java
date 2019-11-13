package com.zegelin.prometheus.exposition.text;

import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.util.Iterator;
import java.util.Map;

public final class TextFormatLabels {
    private static Escaper LABEL_VALUE_ESCAPER = new CharEscaperBuilder()
            .addEscape('\\', "\\\\")
            .addEscape('\n', "\\n")
            .addEscape('"', "\\\"")
            .toEscaper();

    private TextFormatLabels() {}

    public static ByteBuf formatLabels(final Map<String, String> labels) {
        if (labels.isEmpty())
            return Unpooled.EMPTY_BUFFER;

        final StringBuilder stringBuilder = new StringBuilder();
        final Iterator<Map.Entry<String, String>> labelsIterator = labels.entrySet().iterator();

        while (labelsIterator.hasNext()) {
            final Map.Entry<String, String> label = labelsIterator.next();

            stringBuilder.append(label.getKey())
                    .append("=\"")
                    .append(LABEL_VALUE_ESCAPER.escape(label.getValue()))
                    .append('"');

            if (labelsIterator.hasNext()) {
                stringBuilder.append(',');
            }
        }

        return ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, stringBuilder);
    }
}
