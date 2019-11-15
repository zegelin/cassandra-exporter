package com.zegelin.netty;

import info.adams.ryu.RyuFloat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;

public final class Floats {
    public static boolean useFastFloat = true;

    private Floats() {}

    public static int writeFloatString(final ByteBuf buffer, final float f) {
        return ByteBufUtil.writeAscii(buffer, Float.toString(f));
    }

    public static int writeFloatString(final ByteBuffer buffer, final float f) {
        if (useFastFloat) {
            return RyuFloat.floatToString(buffer, f);
        } else {
            byte[] byteBuffer = Float.toString(f).getBytes(US_ASCII);
            int size = byteBuffer.length;
            buffer.put(byteBuffer);
            return size;
        }
    }
}
