package com.zegelin.netty;

import info.adams.ryu.RyuFloat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public final class Floats {
    public static boolean useFastFloat = true;

    private Floats() {}

    public static int writeFloatString(final ByteBuf buffer, final float f) {
        if (useFastFloat) {
            return RyuFloat.floatToString(buffer, f);
        } else {
            return ByteBufUtil.writeAscii(buffer, Float.toString(f));
        }
    }
}
