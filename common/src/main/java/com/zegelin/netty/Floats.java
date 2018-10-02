package com.zegelin.netty;

import info.adams.ryu.RyuFloat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public final class Floats {
    public static boolean useFastFloat = true;

    private Floats() {}

    public static void writeFloatString(final ByteBuf buffer, final float f) {
        if (useFastFloat) {
            RyuFloat.floatToString(buffer, f);
        } else {
            ByteBufUtil.writeAscii(buffer, Float.toString(f));
        }
    }
}
