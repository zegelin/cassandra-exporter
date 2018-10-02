package com.zegelin.netty;

import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public final class Resources {
    private Resources() {}

    public static ByteBuf asByteBuf(final Class<?> clazz, final String name) {
        try (final InputStream stream = clazz.getResourceAsStream(name)) {
            final byte[] bytes = ByteStreams.toByteArray(stream);

            return Unpooled.unmodifiableBuffer(Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(bytes)));

        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
