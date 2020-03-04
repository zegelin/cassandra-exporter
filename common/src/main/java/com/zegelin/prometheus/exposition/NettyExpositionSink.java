package com.zegelin.prometheus.exposition;

import com.zegelin.netty.Floats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.ByteBuffer;

public class NettyExpositionSink implements ExpositionSink<ByteBuf> {
    private int ingestedByteCount = 0;
    private final ByteBuf buffer;

    public NettyExpositionSink(ByteBuf buffer) {
        this.buffer = buffer;
    }

    @Override
    public void writeByte(int asciiChar) {
        ingestedByteCount++;
        buffer.writeByte(asciiChar);
    }

    @Override
    public void writeBytes(ByteBuffer nioBuffer) {
        ingestedByteCount += nioBuffer.remaining();
        buffer.writeBytes(nioBuffer);
    }

    @Override
    public void writeAscii(String asciiString) {
        ingestedByteCount += ByteBufUtil.writeAscii(buffer, asciiString);
    }

    @Override
    public void writeUtf8(String utf8String) {
        ingestedByteCount += ByteBufUtil.writeUtf8(buffer, utf8String);
    }

    @Override
    public void writeFloat(float value) {
        ingestedByteCount += Floats.writeFloatString(buffer, value);
    }

    @Override
    public ByteBuf getBuffer() {
        return buffer;
    }

    @Override
    public int getIngestedByteCount() {
        return ingestedByteCount;
    }
}
