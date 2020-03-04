package com.zegelin.prometheus.exposition;

import com.zegelin.netty.Floats;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

class NioExpositionSink implements ExpositionSink<ByteBuffer> {
    private int ingestedByteCount = 0;
    private final ByteBuffer buffer;

    NioExpositionSink(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void writeByte(int asciiChar) {
        ingestedByteCount++;
        buffer.put((byte) asciiChar);
    }

    @Override
    public void writeBytes(ByteBuffer nioBuffer) {
        ingestedByteCount += nioBuffer.remaining();
        buffer.put(nioBuffer);
    }

    @Override
    public void writeAscii(String asciiString) {
        byte[] byteBuffer = asciiString.getBytes(US_ASCII);
        ingestedByteCount += byteBuffer.length;
        buffer.put(byteBuffer);
    }

    @Override
    public void writeUtf8(String utf8String) {
        byte[] byteBuffer = utf8String.getBytes(UTF_8);
        ingestedByteCount += byteBuffer.length;
        buffer.put(byteBuffer);
    }

    @Override
    public void writeFloat(float value) {
        ingestedByteCount += Floats.writeFloatString(buffer, value);
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public int getIngestedByteCount() {
        return ingestedByteCount;
    }
}
