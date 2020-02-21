package com.zegelin.prometheus.exposition;

import java.nio.ByteBuffer;

public interface ExpositionSink<T> {
    void writeByte(int asciiChar);

    void writeBytes(ByteBuffer nioBuffer);

    void writeAscii(String asciiString);

    void writeUtf8(String utf8String);

    void writeFloat(float value);

    T getBuffer();

    int getIngestedByteCount();
}
