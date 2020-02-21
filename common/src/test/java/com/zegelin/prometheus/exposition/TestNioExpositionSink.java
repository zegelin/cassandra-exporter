package com.zegelin.prometheus.exposition;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNioExpositionSink {

    private NioExpositionSink sink;

    @BeforeMethod
    public void before() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        sink = new NioExpositionSink(buffer);
    }

    @Test
    public void testAsciiCharSize() {
        sink.writeByte('a');
        sink.writeByte('b');
        sink.writeByte('c');

        assertThat(sink.getIngestedByteCount()).isEqualTo(3);
    }

    @Test
    public void testAsciiStringSize() {
        sink.writeAscii("123");
        sink.writeAscii("abc");

        assertThat(sink.getIngestedByteCount()).isEqualTo(6);
    }

    @Test
    public void testUtf8StringSize() {
        sink.writeUtf8("123");
        sink.writeUtf8("abc");
        sink.writeUtf8("åäö");

        assertThat(sink.getIngestedByteCount()).isEqualTo(12);
    }

    @Test
    public void testFloatSize() {
        sink.writeFloat(0.123f);

        assertThat(sink.getIngestedByteCount()).isEqualTo(5);
    }

    @Test
    public void testBufferSize() {
        ByteBuffer buffer = ByteBuffer.wrap("abc".getBytes(US_ASCII));
        sink.writeBytes(buffer);

        assertThat(sink.getIngestedByteCount()).isEqualTo(3);
    }
}
