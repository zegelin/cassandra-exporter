package com.zegelin.prometheus.exposition;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class TestFormattedByteChannel {
    @Mock
    private FormattedExposition formattedExposition;

    private ByteBuffer buffer;
    private FormattedByteChannel channel;

    @BeforeMethod
    public void before() {
        MockitoAnnotations.initMocks(this);
        buffer = ByteBuffer.allocate(128);
        channel = new FormattedByteChannel(formattedExposition);
    }

    @Test
    public void testClosed() {
        when(formattedExposition.isEndOfInput()).thenReturn(true);

        assertThat(channel.read(buffer)).isEqualTo(-1);
        assertThat(channel.isOpen()).isEqualTo(false);
    }

    @Test
    public void testOpen() {
        when(formattedExposition.isEndOfInput()).thenReturn(false);

        assertThat(channel.isOpen()).isEqualTo(true);
    }

    @Test
    public void testOneChunk() {
        when(formattedExposition.isEndOfInput()).thenReturn(false).thenReturn(false).thenReturn(true);
        doAnswer(invocation -> {
            NioExpositionSink sink = invocation.getArgument(0);
            sink.writeAscii("abcdefghij");
            return null;
        }).when(formattedExposition).nextSlice(any(NioExpositionSink.class));

        assertThat(channel.read(buffer)).isEqualTo(10);
        assertThat(channel.isOpen()).isEqualTo(false);
    }
}
