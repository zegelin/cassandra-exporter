package com.zegelin.cassandra.exporter.netty.ssl;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.NotSslRecordException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.SSLHandshakeException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestSuppressingSslExceptionHandler {
    @Mock
    private ChannelHandlerContext context;

    private SuppressingSslExceptionHandler handler;

    @BeforeMethod
    public void before() {
        MockitoAnnotations.initMocks(this);
        handler = new SuppressingSslExceptionHandler();
    }

    @Test
    public void testNotSslExceptionFromJdkImplementationIsMuted() throws Exception {
        handler.exceptionCaught(context, new NotSslRecordException());
        verify(context, times(0)).fireExceptionCaught(any());
    }

    @Test
    public void testSslHandshakeExceptionFromOpenSslImplementationIsMuted() throws Exception {
        handler.exceptionCaught(context, new DecoderException(new SSLHandshakeException("Some HTTP_REQUEST in message")));
        verify(context, times(0)).fireExceptionCaught(any());
    }

    @Test
    public void testOtherExceptionIsPropagated() throws Exception {
        handler.exceptionCaught(context, new NullPointerException());
        verify(context, times(1)).fireExceptionCaught(any());
    }
}
