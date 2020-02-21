package com.zegelin.cassandra.exporter.netty.ssl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.NotSslRecordException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.SSLHandshakeException;

import java.net.InetSocketAddress;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestSuppressingSslExceptionHandler {
    @Mock
    private ChannelHandlerContext context;

    @Mock
    private Channel channel;

    private SuppressingSslExceptionHandler handler;

    @BeforeMethod
    public void before() {
        MockitoAnnotations.initMocks(this);
        handler = new SuppressingSslExceptionHandler();

        when(context.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(InetSocketAddress.createUnresolved("127.0.0.1", 12345));
    }

    @Test
    public void testNotSslExceptionFromJdkImplementationIsMuted() {
        handler.exceptionCaught(context, new NotSslRecordException("Some HTTP_REQUEST in message"));
        verify(context, times(0)).fireExceptionCaught(any());
    }

    @Test
    public void testSslHandshakeExceptionFromOpenSslImplementationIsMuted() {
        handler.exceptionCaught(context, new DecoderException(new SSLHandshakeException("Some HTTP_REQUEST in message")));
        verify(context, times(0)).fireExceptionCaught(any());
    }

    @Test
    public void testNotSslRecordExceptionIsMuted() {
        handler.exceptionCaught(context, new DecoderException(new NotSslRecordException("Some HTTP_REQUEST in message")));
        verify(context, times(0)).fireExceptionCaught(any());
    }

    @Test
    public void testInfoLogDoNotBailOnNullChannel() {
        when(context.channel()).thenReturn(null);
        handler.exceptionCaught(context, new NotSslRecordException("Some HTTP_REQUEST in message"));
        verify(context, times(0)).fireExceptionCaught(any());
    }

    @Test
    public void testInfoLogDoNotBailOnNullRemoteAddress() {
        when(channel.remoteAddress()).thenReturn(null);
        handler.exceptionCaught(context, new NotSslRecordException("Some HTTP_REQUEST in message"));
        verify(context, times(0)).fireExceptionCaught(any());
    }

    @Test
    public void testOtherExceptionIsPropagated() {
        handler.exceptionCaught(context, new NullPointerException());
        verify(context, times(1)).fireExceptionCaught(any());
    }
}
