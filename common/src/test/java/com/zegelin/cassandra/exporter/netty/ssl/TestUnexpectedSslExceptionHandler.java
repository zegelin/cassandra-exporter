package com.zegelin.cassandra.exporter.netty.ssl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public class TestUnexpectedSslExceptionHandler {
    @Mock
    private ChannelHandlerContext context;

    @Mock
    private Channel channel;

    @Mock
    private ReloadWatcher watcher;

    private UnexpectedSslExceptionHandler handler;

    @BeforeMethod
    public void before() {
        MockitoAnnotations.initMocks(this);

        handler = new UnexpectedSslExceptionHandler(watcher);

        when(context.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(InetSocketAddress.createUnresolved("127.0.0.1", 12345));
    }

    @AfterMethod
    public void after() {
        verify(context, times(1)).fireExceptionCaught(any());
        verifyNoMoreInteractions(context, watcher);
    }

    @Test
    public void testUnexpectedSslExceptionCauseForcedReload() {
        handler.exceptionCaught(context, new DecoderException(new SSLException("Received fatal alert: unexpected_message")));
        verify(watcher, times(1)).forceReload();
    }

    @Test
    public void testOtherSslExceptionIsPropagated() {
        handler.exceptionCaught(context, new DecoderException(new SSLException("Other message")));
    }

    @Test
    public void testDecoderExceptionIsPropagated() {
        handler.exceptionCaught(context, new DecoderException());
    }

    @Test
    public void testOtherExceptionIsPropagated() {
        handler.exceptionCaught(context, new NullPointerException());
    }
}
