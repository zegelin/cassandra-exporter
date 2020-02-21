package com.zegelin.cassandra.exporter.netty.ssl;

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

public class UnexpectedSslExceptionHandler extends ChannelHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(UnexpectedSslExceptionHandler.class);

    private final ReloadWatcher reloadWatcher;

    UnexpectedSslExceptionHandler(ReloadWatcher reloadWatcher) {
        this.reloadWatcher = reloadWatcher;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        try {
            if (unexpectedMessage(cause)) {
                logger.warn(cause.getMessage());
                // This may indicate that we're currently using invalid combo of key & cert
                reloadWatcher.forceReload();
            }
        } finally {
            ctx.fireExceptionCaught(cause);
        }
    }

    private boolean unexpectedMessage(Throwable cause) {
        return cause instanceof DecoderException
                && cause.getCause() instanceof SSLException
                && cause.getCause().getMessage().contains("unexpected_message");
    }
}
