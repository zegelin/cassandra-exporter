package com.zegelin.prometheus.netty;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zegelin.prometheus.cassandra.Harvester;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private static List<Channel> channels;

    private static EventLoopGroup eventLoopGroup;

    public static class ChildInitializer extends ChannelInitializer<SocketChannel> {
        private final Harvester harvester;
        private final HttpHandler.HelpExposition helpExposition;

        ChildInitializer(final Harvester harvester, final HttpHandler.HelpExposition helpExposition) {
            this.harvester = harvester;
            this.helpExposition = helpExposition;
        }

        @Override
        public void initChannel(final SocketChannel ch) {
            ch.pipeline()
                    .addLast(new HttpServerCodec())
                    .addLast(new HttpObjectAggregator(1048576))
                    .addLast(new HttpContentCompressor())
                    .addLast(new ChunkedWriteHandler())
                    .addLast(new HttpHandler(harvester, helpExposition));
        }
    }

    public static void start(final List<InetSocketAddress> listenAddresses,
                             final Harvester harvester,
                             final HttpHandler.HelpExposition helpExposition) throws InterruptedException {

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("prometheus-netty-pool-%d")
                .build();

        eventLoopGroup = new NioEventLoopGroup(1, threadFactory);

        final ServerBootstrap bootstrap = new ServerBootstrap();

        bootstrap.group(eventLoopGroup)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChildInitializer(harvester, helpExposition));

        {
            final ImmutableList.Builder<Channel> builder = ImmutableList.builder();

            for (final InetSocketAddress listenAddress : listenAddresses) {
                builder.add(bootstrap.bind(listenAddress).sync().channel());
            }

            Server.channels = builder.build();
        }

        if (logger.isInfoEnabled()) {
            logger.info("cassandra-exporter has started. Listening on {}", Joiner.on(", ").join(
                    listenAddresses.stream()
                            .map(a -> String.format("http://%s:%d", a.getHostString(), a.getPort()))
                            .iterator()
            ));
        }
    }

    public static void stop() {

        if (Server.channels != null) {

            for (final Channel ch : channels) {
                try {
                    ch.closeFuture().sync();
                } catch (final InterruptedException e) {
                    logger.debug("Closing of cassandra-exporter channel resulted in interrupted exception.", e);
                }
            }

            Server.channels = null;

            Server.eventLoopGroup.shutdownGracefully();

            logger.info("cassandra-exporter has stopped");
        }
    }
}
