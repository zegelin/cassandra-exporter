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
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class Server {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Server.class);


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

    public static void start(final List<InetSocketAddress> listenAddresses, final Harvester harvester, final HttpHandler.HelpExposition helpExposition) throws InterruptedException {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("prometheus-netty-pool-%d")
                .build();

        final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1, threadFactory);

        final ServerBootstrap bootstrap = new ServerBootstrap();

        bootstrap.group(eventLoopGroup)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChildInitializer(harvester, helpExposition));

        final List<Channel> channels;
        {
            final ImmutableList.Builder<Channel> builder = ImmutableList.builder();

            for (final InetSocketAddress listenAddress : listenAddresses) {
                final Channel channel = bootstrap.bind(listenAddress).sync().channel();

                builder.add(channel);
            }

            channels = builder.build();
        }

        if (logger.isInfoEnabled()) {
            logger.info("cassandra-exporter started. Listening on {}", Joiner.on(", ").join(
                    listenAddresses.stream()
                            .map(a -> String.format("http://%s:%d", a.getHostString(), a.getPort()))
                            .iterator()
            ));
        }

        // TODO: maybe return a future that the caller can sync on, to wait for the channels to shutdown?
    }
}
