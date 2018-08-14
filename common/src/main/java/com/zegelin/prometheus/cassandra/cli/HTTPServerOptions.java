package com.zegelin.prometheus.cassandra.cli;

import com.google.common.collect.ImmutableList;

import com.zegelin.picocli.InetSocketAddressTypeConverter;
import picocli.CommandLine.Option;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

public class HTTPServerOptions {
    private static final int DEFAULT_PORT = 7890;

    static class ListenInetSocketAddressTypeConverter extends InetSocketAddressTypeConverter {
        @Override
        protected int defaultPort() {
            return DEFAULT_PORT;
        }
    }

    @Option(names = {"-l", "--listen"},
            paramLabel = "[ADDRESS][:PORT]",
            converter = ListenInetSocketAddressTypeConverter.class,
            description = "Listen address (and optional port). " +
                    "ADDRESS may be a hostname, IPv4 dotted or decimal address, or IPv6 address. " +
                    "When ADDRESS is omitted, 0.0.0.0 (wildcard) is substituted. " +
                    "PORT, when specified, must be a valid port number. The default port " + DEFAULT_PORT + " will be substituted if omitted. " +
                    "If ADDRESS is omitted but PORT is specified, PORT must be prefixed with a colon (':'), or PORT will be interpreted as a decimal IPv4 address. " +
                    "This option may be specified more than once to listen on multiple addresses. " +
                    "Defaults to '0.0.0.0:" + DEFAULT_PORT + "'")
    public List<InetSocketAddress> listenAddresses = ImmutableList.of(new InetSocketAddress((InetAddress) null, DEFAULT_PORT));
}
