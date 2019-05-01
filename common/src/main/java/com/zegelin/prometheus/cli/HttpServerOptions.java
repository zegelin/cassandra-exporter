package com.zegelin.prometheus.cli;

import com.zegelin.picocli.InetSocketAddressTypeConverter;
import com.zegelin.prometheus.netty.HttpHandler;
import picocli.CommandLine.Option;

import java.net.InetSocketAddress;
import java.util.List;

public class HttpServerOptions {

    private static final String WILDCARD_ADDRESS = "0.0.0.0";

    private static final int DEFAULT_PORT = 9500;

    static class ListenInetSocketAddressTypeConverter extends InetSocketAddressTypeConverter {
        @Override
        protected int defaultPort() {
            return DEFAULT_PORT;
        }
    }

    @Option(names = {"-l", "--listen"},
            paramLabel = "[ADDRESS][:PORT]",
            defaultValue = WILDCARD_ADDRESS + ":" + DEFAULT_PORT,
            converter = ListenInetSocketAddressTypeConverter.class,
            description = "Listen address (and optional port). " +
                    "ADDRESS may be a hostname, IPv4 dotted or decimal address, or IPv6 address. " +
                    "When ADDRESS is omitted, " + WILDCARD_ADDRESS + " (wildcard) is substituted. " +
                    "PORT, when specified, must be a valid port number. The default port " + DEFAULT_PORT + " will be substituted if omitted. " +
                    "If ADDRESS is omitted but PORT is specified, PORT must be prefixed with a colon (':'), or PORT will be interpreted as a decimal IPv4 address. " +
                    "This option may be specified more than once to listen on multiple addresses. " +
                    "Defaults to '${DEFAULT-VALUE}'")
    public List<InetSocketAddress> listenAddresses;

    @Option(names = {"--family-help"},
            paramLabel = "VALUE",
            defaultValue = "AUTOMATIC",
            description = "Include or exclude metric family help in the exposition format. " +
                    "AUTOMATIC excludes help strings when the user agent is Prometheus and includes them for all other clients (cURL, browsers, etc). " +
                    "Currently Prometheus discards help strings. " +
                    "Excluding help strings saves bandwidth. " +
                    "Can be overridden with the \"?help=true|false\" URI query parameter. " +
                    "Valid values: ${COMPLETION-CANDIDATES}. " +
                    "Defaults to ${DEFAULT-VALUE}."
    )
    public HttpHandler.HelpExposition helpExposition = HttpHandler.HelpExposition.AUTOMATIC;
}
