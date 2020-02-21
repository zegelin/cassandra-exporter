package com.zegelin.cassandra.exporter.cli;

import com.zegelin.cassandra.exporter.netty.HttpHandler;
import com.zegelin.cassandra.exporter.netty.ssl.ClientAuthentication;
import com.zegelin.cassandra.exporter.netty.ssl.SslImplementation;
import com.zegelin.cassandra.exporter.netty.ssl.SslMode;
import com.zegelin.picocli.InetSocketAddressTypeConverter;
import picocli.CommandLine.Option;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

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

    @Option(names = "--ssl",
            paramLabel = "MODE",
            defaultValue = "DISABLE",
            description = "Enable or disable secured communication with SSL. " +
                    "Valid modes: ${COMPLETION-CANDIDATES}. " +
                    "Optional support requires Netty version 4.0.45 or later. " +
                    "Defaults to ${DEFAULT-VALUE}."
    )
    public SslMode sslMode = SslMode.DISABLE;

    @Option(names = "--ssl-implementation",
            paramLabel = "IMPLEMENTATION",
            defaultValue = "DISCOVER",
            description = "SSL implementation to use for secure communication. " +
                    "OpenSSL requires platform specific libraries. " +
                    "Valid implementations: ${COMPLETION-CANDIDATES}. " +
                    "Defaults to ${DEFAULT-VALUE} which will use OpenSSL if required libraries are discoverable."
    )
    public SslImplementation sslImplementation = SslImplementation.DISCOVER;

    @Option(names = "--ssl-ciphers",
            paramLabel = "CIPHER",
            split = ",",
            description = "A comma-separated list of SSL cipher suites to enable, in the order of preference. " +
                    "Defaults to system settings."
    )
    public List<String> sslCiphers;

    @Option(names = "--ssl-protocols",
            paramLabel = "PROTOCOL",
            split = ",",
            description = "A comma-separated list of TLS protocol versions to enable. " +
                    "Defaults to system settings."
    )
    public Set<String> sslProtocols;

    @Option(names = "--ssl-reload-interval",
            paramLabel = "SECONDS",
            defaultValue = "0",
            description = "Interval in seconds by which keys and certificates will be reloaded. " +
                    "Defaults to ${DEFAULT-VALUE} which will disable run-time reload of certificates."
    )
    public long sslReloadIntervalInSeconds = 0L;

    @Option(names = "--ssl-server-key",
            paramLabel = "SERVER-KEY",
            description = "Path to the private key file for the SSL server. " +
                    "Must be provided together with a server-certificate. " +
                    "The file should contain a PKCS#8 private key in PEM format."
    )
    public File sslServerKeyFile;

    @Option(names = "--ssl-server-key-password",
            paramLabel = "SERVER-KEY-PASSWORD",
            description = "Path to the private key password file for the SSL server. " +
                    "This is only required if the server-key is password protected. " +
                    "The file should contain a clear text password for the server-key."
    )
    public File sslServerKeyPasswordFile;

    @Option(names = "--ssl-server-certificate",
            paramLabel = "SERVER-CERTIFICATE",
            description = "Path to the certificate chain file for the SSL server. " +
                    "Must be provided together with a server-key. " +
                    "The file should contain an X.509 certificate chain in PEM format."
    )
    public File sslServerCertificateFile;

    @Option(names = "--ssl-client-authentication",
            paramLabel = "CLIENT-AUTHENTICATION",
            defaultValue = "NONE",
            description = "Set SSL client authentication mode. " +
                    "Valid options: ${COMPLETION-CANDIDATES}. " +
                    "Defaults to ${DEFAULT-VALUE}."
    )
    public ClientAuthentication sslClientAuthentication = ClientAuthentication.NONE;

    @Option(names = "--ssl-trusted-certificate",
            paramLabel = "TRUSTED-CERTIFICATE",
            description = "Path to trusted certificates for verifying the remote endpoint's certificate. " +
                    "The file should contain an X.509 certificate collection in PEM format. " +
                    "Defaults to the system setting."
    )
    public File sslTrustedCertificateFile;

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
