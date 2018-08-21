package com.zegelin.prometheus.cassandra;

import com.zegelin.picocli.JMXServiceURLTypeConverter;
import com.zegelin.prometheus.cli.HttpServerOptions;
import com.zegelin.prometheus.cassandra.cli.HarvesterOptions;


import com.zegelin.prometheus.netty.Server;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.concurrent.Callable;

import static picocli.CommandLine.*;

@Command(mixinStandardHelpOptions = true)
public class Application implements Callable<Void> {
    @Mixin
    private HarvesterOptions harvesterOptions;

    @Mixin
    private HttpServerOptions httpServerOptions;

    @Option(names = "--jmx-service-url", paramLabel = "URL",
            defaultValue = "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi",
            converter = JMXServiceURLTypeConverter.class,
            description = "The JMX service URL of the Cassandra instance to connect to and collect metrics. " +
                    "Defaults to '${DEFAULT-VALUE}'")
    private JMXServiceURL jmxServiceURL;


    @Override
    public Void call() throws Exception {
        final JMXConnector connector = JMXConnectorFactory.connect(jmxServiceURL, null);
        final MBeanServerConnection serverConnection = connector.getMBeanServerConnection();

        final JMXHarvester harvester = new JMXHarvester(serverConnection, harvesterOptions.exclusions, harvesterOptions.globalLabels);

        Server.start(httpServerOptions.listenAddresses, harvester, httpServerOptions.helpExposition);

        return null;
    }

    public static void main(String[] args) {
        CommandLine.call(new Application(), args);
    }
}
