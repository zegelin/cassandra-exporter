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
import java.util.Map;
import java.util.HashMap;

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

    @Option(names = "--jmx-user", paramLabel = "NAME", description = "The JMX authentication user name")
    private String jmxUser;

    @Option(names = "--jmx-password", paramLabel = "PASSWORD", description = "The JMX authentication password")
    private String jmxPassword;

    @Override
    public Void call() throws Exception {
        Map<String, String[]> environment = null;

        if (jmxUser != "" || jmxPassword != "") {
            environment = new HashMap<>();
            environment.put(JMXConnector.CREDENTIALS, new String[] {jmxUser, jmxPassword});
        }

        final JMXConnector connector = JMXConnectorFactory.connect(jmxServiceURL, environment);
        final MBeanServerConnection serverConnection = connector.getMBeanServerConnection();

        final JMXHarvester harvester = new JMXHarvester(serverConnection, harvesterOptions.exclusions, harvesterOptions.globalLabels);

        Server.start(httpServerOptions.listenAddresses, harvester, httpServerOptions.helpExposition);

        return null;
    }

    public static void main(String[] args) {
        CommandLine.call(new Application(), args);
    }
}
