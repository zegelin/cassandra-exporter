package com.zegelin.prometheus.cassandra;

import com.google.common.collect.ImmutableMap;
import com.zegelin.picocli.JMXServiceURLTypeConverter;
import com.zegelin.prometheus.cassandra.cli.HarvesterOptions;
import com.zegelin.prometheus.cli.HttpServerOptions;
import com.zegelin.prometheus.netty.Server;
import picocli.CommandLine;
import picocli.CommandLine.*;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Map;
import java.util.concurrent.Callable;

import static picocli.CommandLine.*;

@Command(name = "cassandra-exporter-standalone", mixinStandardHelpOptions = true, sortOptions = false)
public class Application implements Callable<Void> {
    @Spec
    private Model.CommandSpec commandSpec;

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

    @Option(names = "--jmx-user", paramLabel = "NAME", description = "The JMX authentication user name.")
    private String jmxUser;

    @Option(names = "--jmx-password", paramLabel = "PASSWORD", description = "The JMX authentication password.")
    private String jmxPassword;


    @Override
    public Void call() throws Exception {
        Map<String, String[]> jmxEnvironment = null;

        if (jmxUser != null ^ jmxPassword != null) {
            throw new ParameterException(commandSpec.commandLine(), "Both --jmx-user and --jmx-password are required when either is used.");
        }

        if (jmxUser != null && jmxPassword != null) {
            jmxEnvironment = ImmutableMap.of(
                    JMXConnector.CREDENTIALS, new String[]{jmxUser, jmxPassword}
            );
        }

        final JMXConnector connector = JMXConnectorFactory.connect(jmxServiceURL, jmxEnvironment);
        final MBeanServerConnection serverConnection = connector.getMBeanServerConnection();

        final JMXHarvester harvester = new JMXHarvester(serverConnection, harvesterOptions);

        Server.start(httpServerOptions.listenAddresses, harvester, httpServerOptions.helpExposition);

        return null;
    }

    public static void main(String[] args) {
        CommandLine.call(new Application(), args);
    }
}
