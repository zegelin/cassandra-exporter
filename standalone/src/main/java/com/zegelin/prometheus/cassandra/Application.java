package com.zegelin.prometheus.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.zegelin.picocli.InetSocketAddressTypeConverter;
import com.zegelin.picocli.JMXServiceURLTypeConverter;
import com.zegelin.prometheus.cassandra.cli.HarvesterOptions;
import com.zegelin.prometheus.cli.HttpServerOptions;
import com.zegelin.prometheus.netty.Server;
import picocli.CommandLine;
import picocli.CommandLine.*;

import javax.management.MBeanServerConnection;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.datastax.driver.core.ProtocolOptions.DEFAULT_PORT;

@Command(name = "cassandra-exporter-standalone", mixinStandardHelpOptions = true, sortOptions = false)
public class Application implements Callable<Void> {
    static class CqlInetSocketAddressTypeConverter extends InetSocketAddressTypeConverter {
        @Override
        protected int defaultPort() {
            return DEFAULT_PORT;
        }
    }

    @Spec
    private Model.CommandSpec commandSpec;

    @Mixin
    private HarvesterOptions harvesterOptions;

    @Mixin
    private HttpServerOptions httpServerOptions;

    @Option(names = "--jmx-service-url", paramLabel = "URL",
            defaultValue = "service:jmx:rmi:///jndi/rmi://localhost:7199/jmxrmi",
            converter = JMXServiceURLTypeConverter.class,
            description = "JMX service URL of the Cassandra instance to connect to and collect metrics. " +
                    "Defaults to '${DEFAULT-VALUE}'")
    private JMXServiceURL jmxServiceURL;

    @Option(names = "--jmx-user", paramLabel = "NAME", description = "JMX authentication user name.")
    private String jmxUser;

    @Option(names = "--jmx-password", paramLabel = "PASSWORD", description = "JMX authentication password.")
    private String jmxPassword;


    @Option(names = "--cql-address", paramLabel = "[ADDRESS][:PORT]",
            defaultValue = "localhost:" + DEFAULT_PORT,
            converter = CqlInetSocketAddressTypeConverter.class,
            description = "Address/hostname and optional port for the CQL metadata connection. " +
                    "Defaults to '${DEFAULT-VALUE}'")
    private InetSocketAddress cqlAddress;

    @Option(names = "--cql-user", paramLabel = "NAME", description = "CQL authentication user name.")
    private String cqlUser;

    @Option(names = "--cql-password", paramLabel = "PASSWORD", description = "CQL authentication password.")
    private String cqlPassword;


    @Override
    public Void call() throws Exception {
        final MBeanServerConnection mBeanServerConnection;
        {
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
            mBeanServerConnection = connector.getMBeanServerConnection();

            connector.addConnectionNotificationListener(new NotificationListener() {
                @Override
                public void handleNotification(final Notification notification, final Object handback) {
                    if (notification.getType().equals(JMXConnectionNotification.CLOSED)) {
                        Runtime.getRuntime().exit(-1);
                    }
                }
            }, null, null);
        }

        final RemoteMetadataFactory remoteMetadataFactory;
        {
            if (cqlUser != null ^ cqlPassword != null) {
                throw new ParameterException(commandSpec.commandLine(), "Both --cql-user and --cql-password are required when either is used.");
            }

            final Session session = Cluster.builder()
                    .addContactPointsWithPorts(cqlAddress)
                    .withCredentials(cqlUser, cqlPassword)
                    .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), ImmutableList.of(cqlAddress)))
                    .build()
                    .connect();

            remoteMetadataFactory = new RemoteMetadataFactory(session.getCluster().getMetadata());
        }

        final JMXHarvester harvester = new JMXHarvester(mBeanServerConnection, remoteMetadataFactory, harvesterOptions);

        Server.start(httpServerOptions.listenAddresses, harvester, httpServerOptions.helpExposition);

        return null;
    }

    public static void main(String[] args) {
        final CommandLine commandLine = new CommandLine(new Application());

        commandLine.setCaseInsensitiveEnumValuesAllowed(true);

        commandLine.parseWithHandler(new RunLast(), args);
    }
}
