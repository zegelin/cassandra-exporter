package com.zegelin.prometheus.cassandra;

import com.zegelin.jaxrs.filter.OverrideContentTypeFilter;
import com.zegelin.picocli.JMXServiceURLTypeConverter;
import com.zegelin.prometheus.cassandra.cli.HTTPServerOptions;
import com.zegelin.prometheus.cassandra.cli.HarvesterOptions;
import com.zegelin.prometheus.jaxrs.resource.MetricsResource;
import com.zegelin.prometheus.jaxrs.resource.RootResource;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.netty.httpserver.NettyHttpContainerProvider;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static picocli.CommandLine.*;

@Command(mixinStandardHelpOptions = true)
public class Application implements Callable<Void> {
    @Mixin
    private HarvesterOptions harvesterOptions;

    @Mixin
    private HTTPServerOptions httpServerOptions;

    @Option(names = "--jmx-service-url", paramLabel = "URL",
            defaultValue = "service:jmx:rmi:///jndi/rmi://localhost:7100/jmxrmi",
            converter = JMXServiceURLTypeConverter.class,
            description = "The JMX service URL of the Cassandra instance to connect to and collect metrics. " +
                    "Defaults to '${DEFAULT-VALUE}'")
    private JMXServiceURL jmxServiceURL;


    @Override
    public Void call() throws Exception {
        final JMXConnector connector = JMXConnectorFactory.connect(jmxServiceURL, null);
        final MBeanServerConnection serverConnection = connector.getMBeanServerConnection();

        final JMXHarvester collector = new JMXHarvester(serverConnection, harvesterOptions.exclusions, harvesterOptions.globalLabels);

        return null;
    }



    public static void main(String[] args) throws IOException {
        CommandLine.call(new Application(), args);


//        final JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:7100/jmxrmi");
//
//        final JMXConnector connector = JMXConnectorFactory.connect(serviceURL, null);
//
//        final MBeanServerConnection serverConnection = connector.getMBeanServerConnection();
//
//
//        final JMXHarvester collector = new JMXHarvester(serverConnection);
//
//        final URI baseUri = UriBuilder.fromUri("http://localhost:7890/").build();
//
//        final ResourceConfig resourceConfig = new ResourceConfig(RootResource.class, MetricsResource.class)
//                .register(JacksonFeature.class)
//                .register(OverrideContentTypeFilter.class)
//                .register(new AbstractBinder() {
//                    @Override
//                    protected void configure() {
//                        bind(collector).to(Harvester.class);
//                    }
//                });
//
//        EncodingFilter.enableFor(resourceConfig, GZipEncoder.class);
//
//        JdkHttpServerFactory.createHttpServer(baseUri, resourceConfig);
    }
}
