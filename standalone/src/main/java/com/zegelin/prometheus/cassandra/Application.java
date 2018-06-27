package com.zegelin.prometheus.cassandra;

import com.zegelin.jaxrs.filter.OverrideContentTypeFilter;
import com.zegelin.prometheus.jaxrs.resource.MetricsResource;
import com.zegelin.prometheus.jaxrs.resource.RootResource;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;
import sun.management.Agent;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;

public class Application {
    public static void main(String[] args) throws IOException {
        final JMXServiceURL serviceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:7100/jmxrmi");

        final JMXConnector connector = JMXConnectorFactory.connect(serviceURL, null);

        final MBeanServerConnection serverConnection = connector.getMBeanServerConnection();


        final JMXHarvester collector = new JMXHarvester(serverConnection);

        final URI baseUri = UriBuilder.fromUri("http://localhost:7890/").build();

        final ResourceConfig resourceConfig = new ResourceConfig(RootResource.class, MetricsResource.class)
                .register(JacksonFeature.class)
                .register(OverrideContentTypeFilter.class)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(collector).to(Harvester.class);
                    }
                });

        EncodingFilter.enableFor(resourceConfig, GZipEncoder.class);

        JdkHttpServerFactory.createHttpServer(baseUri, resourceConfig);
    }
}
