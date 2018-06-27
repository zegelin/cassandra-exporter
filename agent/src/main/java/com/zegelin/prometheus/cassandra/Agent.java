package com.zegelin.prometheus.cassandra;

import com.sun.jmx.mbeanserver.JmxMBeanServerBuilder;
import com.zegelin.jaxrs.filter.OverrideContentTypeFilter;
import com.zegelin.prometheus.jaxrs.resource.MetricsResource;
import com.zegelin.prometheus.jaxrs.resource.RootResource;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;


import javax.inject.Inject;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.NotCompliantMBeanException;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.net.URI;

public class Agent {
    public static void premain(final String agentArgs, final Instrumentation instrumentation) throws IOException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        System.setProperty("javax.management.builder.initial", JmxMBeanServerBuilder.class.getCanonicalName());

        /*
            potential config options/arguments:

            port -- port to report metrics on
            address -- address to report metrics on
            topology_labels -- include C* rack, dc, cluster, host ID, etc as labels (default true)
         */

        final MBeanServerInterceptorHarvester collector = new MBeanServerInterceptorHarvester();

        final URI baseUri = UriBuilder.fromUri(agentArgs).build();

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
