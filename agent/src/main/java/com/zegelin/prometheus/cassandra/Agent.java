package com.zegelin.prometheus.cassandra;

import com.sun.jmx.mbeanserver.JmxMBeanServerBuilder;
import com.zegelin.agent.AgentArgumentParser;
import com.zegelin.jaxrs.filter.OverrideContentTypeFilter;
import com.zegelin.prometheus.cassandra.cli.HarvesterOptions;
import com.zegelin.prometheus.jaxrs.resource.MetricsResource;
import com.zegelin.prometheus.jaxrs.resource.RootResource;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;
import picocli.CommandLine;


import javax.management.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class Agent {


    @CommandLine.Command(mixinStandardHelpOptions = true, version = "auto help demo - picocli 3.0")
    static class AgentCommand implements Callable<Void> {
        @CommandLine.Mixin
        HarvesterOptions harvesterOptions;

        @Override
        public Void call() throws Exception {
            return null;
        }
    }

    public static void premain(final String agentArgs, final Instrumentation instrumentation) throws IOException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        System.setProperty("javax.management.builder.initial", JmxMBeanServerBuilder.class.getCanonicalName());

        final List<String> arguments = AgentArgumentParser.parseArguments(agentArgs);

        CommandLine.call(new AgentCommand(), arguments.toArray(new String[]{}));

        final MBeanServerInterceptorHarvester harvester = new MBeanServerInterceptorHarvester();

        final URI baseUri = UriBuilder.fromUri(agentArgs).build();

        final ResourceConfig resourceConfig = new ResourceConfig(RootResource.class, MetricsResource.class)
                .register(JacksonFeature.class)
                .register(OverrideContentTypeFilter.class)
                .register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(harvester).to(Harvester.class);
            }
        });

        EncodingFilter.enableFor(resourceConfig, GZipEncoder.class);

        JdkHttpServerFactory.createHttpServer(baseUri, resourceConfig);
    }

    public static void main(String[] args) throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException, IOException {
        premain(String.join(" ", args), null);
    }
}
