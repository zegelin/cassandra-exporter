package com.zegelin.cassandra.exporter;

import java.lang.instrument.Instrumentation;
import java.util.List;
import java.util.concurrent.Callable;

import com.sun.jmx.mbeanserver.JmxMBeanServerBuilder;
import com.zegelin.agent.AgentArgumentParser;
import com.zegelin.cassandra.exporter.cli.HarvesterOptions;
import com.zegelin.cassandra.exporter.cli.HttpServerOptions;
import com.zegelin.cassandra.exporter.netty.Server;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "cassandra-exporter-agent", mixinStandardHelpOptions = true, sortOptions = false)
public class Agent implements Callable<Void> {

    @Mixin
    private HarvesterOptions harvesterOptions;

    @Mixin
    private HttpServerOptions httpServerOptions;

    @Override
    public Void call() throws Exception {
        System.setProperty("javax.management.builder.initial", JmxMBeanServerBuilder.class.getCanonicalName());

        final MBeanServerInterceptorHarvester harvester = new MBeanServerInterceptorHarvester(harvesterOptions);

        final Server server = Server.start(harvester, httpServerOptions);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop().sync();

            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        return null;
    }

    public static void premain(final String agentArgs, final Instrumentation instrumentation) {
        final List<String> arguments = AgentArgumentParser.parseArguments(agentArgs);

        final CommandLine commandLine = new CommandLine(new Agent());

        commandLine.setCaseInsensitiveEnumValuesAllowed(true);

        commandLine.parseWithHandlers(
                new CommandLine.RunLast(),
                CommandLine.defaultExceptionHandler().andExit(1),
                arguments.toArray(new String[]{})
        );
    }
}
