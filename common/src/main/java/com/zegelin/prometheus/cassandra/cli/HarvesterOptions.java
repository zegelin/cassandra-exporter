package com.zegelin.prometheus.cassandra.cli;

import com.zegelin.netty.Floats;
import com.zegelin.prometheus.cassandra.Harvester;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public class HarvesterOptions {
    private final Set<Path> processedExclusionFiles = new HashSet<>();

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec commandSpec;

    public final Set<Harvester.Exclusion> exclusions = new HashSet<>();

    @Option(names = {"-e", "--exclude"}, paramLabel = "EXCLUSION", arity = "1..*",
            description = "Exclude a metric family or MBean from exposition. " +
                    "EXCLUSION may be the full name of a metric family (wildcards or patterns not allowed) or " +
                    "the ObjectName of a MBean or a ObjectName pattern that matches multiple MBeans. ObjectNames always contain a colon (':'). " +
                    "See the ObjectName JavaDoc for details. " +
                    "If EXCLUSION is prefixed with an '@', it is interpreted (sans @ character) as a path to a file containing multiple EXCLUSION values, one per line. " +
                    "Lines prefixed with '#' are considered comments and are ignored. " +
                    "This option may be specified more than once to define multiple exclusions.")
    void setExclusions(final Set<String> values) {
        for (final String value : values) {
            if (value.startsWith("@")) {
                final Path file = Paths.get(value.substring(1));

                if (processedExclusionFiles.contains(file)) {
                    continue;
                }

                try {
                    Files.lines(file)
                            .filter(line -> !line.startsWith("#"))
                            .map(String::trim)
                            .filter(String::isEmpty)
                            .forEach(line -> this.exclusions.add(Harvester.Exclusion.create(line)));

                    processedExclusionFiles.add(file);

                } catch (final IOException e) {
                    throw new CommandLine.ParameterException(commandSpec.commandLine(),
                            String.format("Failed to read exclusions from '%s'", file), e);
                }

                continue;
            }

            this.exclusions.add(Harvester.Exclusion.create(value));
        }
    }

    @Option(names = {"-g", "--global-labels"}, paramLabel = "LABEL", split = ",",
            converter = GlobalLabelTypeConverter.class,
            description = "Select which global labels to include on all exported metrics. " +
                    "Valid options are: 'cluster_name', 'host_id' (UUID of the node), 'node' (node endpoint IP address), " +
                    "'datacenter', 'rack'. " +
                    "The default is to include all global labels. " +
                    "To disable all global labels use --no-global-labels."
    )
    public Set<Harvester.GlobalLabel> globalLabels = EnumSet.allOf(Harvester.GlobalLabel.class);

    @Option(names = {"--no-global-labels"},
            description = "Disable all global labels.")
    public void setNoGlobalLabels(final boolean noGlobalLabels) {
        this.globalLabels = (noGlobalLabels ? EnumSet.noneOf(Harvester.GlobalLabel.class) : EnumSet.allOf(Harvester.GlobalLabel.class));
    }

    @Option(names = {"--no-fast-float"},
            description = "Disable the use of fast float -> ascii conversion.")
    public void setNoFastFloat(final boolean noFastFloat) {
        Floats.useFastFloat = !noFastFloat;
    }

    @Option(names = {"--enable-per-thread-cpu-times"},
            description = "Collect per-thread CPU times, where each thread gets its own time-series. (EXPERIMENTAL)")
    public boolean perThreadTimingEnabled;
}
