package com.zegelin.prometheus.cassandra.cli;

import com.zegelin.netty.Floats;
import com.zegelin.prometheus.cassandra.FactoriesSupplier;
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
            description = "Select which global labels to include on all exported metrics. " +
                    "Valid options are: 'CLUSTER' (cluster name), 'HOST_ID' (UUID of the node), 'NODE' (node endpoint IP address), " +
                    "'DATACENTER' (DC name), 'RACK' (rack name). " +
                    "The default is to include all global labels except HOST_ID. " +
                    "To disable all global labels use --no-global-labels."
    )
    public Set<Harvester.GlobalLabel> globalLabels = EnumSet.allOf(Harvester.GlobalLabel.class);
    {
        globalLabels.remove(Harvester.GlobalLabel.HOST_ID); // remove host ID by default to avoid creating new time-series on node replace.
    }

    @Option(names = "--no-global-labels", arity = "0",
            description = "Disable all global labels.")
    public void setNoGlobalLabels(final boolean noGlobalLabels) {
        if (!noGlobalLabels) {
            throw new IllegalStateException();
        }

        this.globalLabels = EnumSet.noneOf(Harvester.GlobalLabel.class);
    }


    @Option(names = {"-t", "--table-labels"}, paramLabel = "LABEL", split = ",",
            description = "Select which labels to include on table-level metrics. " +
                    "Valid options are: " +
                    "'TABLE_TYPE' (table, view or index), " +
                    "'INDEX_TYPE' (for indexes -- keys, composites or custom), " +
                    "'INDEX_CLASS' (the index class name for custom indexes),  " +
                    "'COMPACTION_STRATEGY_CLASS' (for tables & views, compaction-related metrics only). " +
                    "The default is to include all table labels. " +
                    "To disable all table labels use --no-table-labels."
    )
    public Set<FactoriesSupplier.TableLabels> tableLabels = EnumSet.allOf(FactoriesSupplier.TableLabels.class);

    @Option(names = {"--no-table-labels"},
            description = "Disable all table labels.")
    public void setNoTableLabels(final boolean noTableLabels) {
        if (!noTableLabels) {
            throw new IllegalStateException();
        }

        this.tableLabels = EnumSet.noneOf(FactoriesSupplier.TableLabels.class);
    }


    @Option(names = "--no-fast-float",
            description = "Disable the use of fast float -> ascii conversion.")
    public void setNoFastFloat(final boolean noFastFloat) {
        Floats.useFastFloat = !noFastFloat;
    }

    @Option(names = "--enable-per-thread-cpu-times",
            description = "Collect per-thread CPU times, where each thread gets its own time-series. (EXPERIMENTAL)")
    public boolean perThreadTimingEnabled = false;

    @Option(names = "--enable-collector-timing",
            description = "Record the cumulative time taken to run each collector and export the results.")
    public boolean collectorTimingEnabled;
}
