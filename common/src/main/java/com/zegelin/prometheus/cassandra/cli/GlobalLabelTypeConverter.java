package com.zegelin.prometheus.cassandra.cli;

import com.zegelin.prometheus.cassandra.Harvester;
import picocli.CommandLine;

public class GlobalLabelTypeConverter implements CommandLine.ITypeConverter<Harvester.GlobalLabel> {
    @Override
    public Harvester.GlobalLabel convert(final String value) throws Exception {
        try {
            return Harvester.GlobalLabel.valueOf(value.toUpperCase());

        } catch (final IllegalArgumentException e) {
            throw new CommandLine.TypeConversionException(String.format("'%s' is not a valid value", value));
        }
    }
}
