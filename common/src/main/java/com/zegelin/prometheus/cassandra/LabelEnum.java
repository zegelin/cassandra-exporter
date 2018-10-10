package com.zegelin.prometheus.cassandra;

import com.google.common.collect.ImmutableMap;

import java.util.Set;
import java.util.function.Supplier;

public interface LabelEnum {
    String labelName();

    static void addIfEnabled(final LabelEnum e, final Set<? extends LabelEnum> enabledLabels, final ImmutableMap.Builder<String, String> mapBuilder, final Supplier<String> valueSupplier) {
        if (enabledLabels.contains(e)) {
            mapBuilder.put(e.labelName(), valueSupplier.get());
        }
    }
}
