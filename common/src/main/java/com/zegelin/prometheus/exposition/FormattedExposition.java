package com.zegelin.prometheus.exposition;

public interface FormattedExposition {
    void nextSlice(final ExpositionSink<?> sink);

    boolean isEndOfInput();
}
