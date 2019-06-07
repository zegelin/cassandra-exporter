package com.zegelin.cassandra.exporter;

import java.util.concurrent.TimeUnit;

public final class MetricValueConversionFunctions {
    private MetricValueConversionFunctions() {}

    public static float neg1ToNaN(final float f) {
        return (f == -1 ? Float.NaN : f);
    }

    public static float percentToRatio(final float f) {
        return f / 100.f;
    }


    private static float MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
    private static float MICROSECONDS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
    private static float NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    public static float millisecondsToSeconds(final float f) {
        return f / MILLISECONDS_PER_SECOND;
    }

    public static float microsecondsToSeconds(final float f) {
        return f / MICROSECONDS_PER_SECOND;
    }

    public static float nanosecondsToSeconds(final float f) {
        return f / NANOSECONDS_PER_SECOND;
    }
}
