package com.zegelin.cassandra.exporter.netty.ssl;

import com.zegelin.cassandra.exporter.cli.HttpServerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReloadWatcher {
    private static final Logger logger = LoggerFactory.getLogger(ReloadWatcher.class);

    private static final long RELOAD_MARGIN_MILLIS = 1000;
    private final long intervalInMs;
    private final Collection<File> files;

    private long nextReloadAt;
    private long reloadedAt;

    public ReloadWatcher(HttpServerOptions httpServerOptions) {
        intervalInMs = TimeUnit.SECONDS.toMillis(httpServerOptions.sslReloadIntervalInSeconds);
        files = Stream.of(httpServerOptions.sslServerKeyFile,
                httpServerOptions.sslServerKeyPasswordFile,
                httpServerOptions.sslServerCertificateFile,
                httpServerOptions.sslTrustedCertificateFile)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        logger.info("Watching {} for changes every {} seconds", this.files, httpServerOptions.sslReloadIntervalInSeconds);
        reset(System.currentTimeMillis());
    }

    private void reset(long now) {
        // Create a 1 second margin to compensate for poor resolution of File.lastModified()
        reloadedAt = now - RELOAD_MARGIN_MILLIS;

        nextReloadAt = now + intervalInMs;
        logger.debug("Next reload at {}", nextReloadAt);
    }

    public synchronized void forceReload() {
        if (!enabled()) {
            return;
        }

        logger.info("Forced reload of exporter certificates on next scrape");

        reloadedAt = 0L;
        nextReloadAt = 0L;
    }

    boolean needReload() {
        if (!enabled()) {
            return false;
        }

        long now = System.currentTimeMillis();

        if (timeToPoll(now)) {
            return reallyNeedReload(now);
        }

        return false;
    }

    private boolean enabled() {
        return intervalInMs > 0;
    }

    private boolean timeToPoll(long now) {
        return now > nextReloadAt;
    }

    private synchronized boolean reallyNeedReload(long now) {
        if (timeToPoll(now)) {
            try {
                return anyFileModified();
            } finally {
                reset(now);
            }
        }
        return false;
    }

    private boolean anyFileModified() {
        return files.stream().anyMatch(f -> f.lastModified() > reloadedAt);
    }
}
