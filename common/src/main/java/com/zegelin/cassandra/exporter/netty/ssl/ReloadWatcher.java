package com.zegelin.cassandra.exporter.netty.ssl;

import com.zegelin.cassandra.exporter.cli.HttpServerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReloadWatcher {
    private static final Logger logger = LoggerFactory.getLogger(ReloadWatcher.class);

    private final long intervalInMs;
    private final Collection<File> files;

    private long reloadAt;
    private long reloadedAt;

    public ReloadWatcher(HttpServerOptions httpServerOptions) {
        intervalInMs = httpServerOptions.sslReloadIntervalInSeconds * 1000;
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
        reloadedAt = now;
        reloadAt = now + intervalInMs;
        logger.debug("Reset reloaded at to {}", reloadedAt);
    }

    boolean needReload() {
        long now = System.currentTimeMillis();

        if (timeToPoll(now)) {
            return reallyNeedReload(now);
        }

        return false;
    }

    private boolean timeToPoll(long now) {
        return intervalInMs > 0 && now > reloadAt;
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
