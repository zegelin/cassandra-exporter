package com.zegelin.cassandra.exporter.netty.ssl;

import com.zegelin.cassandra.exporter.cli.HttpServerOptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

public class TestReloadWatcher {
    public static final long INITIAL_FILE_AGE_MILLIS = 5000;
    public static final long SLEEP_MILLIS = 1001;

    private HttpServerOptions options;
    private ReloadWatcher watcher;

    @BeforeMethod
    public void before() throws IOException {
        options = new HttpServerOptions();
        options.sslReloadIntervalInSeconds = 1;

        options.sslServerKeyFile = givenTemporaryFile("server-key");
        options.sslServerCertificateFile = givenTemporaryFile("server-cert");
        options.sslTrustedCertificateFile = givenTemporaryFile("trusted-cert");

        options.sslServerKeyFile.setLastModified(System.currentTimeMillis() - INITIAL_FILE_AGE_MILLIS);
        options.sslServerCertificateFile.setLastModified(System.currentTimeMillis() - INITIAL_FILE_AGE_MILLIS);
        options.sslTrustedCertificateFile.setLastModified(System.currentTimeMillis() - INITIAL_FILE_AGE_MILLIS);

        watcher = new ReloadWatcher(options);
    }

    @Test
    public void testNoImmediateReload() {
        options.sslServerKeyFile.setLastModified(System.currentTimeMillis());

        assertThat(watcher.needReload()).isFalse();
    }

    @Test
    public void testNoReloadWhenFilesAreUntouched() throws InterruptedException {
        Thread.sleep(SLEEP_MILLIS);

        assertThat(watcher.needReload()).isFalse();
    }

    @Test
    public void testReloadOnceWhenFilesAreTouched() throws InterruptedException {
        Thread.sleep(SLEEP_MILLIS);

        options.sslServerKeyFile.setLastModified(System.currentTimeMillis());
        options.sslServerCertificateFile.setLastModified(System.currentTimeMillis());

        Thread.sleep(SLEEP_MILLIS);

        assertThat(watcher.needReload()).isTrue();

        Thread.sleep(SLEEP_MILLIS);

        assertThat(watcher.needReload()).isFalse();
    }

    // Verify that we reload certificates on next pass again in case files are modified
    // just as we check for reload.
    @Test
    public void testReloadAgainWhenFilesAreTouchedJustAfterReload() throws InterruptedException {
        Thread.sleep(SLEEP_MILLIS);

        options.sslServerKeyFile.setLastModified(System.currentTimeMillis());
        assertThat(watcher.needReload()).isTrue();
        options.sslServerCertificateFile.setLastModified(System.currentTimeMillis());

        Thread.sleep(SLEEP_MILLIS);

        assertThat(watcher.needReload()).isTrue();
    }

    @Test
    public void testReloadWhenForced() throws InterruptedException {
        Thread.sleep(SLEEP_MILLIS);

        watcher.forceReload();

        assertThat(watcher.needReload()).isTrue();
    }

    @Test
    public void testNoReloadWhenDisabled() throws InterruptedException {
        options.sslReloadIntervalInSeconds = 0;
        watcher = new ReloadWatcher(options);

        Thread.sleep(SLEEP_MILLIS);
        options.sslServerKeyFile.setLastModified(System.currentTimeMillis());

        assertThat(watcher.needReload()).isFalse();
    }

    private File givenTemporaryFile(String filename) throws IOException {
        File file = File.createTempFile(filename, "tmp");
        Files.write(file.toPath(), "dummy".getBytes());

        return file;
    }
}
