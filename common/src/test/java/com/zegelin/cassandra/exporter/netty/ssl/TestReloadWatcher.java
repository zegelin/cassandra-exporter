package com.zegelin.cassandra.exporter.netty.ssl;

import com.zegelin.cassandra.exporter.cli.HttpServerOptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

public class TestReloadWatcher {

    private HttpServerOptions options;
    private ReloadWatcher watcher;

    @BeforeMethod
    public void before() throws IOException {
        options = new HttpServerOptions();
        options.sslReloadIntervalInSeconds = 1;
        options.sslServerKeyFile = givenTemporaryFile("server-key");
        options.sslServerCertificateFile = givenTemporaryFile("server-cert");
        options.sslTrustedCertificateFile = givenTemporaryFile("trusted-cert");
        watcher = new ReloadWatcher(options);
    }

    @Test
    public void testNoImmediateReload() {
        options.sslServerKeyFile.setLastModified(System.currentTimeMillis());

        assertThat(watcher.needReload()).isFalse();
    }

    @Test
    public void testNoReloadWhenFilesAreUntouched() throws InterruptedException {
        Thread.sleep(1001);

        assertThat(watcher.needReload()).isFalse();
    }

    @Test
    public void testReloadWhenFilesAreTouched() throws InterruptedException {
        Thread.sleep(1001);
        options.sslServerKeyFile.setLastModified(System.currentTimeMillis());

        assertThat(watcher.needReload()).isTrue();
    }

    private File givenTemporaryFile(String filename) throws IOException {
        File file = File.createTempFile(filename, "tmp");
        Files.write(file.toPath(), "dummy".getBytes());

        return file;
    }
}
