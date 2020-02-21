package com.zegelin.cassandra.exporter.netty.ssl;

import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum SslImplementation {
    OPENSSL(SslProvider.OPENSSL),
    JDK(SslProvider.JDK),
    DISCOVER();

    private final SslProvider provider;

    SslImplementation() {
        provider = null;
    }

    SslImplementation(SslProvider provider) {
        this.provider = provider;
    }

    SslProvider getProvider() {
        if (provider != null) {
            return provider;
        } else {
            if (OpenSsl.isAvailable()) {
                logger().info("Native OpenSSL library discovered for exporter: {}", OpenSsl.versionString());
                return SslProvider.OPENSSL;
            } else {
                logger().info("No native OpenSSL library discovered for exporter - falling back to JDK implementation");
                return SslProvider.JDK;
            }
        }
    }

    // Instead of normal static initialization which cause JVM to bail out.
    private Logger logger() {
        return LoggerFactory.getLogger(SslImplementation.class);
    }
}
