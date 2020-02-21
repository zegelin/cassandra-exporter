package com.zegelin.cassandra.exporter.netty.ssl;

import io.netty.handler.ssl.ClientAuth;

public enum ClientAuthentication {
    NONE(ClientAuth.NONE, false),
    OPTIONAL(ClientAuth.OPTIONAL, false),
    REQUIRE(ClientAuth.REQUIRE, false),
    VALIDATE(ClientAuth.REQUIRE, true);

    private final ClientAuth clientAuth;
    private final boolean hostnameValidation;

    ClientAuthentication(ClientAuth clientAuth, boolean hostnameValidation) {
        this.clientAuth = clientAuth;
        this.hostnameValidation = hostnameValidation;
    }

    ClientAuth getClientAuth() {
        return clientAuth;
    }

    boolean getHostnameValidation() {
        return hostnameValidation;
    }
}
