package com.zegelin.picocli;

import com.google.common.base.Splitter;
import picocli.CommandLine;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

public abstract class InetSocketAddressTypeConverter implements CommandLine.ITypeConverter<InetSocketAddress> {
    @Override
    public InetSocketAddress convert(final String value) throws Exception {
        final List<String> addressParts = Splitter.on(':').limit(2).splitToList(value);

        String hostname = addressParts.get(0).trim();
        hostname = (hostname.length() == 0 ? null : hostname); // an empty hostname == wildcard/any

        int port = defaultPort();
        if (addressParts.size() == 2) {
            try {
                port = Integer.parseInt(addressParts.get(1).trim());

            } catch (final NumberFormatException e) {
                throw new CommandLine.TypeConversionException("Specified port is not a valid number");
            }
        }

        try {
            // why can you pass a null InetAddress, but a null String hostname is an error...
            return (hostname == null ?
                    new InetSocketAddress((InetAddress) null, port) :
                    new InetSocketAddress(hostname, port));

        } catch (final IllegalArgumentException e) {
            // invalid port, etc...
            throw new CommandLine.TypeConversionException(e.getLocalizedMessage());
        }
    }

    protected abstract int defaultPort();
}
