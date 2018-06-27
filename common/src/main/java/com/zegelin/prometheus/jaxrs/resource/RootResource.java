package com.zegelin.prometheus.jaxrs.resource;

import com.google.common.io.ByteStreams;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.StreamingOutput;
import java.io.InputStream;
import java.io.OutputStream;

@Path("/")
public class RootResource {
    @GET
    @Produces("text/html")
    public StreamingOutput getRoot() {
        return (final OutputStream outputStream) -> {
            try (final InputStream resource = RootResource.class.getResourceAsStream("/root.html")) {
                ByteStreams.copy(resource, outputStream);
            }
        };
    }
}
