package com.zegelin.jaxrs.filter;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;

/**
 * A {@link ContainerRequestFilter} that overrides the requests Accept header
 * with the value provided by the x-content-type query parameter.
 *
 * Useful for viewing the different metrics exposition formats via a web browser,
 * as they typically don't allow users to specify the Accept header, instead opting for
 * text/html.
 *
 * e.g. /metrics?x-content-type=text/plain will request the Prometheus raw text exposition format
 * rather than HTML formatted metrics.
 */
@PreMatching
public class OverrideContentTypeFilter implements ContainerRequestFilter {
    @Override
    public void filter(final ContainerRequestContext containerRequestContext) throws IOException {
        final String overrideType = containerRequestContext.getUriInfo().getQueryParameters().getFirst("x-content-type");

        if (overrideType == null)
            return;

        containerRequestContext.getHeaders().putSingle(HttpHeaders.ACCEPT, overrideType);
    }
}
