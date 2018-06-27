//package com.zegelin;
//
//import com.fasterxml.jackson.core.JsonGenerator;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.common.io.ByteStreams;
//import com.zegelin.cassandra.exporter.Agent;
//import org.glassfish.jersey.jackson.internal.jackson.jaxrs.cfg.JaxRSFeature;
//import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
//import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
//import org.glassfish.jersey.message.GZipEncoder;
//import org.glassfish.jersey.server.ResourceConfig;
//
//import javax.ws.rs.*;
//import javax.ws.rs.container.*;
//import javax.ws.rs.core.*;
//import javax.ws.rs.ext.ContextResolver;
//import javax.ws.rs.ext.ExceptionMapper;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.net.URI;
//
//
//public class JerseyTest {
//
//    public static void main(String args[]) {
//
//
//        final URI baseUri = UriBuilder.fromUri("http://localhost/").port(7890).build();
//        final ResourceConfig resourceConfig = new ResourceConfig(RootResource.class, MetricsResource.class)
//                .register(new JacksonJaxbJsonProvider().disable(JaxRSFeature.CACHE_ENDPOINT_WRITERS))
//                .register(FooFeature.class)
//                .register(Agent.OverrideContentTypeFilter.class)
//                .register(GZipEncoder.class)
//                .register(VerboseExceptionMapper.class);
//
//        JdkHttpServerFactory.createHttpServer(baseUri, resourceConfig);
//    }
//
//    public static class FooFeature implements Feature {
//
//        @Override
//        public boolean configure(final FeatureContext context) {
//            context.register(new ContextResolver<ObjectMapper>() {
//                @Override
//                public ObjectMapper getContext(final Class<?> type) {
//                    return new ObjectMapper().configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
//                }
//            });
//
//            return true;
//        }
//    }
//
//
//    public static class VerboseExceptionMapper implements ExceptionMapper<Throwable> {
//        @Override
//        public Response toResponse(final Throwable exception) {
//            return Response
//                    .serverError()
//                    .entity(exception)
//                    .build();
//        }
//    }
//
//
//    /**
//     * A {@link ContainerRequestFilter} that overrides the requests Accept header
//     * with the value provided by the x-content-type query parameter.
//     *
//     * Useful for viewing the different metrics exposition formats via a web browser,
//     * as they typically don't allow users to specify the Accept header, instead opting for
//     * text/html.
//     *
//     * e.g. /metrics?x-content-type=text/plain will request the Prometheus raw text exposition format
//     * rather than HTML formatted metrics.
//     */
//    @PreMatching
//    public static class OverrideContentTypeFilter implements ContainerRequestFilter {
//        @Override
//        public void filter(final ContainerRequestContext containerRequestContext) throws IOException {
//            final String overrideType = containerRequestContext.getUriInfo().getQueryParameters().getFirst("x-content-type");
//
//            if (overrideType == null)
//                return;
//
//            containerRequestContext.getHeaders().putSingle(HttpHeaders.ACCEPT, overrideType);
//        }
//    }
//
//    @Path("/")
//    public static class RootResource {
//        @GET
//        @Produces("text/html")
//        public StreamingOutput getRoot() {
//            return (final OutputStream outputStream) -> {
//                try (final InputStream resource = Agent.RootResource.class.getResourceAsStream("/com/zegelin/cassandra/exporter/root.html")) {
//                    ByteStreams.copy(resource, outputStream);
//                }
//            };
//        }
//    }
//
//    @Path("/metrics")
//    public static class MetricsResource {
//
//        @GET
//        @Produces(MediaType.TEXT_PLAIN)
//        public String doIt() {
//            throw new IllegalStateException("plain text baby!");
//        }
//
//        @GET
//        @Produces(MediaType.APPLICATION_JSON)
//        public Object jsonMetrics(@QueryParam("help") final boolean includeHelp, @Context final ObjectMapper providers) {
//
//            throw new IllegalStateException("eat me!");
//
////            final Map<String, MetricFamily> metricFamilies = new HashMap<>();
////
////            metricFamilies.put("test_metric_family", new CounterMetricFamily("test_metric_family", "this is the help", ImmutableSet.of(new NumericMetric(ImmutableMap.of("label", "value"), 52))));
//
////            ObjectWriterInjector.set(new ObjectWriterModifier() {
////                @Override
////                public ObjectWriter modify(final EndpointConfigBase<?> endpoint, final MultivaluedMap<String, Object> responseHeaders, final Object valueToWrite, final ObjectWriter w, final JsonGenerator g) throws IOException {
////                    w.withView()
////                }
////            });
//
//
////            return metricFamilies;
//        }
//    }
//}
