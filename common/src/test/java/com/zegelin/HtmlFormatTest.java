//package com.zegelin;
//
//import com.fasterxml.jackson.core.JsonGenerator;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.common.base.Stopwatch;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.io.ByteStreams;
//import com.zegelin.cassandra.exporter.Agent;
//import com.zegelin.prometheus.domain.*;
//import org.glassfish.jersey.jackson.internal.jackson.jaxrs.cfg.JaxRSFeature;
//import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
//import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
//import org.glassfish.jersey.message.GZipEncoder;
//import org.glassfish.jersey.server.ResourceConfig;
//
//import javax.ws.rs.*;
//import javax.ws.rs.container.ContainerRequestContext;
//import javax.ws.rs.container.ContainerRequestFilter;
//import javax.ws.rs.container.PreMatching;
//import javax.ws.rs.core.*;
//import javax.ws.rs.ext.ContextResolver;
//import javax.ws.rs.ext.ExceptionMapper;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.io.PrintWriter;
//import java.net.URI;
//import java.util.Map;
//import java.util.Optional;
//import java.util.TreeMap;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
//@SuppressWarnings("Duplicates")
//public class HtmlFormatTest {
//
//    public static void main(String args[]) {
//
//
//        final URI baseUri = UriBuilder.fromUri("http://localhost/").port(7890).build();
//        final ResourceConfig resourceConfig = new ResourceConfig(MetricsResource.class)
//                .register(new JacksonJaxbJsonProvider().disable(JaxRSFeature.CACHE_ENDPOINT_WRITERS))
//                .register(Agent.OverrideContentTypeFilter.class)
//                .register(GZipEncoder.class);
//
//        JdkHttpServerFactory.createHttpServer(baseUri, resourceConfig);
//    }
//
//
//
//    @Path("/metrics")
//    public static class MetricsResource {
//        @GET
//        @Produces(MediaType.TEXT_HTML)
//        public StreamingOutput htmlMetrics() {
//            // TODO: refactor this + cleanup
//
//            return new StreamingOutput() {
//
//                @Override
//                public void write(final OutputStream outputStream) throws IOException, WebApplicationException {
//                    final Stopwatch stopwatch = Stopwatch.createStarted();
//                    final Map<String, MetricFamily> metricFamilies = collector.collect().collect(Collectors.toMap(mf -> mf.name, Function.identity(),
//                            (a, b) -> { throw new IllegalStateException(); },
//                            TreeMap::new
//                    ));
//                    stopwatch.stop();
//
//                    final Map<String, String> globalLabels = collector.globalLabels();
//
//                    try (PrintWriter writer = new PrintWriter(outputStream)) {
//                        final int[] totalMetricFamilies = {0};
//                        final int[] totalMetrics = {0};
//
////                        writer.println("<link rel=\"stylesheet\" href=\"//localhost:8009/styles.css\" />");
//
//                        writer.println("<h1>Cassandra Metrics</h1>");
//
//                        // TOC
//                        {
//                            writer.println("<table>");
//                            writer.println("<thead>");
//                            writer.println("<tr>");
//                            writer.println("<th>Metric Family</th>");
//                            writer.println("<th>Type</th>");
//                            writer.println("<th>Help</th>");
//                            writer.println("</tr>");
//                            writer.println("</thead>");
//
//                            for (final MetricFamily<?> metricFamily : metricFamilies.values()) {
//                                writer.println("<tr>");
//                                writer.printf("<td><a href=\"#%s\"><code>%s</code></a></td>", metricFamily.name, metricFamily.name);
//                                writer.printf("<td>%s</td>", metricFamily.getClass());
//                                writer.printf("<td>%s</td>", Optional.ofNullable(metricFamily.help).orElse("<em class=\"muted\">Not available</em>"));
//                                writer.println("</tr>");
//                            }
//
//                            writer.println("</table>");
//                        }
//
//                        writer.println("<hr />");
//
//                        writer.println("<h2>Node Information</h2>");
//
//                        writer.println("<dl>");
//                        for (final Map.Entry<String, String> label : globalLabels.entrySet()) {
//                            writer.printf("<dt>%s</dt><dd>%s</dd>%n", label.getKey(), label.getValue());
//                        }
//                        writer.println("</dl>");
//
//                        writer.println("<hr />");
//
//                        final MetricFamilyVisitor htmlFormatWriter = new MetricFamilyVisitor() {
//                            <T extends Metric> void printMetricFamily(final MetricFamily<T> metricFamily, final Function<T, Map<String, Object>> valuesFunction) {
//                                totalMetricFamilies[0]++;
//
//                                writer.println("<table>");
//
//                                writer.println("<caption>");
//                                writer.printf("<a id=\"%s\" />", metricFamily.name);
//
//                                writer.printf("<h2><code>%s</code></h2>", metricFamily.name);
//
//                                if (metricFamily.help != null) {
//                                    writer.printf("<p><small>%s</small></p>", metricFamily.help);
//                                }
//
//                                writer.println("<dl>");
//                                writer.printf("<dt>Type</dt><dd>%s</dd>", metricFamily.getClass());
//                                writer.println("</dl>");
//
//                                writer.println("</caption>");
//
//
//                                writer.println("<thead>");
//                                writer.println("<tr><th>Labels</th><th>Value</th></tr>");
//                                writer.println("</thead>");
//
//                                writer.println("<tbody>");
//
//                                for (final T metric : metricFamily.metrics) {
//                                    writer.printf("<tr>");
//                                    writer.printf("<td><dl class=\"labels\">");
//                                    for (final Map.Entry<String, String> label : metric.labels.entrySet()) {
//                                        writer.printf("<dt>%s</dt><dd>%s</dd>", label.getKey(), label.getValue());
//                                    }
//                                    writer.printf("</dl></td>");
//
//                                    writer.printf("<td><dl class=\"values\">");
//                                    final Map<String, Object> values = valuesFunction.apply(metric);
//                                    for (final Map.Entry<String, Object> value : values.entrySet()) {
//                                        writer.printf("<dt>%s</dt><dd>%s</dd>", value.getKey(), value.getValue());
//                                    }
//                                    writer.printf("</dl></td>");
//                                    writer.printf("</tr>");
//
//                                    totalMetrics[0]+=values.size();
//                                }
//
//                                writer.println("</tbody>");
//
//                                writer.println("</table>");
//                            }
//
//                            @Override
//                            public Void visit(final CounterMetricFamily metricFamily) {
//                                printMetricFamily(metricFamily, (m) -> ImmutableMap.of("count", m.value));
//                                return null;
//                            }
//
//                            @Override
//                            public Void visit(final GaugeMetricFamily metricFamily) {
//                                printMetricFamily(metricFamily, (m) -> ImmutableMap.of("value", m.value));
//                                return null;
//                            }
//
//                            @Override
//                            public Void visit(final SummaryMetricFamily metricFamily) {
//                                printMetricFamily(metricFamily, (m) -> {
//                                    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
//
//                                    m.quantiles.forEach((q, v) -> builder.put(String.format("φ %s<sup>th</sup>", q), v));
//
//                                    builder.put("count", m.count);
//                                    builder.put("sum", m.sum);
//
//                                    return builder.build();
//                                });
//
//                                return null;
//                            }
//
//                            @Override
//                            public Void visit(final HistogramMetricFamily metricFamily) {
//                                return null;
//                            }
//
//                            @Override
//                            public Void visit(final UntypedMetricFamily metricFamily) {
//                                return null;
//                            }
//                        };
//
//                        metricFamilies.values().forEach(mf -> mf.accept(htmlFormatWriter));
//
//                        writer.println("<hr />");
//                        writer.printf("<small>Collection time: %s, total number of metric families: %d, total number of time series: %d%n", stopwatch.toString(), totalMetricFamilies[0], totalMetrics[0]);
//
//                    }
//                }
//            };
//        }
//    }
//}
