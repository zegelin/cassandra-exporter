package com.zegelin.prometheus.cassandra;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.sun.jmx.mbeanserver.JmxMBeanServerBuilder;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.domain.*;
import com.zegelin.prometheus.exposition.PrometheusTextFormatWriter;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.filter.EncodingFilter;


import javax.inject.Inject;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.ws.rs.*;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.instrument.Instrumentation;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Agent {
    public static void premain(final String agentArgs, final Instrumentation instrumentation) throws IOException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        System.setProperty("javax.management.builder.initial", JmxMBeanServerBuilder.class.getCanonicalName());

        /*
            potential config options/arguments:

            port -- port to report metrics on
            address -- address to report metrics on
            topology_labels -- include C* rack, dc, cluster, host ID, etc as labels (default true)
         */

        final MetricsCollector collector = new MetricsCollector();

        final URI baseUri = UriBuilder.fromUri(agentArgs).build();
        final ResourceConfig resourceConfig = new ResourceConfig(RootResource.class, MetricsResource.class)
                .register(JacksonFeature.class)
                .register(OverrideContentTypeFilter.class)
                .register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(collector).to(MetricsCollector.class);
            }
        });

        EncodingFilter.enableFor(resourceConfig, GZipEncoder.class);

        JdkHttpServerFactory.createHttpServer(baseUri, resourceConfig);
    }

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
    public static class OverrideContentTypeFilter implements ContainerRequestFilter {
        @Override
        public void filter(final ContainerRequestContext containerRequestContext) throws IOException {
            final String overrideType = containerRequestContext.getUriInfo().getQueryParameters().getFirst("x-content-type");

            if (overrideType == null)
                return;

            containerRequestContext.getHeaders().putSingle(HttpHeaders.ACCEPT, overrideType);
        }
    }

    @Path("/")
    public static class RootResource {
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

    @Path("/metrics")
    public static class MetricsResource {
        final MetricsCollector collector;

        @Inject
        public MetricsResource(final MetricsCollector collector) {
            this.collector = collector;
        }

        @GET
        @Produces(MediaType.TEXT_HTML)
        public StreamingOutput htmlMetrics() {
            return new StreamingOutput() {

                @Override
                public void write(final OutputStream outputStream) throws IOException, WebApplicationException {
                    final Stopwatch stopwatch = Stopwatch.createStarted();
                    final Map<String, MetricFamily> metricFamilies = collector.collect().collect(Collectors.toMap(mf -> mf.name, Function.identity(),
                            (a, b) -> { throw new IllegalStateException(); },
                            TreeMap::new
                    ));
                    stopwatch.stop();

                    final Map<String, String> globalLabels = collector.globalLabels();

                    try (PrintWriter writer = new PrintWriter(outputStream)) {
                        final int[] totalMetricFamilies = {0};
                        final int[] totalMetrics = {0};

                        writer.println("<link rel=\"stylesheet\" href=\"//localhost:8009/styles.css\" />");

                        writer.println("<h1>Cassandra Metrics</h1>");

                        // TOC
                        {
                            writer.println("<table>");
                            writer.println("<thead>");
                            writer.println("<tr>");
                            writer.println("<th>Metric Family</th>");
                            writer.println("<th>Type</th>");
                            writer.println("<th>Help</th>");
                            writer.println("</tr>");
                            writer.println("</thead>");

                            for (final MetricFamily<?> metricFamily : metricFamilies.values()) {
                                writer.println("<tr>");
                                writer.printf("<td><a href=\"#%s\"><code>%s</code></a></td>", metricFamily.name, metricFamily.name);
                                writer.printf("<td>%s</td>", metricFamily.getClass());
                                writer.printf("<td>%s</td>", Optional.ofNullable(metricFamily.help).orElse("<em class=\"muted\">Not available</em>"));
                                writer.println("</tr>");
                            }

                            writer.println("</table>");
                        }

                        writer.println("<hr />");

                        writer.println("<h2>Node Information</h2>");

                        writer.println("<dl>");
                        for (final Map.Entry<String, String> label : globalLabels.entrySet()) {
                            writer.printf("<dt>%s</dt><dd>%s</dd>%n", label.getKey(), label.getValue());
                        }
                        writer.println("</dl>");

                        writer.println("<hr />");

                        final MetricFamilyVisitor htmlFormatWriter = new MetricFamilyVisitor() {
                            <T extends Metric> void printMetricFamily(final MetricFamily<T> metricFamily, final Function<T, Map<String, Object>> valuesFunction) {
                                totalMetricFamilies[0]++;

                                writer.println("<table>");

                                writer.println("<caption>");
                                writer.printf("<a id=\"%s\" />", metricFamily.name);

                                writer.printf("<h2><code>%s</code></h2>", metricFamily.name);

                                if (metricFamily.help != null) {
                                    writer.printf("<p><small>%s</small></p>", metricFamily.help);
                                }

                                writer.println("<dl>");
                                writer.printf("<dt>Type</dt><dd>%s</dd>", metricFamily.getClass());
                                writer.println("</dl>");

                                writer.println("</caption>");


                                writer.println("<thead>");
                                writer.println("<tr><th>Labels</th><th>Value</th></tr>");
                                writer.println("</thead>");

                                writer.println("<tbody>");

                                for (final T metric : metricFamily.metrics) {
                                    writer.printf("<tr>");
                                    writer.printf("<td><dl class=\"labels\">");
                                    for (final Map.Entry<String, String> label : metric.labels.entrySet()) {
                                        writer.printf("<dt>%s</dt><dd>%s</dd>", label.getKey(), label.getValue());
                                    }
                                    writer.printf("</dl></td>");

                                    writer.printf("<td><dl class=\"values\">");
                                    final Map<String, Object> values = valuesFunction.apply(metric);
                                    for (final Map.Entry<String, Object> value : values.entrySet()) {
                                        writer.printf("<dt>%s</dt><dd>%s</dd>", value.getKey(), value.getValue());
                                    }
                                    writer.printf("</dl></td>");
                                    writer.printf("</tr>");

                                    totalMetrics[0]+=values.size();
                                }

                                writer.println("</tbody>");

                                writer.println("</table>");
                            }

                            @Override
                            public Void visit(final CounterMetricFamily metricFamily) {
                                printMetricFamily(metricFamily, (m) -> ImmutableMap.of("count", m.value));
                                return null;
                            }

                            @Override
                            public Void visit(final GaugeMetricFamily metricFamily) {
                                printMetricFamily(metricFamily, (m) -> ImmutableMap.of("value", m.value));
                                return null;
                            }

                            @Override
                            public Void visit(final SummaryMetricFamily metricFamily) {
                                printMetricFamily(metricFamily, (m) -> {
                                    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

                                    m.quantiles.forEach((q, v) -> builder.put(String.format("φ %s<sup>th</sup>", q), v));

                                    builder.put("count", m.count);
                                    builder.put("sum", m.sum);

                                    return builder.build();
                                });

                                return null;
                            }

                            @Override
                            public Void visit(final HistogramMetricFamily metricFamily) {
                                return null;
                            }

                            @Override
                            public Void visit(final UntypedMetricFamily metricFamily) {
                                return null;
                            }
                        };

                        metricFamilies.values().forEach(mf -> mf.accept(htmlFormatWriter));

                        writer.println("<hr />");
                        writer.printf("<small>Collection time: %s, total number of metric families: %d, total number of time series: %d%n", stopwatch.toString(), totalMetricFamilies[0], totalMetrics[0]);

                    }
                }
            };
        }


        @GET
        @Produces(MediaType.TEXT_PLAIN)
        public StreamingOutput textMetrics() {
            return outputStream -> {
                final Map<String, String> globalLabels = collector.globalLabels();

                try (final PrometheusTextFormatWriter writer = new PrometheusTextFormatWriter(outputStream, Instant.now(), globalLabels)) {
                    final Stopwatch stopwatch = Stopwatch.createStarted();

                    collector.collect().sequential().forEach(mf -> mf.accept(writer));

                    {
                        final long collectionTimeNS = stopwatch.stop().elapsed(TimeUnit.NANOSECONDS);
                        final double collectionTimeSeconds = collectionTimeNS / (double) TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);

                        final GaugeMetricFamily collectionTimeGuage = new GaugeMetricFamily("cassandra_scrape_duration_seconds", "Time taken to collect Cassandra metrics",
                                ImmutableSet.of(new NumericMetric(ImmutableMap.of(), collectionTimeSeconds))
                        );

                        collectionTimeGuage.accept(writer);
                    }
                }
            };
        }

        @GET
        @Produces(MediaType.APPLICATION_JSON)
        public Object jsonMetrics(@QueryParam("help") final boolean includeHelp) {
            return collector.collect().collect(Collectors.toMap(mf -> mf.name, Function.identity(),
                    (a, b) -> { throw new IllegalStateException(); },
                    TreeMap::new
            ));
        }
    }
}
