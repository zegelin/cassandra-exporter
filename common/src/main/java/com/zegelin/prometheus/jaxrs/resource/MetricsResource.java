package com.zegelin.prometheus.jaxrs.resource;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.zegelin.prometheus.cassandra.Harvester;
import com.zegelin.prometheus.domain.*;
import com.zegelin.prometheus.exposition.PrometheusTextFormatWriter;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Path("/metrics")
public class MetricsResource {
    final Harvester harvester;

    @Inject
    public MetricsResource(final Harvester harvester) {
        this.harvester = harvester;
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public StreamingOutput htmlMetrics() {
        return new StreamingOutput() {

            @Override
            public void write(final OutputStream outputStream) throws IOException, WebApplicationException {
                final Stopwatch stopwatch = Stopwatch.createStarted();
                final Map<String, MetricFamily> metricFamilies = harvester.collect().collect(Collectors.toMap(mf -> mf.name, Function.identity(),
                        (a, b) -> { throw new IllegalStateException(); },
                        TreeMap::new
                ));
                stopwatch.stop();

                final Map<String, String> globalLabels = harvester.globalLabels();

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
            final Map<String, String> globalLabels = harvester.globalLabels();

            try (final PrometheusTextFormatWriter writer = new PrometheusTextFormatWriter(outputStream, Instant.now(), globalLabels)) {
                final Stopwatch stopwatch = Stopwatch.createStarted();

                harvester.collect().sequential().forEach(mf -> mf.accept(writer));

                {
                    final long collectionTimeNS = stopwatch.stop().elapsed(TimeUnit.NANOSECONDS);
                    final double collectionTimeSeconds = collectionTimeNS / (double) TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);

                    final GaugeMetricFamily collectionTimeGauge = new GaugeMetricFamily("cassandra_scrape_duration_seconds", "Time taken to collect Cassandra metrics",
                            ImmutableSet.of(new NumericMetric(new Labels(ImmutableMap.of()), collectionTimeSeconds))
                    );

                    collectionTimeGauge.accept(writer);
                }

                outputStream.flush();

            } catch (final Exception e) {
                e.printStackTrace();
            }
        };
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Object jsonMetrics(@QueryParam("help") final boolean includeHelp) {
        return harvester.collect().collect(Collectors.toMap(mf -> mf.name, Function.identity(),
                (a, b) -> { throw new IllegalStateException(); },
                TreeMap::new
        ));
    }
}
