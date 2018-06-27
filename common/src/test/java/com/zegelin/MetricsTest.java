package com.zegelin;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.cassandra.gms.FailureDetectorMBean;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class MetricsTest {

    public static void main(String[] args) throws IOException {

        final HttpServer httpServer = HttpServer.create();

        httpServer.bind(new InetSocketAddress("0.0.0.0", 8080), 0);

        httpServer.createContext("/metrics", new TestHandler());

        httpServer.start();
    }

    static class TestHandler implements HttpHandler {
        int requestCount = 0;

        @Override
        public void handle(final HttpExchange httpExchange) throws IOException {
            final OutputStream responseBody = httpExchange.getResponseBody();

            final Headers responseHeaders = httpExchange.getResponseHeaders();

            responseHeaders.set("Content-Type", "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited");

//            t.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
//            t.getResponseHeaders().set("Content-Length", String.valueOf(response.size()));


            httpExchange.sendResponseHeaders(200, 0);

            FailureDetectorMBean mBean = null;



//            final Metrics.MetricFamily.Builder metricFamilyBuilder = Metrics.MetricFamily.newBuilder();
//
//            metricFamilyBuilder.setName("test_metric");
//            metricFamilyBuilder.setType(Metrics.MetricType.GAUGE);
//
//            final Metrics.Metric.Builder metricBuilder = metricFamilyBuilder.addMetricBuilder();
//
//            metricBuilder.getGaugeBuilder().setValue(requestCount++);
//
//            final Metrics.MetricFamily metricFamily = metricFamilyBuilder.build();
//
////            try (PrintStream printStream = new PrintStream(responseBody)) {
////                TextFormat.print(metricFamily, printStream);
////            }
//
//            metricFamily.writeDelimitedTo(responseBody);

            responseBody.flush();

            httpExchange.close();
        }
    }
}
