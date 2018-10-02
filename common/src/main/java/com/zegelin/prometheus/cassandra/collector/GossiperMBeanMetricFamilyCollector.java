package com.zegelin.prometheus.cassandra.collector;

import com.google.common.net.InetAddresses;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.*;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class GossiperMBeanMetricFamilyCollector implements MBeanGroupMetricFamilyCollector {
    private static final Logger logger = LoggerFactory.getLogger(GossiperMBeanMetricFamilyCollector.class);

    private static final long NS_PER_S = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);

    private static final ObjectName GOSSIPER_MBEAN_NAME = ObjectNames.create("org.apache.cassandra.net:type=Gossiper");

    public static final Factory FACTORY = mBean -> {
        if (!GOSSIPER_MBEAN_NAME.apply(mBean.name))
            return null;

        try {
            final Gossiper gossiper = (Gossiper) mBean.object;

            return new GossiperMBeanMetricFamilyCollector(gossiper);

        } catch (final NoClassDefFoundError e) {
            logger.warn("Failed to instantiate GossiperMBeanMetricFamilyCollector. Gossiper metrics will not be available. This is expected in stand-alone mode.", e);

            return null;
        }
    };


    private final Gossiper gossiper;

    private GossiperMBeanMetricFamilyCollector(final Gossiper gossiper) {
        this.gossiper = gossiper;
    }

    @Override
    public String name() {
        return GOSSIPER_MBEAN_NAME.toString();
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Set<Map.Entry<InetAddress, EndpointState>> endpointStates = gossiper.getEndpointStates();

        final List<NumericMetric> generationNumberMetrics = new ArrayList<>(endpointStates.size());
        final List<NumericMetric> downtimeMetrics = new ArrayList<>(endpointStates.size());
        final List<NumericMetric> activeMetrics = new ArrayList<>(endpointStates.size());

        for (final Map.Entry<InetAddress, EndpointState> endpointState : endpointStates) {
            final InetAddress endpoint = endpointState.getKey();
            final EndpointState state = endpointState.getValue();

            final Labels labels = Labels.of("endpoint", InetAddresses.toAddrString(endpoint));

            generationNumberMetrics.add(new NumericMetric(labels, gossiper.getCurrentGenerationNumber(endpoint)));
            downtimeMetrics.add(new NumericMetric(labels, gossiper.getEndpointDowntime(endpoint) / (float) NS_PER_S));
            activeMetrics.add(new NumericMetric(labels, state.isAlive() ? 1 : 0));
        }

        return Stream.of(
                new GaugeMetricFamily("cassandra_endpoint_generation", "Current endpoint generation number.", generationNumberMetrics.stream()),
                new CounterMetricFamily("cassandra_endpoint_downtime_total_seconds", "Endpoint downtime (since server start).", downtimeMetrics.stream()),
                new GaugeMetricFamily("cassandra_endpoint_active", "Endpoint activeness boolean (0 = down, 1 = up).", activeMetrics.stream())
        );
    }
}
