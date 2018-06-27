package com.zegelin.prometheus.cassandra.collector;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;

import javax.management.ObjectName;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class GossiperMBeanMetricFamilyCollector implements MBeanGroupMetricFamilyCollector {
    private static final long NS_PER_S = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);

    private static final ObjectName GOSSIPER_MBEAN_NAME = ObjectNames.create("org.apache.cassandra.net:type=Gossiper");

    public static final Factory FACTORY = mBean -> {
        if (!GOSSIPER_MBEAN_NAME.apply(mBean.name))
            return null;

        // use Gossiper rather than GossiperMBean as the former allows access to the complete endpoint list
        final Gossiper gossiperMBean = (Gossiper) mBean.object;

        return new GossiperMBeanMetricFamilyCollector(gossiperMBean);
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
    public Stream<MetricFamily<?>> collect() {
        final Set<Map.Entry<InetAddress, EndpointState>> endpointStates = gossiper.getEndpointStates();

        final HashSet<NumericMetric> generationNumberMetrics = new HashSet<>(endpointStates.size());
        final HashSet<NumericMetric> downtimeMetrics = new HashSet<>(endpointStates.size());

        for (final Map.Entry<InetAddress, EndpointState> endpointState : endpointStates) {
            final InetAddress endpoint = endpointState.getKey();

            final Labels labels = new Labels(ImmutableMap.of("endpoint", InetAddresses.toAddrString(endpoint)));

            generationNumberMetrics.add(new NumericMetric(labels, gossiper.getCurrentGenerationNumber(endpoint)));
            downtimeMetrics.add(new NumericMetric(labels, gossiper.getEndpointDowntime(endpoint) / (double) NS_PER_S));
        }

        return Stream.of(
                new GaugeMetricFamily("cassandra_endpoint_generation", "Current endpoint generation number.", generationNumberMetrics),
                new GaugeMetricFamily("cassandra_endpoint_downtime_seconds", null, downtimeMetrics)
        );
    }
}
