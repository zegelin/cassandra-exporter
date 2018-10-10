package com.zegelin.prometheus.cassandra.collector;

import com.zegelin.prometheus.cassandra.MetadataFactory;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.NumericMetric;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.zegelin.prometheus.cassandra.CassandraObjectNames.GOSSIPER_MBEAN_NAME;
import static com.zegelin.prometheus.cassandra.MetricValueConversionFunctions.millisecondsToSeconds;

public class InternalGossiperMBeanMetricFamilyCollector extends GossiperMBeanMetricFamilyCollector {
    public static Factory factory(final MetadataFactory metadataFactory) {
        return mBean -> {
            if (!GOSSIPER_MBEAN_NAME.apply(mBean.name))
                return null;

            return new InternalGossiperMBeanMetricFamilyCollector((Gossiper) mBean.object, metadataFactory);
        };
    };


    private final Gossiper gossiper;
    private final MetadataFactory metadataFactory;

    private InternalGossiperMBeanMetricFamilyCollector(final Gossiper gossiper, final MetadataFactory metadataFactory) {
        this.gossiper = gossiper;
        this.metadataFactory = metadataFactory;
    }

    @Override
    protected void collect(final Stream.Builder<NumericMetric> generationNumberMetrics, final Stream.Builder<NumericMetric> downtimeMetrics, final Stream.Builder<NumericMetric> activeMetrics) {
        final Set<Map.Entry<InetAddress, EndpointState>> endpointStates = gossiper.getEndpointStates();

        for (final Map.Entry<InetAddress, EndpointState> endpointState : endpointStates) {
            final InetAddress endpoint = endpointState.getKey();
            final EndpointState state = endpointState.getValue();

            final Labels labels = metadataFactory.endpointLabels(endpoint);

            generationNumberMetrics.add(new NumericMetric(labels, gossiper.getCurrentGenerationNumber(endpoint)));
            downtimeMetrics.add(new NumericMetric(labels, millisecondsToSeconds(gossiper.getEndpointDowntime(endpoint))));
            activeMetrics.add(new NumericMetric(labels, state.isAlive() ? 1 : 0));
        }
    }
}
