package com.zegelin.prometheus.cassandra.collector;

import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.MetadataFactory;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.NumericMetric;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.gms.GossiperMBean;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.stream.Stream;

import static com.zegelin.prometheus.cassandra.CassandraObjectNames.FAILURE_DETECTOR_MBEAN_NAME;
import static com.zegelin.prometheus.cassandra.CassandraObjectNames.GOSSIPER_MBEAN_NAME;
import static com.zegelin.prometheus.cassandra.MetricValueConversionFunctions.millisecondsToSeconds;

public class RemoteGossiperMBeanMetricFamilyCollector extends GossiperMBeanMetricFamilyCollector {
    public static MBeanGroupMetricFamilyCollector.Factory factory(final MetadataFactory metadataFactory) {
        return mBean -> {
            if (GOSSIPER_MBEAN_NAME.apply(mBean.name)) {
                return new RemoteGossiperMBeanMetricFamilyCollector(metadataFactory, (GossiperMBean) mBean.object, null);
            }

            if (FAILURE_DETECTOR_MBEAN_NAME.apply(mBean.name)) {
                return new RemoteGossiperMBeanMetricFamilyCollector(metadataFactory, null, (FailureDetectorMBean) mBean.object);
            }

            return null;
        };
    }

    private final MetadataFactory metadataFactory;
    private final GossiperMBean gossiperMBean;
    private final FailureDetectorMBean failureDetectorMBean;

    private RemoteGossiperMBeanMetricFamilyCollector(final MetadataFactory metadataFactory, final GossiperMBean gossiperMBean, final FailureDetectorMBean failureDetectorMBean) {
        this.metadataFactory = metadataFactory;
        this.gossiperMBean = gossiperMBean;
        this.failureDetectorMBean = failureDetectorMBean;
    }

    @Override
    public MBeanGroupMetricFamilyCollector merge(final MBeanGroupMetricFamilyCollector rawOther) {
        if (!(rawOther instanceof RemoteGossiperMBeanMetricFamilyCollector)) {
            throw new IllegalStateException();
        }

        final RemoteGossiperMBeanMetricFamilyCollector other = (RemoteGossiperMBeanMetricFamilyCollector) rawOther;

        return new RemoteGossiperMBeanMetricFamilyCollector(
                this.metadataFactory,
                this.gossiperMBean != null ? this.gossiperMBean : other.gossiperMBean,
                this.failureDetectorMBean != null ? this.failureDetectorMBean : other.failureDetectorMBean
        );
    }

    @Override
    protected void collect(final Stream.Builder<NumericMetric> generationNumberMetrics, final Stream.Builder<NumericMetric> downtimeMetrics, final Stream.Builder<NumericMetric> activeMetrics) {
        if (failureDetectorMBean == null || gossiperMBean == null) {
            return;
        }

        for (final Map.Entry<String, String> entry : failureDetectorMBean.getSimpleStates().entrySet()) {
            // annoyingly getSimpleStates uses InetAddress.toString() which returns "<host>/<address>"
            // yet getCurrentGenerationNumber, etc, all take IP address strings (and internally call InetAddress.getByName(...))

            final String endpoint = entry.getKey().split("/")[1];
            final String state = entry.getValue();

            final Labels labels = metadataFactory.endpointLabels(endpoint);

            try {
                generationNumberMetrics.add(new NumericMetric(labels, gossiperMBean.getCurrentGenerationNumber(endpoint)));
                downtimeMetrics.add(new NumericMetric(labels, millisecondsToSeconds(gossiperMBean.getEndpointDowntime(endpoint))));

            } catch (final UnknownHostException e) {
                throw new RuntimeException("Failed to collect gossip metrics.", e); // TODO: exception or log?
            }

            activeMetrics.add(new NumericMetric(labels, state.equalsIgnoreCase("UP") ? 1 : 0));
        }
    }
}
