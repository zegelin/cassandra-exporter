package com.zegelin.cassandra.exporter.collector;

import com.zegelin.cassandra.exporter.MBeanGroupMetricFamilyCollector;
import com.zegelin.cassandra.exporter.MetadataFactory;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;
import org.apache.cassandra.gms.FailureDetectorMBean;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.util.Collection;
import java.util.stream.Stream;

import static com.zegelin.cassandra.exporter.CassandraObjectNames.FAILURE_DETECTOR_MBEAN_NAME;

public class FailureDetectorMBeanMetricFamilyCollector extends MBeanGroupMetricFamilyCollector {
    public static Factory factory(final MetadataFactory metadataFactory) {
        return mBean -> {
            if (!FAILURE_DETECTOR_MBEAN_NAME.apply(mBean.name))
                return null;

            return new FailureDetectorMBeanMetricFamilyCollector((FailureDetectorMBean) mBean.object, metadataFactory);
        };
    };


    private final FailureDetectorMBean failureDetector;
    private final MetadataFactory metadataFactory;

    private FailureDetectorMBeanMetricFamilyCollector(final FailureDetectorMBean failureDetector, final MetadataFactory metadataFactory) {
        this.failureDetector = failureDetector;
        this.metadataFactory = metadataFactory;
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<MetricFamily> metricFamilyStreamBuilder = Stream.builder();

        // endpoint phi
        // annoyingly this info is only available as CompositeData
        try {
            @SuppressWarnings("unchecked")
            final Collection<CompositeData> endpointPhiValues = (Collection<CompositeData>) failureDetector.getPhiValues().values();

            final Stream<NumericMetric> phiMetricsStream = endpointPhiValues.stream().map(d -> {
                // the endpoint address is from InetAddress.toString() which returns "<host>/<address>"
                final String endpoint = ((String) d.get("Endpoint")).split("/")[1];
                final Labels labels = metadataFactory.endpointLabels(endpoint);

                return new NumericMetric(labels, ((Double) d.get("PHI")).floatValue());
            });

            metricFamilyStreamBuilder.add(new GaugeMetricFamily("cassandra_endpoint_phi", ":evel of suspicion that an endpoint might be down.", phiMetricsStream));

        } catch (final OpenDataException e) {
            throw new RuntimeException("Unable to collect metric cassandra_endpoint_phi.", e); // TODO: throw or log?
        }

        return metricFamilyStreamBuilder.build();
    }
}
