package com.zegelin.prometheus.cassandra.collector;

import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.util.Collection;
import java.util.stream.Stream;

public class FailureDetectorMBeanMetricFamilyCollector implements MBeanGroupMetricFamilyCollector {
    private static final Logger logger = LoggerFactory.getLogger(FailureDetectorMBeanMetricFamilyCollector.class);

    private static final ObjectName FAILURE_DETECTOR_MBEAN_NAME = ObjectNames.create("org.apache.cassandra.net:type=FailureDetector");

    public static final Factory FACTORY = mBean -> {
        if (!FAILURE_DETECTOR_MBEAN_NAME.apply(mBean.name))
            return null;

        final FailureDetectorMBean failureDetector = (FailureDetectorMBean) mBean.object;

        return new FailureDetectorMBeanMetricFamilyCollector(failureDetector);
    };


    private final FailureDetectorMBean failureDetector;

    private FailureDetectorMBeanMetricFamilyCollector(final FailureDetectorMBean failureDetector) {
        this.failureDetector = failureDetector;
    }

    @Override
    public String name() {
        return FAILURE_DETECTOR_MBEAN_NAME.toString();
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<MetricFamily> streamBuilder = Stream.builder();

        // endpoint phi
        try {
            @SuppressWarnings("unchecked")
            final Collection<CompositeData> endpointPhiValues = (Collection<CompositeData>) failureDetector.getPhiValues().values();

            final Stream<NumericMetric> phiMetricsStream = endpointPhiValues.stream().map(d -> {
                final Labels labels = Labels.of("endpoint", ((String) d.get("Endpoint")).substring(1));

                return new NumericMetric(labels, ((Double) d.get("PHI")).floatValue());
            });

            streamBuilder.add(new GaugeMetricFamily("cassandra_endpoint_phi", null, phiMetricsStream));

        } catch (final OpenDataException e) {
            logger.warn("Unable to collect metric cassandra_endpoint_phi.", e);
        }

        return streamBuilder.build();
    }
}
