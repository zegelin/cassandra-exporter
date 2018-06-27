package com.zegelin.prometheus.cassandra;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.service.StorageServiceMBean;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class JMXHarvester extends BaseHarvester {
    private static final Logger logger = LoggerFactory.getLogger(JMXHarvester.class);

    private final MBeanServerConnection mBeanServerConnection;

    @SuppressWarnings("FieldCanBeLocal")
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public JMXHarvester(final MBeanServerConnection mBeanServerConnection) {
        this.mBeanServerConnection = mBeanServerConnection;

        // periodically scan for new/destroyed MBeans
        scheduledExecutorService.scheduleWithFixedDelay(this::reconcileMBeans, 0, 30, TimeUnit.SECONDS);
    }

    private Set<ObjectInstance> currentMBeans = ImmutableSet.of();

    private void reconcileMBeans() {
        try {
            final Set<ObjectInstance> mBeans = mBeanServerConnection.queryMBeans(null, null);

            // unregister
            {
                final Set<ObjectInstance> removedMBeans = Sets.difference(currentMBeans, mBeans);

                for (final ObjectInstance instance : removedMBeans) {
                    unregisterMBean(instance.getObjectName());
                }
            }

            // register
            {
                final Set<ObjectInstance> addedMBeans = Sets.difference(mBeans, currentMBeans);

                for (final ObjectInstance instance : addedMBeans) {
                    final Class interfaceClass = MBEAN_INTERFACES.get(instance.getClassName());

                    if (interfaceClass == null) {
                        logger.debug("Cannot register MBean {}. Unrecognised class.", instance);
                        continue;
                    }

                    final ObjectName objectName = instance.getObjectName();
                    final Object mBeanProxy = JMX.newMBeanProxy(mBeanServerConnection, objectName, interfaceClass);

                    registerMBean(mBeanProxy, objectName);
                }
            }

            currentMBeans = mBeans;

        } catch (final IOException e) {
            logger.error("Failed to reconcile MBeans.", e);
        }
    }

    private static final Map<String, Class> MBEAN_INTERFACES = ImmutableMap.<String, Class>builder()
        .put("org.apache.cassandra.metrics.CassandraMetricsRegistry$JmxGauge", CassandraMetricsRegistry.JmxGaugeMBean.class)
        .put("org.apache.cassandra.metrics.CassandraMetricsRegistry$JmxCounter", CassandraMetricsRegistry.JmxCounterMBean.class)
        .put("org.apache.cassandra.metrics.CassandraMetricsRegistry$JmxMeter", CassandraMetricsRegistry.JmxMeterMBean.class)
        .put("org.apache.cassandra.metrics.CassandraMetricsRegistry$JmxHistogram", CassandraMetricsRegistry.JmxHistogramMBean.class)
        .put("org.apache.cassandra.metrics.CassandraMetricsRegistry$JmxTimer", CassandraMetricsRegistry.JmxTimerMBean.class)

        .put("org.apache.cassandra.gms.FailureDetector", FailureDetectorMBean.class)
        .put("org.apache.cassandra.locator.EndpointSnitchInfo", EndpointSnitchInfoMBean.class)
        .put("org.apache.cassandra.service.StorageService", StorageServiceMBean.class)
        .build();
}
