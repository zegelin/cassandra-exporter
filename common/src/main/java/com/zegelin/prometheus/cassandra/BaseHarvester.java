package com.zegelin.prometheus.cassandra;

import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.NamedObject;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.domain.MetricFamily;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.zegelin.prometheus.cassandra.Factories.MBEAN_METRIC_FAMILY_COLLECTOR_FACTORIES;

public abstract class BaseHarvester implements Harvester {
    private static final Logger logger = LoggerFactory.getLogger(BaseHarvester.class);

    private final Map<String, MBeanGroupMetricFamilyCollector> mBeanCollectorsByName = Collections.synchronizedMap(new HashMap<>());
    private final Map<ObjectName, String> mBeanNameToCollectorNameMap = Collections.synchronizedMap(new HashMap<>());

    private StorageServiceMBean storageService;
    private EndpointSnitchInfoMBean endpointSnitchInfo;

    private final Map<ObjectName, Consumer<Object>> requiredMBeansRegistry = ImmutableMap.<ObjectName, Consumer<Object>>builder()
            .put(ObjectNames.create("org.apache.cassandra.db:type=EndpointSnitchInfo"), (o) -> endpointSnitchInfo = (EndpointSnitchInfoMBean) o)
            .put(ObjectNames.create("org.apache.cassandra.db:type=StorageService"), (o) -> storageService = (StorageServiceMBean) o)
            .build();

    private final CountDownLatch requiredMBeansLatch = new CountDownLatch(requiredMBeansRegistry.size());


    protected void registerMBean(final Object mBean, final ObjectName name) {
        maybeRegisterRequiredMBean(mBean, name);

        final NamedObject<Object> namedMBean = new NamedObject<>(name, mBean);

        for (final MBeanGroupMetricFamilyCollector.Factory factory : MBEAN_METRIC_FAMILY_COLLECTOR_FACTORIES) {
            try {
                final MBeanGroupMetricFamilyCollector collector = factory.createCollector(namedMBean);

                if (collector == null)
                    continue;

                mBeanCollectorsByName.merge(collector.name(), collector, MBeanGroupMetricFamilyCollector::merge);
                mBeanNameToCollectorNameMap.put(name, collector.name());

                break;

            } catch (final Exception e) {
                logger.warn("Failed to register harvester for MBean {}", name, e);
            }
        }
    }

    private void maybeRegisterRequiredMBean(final Object object, final ObjectName name) {
        for (final Map.Entry<ObjectName, Consumer<Object>> registryEntry : requiredMBeansRegistry.entrySet()) {
            if (registryEntry.getKey().apply(name)) {
                registryEntry.getValue().accept(object);

                requiredMBeansLatch.countDown();
            }
        }
    }

    protected void unregisterMBean(final ObjectName mBeanName) {
        final String collectorName = mBeanNameToCollectorNameMap.get(mBeanName);

        if (collectorName == null) {
            // no harvester registered
            return;
        }

        mBeanCollectorsByName.compute(collectorName, (k, v) -> v.removeMBean(mBeanName));
    }

    public Stream<MetricFamily> collect() {
        return mBeanCollectorsByName.entrySet().parallelStream().flatMap((e) -> {
            try {
                return e.getValue().collect();

            } catch (final Exception exception) {
                logger.warn("Metrics harvester {} failed to collect. Skipping.", e.getKey(), exception);

                return Stream.empty();
            }
        });
    }

    public Map<String, String> globalLabels() {
        try {
            requiredMBeansLatch.await();

        } catch (final InterruptedException e) {
            logger.warn("Interrupted while waiting for required MBeans to be registered.", e);

            return ImmutableMap.of();
        }

        final String hostId = storageService.getLocalHostId();
        final String endpoint = storageService.getHostIdToEndpoint().get(hostId);

        return ImmutableMap.<String, String>builder()
                .put("cassandra_cluster_name", storageService.getClusterName())
                .put("cassandra_host_id", hostId)
                .put("cassandra_node", endpoint)
                .put("cassandra_datacenter", endpointSnitchInfo.getDatacenter())
                .put("cassandra_rack", endpointSnitchInfo.getRack())
                .build();
    }
}
