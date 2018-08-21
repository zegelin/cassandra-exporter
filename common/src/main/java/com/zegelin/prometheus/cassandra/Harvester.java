package com.zegelin.prometheus.cassandra;

import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.NamedObject;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class Harvester {
    private static final Logger logger = LoggerFactory.getLogger(Harvester.class);

    public enum GlobalLabel {
        CLUSTER_NAME,
        HOST_ID,
        NODE,
        DATACENTER,
        RACK;
    }

    public static abstract class Exclusion {
        boolean excluded(final ObjectName name) {
            return false;
        }

        boolean excluded(final MBeanGroupMetricFamilyCollector collector) {
            return false;
        }

        @Override
        public abstract int hashCode();

        @Override
        public abstract boolean equals(final Object obj);

        public static Exclusion create(final String value) {
            try {
                return new MBeanExclusion(ObjectName.getInstance(value));

            } catch (final MalformedObjectNameException e) {

                return new CollectorExclusion(value);
            }
        }
    }

    private static class MBeanExclusion extends Exclusion {
        private final ObjectName objectNameOrPattern;

        private MBeanExclusion(final ObjectName objectNameOrPattern) {
            this.objectNameOrPattern = objectNameOrPattern;
        }

        @Override
        boolean excluded(final ObjectName name) {
            return objectNameOrPattern.apply(name);
        }

        @Override
        public int hashCode() {
            return objectNameOrPattern.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final MBeanExclusion that = (MBeanExclusion) o;
            return Objects.equals(objectNameOrPattern, that.objectNameOrPattern);
        }
    }

    private static class CollectorExclusion extends Exclusion {
        private final String collectorName;

        private CollectorExclusion(final String collectorName) {
            this.collectorName = collectorName;
        }

        @Override
        boolean excluded(final MBeanGroupMetricFamilyCollector collector) {
            return collectorName.equals(collector.name());
        }

        @Override
        public int hashCode() {
            return collectorName.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final CollectorExclusion that = (CollectorExclusion) o;
            return Objects.equals(collectorName, that.collectorName);
        }
    }

    private final List<MBeanGroupMetricFamilyCollector.Factory> collectorFactories;

    private final Map<String, MBeanGroupMetricFamilyCollector> mBeanCollectorsByName = Collections.synchronizedMap(new HashMap<>());
    private final Map<ObjectName, String> mBeanNameToCollectorNameMap = Collections.synchronizedMap(new HashMap<>());

    private StorageServiceMBean storageService;
    private EndpointSnitchInfoMBean endpointSnitchInfo;

    private final Map<ObjectName, Consumer<Object>> requiredMBeansRegistry = ImmutableMap.<ObjectName, Consumer<Object>>builder()
            .put(ObjectNames.create("org.apache.cassandra.db:type=EndpointSnitchInfo"), (o) -> endpointSnitchInfo = (EndpointSnitchInfoMBean) o)
            .put(ObjectNames.create("org.apache.cassandra.db:type=StorageService"), (o) -> storageService = (StorageServiceMBean) o)
            .build();

    private final CountDownLatch requiredMBeansLatch = new CountDownLatch(requiredMBeansRegistry.size());

    private final Set<Exclusion> exclusions;
    private final Set<GlobalLabel> enabledGlobalLabels;


    protected Harvester(final Supplier<List<MBeanGroupMetricFamilyCollector.Factory>> factoriesSupplier, final Set<Exclusion> exclusions, final Set<GlobalLabel> enabledGlobalLabels) {
        this.collectorFactories = factoriesSupplier.get();
        this.exclusions = exclusions;
        this.enabledGlobalLabels = enabledGlobalLabels;
    }


    protected void registerMBean(final Object mBean, final ObjectName name) {
        maybeRegisterRequiredMBean(mBean, name);

        if (isExcluded(name)) {
            return;
        }

        final NamedObject<Object> namedMBean = new NamedObject<>(name, mBean);

        for (final MBeanGroupMetricFamilyCollector.Factory factory : collectorFactories) {
            try {
                final MBeanGroupMetricFamilyCollector collector = factory.createCollector(namedMBean);

                if (collector == null) {
                    continue;
                }

                if (isExcluded(collector)) {
                    continue;
                }

                mBeanCollectorsByName.merge(collector.name(), collector, MBeanGroupMetricFamilyCollector::merge);
                mBeanNameToCollectorNameMap.put(name, collector.name());

                break;

            } catch (final Exception e) {
                logger.warn("Failed to register collector for MBean {}", name, e);
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

    private boolean isExcluded(final ObjectName objectName) {
        for (final Exclusion exclusion : exclusions) {
            if (exclusion.excluded(objectName))
                return true;
        }

        return false;
    }

    private boolean isExcluded(final MBeanGroupMetricFamilyCollector collector) {
        for (final Exclusion exclusion : exclusions) {
            if (exclusion.excluded(collector))
                return true;
        }

        return false;
    }

    public Stream<MetricFamily<?>> collect() {
        return mBeanCollectorsByName.entrySet().parallelStream().flatMap((e) -> {
            try {
                return e.getValue().collect();

            } catch (final Exception exception) {
                logger.warn("Metrics collector {} failed to collect. Skipping.", e.getKey(), exception);

                return Stream.empty();
            }
        });
    }

    public Labels globalLabels() {
        try {
            requiredMBeansLatch.await();

        } catch (final InterruptedException e) {
            logger.warn("Interrupted while waiting for required MBeans to be registered.", e);

            return new Labels(ImmutableMap.of());
        }

        final String hostId = storageService.getLocalHostId();
        final String endpoint = storageService.getHostIdToEndpoint().get(hostId);

        final ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();

        for (final GlobalLabel label : enabledGlobalLabels) {
            switch (label) {
                case CLUSTER_NAME:
                    mapBuilder.put("cassandra_cluster_name", storageService.getClusterName());
                    break;

                case HOST_ID:
                    mapBuilder.put("cassandra_host_id", hostId);
                    break;

                case NODE:
                    mapBuilder.put("cassandra_node", endpoint);
                    break;

                case DATACENTER:
                    mapBuilder.put("cassandra_datacenter", endpointSnitchInfo.getDatacenter());
                    break;

                case RACK:
                    mapBuilder.put("cassandra_rack", endpointSnitchInfo.getRack());
                    break;

                default:
                    throw new IllegalStateException();
            }
        }

        return new Labels(mapBuilder.build());
    }
}
