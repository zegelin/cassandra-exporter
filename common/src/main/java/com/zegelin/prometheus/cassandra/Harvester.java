package com.zegelin.prometheus.cassandra;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zegelin.jmx.NamedObject;
import com.zegelin.prometheus.cassandra.cli.HarvesterOptions;
import com.zegelin.prometheus.domain.CounterMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.zegelin.prometheus.cassandra.CassandraObjectNames.ENDPOINT_SNITCH_INFO_MBEAN_NAME;
import static com.zegelin.prometheus.cassandra.CassandraObjectNames.STORAGE_SERVICE_MBEAN_NAME;
import static com.zegelin.prometheus.cassandra.MetricValueConversionFunctions.nanosecondsToSeconds;

public abstract class Harvester {
    private static final Logger logger = LoggerFactory.getLogger(Harvester.class);

    public enum GlobalLabel implements LabelEnum {
        CLUSTER,
        HOST_ID,
        NODE,
        DATACENTER,
        RACK;

        @Override
        public String labelName() {
            return "cassandra_" + name().toLowerCase();
        }
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
            .put(ENDPOINT_SNITCH_INFO_MBEAN_NAME, (o) -> endpointSnitchInfo = (EndpointSnitchInfoMBean) o)
            .put(STORAGE_SERVICE_MBEAN_NAME, (o) -> storageService = (StorageServiceMBean) o)
            .build();

    private final CountDownLatch requiredMBeansLatch = new CountDownLatch(requiredMBeansRegistry.size());

    private final Set<Exclusion> exclusions;
    private final Set<GlobalLabel> enabledGlobalLabels;

    private final boolean collectorTimingEnabled;
    private final Map<String, Stopwatch> collectionTimes = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("cassandra-exporter-harvester-defer-%d")
            .setDaemon(true)
            .build());


    protected Harvester(final MetadataFactory metadataFactory, final HarvesterOptions options) {
        this.collectorFactories = new ArrayList<>(new FactoriesSupplier(metadataFactory, options).get());
        this.exclusions = options.exclusions;
        this.enabledGlobalLabels = options.globalLabels;
        this.collectorTimingEnabled = options.collectorTimingEnabled;
    }

    protected void addCollectorFactory(final MBeanGroupMetricFamilyCollector.Factory factory) {
        collectorFactories.add(factory);
    }


    private void defer(final Runnable runnable) {
        scheduledExecutorService.schedule(runnable, 1, TimeUnit.SECONDS);
    }


    protected void registerMBean(final Object mBean, final ObjectName name) {
        maybeRegisterRequiredMBean(mBean, name);

        if (isExcluded(name)) {
            return;
        }

        defer(() -> {
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

                } catch (final Exception e) {
                    logger.warn("Failed to register collector for MBean {}", name, e);
                }
            }
        });
    }

    protected void unregisterMBean(final ObjectName mBeanName) {
        final String collectorName = mBeanNameToCollectorNameMap.get(mBeanName);

        if (collectorName == null) {
            // no harvester registered
            return;
        }

        defer(() -> {
            mBeanCollectorsByName.compute(collectorName, (k, v) -> v.removeMBean(mBeanName));
        });
    }

    private void maybeRegisterRequiredMBean(final Object object, final ObjectName name) {
        for (final Map.Entry<ObjectName, Consumer<Object>> registryEntry : requiredMBeansRegistry.entrySet()) {
            if (registryEntry.getKey().apply(name)) {
                registryEntry.getValue().accept(object);

                requiredMBeansLatch.countDown();
            }
        }
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

    public Stream<MetricFamily> collect() {
        final Stream<MetricFamily> metricFamilies = mBeanCollectorsByName.entrySet().parallelStream().flatMap((e) -> {
            final Stopwatch stopwatch = (collectorTimingEnabled ?
                    collectionTimes.computeIfAbsent(e.getKey(), (k) -> Stopwatch.createUnstarted()) :
                    null);

            try {
                if (stopwatch != null) {
                    stopwatch.start();
                }

                final Stream<MetricFamily> metricFamilyStream = e.getValue().collect();

                if (collectorTimingEnabled) {
                    // call cache (collect sub-streams) and collect to time the actual collection
                    return metricFamilyStream.map(MetricFamily::cache).collect(Collectors.toList()).stream();

                } else {
                    return metricFamilyStream;
                }

            } catch (final Exception exception) {
                logger.warn("Metrics collector {} failed to collect. Skipping.", e.getKey(), exception);

                return Stream.empty();

            } finally {
                if (stopwatch != null) {
                    stopwatch.stop();
                }
            }
        });

        if (collectorTimingEnabled) {
            return Stream.concat(metricFamilies, collectTimings());

        } else {
            return metricFamilies;
        }
    }

    private Stream<MetricFamily> collectTimings() {
        final Stream<NumericMetric> timingMetrics = collectionTimes.entrySet().stream()
                .map(e -> new Object() {
                    final String metricFamilyName = e.getKey();
                    final long cumulativeCollectionTime = e.getValue().elapsed(TimeUnit.NANOSECONDS);
                })
                .map(e -> new NumericMetric(Labels.of("collector", e.metricFamilyName), nanosecondsToSeconds(e.cumulativeCollectionTime)));

        return Stream.of(
                new CounterMetricFamily("cassandra_exporter_collection_time_seconds_total", "Cumulative time taken to run each metrics collector.", timingMetrics)
        );
    }

    public Labels globalLabels() {
        // TODO: memoize the result of this function

        try {
            requiredMBeansLatch.await();

        } catch (final InterruptedException e) {
            logger.warn("Interrupted while waiting for required MBeans to be registered.", e);

            return new Labels(ImmutableMap.of());
        }

        final String hostId = storageService.getLocalHostId();
        final String endpoint = storageService.getHostIdToEndpoint().get(hostId);

        final ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();

        LabelEnum.addIfEnabled(GlobalLabel.CLUSTER, enabledGlobalLabels, mapBuilder, storageService::getClusterName);
        LabelEnum.addIfEnabled(GlobalLabel.HOST_ID, enabledGlobalLabels, mapBuilder, () -> hostId);
        LabelEnum.addIfEnabled(GlobalLabel.NODE, enabledGlobalLabels, mapBuilder, () -> endpoint);
        LabelEnum.addIfEnabled(GlobalLabel.DATACENTER, enabledGlobalLabels, mapBuilder, endpointSnitchInfo::getDatacenter);
        LabelEnum.addIfEnabled(GlobalLabel.RACK, enabledGlobalLabels, mapBuilder, endpointSnitchInfo::getRack);

        return new Labels(mapBuilder.build());
    }
}
