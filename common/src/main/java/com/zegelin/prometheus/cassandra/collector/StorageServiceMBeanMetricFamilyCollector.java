package com.zegelin.prometheus.cassandra.collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector;
import com.zegelin.prometheus.domain.GaugeMetricFamily;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;
import com.zegelin.prometheus.domain.NumericMetric;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class StorageServiceMBeanMetricFamilyCollector implements MBeanGroupMetricFamilyCollector {
    private static final Logger logger = LoggerFactory.getLogger(StorageServiceMBeanMetricFamilyCollector.class);

    private static final ObjectName STORAGE_SERVICE_MBEAN_NAME = ObjectNames.create("org.apache.cassandra.db:type=StorageService");

    public static final MBeanGroupMetricFamilyCollector.Factory FACTORY = mBean -> {
        if (!STORAGE_SERVICE_MBEAN_NAME.apply(mBean.name))
            return null;

        final StorageServiceMBean storageServiceMBean = (StorageServiceMBean) mBean.object;

        return new StorageServiceMBeanMetricFamilyCollector(storageServiceMBean);
    };


    private final Map<Labels, FileStore> labeledFileStores;

    private StorageServiceMBeanMetricFamilyCollector(final StorageServiceMBean storageServiceMBean) {
        // determine the set of FileStores (i.e., mountpoints) for the Cassandra data/CL/cache directories
        final ImmutableList<String> directories = ImmutableList.<String>builder()
                .add(storageServiceMBean.getAllDataFileLocations())
                .add(storageServiceMBean.getCommitLogLocation())
                .add(storageServiceMBean.getSavedCachesLocation())
                .build();

        final Map<Labels, FileStore> labeledFileStores = new HashMap<>();

        for (final String directory : directories) {
            try {
                final FileStore fileStore = Files.getFileStore(Paths.get(directory));

                labeledFileStores.put(Labels.of("spec", fileStore.name()), fileStore);

            } catch (final IOException e) {
                logger.error("Failed to get FileStore for directory {}.", directory, e);
            }
        }

        this.labeledFileStores = ImmutableMap.copyOf(labeledFileStores);
    }

    @Override
    public String name() {
        return STORAGE_SERVICE_MBEAN_NAME.toString();
    }

    @Override
    public Stream<MetricFamily> collect() {
        final Stream.Builder<NumericMetric> fileStoreTotalSpaceMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> fileStoreUsableSpaceMetrics = Stream.builder();
        final Stream.Builder<NumericMetric> fileStoreUnallocatedSpaceMetrics = Stream.builder();

        for (final Map.Entry<Labels, FileStore> entry : labeledFileStores.entrySet()) {
            final Labels labels = entry.getKey();
            final FileStore fileStore = entry.getValue();

            try {
                fileStoreTotalSpaceMetrics.add(new NumericMetric(labels, fileStore.getTotalSpace()));
                fileStoreUsableSpaceMetrics.add(new NumericMetric(labels, fileStore.getUsableSpace()));
                fileStoreUnallocatedSpaceMetrics.add(new NumericMetric(labels, fileStore.getUnallocatedSpace()));

            } catch (final IOException e) {
                logger.warn("Failed to get FileStore {} consumption metrics.", fileStore, e);
            }
        }

        return Stream.of(
                new GaugeMetricFamily("cassandra_storage_filesystem_bytes_total", null, fileStoreTotalSpaceMetrics.build()),
                new GaugeMetricFamily("cassandra_storage_filesystem_usable_bytes", null, fileStoreUsableSpaceMetrics.build()),
                new GaugeMetricFamily("cassandra_storage_filesystem_unallocated_bytes", null, fileStoreUnallocatedSpaceMetrics.build())
        );
    }
}
