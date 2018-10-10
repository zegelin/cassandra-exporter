package com.zegelin.prometheus.cassandra;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.NamedObject;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector.Factory;
import com.zegelin.prometheus.cassandra.cli.HarvesterOptions;
import com.zegelin.prometheus.cassandra.collector.CachingCollector;
import com.zegelin.prometheus.cassandra.collector.FailureDetectorMBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.collector.LatencyMetricGroupSummaryCollector;
import com.zegelin.prometheus.cassandra.collector.StorageServiceMBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.collector.dynamic.FunctionalMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.collector.jvm.*;
import com.zegelin.prometheus.domain.Labels;
import com.zegelin.prometheus.domain.MetricFamily;

import javax.management.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.zegelin.jmx.ObjectNames.format;
import static com.zegelin.prometheus.cassandra.CollectorFunctions.*;
import static com.zegelin.prometheus.cassandra.collector.CachingCollector.cache;

@SuppressWarnings("SameParameterValue")
public class FactoriesSupplier implements Supplier<List<Factory>> {
    /**
     * A builder of {@see MBeanGroupMetricFamilyCollector.Factory}s
     */
    private static class FactoryBuilder {
        private final CollectorConstructor collectorConstructor;
        private final QueryExp objectNameQuery;
        private final String metricFamilyName;

        private String help;

        interface LabelMaker extends Function<Map<String, String>, Map<String, String>> {
            @Override
            Map<String, String> apply(final Map<String, String> keyPropertyList);
        }

        private final List<LabelMaker> labelMakers = new LinkedList<>();


        FactoryBuilder(final CollectorConstructor collectorConstructor, final QueryExp objectNameQuery, final String metricFamilyName) {
            this.collectorConstructor = collectorConstructor;
            this.objectNameQuery = objectNameQuery;
            this.metricFamilyName = metricFamilyName;
        }

        FactoryBuilder withLabelMaker(final LabelMaker labelMaker) {
            labelMakers.add(labelMaker);

            return this;
        }

        FactoryBuilder withHelp(final String help) {
            this.help = help;

            return this;
        }

        Factory build() {
            return mBean -> {
                try {
                    if (!objectNameQuery.apply(mBean.name))
                        return null;
                } catch (final BadStringOperationException | BadBinaryOpValueExpException | BadAttributeValueExpException | InvalidApplicationException e) {
                    throw new IllegalStateException("Failed to apply object name query to object name.", e);
                }

                final Map<String, String> keyPropertyList = mBean.name.getKeyPropertyList();

                final Map<String, String> rawLabels = new HashMap<>();
                {
                    for (final LabelMaker labelMaker : labelMakers) {
                        rawLabels.putAll(labelMaker.apply(keyPropertyList));
                    }
                }

                final String name = String.format("cassandra_%s", metricFamilyName);

                return collectorConstructor.groupCollectorForMBean(name, help, new Labels(rawLabels), mBean);
            };
        }

        @FunctionalInterface
        public interface CollectorConstructor {
            MBeanGroupMetricFamilyCollector groupCollectorForMBean(final String name, final String help, final Labels labels, final NamedObject<?> mBean);
        }
    }

    private final MetadataFactory metadataFactory;
    private final boolean perThreadTimingEnabled;
    private final Set<TableLabels> tableLabels;

    public FactoriesSupplier(final MetadataFactory metadataFactory, final HarvesterOptions options) {
        this.metadataFactory = metadataFactory;
        this.perThreadTimingEnabled = options.perThreadTimingEnabled;
        this.tableLabels = options.tableLabels;
    }


    private Factory bufferPoolMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=BufferPool,name=%s", jmxName);
        final String metricFamilyName = String.format("buffer_pool_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }


    private Factory cqlMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        return cqlMetricFactory(collectorConstructor, jmxName, familyNameSuffix, help, ImmutableMap.of());
    }

    private Factory cqlMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help, final Map<String, String> labels) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=CQL,name=%s", jmxName);
        final String metricFamilyName = String.format("cql_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> labels)
                .build();
    }


    private Factory cacheMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Cache,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("cache_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> ImmutableMap.of(
                        "cache", keyPropertyList.get("scope").replaceAll("Cache", "").toLowerCase()
                ))
                .build();
    }


    private Factory clientMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Client,name=%s", jmxName);
        final String metricFamilyName = String.format("client_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private Factory clientRequestMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=ClientRequest,name=%s,scope=*-*", jmxName);
        final String metricFamilyName = String.format("client_request_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> {
                    final String scope = keyPropertyList.get("scope");

                    final Pattern scopePattern = Pattern.compile("(?<operation>.*?)(-(?<consistency>.*))?");
                    final Matcher matcher = scopePattern.matcher(scope);

                    if (!matcher.matches())
                        throw new IllegalStateException();

                    final String operation = matcher.group("operation").toLowerCase();
                    final String consistency = matcher.group("consistency");

                    return ImmutableMap.of(
                            "operation", operation,
                            "consistency", consistency
                    );
                })
                .build();
    }

    private Factory commitLogMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=CommitLog,name=%s", jmxName);
        final String metricFamilyName = String.format("commit_log_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private Factory messagingMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Messaging,name=%s", jmxName);
        final String metricFamilyName = String.format("messaging_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> {
                    final String name = keyPropertyList.get("name");

                    final Pattern namePattern = Pattern.compile("(?<datacenter>.*?)-Latency");
                    final Matcher matcher = namePattern.matcher(name);

                    if (!matcher.matches())
                        throw new IllegalStateException();

                    return ImmutableMap.of("datacenter", matcher.group("datacenter"));
                })
                .build();
    }

    private Factory memtablePoolMetricsFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=MemtablePool,name=%s", jmxName);
        final String metricFamilyName = String.format("memtable_pool_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private Factory storageMetric(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Storage,name=%s", jmxName);
        final String metricFamilyName = String.format("storage_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    public enum TableLabels implements LabelEnum {
        TABLE_TYPE,
        INDEX_TYPE,
        INDEX_CLASS,
        COMPACTION_STRATEGY_CLASS;

        @Override
        public String labelName() {
            return name().toLowerCase();
        }
    }

    private Factory tableMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        return tableMetricFactory(collectorConstructor, jmxName, familyNameSuffix, help, ImmutableMap.of());
    }

    private Factory tableCompactionMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        return tableMetricFactory(collectorConstructor, jmxName, familyNameSuffix, help, true, ImmutableMap.of());
    }

    private Factory tableMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help, final Map<String, String> extraLabels) {
        return tableMetricFactory(collectorConstructor, jmxName, familyNameSuffix, help, false, extraLabels);
    }

    private Factory tableMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help, final boolean includeCompactionLabels, final Map<String, String> extraLabels) {
        final QueryExp objectNameQuery = Query.or(
                format("org.apache.cassandra.metrics:type=Table,keyspace=*,scope=*,name=%s", jmxName),
                format("org.apache.cassandra.metrics:type=IndexTable,keyspace=*,scope=*,name=%s", jmxName)
        );

        final String metricFamilyName = String.format("table_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNameQuery, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> {
                    final String keyspaceName = keyPropertyList.get("keyspace");
                    final String tableName, indexName;
                    {
                        final String[] nameParts = keyPropertyList.get("scope").split("\\.");

                        tableName = nameParts[0];
                        indexName = (nameParts.length > 1) ? nameParts[1] : null;
                    }

                    final ImmutableMap.Builder<String, String> labelsBuilder = ImmutableMap.<String, String>builder()
                            .putAll(extraLabels)
                            .put("keyspace", keyspaceName);

                    if (indexName != null) {
                        labelsBuilder.put("table", tableName)
                                .put("index", indexName);

                        LabelEnum.addIfEnabled(TableLabels.TABLE_TYPE, tableLabels, labelsBuilder, () -> "index");

                        final Optional<MetadataFactory.IndexMetadata> indexMetadata = metadataFactory.indexMetadata(keyspaceName, tableName, indexName);

                        indexMetadata.ifPresent(m -> {
                            LabelEnum.addIfEnabled(TableLabels.INDEX_TYPE, tableLabels, labelsBuilder, () -> m.indexType().name().toLowerCase());
                            m.customClassName().ifPresent(s -> LabelEnum.addIfEnabled(TableLabels.INDEX_CLASS, tableLabels, labelsBuilder, () -> s));
                        });

                    } else {
                        labelsBuilder.put("table", tableName);

                        final Optional<MetadataFactory.TableMetadata> tableMetadata = metadataFactory.tableOrViewMetadata(keyspaceName, tableName);

                        tableMetadata.ifPresent(m -> {
                            LabelEnum.addIfEnabled(TableLabels.TABLE_TYPE, tableLabels, labelsBuilder, () -> m.isView() ? "view" : "table");

                            if (includeCompactionLabels) {
                                LabelEnum.addIfEnabled(TableLabels.COMPACTION_STRATEGY_CLASS, tableLabels, labelsBuilder, m::compactionStrategyClassName);
                            }
                        });
                    }

                    return labelsBuilder.build();
                })
                .build();
    }

    private Factory threadPoolMetric(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=ThreadPools,path=*,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("thread_pool_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> ImmutableMap.of(
                        "group", keyPropertyList.get("path"),
                        "pool", keyPropertyList.get("scope")
                ))
                .build();
    }

    private Factory rowIndexMetric(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Index,scope=RowIndexEntry,name=%s", jmxName);
        final String metricFamilyName = String.format("row_index_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName).build();
    }

    private Factory droppedMessagesMetric(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=DroppedMessage,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("dropped_messages_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> ImmutableMap.of("message_type", keyPropertyList.get("scope")))
                .build();
    }

    private Factory compactionMetric(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Compaction,name=%s", jmxName);
        final String metricFamilyName = String.format("compaction_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private Factory connectionMetric(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Connection,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("endpoint_connection_%s", familyNameSuffix);

        return new FactoryBuilder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> {
                    final HashMap<String, String> labels = new HashMap<>();

                    {
                        final String endpoint = keyPropertyList.get("scope"); // IP address of other node
                        labels.putAll(metadataFactory.endpointLabels(endpoint));
                    }

                    labels.computeIfAbsent("task_type", k -> {
                        final String name = keyPropertyList.get("name");
                        final Pattern namePattern = Pattern.compile("(?<type>.*)Message.*Tasks");

                        final Matcher matcher = namePattern.matcher(name);

                        if (!matcher.matches())
                            return null;

                        return matcher.group("type").toLowerCase();
                    });

                    return labels;
                })
                .build();
    }

    // TODO: these two functions can possibly be combined further
    private static FactoryBuilder.CollectorConstructor timerAsSummaryCollectorConstructor() {
        return (name, help, labels, mBean) -> {
            final NamedObject<SamplingCounting> samplingCountingNamedObject = CassandraMetricsUtilities.jmxTimerMBeanAsSamplingCounting(mBean);

            return new FunctionalMetricFamilyCollector<>(name, help, ImmutableMap.of(labels, samplingCountingNamedObject), samplingAndCountingAsSummary(MetricValueConversionFunctions::microsecondsToSeconds));
        };
    }

    private static FactoryBuilder.CollectorConstructor histogramAsSummaryCollectorConstructor() {
        return (name, help, labels, mBean) -> {
            final NamedObject<SamplingCounting> samplingCountingNamedObject = CassandraMetricsUtilities.jmxHistogramAsSamplingCounting(mBean);

            return new FunctionalMetricFamilyCollector<>(name, help, ImmutableMap.of(labels, samplingCountingNamedObject), samplingAndCountingAsSummary());
        };
    }


    private static <T> FactoryBuilder.CollectorConstructor functionalCollector(final FunctionalMetricFamilyCollector.CollectorFunction<T> function) {
        return (final String name, final String help, final Labels labels, final NamedObject<?> mBean) ->
                new FunctionalMetricFamilyCollector<>(name, help, ImmutableMap.of(labels, mBean.<T>cast()), function);
    }


//    private static <T> FunctionalMetricFamilyCollector.CollectorFunction<T> cache(final FunctionalMetricFamilyCollector.CollectorFunction<T> fn, final long duration, final TimeUnit unit) {
//        final LoadingCache<FunctionalMetricFamilyCollector.LabeledObjectGroup<T>, List<MetricFamily>> cache = CacheBuilder.newBuilder()
//                .expireAfterWrite(duration, unit)
//                .build(new CacheLoader<FunctionalMetricFamilyCollector.LabeledObjectGroup<T>, List<MetricFamily>>() {
//                    @Override
//                    public List<MetricFamily> load(final FunctionalMetricFamilyCollector.LabeledObjectGroup<T> key) throws Exception {
//                        return fn.apply(key).map(MetricFamily::cache).collect(Collectors.toList()); // store Stream as a List, since Streams can't be replayed
//                    }
//                });
//
//        return labeledObjectGroup -> cache.getUnchecked(labeledObjectGroup).stream();
//    }

//    private static <T> Factory cache(final Factory cache) {
//        return mBean -> {
//            final MBeanGroupMetricFamilyCollector collector = cache.createCollector(mBean);
//
//            if (collector == null) {
//                return null;
//            }
//
//            return new MBeanGroupMetricFamilyCollector() {
//
//                @Override
//                public Stream<MetricFamily> collect() {
//                    return null;
//                }
//            }
//        }
//    }


    @Override
    public List<Factory> get() {
        final ImmutableList.Builder<Factory> builder = ImmutableList.builder();

        builder.add(FailureDetectorMBeanMetricFamilyCollector.factory(metadataFactory));
        builder.add(cache(StorageServiceMBeanMetricFamilyCollector.factory(metadataFactory), 5, TimeUnit.MINUTES));

        builder.add(MemoryPoolMXBeanMetricFamilyCollector.FACTORY);
        builder.add(GarbageCollectorMXBeanMetricFamilyCollector.FACTORY);
        builder.add(BufferPoolMXBeanMetricFamilyCollector.FACTORY);
        builder.add(cache(OperatingSystemMXBeanMetricFamilyCollector.FACTORY, 5, TimeUnit.MINUTES));
        builder.add(ThreadMXBeanMetricFamilyCollector.factory(perThreadTimingEnabled));


        // org.apache.cassandra.metrics.BufferPoolMetrics
        {
            builder.add(bufferPoolMetricFactory(functionalCollector(meterAsCounter()), "Misses", "misses_total", "Total number of requests to the BufferPool requiring allocation of a new ByteBuffer."));
            builder.add(bufferPoolMetricFactory(functionalCollector(numericGaugeAsGauge()), "Size", "size_bytes", "Current size in bytes of the global BufferPool."));
        }


        // org.apache.cassandra.metrics.CQLMetrics
        {
            builder.add(cqlMetricFactory(functionalCollector(numericGaugeAsGauge()), "PreparedStatementsCount", "prepared_statements", "The current number of CQL and Thrift prepared statements in the statement cache."));
            builder.add(cqlMetricFactory(functionalCollector(counterAsCounter()), "PreparedStatementsEvicted", "prepared_statements_evicted_total", "Total number of CQL and Thrift prepared statements evicted from the statement cache."));
            builder.add(cqlMetricFactory(functionalCollector(counterAsCounter()), "PreparedStatementsExecuted", "statements_executed_total", "Total number of CQL statements executed.", ImmutableMap.of("statement_type", "prepared")));
            builder.add(cqlMetricFactory(functionalCollector(counterAsCounter()), "RegularStatementsExecuted", "statements_executed_total", "Total number of CQL statements executed.", ImmutableMap.of("statement_type", "regular")));
        }


        // org.apache.cassandra.metrics.CacheMetrics/org.apache.cassandra.metrics.CacheMissMetrics
        {
            // common cache metrics
            builder.add(cacheMetricFactory(functionalCollector(numericGaugeAsGauge()), "Capacity", "capacity_bytes", null));
            builder.add(cacheMetricFactory(functionalCollector(meterAsCounter()), "Requests", "requests_total", null));
            builder.add(cacheMetricFactory(functionalCollector(numericGaugeAsGauge()), "Size", "estimated_size_bytes", null));
            builder.add(cacheMetricFactory(functionalCollector(numericGaugeAsGauge()), "Entries", "entries", null));

            // TODO: somehow make hits/misses common across all caches?
            // org.apache.cassandra.metrics.CacheMetrics
            builder.add(cacheMetricFactory(functionalCollector(meterAsCounter()), "Hits", "hits_total", null));

            // org.apache.cassandra.metrics.CacheMissMetrics
            // "Misses" -- ignored, as "MissLatency" also includes a total count
            builder.add(cacheMetricFactory(timerAsSummaryCollectorConstructor(), "MissLatency", "miss_latency_seconds", null));
        }


        // org.apache.cassandra.metrics.ClientMetrics
        {
            builder.add(clientMetricFactory(functionalCollector(meterAsCounter()), "AuthFailure", "authentication_failures_total", "Total number of failed client authentication requests."));
            builder.add(clientMetricFactory(functionalCollector(meterAsCounter()), "AuthSuccess", "authentication_successes_total", "Total number of successful client authentication requests."));
            builder.add(clientMetricFactory(functionalCollector(numericGaugeAsGauge()), "connectedNativeClients", "native_connections", "Current number of CQL connections."));
            builder.add(clientMetricFactory(functionalCollector(numericGaugeAsGauge()), "connectedThriftClients", "thrift_connections", "Current number of Thrift connections."));
        }


        // org.apache.cassandra.metrics.ClientRequestMetrics
        {
            builder.add(clientRequestMetricFactory(functionalCollector(meterAsCounter()), "Timeouts", "timeouts_total", "Total number of timeouts encountered (since server start)."));
            builder.add(clientRequestMetricFactory(functionalCollector(meterAsCounter()), "Unavailables", "unavailable_exceptions_total", "Total number of UnavailableExceptions thrown."));
            builder.add(clientRequestMetricFactory(functionalCollector(meterAsCounter()), "Failures", "failures_total", "Total number of failed requests."));

            builder.add(clientRequestMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "Latency", "latency_seconds", "Request latency."));
            builder.add(clientRequestMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "TotalLatency", "latency_seconds", "Total request duration."));
        }


        // org.apache.cassandra.metrics.CASClientRequestMetrics
        {
            builder.add(clientRequestMetricFactory(functionalCollector(counterAsCounter()), "ConditionNotMet", "cas_write_precondition_not_met_total", "Total number of transaction preconditions did not match current values (since server start).")); // TODO: name
            builder.add(clientRequestMetricFactory(histogramAsSummaryCollectorConstructor(), "ContentionHistogram", "cas_contentions", ""));
            builder.add(clientRequestMetricFactory(functionalCollector(counterAsCounter()), "UnfinishedCommit", "cas_unfinished_commits_total", null));
        }


        // org.apache.cassandra.metrics.ViewWriteMetrics
        {
            builder.add(clientRequestMetricFactory(functionalCollector(counterAsCounter()), "ViewReplicasAttempted", "view_replica_writes_attempted_total", null));
            builder.add(clientRequestMetricFactory(functionalCollector(counterAsCounter()), "ViewReplicasSuccess", "view_replica_writes_successful_total", null));
            builder.add(clientRequestMetricFactory(timerAsSummaryCollectorConstructor(), "ViewWriteLatency", "view_write_latency_seconds", null));
        }


        // org.apache.cassandra.metrics.CommitLogMetrics
        {
            builder.add(commitLogMetricFactory(functionalCollector(numericGaugeAsCounter()), "CompletedTasks", "completed_tasks_total", "Total number of commit log messages written (since server start)."));
            builder.add(commitLogMetricFactory(functionalCollector(numericGaugeAsGauge()), "PendingTasks", "pending_tasks", "Number of commit log messages written not yet fsync’d."));
            builder.add(commitLogMetricFactory(functionalCollector(numericGaugeAsGauge()), "TotalCommitLogSize", "size_bytes", "Current size used by all commit log segments."));
            builder.add(commitLogMetricFactory(timerAsSummaryCollectorConstructor(), "WaitingOnSegmentAllocation", "segment_allocation_latency_seconds", null));
            builder.add(commitLogMetricFactory(timerAsSummaryCollectorConstructor(), "WaitingOnCommit", "commit_latency_seconds", null));

        }


        // org.apache.cassandra.metrics.ConnectionMetrics
        {
            // Large, Small, Gossip
            builder.add(connectionMetric(functionalCollector(numericGaugeAsGauge()), "*MessagePendingTasks", "pending_tasks", null));
            builder.add(connectionMetric(functionalCollector(numericGaugeAsCounter()), "*MessageCompletedTasks", "completed_tasks_total", null));
            builder.add(connectionMetric(functionalCollector(numericGaugeAsCounter()), "*MessageDroppedTasks", "dropped_tasks_total", null));
            builder.add(connectionMetric(functionalCollector(meterAsCounter()), "Timeouts", "timeouts_total", null));
        }


        // org.apache.cassandra.metrics.CompactionMetrics
        {
            builder.add(compactionMetric(functionalCollector(counterAsCounter()),"BytesCompacted", "bytes_compacted_total", "Total number of bytes compacted (since server start)."));
            builder.add(compactionMetric(functionalCollector(numericGaugeAsCounter()), "CompletedTasks", "completed_tasks_total", "Total number of completed compaction tasks (since server start)."));
            // "PendingTasks" ignored -- it's an aggregate of the table-level metrics (see the table metric "PendingCompactions")
            builder.add(compactionMetric(functionalCollector(meterAsCounter()), "TotalCompactionsCompleted", "completed_total", "Total number of compactions (since server start)."));
        }


        // org.apache.cassandra.metrics.DroppedMessageMetrics
        {
            builder.add(droppedMessagesMetric(functionalCollector(meterAsCounter()), "Dropped", "total", null));
            builder.add(droppedMessagesMetric(timerAsSummaryCollectorConstructor(), "InternalDroppedLatency", "internal_latency_seconds", null));
            builder.add(droppedMessagesMetric(timerAsSummaryCollectorConstructor(), "CrossNodeDroppedLatency", "cross_node_latency_seconds", null));
        }


//        // org.apache.cassandra.db.RowIndexEntry
//        // TODO: better naming
//        {
//            builder.add(rowIndexMetric("IndexInfoCount", "info_count"));
//            builder.add(rowIndexMetric("IndexInfoGets", "info_gets"));
//            builder.add(rowIndexMetric("IndexedEntrySize", "entry_size_bytes"));
//        }


        // org.apache.cassandra.utils.memory.MemtablePool
        {
            builder.add(memtablePoolMetricsFactory(timerAsSummaryCollectorConstructor(), "BlockedOnAllocation", "allocation_latency_seconds", null));
        }


        // org.apache.cassandra.metrics.MessagingMetrics
        {
            builder.add(messagingMetricFactory(timerAsSummaryCollectorConstructor(), "*-Latency", "cross_node_latency_seconds", null));
        }


        // org.apache.cassandra.metrics.StorageMetrics
        {
            builder.add(storageMetric(functionalCollector(counterAsCounter()), "Exceptions", "exceptions_total", null));
            builder.add(storageMetric(functionalCollector(counterAsGauge()), "Load", "load_bytes", null));
            builder.add(storageMetric(functionalCollector(counterAsCounter()), "TotalHints", "hints_total", null));
            builder.add(storageMetric(functionalCollector(counterAsCounter()), "TotalHintsInProgress", "hints_in_progress", null));
        }


        // org.apache.cassandra.metrics.TableMetrics (includes secondary indexes and MVs)
        {
            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "MemtableOnHeapSize", "memory_used_bytes", null, ImmutableMap.of("region", "on_heap", "pool", "memtable")));
            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "MemtableOffHeapSize", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "memtable")));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "MemtableLiveDataSize", "memtable_live_bytes", null));

            // AllMemtables* just include the secondary-index table stats... Those are already collected separately
//            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "AllMemtablesHeapSize", "memory_used_bytes", null));
//            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "AllMemtablesOffHeapSize", null, null));
//            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "AllMemtablesLiveDataSize", null, null));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "MemtableColumnsCount", "memtable_columns", null));
            builder.add(tableMetricFactory(functionalCollector(counterAsCounter()), "MemtableSwitchCount", "memtable_switches", null));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge(MetricValueConversionFunctions::neg1ToNaN)), "CompressionRatio", "compression_ratio", null));

            builder.add(tableMetricFactory(functionalCollector(histogramGaugeAsSummary()), "EstimatedPartitionSizeHistogram", "estimated_partition_size_bytes", null));
            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge(MetricValueConversionFunctions::neg1ToNaN)), "EstimatedPartitionCount", "estimated_partitions", null));

            builder.add(tableMetricFactory(functionalCollector(histogramGaugeAsSummary()), "EstimatedColumnCountHistogram", "estimated_columns", null));

            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "SSTablesPerReadHistogram", "sstables_per_read", null));
//
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "ReadLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "read")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "ReadTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "read")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "RangeLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "range_read")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "RangeTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "range_read")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "WriteLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "write")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "WriteTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "write")));

            builder.add(tableMetricFactory(functionalCollector(counterAsGauge()), "PendingFlushes", "pending_flushes", null));
            builder.add(tableMetricFactory(functionalCollector(counterAsCounter()), "BytesFlushed", "flushed_bytes_total", null));

            builder.add(tableCompactionMetricFactory(functionalCollector(counterAsCounter()), "CompactionBytesWritten", "compaction_bytes_written_total", null));
            builder.add(tableCompactionMetricFactory(functionalCollector(numericGaugeAsGauge()), "PendingCompactions", "estimated_pending_compactions", null));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "LiveSSTableCount", "live_sstables", null));

            builder.add(tableMetricFactory(functionalCollector(counterAsGauge()), "LiveDiskSpaceUsed", "live_disk_space_bytes", null));
            builder.add(tableMetricFactory(functionalCollector(counterAsGauge()), "TotalDiskSpaceUsed", "disk_space_bytes", null));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "MaxPartitionSize", "partition_size_maximum_bytes", null));
            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "MeanPartitionSize", "partition_size_mean_bytes", null));
            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "MinPartitionSize", "partition_size_minimum_bytes", null));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsCounter()), "BloomFilterFalsePositives", "bloom_filter_false_positives_total", null));
            // "RecentBloomFilterFalsePositives" -- ignored. returns the value since the last metric read
            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "BloomFilterFalseRatio", "bloom_filter_false_ratio", null));
            // "RecentBloomFilterFalseRatio" -- ignored. returns the value since the last metric read (same as "RecentBloomFilterFalsePositives")
            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "BloomFilterDiskSpaceUsed", "bloom_filter_disk_space_used_bytes", null));
            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "BloomFilterOffHeapMemoryUsed", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "bloom_filter")));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "IndexSummaryOffHeapMemoryUsed", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "index_summary")));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "CompressionMetadataOffHeapMemoryUsed", "compression_metadata_offheap_bytes", null));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "KeyCacheHitRate", "key_cache_hit_ratio", null)); // it'd be nice if the individual requests/hits/misses values were exposed

            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "TombstoneScannedHistogram", "tombstones_scanned", null));
            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "LiveScannedHistogram", "live_rows_scanned", null));

            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "ColUpdateTimeDeltaHistogram", "column_update_time_delta_seconds", null));

            builder.add(tableMetricFactory(timerAsSummaryCollectorConstructor(), "ViewLockAcquireTime", "view_lock_acquisition_seconds", null));
            builder.add(tableMetricFactory(timerAsSummaryCollectorConstructor(), "ViewReadTime", "view_read_seconds", null));

            builder.add(cache(tableMetricFactory(functionalCollector(numericGaugeAsGauge()), "SnapshotsSize", "snapshots_size_bytes_total", null), 5, TimeUnit.MINUTES)); // TODO: maybe make configurable

            builder.add(tableMetricFactory(functionalCollector(counterAsGauge()), "RowCacheHit", "row_cache_hits", null));
            builder.add(tableMetricFactory(functionalCollector(counterAsGauge()), "RowCacheHitOutOfRange", "row_cache_misses", null, ImmutableMap.of("miss_type", "out_of_range")));
            builder.add(tableMetricFactory(functionalCollector(counterAsGauge()), "RowCacheMiss", "row_cache_misses", null, ImmutableMap.of("miss_type", "miss")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasPrepareLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_prepare")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasPrepareTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_prepare")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasProposeLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_propose")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasProposeTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_propose")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasCommitLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_commit")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasCommitTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_commit")));

            builder.add(tableMetricFactory(functionalCollector(numericGaugeAsGauge(MetricValueConversionFunctions::percentToRatio)), "PercentRepaired", "repaired_ratio", null));

            builder.add(tableMetricFactory(timerAsSummaryCollectorConstructor(), "CoordinatorReadLatency", "coordinator_latency_seconds", null, ImmutableMap.of("operation", "read")));
            builder.add(tableMetricFactory(timerAsSummaryCollectorConstructor(), "CoordinatorScanLatency", "coordinator_latency_seconds", null, ImmutableMap.of("operation", "scan")));

            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "WaitingOnFreeMemtableSpace", "free_memtable_latency_seconds", null));

            builder.add(tableMetricFactory(functionalCollector(counterAsCounter()), "DroppedMutations", "dropped_mutations_total", null));

            builder.add(tableMetricFactory(functionalCollector(counterAsCounter()), "SpeculativeRetries", "speculative_retries_total", null));
        }


        // org.apache.cassandra.metrics.ThreadPoolMetrics
        {
            builder.add(threadPoolMetric(functionalCollector(numericGaugeAsGauge()), "ActiveTasks", "active_tasks", null));
            builder.add(threadPoolMetric(functionalCollector(numericGaugeAsCounter()), "CompletedTasks", "completed_tasks_total", null));
            builder.add(threadPoolMetric(functionalCollector(counterAsCounter()), "TotalBlockedTasks", "blocked_tasks_total", null));
            builder.add(threadPoolMetric(functionalCollector(counterAsGauge()), "CurrentlyBlockedTasks", "blocked_tasks", null));
            builder.add(threadPoolMetric(functionalCollector(numericGaugeAsGauge()), "MaxPoolSize", "maximum_tasks", null));
        }


        return builder.build();
    }
}
