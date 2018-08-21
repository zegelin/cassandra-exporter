package com.zegelin.prometheus.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.NamedObject;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector.Factory;
import com.zegelin.prometheus.cassandra.collector.FailureDetectorMBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.collector.GossiperMBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.collector.LatencyMetricGroupSummaryCollector;
import com.zegelin.prometheus.cassandra.collector.dynamic.FunctionalMetricFamilyCollector;
import com.zegelin.prometheus.domain.Labels;

import javax.management.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.zegelin.jmx.ObjectNames.format;
import static com.zegelin.prometheus.cassandra.CollectorFunctions.*;

@SuppressWarnings("SameParameterValue")
public class FactoriesSupplier implements Supplier<List<Factory>> {
    private static final Function<Float, Float> MILLISECONDS_TO_SECONDS = (f -> f / 1.0E3f);
    private static final Function<Float, Float> MICROSECONDS_TO_SECONDS = (f -> f / 1.0E6f);

    private static final Function<Float, Float> NEG1_TO_NAN = (f -> (f == -1 ? Float.NaN : f));

    private static final Function<Float, Float> PERCENT_TO_RATIO = (f -> f / 100.f);

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

        private final List<LabelMaker> labelMakers = new ArrayList<>();


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
            return new Factory() {
                @Override
                public MBeanGroupMetricFamilyCollector createCollector(final NamedObject<?> mBean) {
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
                }
            };
        }

        @FunctionalInterface
        public interface CollectorConstructor {
            MBeanGroupMetricFamilyCollector groupCollectorForMBean(final String name, final String help, final Labels labels, final NamedObject<?> mBean);
        }
    }

    private final MetadataFactory metadataFactory;

    public FactoriesSupplier(final MetadataFactory metadataFactory) {
        this.metadataFactory = metadataFactory;
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

    private Factory tableMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        return tableMetricFactory(collectorConstructor, jmxName, familyNameSuffix, help, ImmutableMap.of());
    }

    private Factory tableMetricFactory(final FactoryBuilder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help, final Map<String, String> extraLabels) {
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
                                .put("index", indexName)
                                .put("table_type", "index");

                        final Optional<MetadataFactory.IndexMetadata> indexMetadata = metadataFactory.indexMetadata(keyspaceName, tableName, indexName);

                        indexMetadata.ifPresent(m -> {
                            labelsBuilder.put("table_id", m.id().toString());
                            labelsBuilder.put("index_type", m.indexType().toString());

                            m.customClassName().ifPresent(s -> labelsBuilder.put("index_class_name", s));
                        });

                    } else {
                        labelsBuilder.put("table", tableName);

                        final Optional<MetadataFactory.TableMetadata> tableMetadata = metadataFactory.tableOrViewMetadata(keyspaceName, tableName);

                        tableMetadata.ifPresent(m -> {
                            labelsBuilder.put("table_id", m.id().toString());
                            labelsBuilder.put("table_type", m.isView() ? "view" : "table");
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

                    labels.put("endpoint", keyPropertyList.get("scope")); // IP address of node

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

            return new FunctionalMetricFamilyCollector<>(name, help, ImmutableMap.of(labels, samplingCountingNamedObject), samplingAndCountingAsSummary(MICROSECONDS_TO_SECONDS));
        };
    }

    private static FactoryBuilder.CollectorConstructor histogramAsSummaryCollectorConstructor() {
        return (name, help, labels, mBean) -> {
            final NamedObject<SamplingCounting> samplingCountingNamedObject = CassandraMetricsUtilities.jmxHistogramAsSamplingCounting(mBean);

            return new FunctionalMetricFamilyCollector<>(name, help, ImmutableMap.of(labels, samplingCountingNamedObject), samplingAndCountingAsSummary());
        };
    }

    private static <T> FactoryBuilder.CollectorConstructor asCollectorConstructor(final FunctionalMetricFamilyCollector.CollectorFunction<T> function) {
        return (final String name, final String help, final Labels labels, final NamedObject<?> mBean) ->
                new FunctionalMetricFamilyCollector<>(name, help, ImmutableMap.of(labels, mBean.<T>cast()), function);
    }

//    private static <T> CollectorFunction<T> cache(final CollectorFunction<T> fn) {
//        return labeledObjectGroup -> {
//            return Suppliers.memoizeWithExpiration(() -> fn.apply(labeledObjectGroup), 10, TimeUnit.SECONDS).;
//        }
//
//    }


    @Override
    public List<Factory> get() {
        final ImmutableList.Builder<Factory> builder = ImmutableList.builder();

        builder.add(FailureDetectorMBeanMetricFamilyCollector.FACTORY);
        builder.add(GossiperMBeanMetricFamilyCollector.FACTORY);

        // org.apache.cassandra.metrics.BufferPoolMetrics
        {
            builder.add(bufferPoolMetricFactory(asCollectorConstructor(meterAsCounter()), "Misses", "misses_total", "Total number of requests to the BufferPool requiring allocation of a new ByteBuffer."));
            builder.add(bufferPoolMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "Size", "size_bytes", "Current size in bytes of the global BufferPool."));
        }


        // org.apache.cassandra.metrics.CQLMetrics
        {
            builder.add(cqlMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "PreparedStatementsCount", "prepared_statements", "The current number of CQL and Thrift prepared statements in the statement cache."));
            builder.add(cqlMetricFactory(asCollectorConstructor(counterAsCounter()), "PreparedStatementsEvicted", "prepared_statements_evicted_total", "Total number of CQL and Thrift prepared statements evicted from the statement cache."));
            builder.add(cqlMetricFactory(asCollectorConstructor(counterAsCounter()), "PreparedStatementsExecuted", "statements_executed_total", "Total number of CQL statements executed.", ImmutableMap.of("statement_type", "prepared")));
            builder.add(cqlMetricFactory(asCollectorConstructor(counterAsCounter()), "RegularStatementsExecuted", "statements_executed_total", "Total number of CQL statements executed.", ImmutableMap.of("statement_type", "regular")));
        }


        // org.apache.cassandra.metrics.CacheMetrics/org.apache.cassandra.metrics.CacheMissMetrics
        {
            // common cache metrics
            builder.add(cacheMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "Capacity", "capacity_bytes", null));
            builder.add(cacheMetricFactory(asCollectorConstructor(meterAsCounter()), "Requests", "requests_total", null));
            builder.add(cacheMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "Size", "estimated_size_bytes", null));
            builder.add(cacheMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "Entries", "entries", null));

            // TODO: somehow make hits/misses common across all caches?
            // org.apache.cassandra.metrics.CacheMetrics
            builder.add(cacheMetricFactory(asCollectorConstructor(meterAsCounter()), "Hits", "hits_total", null));

            // org.apache.cassandra.metrics.CacheMissMetrics
            // "Misses" -- ignored, as "MissLatency" also includes a total count
            builder.add(cacheMetricFactory(timerAsSummaryCollectorConstructor(), "MissLatency", "miss_latency_seconds", null));
        }


        // org.apache.cassandra.metrics.ClientMetrics
        {
            builder.add(clientMetricFactory(asCollectorConstructor(meterAsCounter()), "AuthFailure", "authentication_failures_total", "Total number of failed client authentication requests."));
            builder.add(clientMetricFactory(asCollectorConstructor(meterAsCounter()), "AuthSuccess", "authentication_successes_total", "Total number of successful client authentication requests."));
            builder.add(clientMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "connectedNativeClients", "native_connections", "Current number of CQL connections."));
            builder.add(clientMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "connectedThriftClients", "thrift_connections", "Current number of Thrift connections."));
        }


        // org.apache.cassandra.metrics.ClientRequestMetrics
        {
            builder.add(clientRequestMetricFactory(asCollectorConstructor(meterAsCounter()), "Timeouts", "timeouts_total", "Total number of timeouts encountered (since server start)."));
            builder.add(clientRequestMetricFactory(asCollectorConstructor(meterAsCounter()), "Unavailables", "unavailable_exceptions_total", "Total number of UnavailableExceptions thrown."));
            builder.add(clientRequestMetricFactory(asCollectorConstructor(meterAsCounter()), "Failures", "failures_total", "Total number of failed requests."));

            builder.add(clientRequestMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "Latency", "latency_seconds", "Request latency."));
            builder.add(clientRequestMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "TotalLatency", "latency_seconds", "Total request duration."));
        }


        // org.apache.cassandra.metrics.CASClientRequestMetrics
        {
            builder.add(clientRequestMetricFactory(asCollectorConstructor(counterAsCounter()), "ConditionNotMet", "cas_write_precondition_not_met_total", "Total number of transaction preconditions did not match current values (since server start).")); // TODO: name
            builder.add(clientRequestMetricFactory(histogramAsSummaryCollectorConstructor(), "ContentionHistogram", "cas_contentions", ""));
            builder.add(clientRequestMetricFactory(asCollectorConstructor(counterAsCounter()), "UnfinishedCommit", "cas_unfinished_commits_total", null));
        }


        // org.apache.cassandra.metrics.ViewWriteMetrics
        {
            builder.add(clientRequestMetricFactory(asCollectorConstructor(counterAsCounter()), "ViewReplicasAttempted", "view_replica_writes_attempted_total", null));
            builder.add(clientRequestMetricFactory(asCollectorConstructor(counterAsCounter()), "ViewReplicasSuccess", "view_replica_writes_successful_total", null));
            builder.add(clientRequestMetricFactory(timerAsSummaryCollectorConstructor(), "ViewWriteLatency", "view_write_latency_seconds", null));
        }


        // org.apache.cassandra.metrics.CommitLogMetrics
        {
            builder.add(commitLogMetricFactory(asCollectorConstructor(numericGaugeAsCounter()), "CompletedTasks", "completed_tasks_total", "Total number of commit log messages written (since server start)."));
            builder.add(commitLogMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "PendingTasks", "pending_tasks", "Number of commit log messages written not yet fsync’d."));
            builder.add(commitLogMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "TotalCommitLogSize", "size_bytes", "Current size used by all commit log segments."));
            builder.add(commitLogMetricFactory(timerAsSummaryCollectorConstructor(), "WaitingOnSegmentAllocation", "segment_allocation_latency_seconds", null));
            builder.add(commitLogMetricFactory(timerAsSummaryCollectorConstructor(), "WaitingOnCommit", "commit_latency_seconds", null));

        }


        // org.apache.cassandra.metrics.ConnectionMetrics
        {
            // Large, Small, Gossip
            builder.add(connectionMetric(asCollectorConstructor(numericGaugeAsGauge()), "*MessagePendingTasks", "pending_tasks", null));
            builder.add(connectionMetric(asCollectorConstructor(numericGaugeAsCounter()), "*MessageCompletedTasks", "completed_tasks_total", null));
            builder.add(connectionMetric(asCollectorConstructor(numericGaugeAsCounter()), "*MessageDroppedTasks", "dropped_tasks_total", null));
            builder.add(connectionMetric(asCollectorConstructor(meterAsCounter()), "Timeouts", "timeouts_total", null));
        }


        // org.apache.cassandra.metrics.CompactionMetrics
        {
            builder.add(compactionMetric(asCollectorConstructor(counterAsCounter()),"BytesCompacted", "bytes_compacted_total", "Total number of bytes compacted (since server start)."));
            builder.add(compactionMetric(asCollectorConstructor(numericGaugeAsCounter()), "CompletedTasks", "completed_tasks_total", "Total number of completed compaction tasks (since server start)."));
            builder.add(compactionMetric(asCollectorConstructor(numericGaugeAsGauge()), "PendingTasks", "pending_tasks", "Estimated number of compactions remaining."));
            builder.add(compactionMetric(asCollectorConstructor(meterAsCounter()), "TotalCompactionsCompleted", "completed_total", "Total number of compactions (since server start)."));
        }


        // org.apache.cassandra.metrics.DroppedMessageMetrics
        {
            builder.add(droppedMessagesMetric(asCollectorConstructor(meterAsCounter()), "Dropped", "total", null));
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
            builder.add(storageMetric(asCollectorConstructor(counterAsCounter()), "Exceptions", "exceptions_total", null));
            builder.add(storageMetric(asCollectorConstructor(counterAsGauge()), "Load", "load_bytes", null));
            builder.add(storageMetric(asCollectorConstructor(counterAsCounter()), "TotalHints", "hints_total", null));
            builder.add(storageMetric(asCollectorConstructor(counterAsCounter()), "TotalHintsInProgress", "hints_in_progress", null));
        }


        // org.apache.cassandra.metrics.TableMetrics (includes secondary indexes and MVs)
        {
            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "MemtableOnHeapSize", "memory_used_bytes", null, ImmutableMap.of("region", "on_heap", "pool", "memtable")));
            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "MemtableOffHeapSize", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "memtable")));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "MemtableLiveDataSize", "memtable_live_bytes", null));

            // AllMemtables* just include the secondary-index table stats... Those are already collected separately
//            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "AllMemtablesHeapSize", "memory_used_bytes", null));
//            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "AllMemtablesOffHeapSize", null, null));
//            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "AllMemtablesLiveDataSize", null, null));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "MemtableColumnsCount", "memtable_columns", null));
            builder.add(tableMetricFactory(asCollectorConstructor(counterAsCounter()), "MemtableSwitchCount", "memtable_switches", null));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge(NEG1_TO_NAN)), "CompressionRatio", "compression_ratio", null));

            builder.add(tableMetricFactory(asCollectorConstructor(histogramGaugeAsSummary()), "EstimatedPartitionSizeHistogram", "estimated_partition_size_bytes", null));
            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge(NEG1_TO_NAN)), "EstimatedPartitionCount", "estimated_partitions", null));

            builder.add(tableMetricFactory(asCollectorConstructor(histogramGaugeAsSummary()), "EstimatedColumnCountHistogram", "estimated_columns", null));

            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "SSTablesPerReadHistogram", "sstables_per_read", null));
//
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "ReadLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "read")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "ReadTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "read")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "RangeLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "range_read")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "RangeTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "range_read")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "WriteLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "write")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "WriteTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "write")));

            builder.add(tableMetricFactory(asCollectorConstructor(counterAsGauge()), "PendingFlushes", "pending_flushes", null));
            builder.add(tableMetricFactory(asCollectorConstructor(counterAsCounter()), "BytesFlushed", "flushed_bytes_total", null));

            builder.add(tableMetricFactory(asCollectorConstructor(counterAsCounter()), "CompactionBytesWritten", "compaction_bytes_written_total", null));
            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "PendingCompactions", "estimated_pending_compactions", null));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "LiveSSTableCount", "live_sstables", null));

            builder.add(tableMetricFactory(asCollectorConstructor(counterAsGauge()), "LiveDiskSpaceUsed", "live_disk_space_bytes", null));
            builder.add(tableMetricFactory(asCollectorConstructor(counterAsGauge()), "TotalDiskSpaceUsed", "disk_space_bytes", null));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "MaxPartitionSize", "partition_size_maximum_bytes", null));
            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "MeanPartitionSize", "partition_size_mean_bytes", null));
            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "MinPartitionSize", "partition_size_minimum_bytes", null));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsCounter()), "BloomFilterFalsePositives", "bloom_filter_false_positives_total", null));
            // "RecentBloomFilterFalsePositives" -- ignored. returns the value since the last metric read
            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "BloomFilterFalseRatio", "bloom_filter_false_ratio", null));
            // "RecentBloomFilterFalseRatio" -- ignored. returns the value since the last metric read (same as "RecentBloomFilterFalsePositives")
            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "BloomFilterDiskSpaceUsed", "bloom_filter_disk_space_used_bytes", null));
            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "BloomFilterOffHeapMemoryUsed", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "bloom_filter")));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "IndexSummaryOffHeapMemoryUsed", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "index_summary")));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "CompressionMetadataOffHeapMemoryUsed", "compression_metadata_offheap_bytes", null));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "KeyCacheHitRate", "key_cache_hit_ratio", null)); // it'd be nice if the individual requests/hits/misses values were exposed

            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "TombstoneScannedHistogram", "tombstones_scanned", null));
            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "LiveScannedHistogram", "live_rows_scanned", null));

            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "ColUpdateTimeDeltaHistogram", "column_update_time_delta_seconds", null));

            builder.add(tableMetricFactory(timerAsSummaryCollectorConstructor(), "ViewLockAcquireTime", "view_lock_acquisition_seconds", null));
            builder.add(tableMetricFactory(timerAsSummaryCollectorConstructor(), "ViewReadTime", "view_read_seconds", null));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge()), "SnapshotsSize", "snapshots_size_total_bytes", null)); // TODO: expensive -- cache. does a walk of the filesystem

            builder.add(tableMetricFactory(asCollectorConstructor(counterAsGauge()), "RowCacheHitOutOfRange", "row_cache_misses", null, ImmutableMap.of("miss_type", "out_of_range")));
            builder.add(tableMetricFactory(asCollectorConstructor(counterAsGauge()), "RowCacheHit", "row_cache_hits", null));
            builder.add(tableMetricFactory(asCollectorConstructor(counterAsGauge()), "RowCacheMiss", "row_cache_misses", null, ImmutableMap.of("miss_type", "miss")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasPrepareLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_prepare")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasPrepareTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_prepare")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasProposeLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_propose")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasProposeTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_propose")));

            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasCommitLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_commit")));
            builder.add(tableMetricFactory(LatencyMetricGroupSummaryCollector::collectorForMBean, "CasCommitTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_commit")));

            builder.add(tableMetricFactory(asCollectorConstructor(numericGaugeAsGauge(PERCENT_TO_RATIO)), "PercentRepaired", "repaired_ratio", null));

            builder.add(tableMetricFactory(timerAsSummaryCollectorConstructor(), "CoordinatorReadLatency", "coordinator_latency_seconds", null, ImmutableMap.of("operation", "read")));
            builder.add(tableMetricFactory(timerAsSummaryCollectorConstructor(), "CoordinatorScanLatency", "coordinator_latency_seconds", null, ImmutableMap.of("operation", "scan")));

            builder.add(tableMetricFactory(histogramAsSummaryCollectorConstructor(), "WaitingOnFreeMemtableSpace", "free_memtable_latency_seconds", null));

            builder.add(tableMetricFactory(asCollectorConstructor(counterAsCounter()), "DroppedMutations", "dropped_mutations_total", null));

            builder.add(tableMetricFactory(asCollectorConstructor(counterAsCounter()), "SpeculativeRetries", "speculative_retries_total", null));
        }


        // org.apache.cassandra.metrics.ThreadPoolMetrics
        {
            builder.add(threadPoolMetric(asCollectorConstructor(numericGaugeAsGauge()), "ActiveTasks", "active_tasks", null));
            builder.add(threadPoolMetric(asCollectorConstructor(numericGaugeAsCounter()), "CompletedTasks", "completed_tasks_total", null));
            builder.add(threadPoolMetric(asCollectorConstructor(counterAsCounter()), "TotalBlockedTasks", "blocked_tasks_total", null));
            builder.add(threadPoolMetric(asCollectorConstructor(counterAsGauge()), "CurrentlyBlockedTasks", "blocked_tasks", null));
            builder.add(threadPoolMetric(asCollectorConstructor(numericGaugeAsGauge()), "MaxPoolSize", "maximum_tasks", null));
        }


        return builder.build();
    }
}
