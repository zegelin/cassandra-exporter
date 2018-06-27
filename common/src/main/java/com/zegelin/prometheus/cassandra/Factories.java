package com.zegelin.prometheus.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.zegelin.jmx.NamedObject;
import com.zegelin.prometheus.cassandra.MBeanGroupMetricFamilyCollector.Factory;
import com.zegelin.prometheus.cassandra.collector.FailureDetectorMBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.collector.GossiperMBeanMetricFamilyCollector;
import com.zegelin.prometheus.cassandra.collector.LatencyMetricGroupCollector;
import com.zegelin.prometheus.cassandra.collector.dynamic.FunctionCollector;
import com.zegelin.prometheus.cassandra.collector.dynamic.FunctionCollector.CollectorFunction;
import com.zegelin.prometheus.domain.CounterMetricFamily;
import com.zegelin.prometheus.domain.Metric;
import com.zegelin.prometheus.domain.MetricFamily;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.schema.IndexMetadata;

import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.zegelin.jmx.ObjectNames.format;
import static com.zegelin.prometheus.cassandra.CollectorFunctions.asCollector;

@SuppressWarnings("SameParameterValue")
public final class Factories {
    private static Factory bufferPoolMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=BufferPool,name=%s", jmxName);
        final String metricFamilyName = String.format("buffer_pool_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }


    private static Factory cqlMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        return cqlMetric(collectorConstructor, jmxName, familyNameSuffix, help, ImmutableMap.of());
    }

    private static Factory cqlMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help, final Map<String, String> labels) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=CQL,name=%s", jmxName);
        final String metricFamilyName = String.format("cql_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> labels)
                .build();
    }


    private static Factory cacheMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Cache,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("cache_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> ImmutableMap.of(
                        "cache", keyPropertyList.get("scope").replaceAll("Cache", "").toLowerCase()
                ))
                .build();
    }


    private static Factory clientMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Client,name=%s", jmxName);
        final String metricFamilyName = String.format("client_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static Factory clientRequestMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=ClientRequest,name=%s,scope=*-*", jmxName);
        final String metricFamilyName = String.format("client_request_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
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

    private static Factory commitLogMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=CommitLog,name=%s", jmxName);
        final String metricFamilyName = String.format("commit_log_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static Factory messagingMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Messaging,name=%s", jmxName);
        final String metricFamilyName = String.format("messaging_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
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

    private static Factory memtablePoolMetrics(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=MemtablePool,name=%s", jmxName);
        final String metricFamilyName = String.format("memtable_pool_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static Factory storageMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Storage,name=%s", jmxName);
        final String metricFamilyName = String.format("storage_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static Factory tableMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        return tableMetric(collectorConstructor, jmxName, familyNameSuffix, help, ImmutableMap.of());
    }

    private static Factory tableMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help, final Map<String, String> extraLabels) {
        final QueryExp objectNameQuery = Query.or(
                format("org.apache.cassandra.metrics:type=Table,keyspace=*,scope=*,name=%s", jmxName),
                format("org.apache.cassandra.metrics:type=IndexTable,keyspace=*,scope=*,name=%s", jmxName)
        );

        final String metricFamilyName = String.format("table_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNameQuery, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> {
                    final String keyspaceName = keyPropertyList.get("keyspace");
                    final String tableName, indexName;
                    {
                        final String[] nameParts = keyPropertyList.get("scope").split("\\.");

                        tableName = nameParts[0];
                        indexName = (nameParts.length > 1) ? nameParts[1] : null;
                    }

                    final ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
                    mapBuilder.putAll(extraLabels)
                            .put("keyspace", keyspaceName)
                            .put("table", tableName);

//                    // there doesn't appear to be an easy way to get the CFMetaData for an index
//                    final CFMetaData tableMetadata = Schema.instance.getCFMetaData(keyspaceName, tableName);
//
//                    if (tableMetadata == null)
//                        throw new IllegalStateException(String.format("Unable to get metadata for table %s", tableName));
//
//                    if (indexName != null) {
//                        final IndexMetadata indexMetadata = tableMetadata.getIndexes().get(indexName)
//                                .orElseThrow(() -> new IllegalStateException(String.format("Can't find metadata for index %s of table %s.", indexName, tableName)));
//
//                        mapBuilder.put("table", indexName)
//                                .put("table_type", "index");
//
//                    } else {
//                        mapBuilder.put("table", tableName);
//
//                        if (tableMetadata.isView()) {
//                            mapBuilder.put("table_type", "view");
//
//                        } else {
//                            mapBuilder.put("table_type", "table");
//                        }
//                    }

                    return mapBuilder.build();
                })
                .build();
    }

    private static Factory threadPoolMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=ThreadPools,path=*,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("thread_pool_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> ImmutableMap.of(
                        "group", keyPropertyList.get("path"),
                        "pool", keyPropertyList.get("scope")
                ))
                .build();
    }

    private static Factory rowIndexMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Index,scope=RowIndexEntry,name=%s", jmxName);
        final String metricFamilyName = String.format("row_index_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName).build();
    }

    private static Factory droppedMessagesMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=DroppedMessage,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("dropped_messages_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> ImmutableMap.of("message_type", keyPropertyList.get("scope")))
                .build();
    }

    private static Factory compactionMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Compaction,name=%s", jmxName);
        final String metricFamilyName = String.format("compaction_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static Factory connectionMetric(final Factory.Builder.CollectorConstructor collectorConstructor, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Connection,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("endpoint_connection_%s", familyNameSuffix);

        return new Factory.Builder(collectorConstructor, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> {
                    final HashMap<String, String> labels = new HashMap<>();

                    labels.put("endpoint", keyPropertyList.get("scope")); // IP address of node

                    labels.computeIfAbsent("task_type", k -> {
                        final String name = keyPropertyList.get("name");
                        final Pattern namePattern = Pattern.compile("(?<kind>.*)Message.*Tasks");

                        final Matcher matcher = namePattern.matcher(name);

                        if (!matcher.matches())
                            return null;

                        return matcher.group("kind").toLowerCase();
                    });

                    return labels;
                })
                .build();
    }


    static <A, B> Factory.Builder.CollectorConstructor pick(CollectorFunction<A> aFn, CollectorFunction<B> bFn) {
        return (name, help, labels, mBean) -> {
            try {
                final NamedObject<A> object = CassandraMetricsUtilities.metricForMBean(mBean);

                return new FunctionCollector<>(name, help, ImmutableMap.of(labels, object), aFn);

            } catch (Exception e) {
                return new FunctionCollector<>(name, help, ImmutableMap.of(labels, mBean.cast()), bFn);
            }
        };
    }

    private static final Function<Number, Number> MS_TO_SECONDS = (number -> number.doubleValue() / 1000.d);

    private static final Function<Number, Number> NEG1_TO_NAN = (number -> (number.intValue() == -1 ? Float.NaN : number));

    private static final Function<Number, Number> PERCENT_TO_RATIO = (number -> number.doubleValue() / 100.d);


    public static final List<Factory> MBEAN_METRIC_FAMILY_COLLECTOR_FACTORIES;
    static {
        final ImmutableList.Builder<Factory> builder = ImmutableList.builder();

        builder.add(FailureDetectorMBeanMetricFamilyCollector.FACTORY);
        builder.add(GossiperMBeanMetricFamilyCollector.FACTORY);

        // org.apache.cassandra.metrics.BufferPoolMetrics
        {
            builder.add(bufferPoolMetric(CollectorFunctions.meterAsCounter().asCollector(), "Misses", "misses_total", "Total number of requests to the BufferPool requiring allocation of a new ByteBuffer."));
            builder.add(bufferPoolMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "Size", "size_bytes", "Current size in bytes of the global BufferPool."));
        }


        // org.apache.cassandra.metrics.CQLMetrics
        {
            builder.add(cqlMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "PreparedStatementsCount", "prepared_statements", "The current number of CQL and Thrift prepared statements in the statement cache."));
            builder.add(cqlMetric(CollectorFunctions.counterAsCounter().asCollector(), "PreparedStatementsEvicted", "prepared_statements_evicted_total", "Total number of CQL and Thrift prepared statements evicted from the statement cache."));
            builder.add(cqlMetric(CollectorFunctions.counterAsCounter().asCollector(), "PreparedStatementsExecuted", "statements_executed_total", "Total number of CQL statements executed.", ImmutableMap.of("statement_type", "prepared")));
            builder.add(cqlMetric(CollectorFunctions.counterAsCounter().asCollector(), "RegularStatementsExecuted", "statements_executed_total", "Total number of CQL statements executed.", ImmutableMap.of("statement_type", "regular")));
        }

        // org.apache.cassandra.metrics.CacheMetrics/org.apache.cassandra.metrics.CacheMissMetrics
        {
            // common cache metrics
            builder.add(cacheMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "Capacity", "capacity_bytes", null));
            builder.add(cacheMetric(CollectorFunctions.meterAsCounter().asCollector(), "Requests", "requests_total", null));
            builder.add(cacheMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "Size", "estimated_size_bytes", null));
            builder.add(cacheMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "Entries", "entries", null));

            // TODO: make hits/misses common across all caches
            // org.apache.cassandra.metrics.CacheMetrics
            builder.add(cacheMetric(CollectorFunctions.meterAsCounter().asCollector(), "Hits", "hits_total", null));

            // org.apache.cassandra.metrics.CacheMissMetrics
            // "Misses" -- ignored, as "MissLatency" also includes a total count
            builder.add(cacheMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "MissLatency", "miss_latency_seconds", null)); // TODO: convert/scale value to seconds (microseconds)
        }

        // org.apache.cassandra.metrics.ClientMetrics
        {
            builder.add(clientMetric(CollectorFunctions.meterAsCounter().asCollector(), "AuthFailure", "authentication_failures_total", "Total number of failed client authentication requests."));
            builder.add(clientMetric(CollectorFunctions.meterAsCounter().asCollector(), "AuthSuccess", "authentication_successes_total", "Total number of successful client authentication requests."));
            builder.add(clientMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "connectedNativeClients", "native_connections", "Current number of CQL connections."));
            builder.add(clientMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "connectedThriftClients", "thrift_connections", "Current number of Thrift connections."));
        }

        // org.apache.cassandra.metrics.ClientRequestMetrics
        {
            builder.add(clientRequestMetric(CollectorFunctions.meterAsCounter().asCollector(), "Timeouts", "timeouts_total", "Total number of timeouts encountered (since server start)."));
            builder.add(clientRequestMetric(CollectorFunctions.meterAsCounter().asCollector(), "Unavailables", "unavailable_exceptions_total", "Total number of UnavailableExceptions thrown."));
            builder.add(clientRequestMetric(CollectorFunctions.meterAsCounter().asCollector(), "Failures", "failures_total", "Total number of failed requests."));

            builder.add(clientRequestMetric(LatencyMetricGroupCollector::asSummary, "Latency", "latency_seconds", "Request latency."));
            builder.add(clientRequestMetric(LatencyMetricGroupCollector::asSummary, "TotalLatency", "latency_seconds", "Total request duration."));  // TODO: is in microseconds
        }


        // org.apache.cassandra.metrics.CASClientRequestMetrics
        {
            builder.add(clientRequestMetric(CollectorFunctions.counterAsCounter().asCollector(), "ConditionNotMet", "cas_write_precondition_not_met_total", "Total number of transaction preconditions did not match current values (since server start).")); // TODO: name
            builder.add(clientRequestMetric(pick(CollectorFunctions.samplingAndCountingAsSummary(), CollectorFunctions.histogramAsSummary()), "ContentionHistogram", "cas_contentions", ""));
            builder.add(clientRequestMetric(CollectorFunctions.counterAsCounter().asCollector(), "UnfinishedCommit", "cas_unfinished_commits_total", null));
        }

        // org.apache.cassandra.metrics.ViewWriteMetrics
        {
            builder.add(clientRequestMetric(CollectorFunctions.counterAsCounter().asCollector(), "ViewReplicasAttempted", "view_replica_writes_attempted_total", null));
            builder.add(clientRequestMetric(CollectorFunctions.counterAsCounter().asCollector(), "ViewReplicasSuccess", "view_replica_writes_successful_total", null));
            builder.add(clientRequestMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "ViewWriteLatency", "view_write_latency_seconds", null)); // TODO: is in ms
        }

        // org.apache.cassandra.metrics.CommitLogMetrics
        {
            builder.add(commitLogMetric(CollectorFunctions.numericGaugeAsCounter().asCollector(), "CompletedTasks", "completed_tasks_total", "Total number of commit log messages written (since server start)."));
            builder.add(commitLogMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "PendingTasks", "pending_tasks", "Number of commit log messages written not yet fsync’d."));
            builder.add(commitLogMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "TotalCommitLogSize", "size_bytes", "Current size used by all commit log segments."));
            builder.add(commitLogMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "WaitingOnSegmentAllocation", "segment_allocation_latency_seconds", null)); // TODO: is in microseconds
            builder.add(commitLogMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "WaitingOnCommit", "commit_latency_seconds", null)); // TODO: is in microseconds

        }

        // org.apache.cassandra.metrics.ConnectionMetrics
        {
            // Large, Small, Gossip
            builder.add(connectionMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "*MessagePendingTasks", "pending_tasks", null));
            builder.add(connectionMetric(CollectorFunctions.numericGaugeAsCounter().asCollector(), "*MessageCompletedTasks", "completed_tasks_total", null));
            builder.add(connectionMetric(CollectorFunctions.numericGaugeAsCounter().asCollector(), "*MessageDroppedTasks", "dropped_tasks_total", null));
            builder.add(connectionMetric(CollectorFunctions.meterAsCounter().asCollector(), "Timeouts", "timeouts_total", null));
        }

        // org.apache.cassandra.metrics.CompactionMetrics
        {
            builder.add(compactionMetric(CollectorFunctions.counterAsCounter().asCollector(),"BytesCompacted", "bytes_compacted_total", "Total number of bytes compacted (since server start)."));
            builder.add(compactionMetric(CollectorFunctions.numericGaugeAsCounter().asCollector(), "CompletedTasks", "completed_tasks_total", "Total number of completed compaction tasks (since server start)."));
            builder.add(compactionMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "PendingTasks", "pending_tasks", "Estimated number of compactions remaining."));
            builder.add(compactionMetric(CollectorFunctions.meterAsCounter().asCollector(), "TotalCompactionsCompleted", "completed_total", "Total number of compactions (since server start)."));
        }

        // org.apache.cassandra.metrics.DroppedMessageMetrics
        {
            builder.add(droppedMessagesMetric(CollectorFunctions.meterAsCounter().asCollector(), "Dropped", "total", null));
            builder.add(droppedMessagesMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "InternalDroppedLatency", "internal_latency_seconds", null)); // TODO: is in ms
            builder.add(droppedMessagesMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "CrossNodeDroppedLatency", "cross_node_latency_seconds", null)); // TODO: is in ms
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
            builder.add(memtablePoolMetrics(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "BlockedOnAllocation", "allocation_latency_seconds", null)); // TODO: is in ms
        }

        // org.apache.cassandra.metrics.MessagingMetrics
        {
            builder.add(messagingMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "*-Latency", "cross_node_latency_seconds", null)); // TODO: is in ms
        }

        // org.apache.cassandra.metrics.StorageMetrics
        {
            builder.add(storageMetric(CollectorFunctions.counterAsCounter().asCollector(), "Exceptions", "exceptions_total", null));
            builder.add(storageMetric(CollectorFunctions.counterAsGauge().asCollector(), "Load", "load_bytes", null));
            builder.add(storageMetric(CollectorFunctions.counterAsCounter().asCollector(), "TotalHints", "hints_total", null));
            builder.add(storageMetric(CollectorFunctions.counterAsCounter().asCollector(), "TotalHintsInProgress", "hints_in_progress", null));
        }


        // org.apache.cassandra.metrics.TableMetrics (includes secondary indexes and MVs)
        {
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "MemtableOnHeapSize", "memory_used_bytes", null, ImmutableMap.of("region", "on_heap", "pool", "memtable")));
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "MemtableOffHeapSize", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "memtable")));

            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "MemtableLiveDataSize", "memtable_live_bytes", null));

            // AllMemtables* just include the secondary-index table stats... Those are already collected separately
//            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "AllMemtablesHeapSize", "memory_used_bytes", null));
//            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "AllMemtablesOffHeapSize", null, null));
//            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "AllMemtablesLiveDataSize", null, null));

            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "MemtableColumnsCount", "memtable_columns", null));
            builder.add(tableMetric(CollectorFunctions.counterAsCounter().asCollector(), "MemtableSwitchCount", "memtable_switches", null));

            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge(NEG1_TO_NAN).asCollector(), "CompressionRatio", "compression_ratio", null));



            builder.add(tableMetric(CollectorFunctions.histogramGaugeAsSummary().asCollector(), "EstimatedPartitionSizeHistogram", "estimated_partition_size_bytes", null));
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge(NEG1_TO_NAN).asCollector(), "EstimatedPartitionCount", "estimated_partitions", null)); // TODO: convert -1 to NaN

            builder.add(tableMetric(CollectorFunctions.histogramGaugeAsSummary().asCollector(), "EstimatedColumnCountHistogram", "estimated_columns", null));

            builder.add(tableMetric(pick(CollectorFunctions.samplingAndCountingAsSummary(), CollectorFunctions.histogramAsSummary()), "SSTablesPerReadHistogram", "sstables_per_read", null));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "ReadLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "read")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "ReadTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "read")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "RangeLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "range_read")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "RangeTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "range_read")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "WriteLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "write")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "WriteTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "write")));

            builder.add(tableMetric(CollectorFunctions.counterAsGauge().asCollector(), "PendingFlushes", "pending_flushes", null));
            builder.add(tableMetric(CollectorFunctions.counterAsCounter().asCollector(), "BytesFlushed", "flushed_bytes_total", null));

            builder.add(tableMetric(CollectorFunctions.counterAsCounter().asCollector(), "CompactionBytesWritten", "compaction_bytes_written_total", null));
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "PendingCompactions", "estimated_pending_compactions", null));

            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "LiveSSTableCount", "live_sstables", null));

            builder.add(tableMetric(CollectorFunctions.counterAsGauge().asCollector(), "LiveDiskSpaceUsed", "live_disk_space_bytes", null));
            builder.add(tableMetric(CollectorFunctions.counterAsGauge().asCollector(), "TotalDiskSpaceUsed", "disk_space_bytes", null));

            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "MaxPartitionSize", "partition_size_maximum_bytes", null));
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "MeanPartitionSize", "partition_size_mean_bytes", null));
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "MinPartitionSize", "partition_size_minimum_bytes", null));

            builder.add(tableMetric(CollectorFunctions.numericGaugeAsCounter().asCollector(), "BloomFilterFalsePositives", "bloom_filter_false_positives_total", null));
            // "RecentBloomFilterFalsePositives" -- ignored. returns the value since the last metric read
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "BloomFilterFalseRatio", "bloom_filter_false_ratio", null));
            // "RecentBloomFilterFalseRatio" -- ignored. same as "RecentBloomFilterFalsePositives"
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "BloomFilterDiskSpaceUsed", "bloom_filter_disk_space_used_bytes", null));
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "BloomFilterOffHeapMemoryUsed", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "bloom_filter")));

            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "IndexSummaryOffHeapMemoryUsed", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "index_summary")));

            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "CompressionMetadataOffHeapMemoryUsed", "compression_metadata_offheap_bytes", null));

            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "KeyCacheHitRate", "key_cache_hit_ratio", null)); // it'd be nice if the individual requests/hits/misses values were exposed

            builder.add(tableMetric(pick(CollectorFunctions.samplingAndCountingAsSummary(), CollectorFunctions.histogramAsSummary()), "TombstoneScannedHistogram", "tombstones_scanned", null));
            builder.add(tableMetric(pick(CollectorFunctions.samplingAndCountingAsSummary(), CollectorFunctions.histogramAsSummary()), "LiveScannedHistogram", "live_rows_scanned", null));

            builder.add(tableMetric(pick(CollectorFunctions.samplingAndCountingAsSummary(), CollectorFunctions.histogramAsSummary()), "ColUpdateTimeDeltaHistogram", "column_update_time_delta_seconds", null)); // TODO: is in mx

            builder.add(tableMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "ViewLockAcquireTime", "view_lock_acquisition_seconds", null)); // TODO: is in ms
            builder.add(tableMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "ViewReadTime", "view_read_seconds", null)); // TODO: is in ms

//            builder.add(tableMetric("SnapshotsSize", null)); // EXPENSIVE -- does a walk of the filesystem! -- need to cache

            builder.add(tableMetric(CollectorFunctions.counterAsGauge().asCollector(), "RowCacheHitOutOfRange", "row_cache_misses", null, ImmutableMap.of("miss_type", "out_of_range")));
            builder.add(tableMetric(CollectorFunctions.counterAsGauge().asCollector(), "RowCacheHit", "row_cache_hits", null));
            builder.add(tableMetric(CollectorFunctions.counterAsGauge().asCollector(), "RowCacheMiss", "row_cache_misses", null, ImmutableMap.of("miss_type", "miss")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasPrepareLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_prepare")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasPrepareTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_prepare")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasProposeLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_propose")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasProposeTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_propose")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasCommitLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_commit")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasCommitTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_commit")));

            //
            builder.add(tableMetric(CollectorFunctions.numericGaugeAsGauge(PERCENT_TO_RATIO).asCollector(), "PercentRepaired", "repaired_ratio", null)); // TODO: scale to 0-1

            builder.add(tableMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "CoordinatorReadLatency", "coordinator_latency_seconds", null, ImmutableMap.of("operation", "read"))); // TODO is in ms
            builder.add(tableMetric(CollectorFunctions.timerAsSummary(MS_TO_SECONDS).asCollector(), "CoordinatorScanLatency", "coordinator_latency_seconds", null, ImmutableMap.of("operation", "scan"))); // TODO is in ms

            builder.add(tableMetric(CollectorFunctions.histogramAsSummary(MS_TO_SECONDS).asCollector(), "WaitingOnFreeMemtableSpace", "free_memtable_latency_seconds", null)); // TODO: is in ms

            builder.add(tableMetric(CollectorFunctions.counterAsCounter().asCollector(), "DroppedMutations", "dropped_mutations_total", null));

            builder.add(tableMetric(CollectorFunctions.counterAsCounter().asCollector(), "SpeculativeRetries", "speculative_retries_total", null));
        }

        // org.apache.cassandra.metrics.ThreadPoolMetrics
        {
            builder.add(threadPoolMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "ActiveTasks", "active_tasks", null));
            builder.add(threadPoolMetric(CollectorFunctions.numericGaugeAsCounter().asCollector(), "CompletedTasks", "completed_tasks_total", null));
            builder.add(threadPoolMetric(CollectorFunctions.counterAsCounter().asCollector(), "TotalBlockedTasks", "blocked_tasks_total", null));
            builder.add(threadPoolMetric(CollectorFunctions.counterAsGauge().asCollector(), "CurrentlyBlockedTasks", "blocked_tasks", null));
            builder.add(threadPoolMetric(CollectorFunctions.numericGaugeAsGauge().asCollector(), "MaxPoolSize", "maximum_tasks", null));
        }


        MBEAN_METRIC_FAMILY_COLLECTOR_FACTORIES = builder.build();
    }
}
