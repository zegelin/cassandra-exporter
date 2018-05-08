package com.zegelin.prometheus.cassandra;

import com.codahale.metrics.Gauge;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.sun.jmx.mbeanserver.JmxMBeanServer;
import com.zegelin.jmx.DelegatingMBeanServerInterceptor;;
import com.zegelin.jmx.ObjectNames;
import com.zegelin.prometheus.cassandra.collector.*;
import com.zegelin.prometheus.cassandra.collector.dynamic.FunctionCollector;
import com.zegelin.prometheus.cassandra.collector.dynamic.FunctionCollector.CollectorFunction;
import com.zegelin.prometheus.cassandra.collector.dynamic.GroupThing;
import com.zegelin.prometheus.cassandra.collector.FooBar;
import com.zegelin.prometheus.domain.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.zegelin.jmx.ObjectNames.format;


@SuppressWarnings("SameParameterValue")
public class MetricsCollector {
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);

    private static MBeanMetricFamilyCollectorFactory bufferPoolMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=BufferPool,name=%s", jmxName);
        final String metricFamilyName = String.format("buffer_pool_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }


    private static MBeanMetricFamilyCollectorFactory cqlMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        return cqlMetric(fooBar, jmxName, familyNameSuffix, help, ImmutableMap.of());
    }

    private static MBeanMetricFamilyCollectorFactory cqlMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help, final Map<String, String> labels) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=CQL,name=%s", jmxName);
        final String metricFamilyName = String.format("cql_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> labels)
                .build();
    }


    private static MBeanMetricFamilyCollectorFactory cacheMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Cache,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("cache_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> ImmutableMap.of(
                        "cache", keyPropertyList.get("scope").replaceAll("Cache", "").toLowerCase()
                ))
                .build();
    }


    private static MBeanMetricFamilyCollectorFactory clientMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Client,name=%s", jmxName);
        final String metricFamilyName = String.format("client_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static MBeanMetricFamilyCollectorFactory clientRequestMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=ClientRequest,name=%s,scope=*-*", jmxName);
        final String metricFamilyName = String.format("client_request_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
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

    private static MBeanMetricFamilyCollectorFactory commitLogMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=CommitLog,name=%s", jmxName);
        final String metricFamilyName = String.format("commit_log_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static MBeanMetricFamilyCollectorFactory messagingMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Messaging,name=%s", jmxName);
        final String metricFamilyName = String.format("messaging_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
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

    private static MBeanMetricFamilyCollectorFactory memtablePoolMetrics(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=MemtablePool,name=%s", jmxName);
        final String metricFamilyName = String.format("memtable_pool_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static MBeanMetricFamilyCollectorFactory storageMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Storage,name=%s", jmxName);
        final String metricFamilyName = String.format("storage_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static MBeanMetricFamilyCollectorFactory tableMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        return tableMetric(fooBar, jmxName, familyNameSuffix, help, ImmutableMap.of());
    }

    private static MBeanMetricFamilyCollectorFactory tableMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help, final Map<String, String> extraLabels) {
        final QueryExp objectNameQuery = Query.or(
                format("org.apache.cassandra.metrics:type=Table,keyspace=*,scope=*,name=%s", jmxName),
                format("org.apache.cassandra.metrics:type=IndexTable,keyspace=*,scope=*,name=%s", jmxName)
        );

        final String metricFamilyName = String.format("table_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNameQuery, metricFamilyName)
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
                            .put("keyspace", keyspaceName);

                    // there doesn't appear to be an easy way to get the CFMetaData for an index
                    final CFMetaData tableMetadata = Schema.instance.getCFMetaData(keyspaceName, tableName);

                    if (tableMetadata == null)
                        throw new IllegalStateException(String.format("Unable to get metadata for table %s", tableName));

                    if (indexName != null) {
                        final IndexMetadata indexMetadata = tableMetadata.getIndexes().get(indexName)
                                .orElseThrow(() -> new IllegalStateException(String.format("Can't find metadata for index %s of table %s.", indexName, tableName)));

                        mapBuilder.put("table", indexName)
                                .put("table_type", "index");

                    } else {
                        mapBuilder.put("table", tableName);

                        if (tableMetadata.isView()) {
                            mapBuilder.put("table_type", "view");

                        } else {
                            mapBuilder.put("table_type", "table");
                        }
                    }

                    return mapBuilder.build();
                })
                .build();
    }

    private static MBeanMetricFamilyCollectorFactory threadPoolMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=ThreadPools,path=*,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("thread_pool_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> ImmutableMap.of(
                        "group", keyPropertyList.get("path"),
                        "pool", keyPropertyList.get("scope")
                ))
                .build();
    }

    private static MBeanMetricFamilyCollectorFactory rowIndexMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Index,scope=RowIndexEntry,name=%s", jmxName);
        final String metricFamilyName = String.format("row_index_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName).build();
    }

    private static MBeanMetricFamilyCollectorFactory droppedMessagesMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=DroppedMessage,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("dropped_messages_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .withLabelMaker(keyPropertyList -> ImmutableMap.of("message_type", keyPropertyList.get("scope")))
                .build();
    }

    private static MBeanMetricFamilyCollectorFactory compactionMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Compaction,name=%s", jmxName);
        final String metricFamilyName = String.format("compaction_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
                .withHelp(help)
                .build();
    }

    private static MBeanMetricFamilyCollectorFactory connectionMetric(final FooBar fooBar, final String jmxName, final String familyNameSuffix, final String help) {
        final ObjectName objectNamePattern = format("org.apache.cassandra.metrics:type=Connection,scope=*,name=%s", jmxName);
        final String metricFamilyName = String.format("endpoint_connection_%s", familyNameSuffix);

        return new MBeanMetricFamilyCollectorFactoryBuilder(fooBar, objectNamePattern, metricFamilyName)
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


    private static <X> FooBar fooBarForFunction(final CollectorFunction<X> collectorFunction) {
        return (name, help, labels, mBean) -> {
            final NamedObject<X> x = CassandraMetricsUtilities.metricForMBean(mBean);
            return new FunctionCollector<>(name, help, ImmutableMap.of(labels, x), collectorFunction);
        };
    }

    private static final List<MBeanMetricFamilyCollectorFactory> MBEAN_METRIC_FAMILY_COLLECTOR_FACTORIES;
    static {
        final ImmutableList.Builder<MBeanMetricFamilyCollectorFactory> builder = ImmutableList.builder();

        builder.add(FailureDetectorMBeanMetricFamilyCollector.FACTORY);
        builder.add(GossiperMBeanMetricFamilyCollector.FACTORY);

        // org.apache.cassandra.metrics.BufferPoolMetrics
        {
            builder.add(bufferPoolMetric(fooBarForFunction(Functions::meterAsCounter), "Misses", "misses_total", "Total number of requests to the BufferPool requiring allocation of a new ByteBuffer."));
            builder.add(bufferPoolMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "Size", "size_bytes", "Current size in bytes of the global BufferPool."));
        }

        // org.apache.cassandra.metrics.CQLMetrics
        {
            builder.add(cqlMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "PreparedStatementsCount", "prepared_statements", "The current number of CQL and Thrift prepared statements in the statement cache."));
            builder.add(cqlMetric(fooBarForFunction(Functions::counterAsCounter), "PreparedStatementsEvicted", "prepared_statements_evicted_total", "Total number of CQL and Thrift prepared statements evicted from the statement cache."));
            builder.add(cqlMetric(fooBarForFunction(Functions::counterAsCounter), "PreparedStatementsExecuted", "statements_executed_total", "Total number of CQL statements executed.", ImmutableMap.of("statement_type", "prepared")));
            builder.add(cqlMetric(fooBarForFunction(Functions::counterAsCounter), "RegularStatementsExecuted", "statements_executed_total", "Total number of CQL statements executed.", ImmutableMap.of("statement_type", "regular")));
        }

        // org.apache.cassandra.metrics.CacheMetrics/org.apache.cassandra.metrics.CacheMissMetrics
        {
            // common cache metrics
            builder.add(cacheMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "Capacity", "capacity_bytes", null));
            builder.add(cacheMetric(fooBarForFunction(Functions::meterAsCounter), "Requests", "requests_total", null));
            builder.add(cacheMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "Size", "estimated_size_bytes", null));
            builder.add(cacheMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "Entries", "entries", null));

            // TODO: make hits/misses common across all caches
            // org.apache.cassandra.metrics.CacheMetrics
            builder.add(cacheMetric(fooBarForFunction(Functions::meterAsCounter), "Hits", "hits_total", null));

            // org.apache.cassandra.metrics.CacheMissMetrics
            // "Misses" -- ignored, as "MissLatency" also includes a total count
            builder.add(cacheMetric(fooBarForFunction(Functions::timerAsSummary), "MissLatency", "miss_latency_seconds", null)); // TODO: convert/scale value to seconds (microseconds)
        }
        
        // org.apache.cassandra.metrics.ClientMetrics
        {
            builder.add(clientMetric(fooBarForFunction(Functions::meterAsCounter), "AuthFailure", "authentication_failures_total", "Total number of failed client authentication requests."));
            builder.add(clientMetric(fooBarForFunction(Functions::meterAsCounter), "AuthSuccess", "authentication_successes_total", "Total number of successful client authentication requests."));
            builder.add(clientMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "connectedNativeClients", "native_connections", "Current number of CQL connections."));
            builder.add(clientMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "connectedThriftClients", "thrift_connections", "Current number of Thrift connections."));
        }

        // org.apache.cassandra.metrics.ClientRequestMetrics
        {
            builder.add(clientRequestMetric(fooBarForFunction(Functions::meterAsCounter), "Timeouts", "timeouts_total", "Total number of timeouts encountered (since server start)."));
            builder.add(clientRequestMetric(fooBarForFunction(Functions::meterAsCounter), "Unavailables", "unavailable_exceptions_total", "Total number of UnavailableExceptions thrown."));
            builder.add(clientRequestMetric(fooBarForFunction(Functions::meterAsCounter), "Failures", "failures_total", "Total number of failed requests."));

            builder.add(clientRequestMetric(LatencyMetricGroupCollector::asSummary, "Latency", "latency_seconds", "Request latency."));
            builder.add(clientRequestMetric(LatencyMetricGroupCollector::asSummary, "TotalLatency", "latency_seconds", "Total request duration."));  // TODO: is in microseconds
        }


        // org.apache.cassandra.metrics.CASClientRequestMetrics
        {
            builder.add(clientRequestMetric(fooBarForFunction(Functions::counterAsCounter), "ConditionNotMet", "cas_write_precondition_not_met_total", "Total number of transaction preconditions did not match current values (since server start).")); // TODO: name
            builder.add(clientRequestMetric(fooBarForFunction(Functions::histogramAsSummary), "ContentionHistogram", "cas_contentions", ""));
            builder.add(clientRequestMetric(fooBarForFunction(Functions::counterAsCounter), "UnfinishedCommit", "cas_unfinished_commits_total", null));
        }

        // org.apache.cassandra.metrics.ViewWriteMetrics
        {
            builder.add(clientRequestMetric(fooBarForFunction(Functions::counterAsCounter), "ViewReplicasAttempted", "view_replica_writes_attempted_total", null));
            builder.add(clientRequestMetric(fooBarForFunction(Functions::counterAsCounter), "ViewReplicasSuccess", "view_replica_writes_successful_total", null));
            builder.add(clientRequestMetric(fooBarForFunction(Functions::timerAsSummary), "ViewWriteLatency", "view_write_latency_seconds", null)); // TODO: is in ms
        }

        // org.apache.cassandra.metrics.CommitLogMetrics
        {
            builder.add(commitLogMetric(fooBarForFunction(Functions::numericGaugeAsCounter), "CompletedTasks", "completed_tasks_total", "Total number of commit log messages written (since server start)."));
            builder.add(commitLogMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "PendingTasks", "pending_tasks", "Number of commit log messages written not yet fsync’d."));
            builder.add(commitLogMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "TotalCommitLogSize", "size_bytes", "Current size used by all commit log segments."));
            builder.add(commitLogMetric(fooBarForFunction(Functions::timerAsSummary), "WaitingOnSegmentAllocation", "segment_allocation_latency_seconds", null)); // TODO: is in microseconds
            builder.add(commitLogMetric(fooBarForFunction(Functions::timerAsSummary), "WaitingOnCommit", "commit_latency_seconds", null)); // TODO: is in microseconds

        }

        // org.apache.cassandra.metrics.ConnectionMetrics
        {
            // Large, Small, Gossip
            builder.add(connectionMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "*MessagePendingTasks", "pending_tasks", null));
            builder.add(connectionMetric(fooBarForFunction(Functions::numericGaugeAsCounter), "*MessageCompletedTasks", "completed_tasks_total", null));
            builder.add(connectionMetric(fooBarForFunction(Functions::numericGaugeAsCounter), "*MessageDroppedTasks", "dropped_tasks_total", null));
            builder.add(connectionMetric(fooBarForFunction(Functions::meterAsCounter), "Timeouts", "timeouts_total", null));
        }

        // org.apache.cassandra.metrics.CompactionMetrics
        {
            builder.add(compactionMetric(fooBarForFunction(Functions::counterAsCounter),"BytesCompacted", "bytes_compacted_total", "Total number of bytes compacted (since server start)."));
            builder.add(compactionMetric(fooBarForFunction(Functions::numericGaugeAsCounter), "CompletedTasks", "completed_tasks_total", "Total number of completed compaction tasks (since server start)."));
            builder.add(compactionMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "PendingTasks", "pending_tasks", "Estimated number of compactions remaining."));
            builder.add(compactionMetric(fooBarForFunction(Functions::meterAsCounter), "TotalCompactionsCompleted", "completed_total", "Total number of compactions (since server start)."));
        }

        // org.apache.cassandra.metrics.DroppedMessageMetrics
        {
            builder.add(droppedMessagesMetric(fooBarForFunction(Functions::meterAsCounter), "Dropped", "total", null));
            builder.add(droppedMessagesMetric(fooBarForFunction(Functions::timerAsSummary), "InternalDroppedLatency", "internal_latency_seconds", null)); // TODO: is in ms
            builder.add(droppedMessagesMetric(fooBarForFunction(Functions::timerAsSummary), "CrossNodeDroppedLatency", "cross_node_latency_seconds", null)); // TODO: is in ms
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
            builder.add(memtablePoolMetrics(fooBarForFunction(Functions::timerAsSummary), "BlockedOnAllocation", "allocation_latency_seconds", null)); // TODO: is in ms
        }

        // org.apache.cassandra.metrics.MessagingMetrics
        {
            builder.add(messagingMetric(fooBarForFunction(Functions::timerAsSummary), "*-Latency", "cross_node_latency_seconds", null)); // TODO: is in ms
        }

        // org.apache.cassandra.metrics.StorageMetrics
        {
            builder.add(storageMetric(fooBarForFunction(Functions::counterAsCounter), "Exceptions", "exceptions_total", null));
            builder.add(storageMetric(fooBarForFunction(Functions::counterAsGauge), "Load", "load_bytes", null));
            builder.add(storageMetric(fooBarForFunction(Functions::counterAsCounter), "TotalHints", "hints_total", null));
            builder.add(storageMetric(fooBarForFunction(Functions::counterAsCounter), "TotalHintsInProgress", "hints_in_progress", null));
        }


        // org.apache.cassandra.metrics.TableMetrics (includes secondary indexes and MVs)
        {
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "MemtableOnHeapSize", "memory_used_bytes", null, ImmutableMap.of("region", "on_heap", "pool", "memtable")));
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "MemtableOffHeapSize", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "memtable")));

            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "MemtableLiveDataSize", "memtable_live_bytes", null));

            // AllMemtables* just include the secondary-index table stats... Those are already collected separately
//            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "AllMemtablesHeapSize", "memory_used_bytes", null));
//            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "AllMemtablesOffHeapSize", null, null));
//            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "AllMemtablesLiveDataSize", null, null));

            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "MemtableColumnsCount", "memtable_columns", null));
            builder.add(tableMetric(fooBarForFunction(Functions::counterAsCounter), "MemtableSwitchCount", "memtable_switches", null));

            builder.add(tableMetric(fooBarForFunction((CollectorFunction<Gauge<Number>>) group -> Functions.numericGaugeAsGauge(new GroupThing<Gauge<Number>>() {
                @Override
                public String name() {
                    return group.name();
                }

                @Override
                public String help() {
                    return group.help();
                }

                @Override
                public Map<? extends Map<String, String>, Gauge<Number>> labeledMBeans() {
                    return Maps.transformValues(group.labeledMBeans(), g -> () -> {
                        final Number value = g.getValue();

                        return (value.intValue() == -1 ? Float.NaN : value);
                    });
                }
            })), "CompressionRatio", "compression_ratio", null)); // TODO: convert -1 to NaN

            builder.add(tableMetric(fooBarForFunction(Functions::histogramGaugeAsSummary), "EstimatedPartitionSizeHistogram", "estimated_partition_size_bytes", null));
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "EstimatedPartitionCount", "estimated_partitions", null)); // TODO: convert -1 to NaN

            builder.add(tableMetric(fooBarForFunction(Functions::histogramGaugeAsSummary), "EstimatedColumnCountHistogram", "estimated_columns", null));

            builder.add(tableMetric(fooBarForFunction(Functions::histogramAsSummary), "SSTablesPerReadHistogram", "sstables_per_read", null));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "ReadLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "read")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "ReadTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "read")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "RangeLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "range_read")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "RangeTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "range_read")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "WriteLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "write")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "WriteTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "write")));

            builder.add(tableMetric(fooBarForFunction(Functions::counterAsGauge), "PendingFlushes", "pending_flushes", null));
            builder.add(tableMetric(fooBarForFunction(Functions::counterAsCounter), "BytesFlushed", "flushed_bytes_total", null));

            builder.add(tableMetric(fooBarForFunction(Functions::counterAsCounter), "CompactionBytesWritten", "compaction_bytes_written_total", null));
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "PendingCompactions", "estimated_pending_compactions", null));

            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "LiveSSTableCount", "live_sstables", null));

            builder.add(tableMetric(fooBarForFunction(Functions::counterAsGauge), "LiveDiskSpaceUsed", "live_disk_space_bytes", null));
            builder.add(tableMetric(fooBarForFunction(Functions::counterAsGauge), "TotalDiskSpaceUsed", "disk_space_bytes", null));

            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "MaxPartitionSize", "partition_size_maximum_bytes", null));
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "MeanPartitionSize", "partition_size_mean_bytes", null));
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "MinPartitionSize", "partition_size_minimum_bytes", null));

            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsCounter), "BloomFilterFalsePositives", "bloom_filter_false_positives_total", null));
            // "RecentBloomFilterFalsePositives" -- ignored. returns the value since the last metric read
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "BloomFilterFalseRatio", "bloom_filter_false_ratio", null));
            // "RecentBloomFilterFalseRatio" -- ignored. same as "RecentBloomFilterFalsePositives"
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "BloomFilterDiskSpaceUsed", "bloom_filter_disk_space_used_bytes", null));
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "BloomFilterOffHeapMemoryUsed", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "bloom_filter")));

            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "IndexSummaryOffHeapMemoryUsed", "memory_used_bytes", null, ImmutableMap.of("region", "off_heap", "pool", "index_summary")));

            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "CompressionMetadataOffHeapMemoryUsed", "compression_metadata_offheap_bytes", null));

            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "KeyCacheHitRate", "key_cache_hit_ratio", null)); // it'd be nice if the individual requests/hits/misses values were exposed

            builder.add(tableMetric(fooBarForFunction(Functions::histogramAsSummary), "TombstoneScannedHistogram", "tombstones_scanned", null));
            builder.add(tableMetric(fooBarForFunction(Functions::histogramAsSummary), "LiveScannedHistogram", "live_rows_scanned", null));

            builder.add(tableMetric(fooBarForFunction(Functions::histogramAsSummary), "ColUpdateTimeDeltaHistogram", "column_update_time_delta_seconds", null)); // TODO: is in mx

            builder.add(tableMetric(fooBarForFunction(Functions::timerAsSummary), "ViewLockAcquireTime", "view_lock_acquisition_seconds", null)); // TODO: is in ms
            builder.add(tableMetric(fooBarForFunction(Functions::timerAsSummary), "ViewReadTime", "view_read_seconds", null)); // TODO: is in ms

//            builder.add(tableMetric("SnapshotsSize", null)); // EXPENSIVE -- does a walk of the filesystem! -- need to cache

            builder.add(tableMetric(fooBarForFunction(Functions::counterAsGauge), "RowCacheHitOutOfRange", "row_cache_misses", null, ImmutableMap.of("miss_type", "out_of_range")));
            builder.add(tableMetric(fooBarForFunction(Functions::counterAsGauge), "RowCacheHit", "row_cache_hits", null));
            builder.add(tableMetric(fooBarForFunction(Functions::counterAsGauge), "RowCacheMiss", "row_cache_misses", null, ImmutableMap.of("miss_type", "miss")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasPrepareLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_prepare")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasPrepareTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_prepare")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasProposeLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_propose")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasProposeTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_propose")));

            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasCommitLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_commit")));
            builder.add(tableMetric(LatencyMetricGroupCollector::asSummary, "CasCommitTotalLatency", "operation_latency_seconds", null, ImmutableMap.of("operation", "cas_commit")));

            //
            builder.add(tableMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "PercentRepaired", "repaired_percent", null)); // TODO: scale to 0-1

            builder.add(tableMetric(fooBarForFunction(Functions::timerAsSummary), "CoordinatorReadLatency", "coordinator_latency_seconds", null, ImmutableMap.of("operation", "read"))); // TODO is in ms
            builder.add(tableMetric(fooBarForFunction(Functions::timerAsSummary), "CoordinatorScanLatency", "coordinator_latency_seconds", null, ImmutableMap.of("operation", "scan"))); // TODO is in ms

            builder.add(tableMetric(fooBarForFunction(Functions::histogramAsSummary), "WaitingOnFreeMemtableSpace", "free_memtable_latency_seconds", null)); // TODO: is in ms

            builder.add(tableMetric(fooBarForFunction(Functions::counterAsCounter), "DroppedMutations", "dropped_mutations_total", null));

            builder.add(tableMetric(fooBarForFunction(Functions::counterAsCounter), "SpeculativeRetries", "speculative_retries_total", null));
        }

        // org.apache.cassandra.metrics.ThreadPoolMetrics
        {
            builder.add(threadPoolMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "ActiveTasks", "active_tasks", null));
            builder.add(threadPoolMetric(fooBarForFunction(Functions::numericGaugeAsCounter), "CompletedTasks", "completed_tasks_total", null));
            builder.add(threadPoolMetric(fooBarForFunction(Functions::counterAsCounter), "TotalBlockedTasks", "blocked_tasks_total", null));
            builder.add(threadPoolMetric(fooBarForFunction(Functions::counterAsGauge), "CurrentlyBlockedTasks", "blocked_tasks", null));
            builder.add(threadPoolMetric(fooBarForFunction(Functions::numericGaugeAsGauge), "MaxPoolSize", "maximum_tasks", null));
        }

        MBEAN_METRIC_FAMILY_COLLECTOR_FACTORIES = builder.build();
    }


    private final Map<String, MBeanMetricFamilyCollector> mBeanCollectorsByName = Collections.synchronizedMap(new HashMap<>());
    private final Map<ObjectName, String> mBeanNameToCollectorNameMap = Collections.synchronizedMap(new HashMap<>());

    private StorageServiceMBean storageService;
    private EndpointSnitchInfoMBean endpointSnitchInfo;

    private final Map<ObjectName, Consumer<Object>> requiredBeansRegistry = ImmutableMap.<ObjectName, Consumer<Object>>builder()
            .put(ObjectNames.create("org.apache.cassandra.db:type=EndpointSnitchInfo"), (o) -> endpointSnitchInfo = (EndpointSnitchInfoMBean) o)
            .put(ObjectNames.create("org.apache.cassandra.db:type=StorageService"), (o) -> storageService = (StorageServiceMBean) o)
            .build();

    private final CountDownLatch requiredBeansLatch = new CountDownLatch(requiredBeansRegistry.size());

    class MBeanServerInterceptor extends DelegatingMBeanServerInterceptor {
        MBeanServerInterceptor(final MBeanServer delegate) {
            super(delegate);
        }

        @Override
        public ObjectInstance registerMBean(final Object object, ObjectName name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
            // delegate first so that any exceptions (such as InstanceAlreadyExistsException) will throw first before additional processing occurs.
            final ObjectInstance objectInstance = super.registerMBean(object, name);

            // a MBean can provide its own name upon registration
            name = objectInstance.getObjectName();

            // register required MBeans
            for (final Map.Entry<ObjectName, Consumer<Object>> registryEntry : requiredBeansRegistry.entrySet()) {
                if (registryEntry.getKey().apply(name)) {
                    registryEntry.getValue().accept(object);

                    requiredBeansLatch.countDown();
                }
            }

            final NamedObject<Object> namedMBean = new NamedObject<>(name, object);

            for (final MBeanMetricFamilyCollectorFactory factory : MBEAN_METRIC_FAMILY_COLLECTOR_FACTORIES) {
                try {
                    final MBeanMetricFamilyCollector collector = factory.createCollector(namedMBean);

                    if (collector == null)
                        continue;

                    mBeanCollectorsByName.merge(collector.name(), collector, MBeanMetricFamilyCollector::merge);
                    mBeanNameToCollectorNameMap.put(name, collector.name());

                    break;

                } catch (final Exception e) {
                    logger.warn("Failed to register collector for MBean {}", name, e);
                }
            }

            return objectInstance;
        }

        @Override
        public void unregisterMBean(final ObjectName mBeanName) throws InstanceNotFoundException, MBeanRegistrationException {
            try {
                final String collectorName = mBeanNameToCollectorNameMap.get(mBeanName);

                if (collectorName == null) {
                    // no collector registered
                    return;
                }

                mBeanCollectorsByName.compute(collectorName, (k, v) -> v.removeMBean(mBeanName));

            } finally {
                super.unregisterMBean(mBeanName);
            }
        }
    }

    public MetricsCollector() {
        registerMBeanServerInterceptor();
    }

    private void registerMBeanServerInterceptor() {
        final JmxMBeanServer mBeanServer = (JmxMBeanServer) ManagementFactory.getPlatformMBeanServer();

        final MBeanServerInterceptor interceptor = new MBeanServerInterceptor(mBeanServer.getMBeanServerInterceptor());

        mBeanServer.setMBeanServerInterceptor(interceptor);
    }

    public Stream<MetricFamily> collect() {
        return mBeanCollectorsByName.entrySet().parallelStream().flatMap((e) -> {
            try {
                return e.getValue().collect();

            } catch (final Exception exception) {
                logger.warn("Metrics collector {} failed to collect. Skipping.", e.getKey(), exception);

                return Stream.empty();
            }
        });
    }

    public Map<String, String> globalLabels() {
//        requiredBeansLatch.await();

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
