# cassandra-exporter

*Project Status: alpha*

## Introduction

*cassandra-exporter* is a Java agent that exports Cassandra metrics to [Prometheus](http://prometheus.io).

It enables high performance collection of Cassandra metrics and follows the Prometheus best practices for metrics naming and labeling.

For example, the following PromQL query will return an estimate of the number of pending compactions per keyspace, per node.

    sum(cassandra_table_estimated_pending_compactions) by (cassandra_node, keyspace)


## Compatibility

*cassandra-exporter* is has been tested with:

| Component       | Version       |
|-----------------|---------------|
| Apache Cassandra| 3.11.2        |
| Prometheus      | 2.0 and later |

Other Cassandra and Prometheus versions will be tested for compatibility in the future.

## Usage

Download the latest release and copy `cassandra-exporter-agent-<version>.jar` to `$CASSANDRA_HOME/lib` (typically `/usr/share/cassandra/lib` in most package installs).

Then edit `$CASSANDRA_CONF/cassandra-env.sh` (typically `/etc/cassandra/cassandra-env.sh`) and append the following:

    JVM_OPTS="$JVM_OPTS -javaagent:$CASSANDRA_HOME/lib/cassandra-exporter-agent-<version>.jar=http://localhost:9998/"

Then (re-)start Cassandra.

Prometheus metrics will be available at `http://localhost:9998/metrics`.

Configure Prometheus to scrape the endpoint by adding the following to `prometheus.yml`:

    scrape_configs:
      ...
      
      - job_name: 'cassandra'
        static_configs:
          - targets: ['<cassandra node IP>:9998']

See the [Prometheus documentation](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#%3Cscrape_config%3E) for more details on configuring scrape targets.

Viewing the exposed endpoint in a web browser will display a HTML version of the exported metrics.

To view the raw, plain text metrics (in the Prometheus text exposition format), either request the endpoint with a HTTP client that prefers plain text
(or one that can specify the `Accept: text/plain` header) or add the following query parameter to the URL: `?x-content-type=text/plain`.

An experimental JSON output is also provided, via `Accept: application/json` or `?x-content-type=application/json`.
The format/structure of this output is subject to change.

## Options

Currently only the HTTP endpoint (address & port) can be configured.


## Features

### Performance

JMX is *slow*, really slow. JMX adds significant overhead to every method invocation on exported MBean methods, even when those methods are called from within the same JVM.
On a 300-ish table Cassandra node, trying to collect all exposed metrics via JVM resulted in a collection time that was upwards of 2-3 *seconds*.
For exporters that run as a separate process there is additional overhead of inter-process communications and that time can reach the 10's of seconds.

*cassandra-exporter* on the same node collects all metrics in 10-20 *milliseconds*.

### Best practices

The exporter follows Prometheus best practices for metric names, labels and data types.

Aggregate metrics, such as the aggregated table metrics at the keyspace and node level, are skipped. Instead these should be aggregated using PromQL queries or Prometheus recording rules.

Metrics are coalesced when appropriate so they share the same name, opting for *labels* to differentiate indiviual time series. For example, each table level metric has a constant name and at minimum a `table` & `keyspace` label, which allows for complex PromQL queries.

For example the `cassandra_table_operation_latency_seconds[_count|_sum]` summary metric combines read, write, range read, CAS prepare, CAS propose and CAS commit latency metrics together into a single metric family.
A summary exposes percentiles (via the `quantile` label), a total count of recorded samples (via the `_count` metric),
and (if available, `NaN` otherwise) an accumulated sum of all samples  (via the `_sum` metric).

Individual time-series are separated by different labels. In this example, the operation type is exported as the `operation` label.
The source `keyspace`, `table`, `table_type` (table, view or index), `table_id` (CF UUID), and numerous other metadata labels are available.

    cassandra_table_operation_latency_seconds_count{keyspace="system_schema",table="tables",table_type="table",operation="read",...}
    cassandra_table_operation_latency_seconds_count{keyspace="system_schema",table="tables",table_type="table",operation="write",...}

    cassandra_table_operation_latency_seconds_count{keyspace="system_schema",table="keyspaces",table_type="table",operation="read",...}
    cassandra_table_operation_latency_seconds_count{keyspace="system_schema",table="keyspaces",table_type="table",operation="write",...}

These metrics can then be queried:

    sum(cassandra_table_operation_latency_seconds_count) by (keyspace, operation) # total operations by keyspace & type


Element                                              | Value
---------------------------------------------------- |------
`{keyspace="system",operation="write"}`              | 13989
`{keyspace="system",operation="cas_commit"}`         | 0
`{keyspace="system",operation="cas_prepare"}`        | 0
`{keyspace="system",operation="cas_propose"}`        | 0
`{keyspace="system",operation="range_read"}`         | 10894
`{keyspace="system",operation="read"}`               | 74
`{keyspace="system_schema",operation="write"}`       | 78
`{keyspace="system_schema",operation="cas_commit"}`  | 0
`{keyspace="system_schema",operation="cas_prepare"}` | 0
`{keyspace="system_schema",operation="cas_propose"}` | 0
`{keyspace="system_schema",operation="range_read"}`  | 75
`{keyspace="system_schema",operation="read"}`        | 618

### Global Labels

The exporter does attach global labels to the exported metrics. At this time these cannot be disabled without recompiling the agent.

These labels are:

- `cassandra_cluster_name`

    The name of the cluster, as specified in cassandra.yaml
    
- `cassandra_host_id`

    The unique UUID of the node
    
- `cassandra_node`

    The IP address of the node
    
- `cassandra_datacenter`

    The configured data center name of the node
    
- `cassandra_rack`

    The configured rack name of the node
    
These labels allow aggregation of metrics at the cluster, data center and rack levels.

While these labels could be defined in the prometheus scrape config, the authors feel that having these labels be automatically
applied simplifies things, especially when Prometheus is monitoring multiple clusters across numerous DCs and racks.


### JMX Standalone (Experimental)

While it is preferable to run *cassandra-exporter* as a Java agent for performance, it can instead be run as an external application if required.
Metrics will be queried via JMX.

The set of metrics should be identical, but currently some additional metadata labels attached to the `cassandra_table_*` metrics will
not be available.

This was originally designed to assist with benchmarking and development of the exporter. Currently the JMX RMI service URL and HTTP endpoint
values are hard-coded. The application will need to be recompiled if these parameters need to be changed.


## Exported Metrics

See the [Exported Metrics](https://github.com/zegelin/cassandra-exporter/wiki/Exported-Metrics) wiki page for a list.

We suggest viewing the metrics endpoint (e.g., <http://localhost:9998/metrics>) in a browser to get an understanding of what metrics
are exported by your Cassandra node.

## Unstable, Missing & Future Features

See the [project issue tracker](https://github.com/zegelin/cassandra-exporter/issues) for a complete list.

- Configuration parameters

    Currently only the listen address & port can be configured.

    Allow configuration of:

    - listen address and port
    - exported metrics (aka, blacklist certain metrics)
    - enable/disable global labels
    - exclude help from JSON

- JVM metrics

    Future versions should add support for collecting and exporting JVM metrics (memory, GC pause times, etc).

- Add some example queries
- Add Grafana dashboard templates
- Documentation improvements
- Improve standalone JMX exporter
    - Configuration parameters