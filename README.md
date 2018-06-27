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

Download the latest release and copy `cassandra-exporter-<version>.jar` to `$CASSANDRA_HOME/lib` (typically `/usr/share/cassandra/lib` in most package installs).

Then edit `$CASSANDRA_CONF/cassandra-env.sh` (typically `/etc/cassandra/cassandra-env.sh`) and append the following:

    JVM_OPTS="$JVM_OPTS -javaagent:$CASSANDRA_HOME/lib/prometheus-cassandra-<version>.jar=http://localhost:9998/"

Then (re-)start Cassandra.

Prometheus metrics will be available at `http://localhost:9998/metrics`.

Configure Prometheus to scrape the endpoint by adding the following to `prometheus.yml`:

    scrape_configs:
      ...
      
      - job_name: 'cassandra'
        static_configs:
          - targets: ['<cassandra node IP>:9998']

See the Prometheus documentation for more details on configuring scrape targets.

Viewing the exposed endpoint in a web browser will display a HTML version of the exported metrics.

To view the raw, plain text metrics either request the endpoint with with a client that prefers plain text
(or can directly specify the `Accept: text/plain` header) `?x-content-type=text/plain`



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

For example the `cassandra_table_operation_latency_seconds[_count|__sum]` summary metric groups read, write, range read, CAS prepare, CAS propose and CAS commit latency metrics together.
The operation type is exported as the `operation` label.

    cassandra_table_operation_latency_seconds_count{keyspace="system_schema",table="tables",table_type="table",operation="read"}
    cassandra_table_operation_latency_seconds_count{keyspace="system_schema",table="tables",table_type="table",operation="write"}

    cassandra_table_operation_latency_seconds_count{keyspace="system_schema",table="keyspaces",table_type="table",operation="read"}
    cassandra_table_operation_latency_seconds_count{keyspace="system_schema",table="keyspaces",table_type="table",operation="write"}

These metrics can then be queried:

    sum(cassandra_table_operation_latency_seconds_count) by (keyspace, operation) # total operrations by keyspace & type
k

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

## Exported Metrics

See the [Exported Metrics](https://github.com/zegelin/cassandra-exporter/wiki/Exported-Metrics) wiki page for a list.

## Unstable, Missing & Future Features

See the [project issue tracker](https://github.com/zegelin/cassandra-exporter/issues) for a complete list.

- Configuration parameters

    Currently only the listen address & port can be configured.

    Allow configuration of:

    - listen address and port
    - exported metrics (aka, blacklist certain metrics)
    - enable/disable global labels

- JVM metrics

    Future versions should add support for collecting and exporting JVM metrics (memory, GC pause times, etc).

- Add some example queries
- Add Grafana dashboard templates