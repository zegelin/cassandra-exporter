package org.apache.cassandra;

// this package exists to contain all the MBean interfaces used by the exporter
// and prevents us from adding Cassandra as a hard dependency (and bloating the output jar)