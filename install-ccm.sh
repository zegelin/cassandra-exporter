#!/usr/bin/env bash

find . -path '*/node*/conf/cassandra-env.sh' | while read file; do
    echo "Processing $file"

    port=$((19499+$(echo ${file} | sed 's/[^0-9]*//g')))

    sed -i -e "/cassandra-exporter/d" "${file}"

    echo "JVM_OPTS=\"\$JVM_OPTS -javaagent:/home/adam/Projects/cassandra-exporter/agent/target/cassandra-exporter-agent-0.9.4-SNAPSHOT.jar=--listen=:${port},--cache=true,--enable-collector-timing\"" >> \
        "${file}"
done;