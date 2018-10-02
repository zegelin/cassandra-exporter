#!/usr/bin/env bash

find . -path '*/node*/conf/cassandra-env.sh' | while read file; do
    echo "Processing $file"

    port=$((9499+$(echo ${file} | sed 's/[^0-9]*//g')))

    echo "JVM_OPTS=\"\$JVM_OPTS -javaagent:/home/adam/Projects/cassandra-exporter/agent/target/cassandra-exporter-agent-0.9.3-SNAPSHOT.jar=--listen=:${port}\"" >> \
        ${file}

done;