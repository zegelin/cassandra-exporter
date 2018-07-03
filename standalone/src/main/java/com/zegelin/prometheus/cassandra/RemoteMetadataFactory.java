package com.zegelin.prometheus.cassandra;

import com.datastax.driver.core.*;

import java.util.Optional;

public class RemoteMetadataFactory implements MetadataFactory {

    private final Metadata clusterMetadata;

    public RemoteMetadataFactory() {
        clusterMetadata = null;
    }

    @Override
    public Optional<IndexMetadata> indexMetadata(final String keyspaceName, final String tableName, final String indexName) {
//        final com.datastax.driver.core.TableMetadata table = clusterMetadata.getKeyspace(keyspaceName).getTable(tableName);
//
//        final com.datastax.driver.core.IndexMetadata index = table.getIndex(indexName);
//
//        index.

        return Optional.empty();
    }

    @Override
    public Optional<TableMetadata> tableOrViewMetadata(final String keyspaceName, final String tableName) {
        return Optional.empty();
    }

//    void foo() {
//        Cluster cluster = null;
//
//        final Session connect = cluster.connect();
//
//        connect.getCluster().getMetadata().getKeyspace("foo").getMaterializedView("foo").getBaseTable().getIndex("foo").getKind()
//    }

}
