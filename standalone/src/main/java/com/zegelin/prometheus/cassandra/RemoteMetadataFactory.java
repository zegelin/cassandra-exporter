package com.zegelin.prometheus.cassandra;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.Metadata;

import java.net.InetAddress;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RemoteMetadataFactory extends MetadataFactory {
    private final Metadata clusterMetadata;

    RemoteMetadataFactory(final Metadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
    }

    @Override
    public Optional<IndexMetadata> indexMetadata(final String keyspaceName, final String tableName, final String indexName) {
        return Optional.ofNullable(clusterMetadata.getKeyspace(keyspaceName))
                .flatMap(k -> Optional.ofNullable(k.getTable(tableName)))
                .flatMap(t -> Optional.ofNullable(t.getIndex(indexName)))
                .map(i -> new IndexMetadata() {
                    @Override
                    public IndexType indexType() {
                        return IndexType.valueOf(i.getKind().name());
                    }

                    @Override
                    public Optional<String> customClassName() {
                        return Optional.ofNullable(i.getIndexClassName());
                    }
                });
    }

    @Override
    public Optional<TableMetadata> tableOrViewMetadata(final String keyspaceName, final String tableOrViewName) {
        return Optional.ofNullable(clusterMetadata.getKeyspace(keyspaceName))
                .flatMap(k -> {
                    final AbstractTableMetadata tableMetadata = k.getTable(tableOrViewName);
                    final AbstractTableMetadata materializedViewMetadata = k.getMaterializedView(tableOrViewName);

                    return Optional.ofNullable(tableMetadata != null ? tableMetadata : materializedViewMetadata);
                })
                .map(m -> new TableMetadata() {
                    @Override
                    public String compactionStrategyClassName() {
                        return m.getOptions().getCompaction().get("class");
                    }

                    @Override
                    public boolean isView() {
                        return (m instanceof MaterializedViewMetadata);
                    }
                });
    }

    @Override
    public Set<String> keyspaces() {
        return clusterMetadata.getKeyspaces().stream().map(KeyspaceMetadata::getName).collect(Collectors.toSet());
    }

    @Override
    public Optional<EndpointMetadata> endpointMetadata(final InetAddress endpoint) {
        return clusterMetadata.getAllHosts().stream()
                .filter(h -> h.getBroadcastAddress().equals(endpoint))
                .findFirst()
                .map(h -> new EndpointMetadata() {
                    @Override
                    public String dataCenter() {
                        return h.getDatacenter();
                    }

                    @Override
                    public String rack() {
                        return h.getRack();
                    }
                });
    }
}
