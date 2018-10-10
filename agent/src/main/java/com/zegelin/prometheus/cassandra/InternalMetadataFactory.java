package com.zegelin.prometheus.cassandra;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;

import java.net.InetAddress;
import java.util.Optional;
import java.util.Set;



public class InternalMetadataFactory extends MetadataFactory {
    private static Optional<CFMetaData> getCFMetaData(final String keyspaceName, final String tableName) {
        return Optional.ofNullable(Schema.instance.getCFMetaData(keyspaceName, tableName));
    }

    @Override
    public Optional<IndexMetadata> indexMetadata(final String keyspaceName, final String tableName, final String indexName) {
        return getCFMetaData(keyspaceName, tableName)
                .flatMap(m -> m.getIndexes().get(indexName))
                .map(m -> {
                    final IndexMetadata.IndexType indexType = IndexMetadata.IndexType.valueOf(m.kind.toString());
                    final Optional<String> className = Optional.ofNullable(m.options.get("class_name"));

                    return new IndexMetadata() {
                        @Override
                        public IndexType indexType() {
                            return indexType;
                        }

                        @Override
                        public Optional<String> customClassName() {
                            return className;
                        }
                    };
                });
    }

    @Override
    public Optional<TableMetadata> tableOrViewMetadata(final String keyspaceName, final String tableOrViewName) {
        return getCFMetaData(keyspaceName, tableOrViewName)
                .map(m -> new TableMetadata() {
                    @Override
                    public String compactionStrategyClassName() {
                        return m.params.compaction.klass().getCanonicalName();
                    }

                    @Override
                    public boolean isView() {
                        return m.isView();
                    }
                });
    }

    @Override
    public Set<String> keyspaces() {
        return Schema.instance.getKeyspaces();
    }

    @Override
    public Optional<EndpointMetadata> endpointMetadata(final InetAddress endpoint) {
        return Optional.empty();
//        final IEndpointSnitch endpointSnitch = DatabaseDescriptor.getEndpointSnitch();
//
//        endpointSnitch.getDatacenter(endpoint)
//
//        return Optional.ofNullable(endpointSnitch).map(snitch -> {
//
//        })
//
//        return Gossiper.instance.getEndpointStates()
    }
}
