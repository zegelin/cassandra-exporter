package com.zegelin.prometheus.cassandra;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;

import java.util.Optional;
import java.util.UUID;


public class InternalMetadataFactory implements MetadataFactory {
    private static Optional<CFMetaData> getCFMetaData(final String keyspaceName, final String tableName) {
        return Optional.ofNullable(Schema.instance.getCFMetaData(keyspaceName, tableName));
    }

    @Override
    public Optional<IndexMetadata> indexMetadata(final String keyspaceName, final String tableName, final String indexName) {
        return getCFMetaData(keyspaceName, tableName)
                .flatMap(m -> m.getIndexes().get(indexName))
                .map(m -> {
                    final IndexMetadata.Kind kind = IndexMetadata.Kind.valueOf(m.kind.toString());
                    final Optional<String> className = Optional.ofNullable(m.options.get("class_name"));

                    return new IndexMetadata() {
                        @Override
                        public Kind kind() {
                            return kind;
                        }

                        @Override
                        public UUID id() {
                            return m.id;
                        }

                        @Override
                        public Optional<String> customClassName() {
                            return className;
                        }
                    };
                });
    }

    @Override
    public Optional<TableMetadata> tableOrViewMetadata(final String keyspaceName, final String tableName) {
        return getCFMetaData(keyspaceName, tableName)
                .map(m -> {
                    return new TableMetadata() {
                        @Override
                        public UUID id() {
                            return m.cfId;
                        }
                    };
                });
    }
}
