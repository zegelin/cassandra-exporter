package com.zegelin.prometheus.cassandra;

import java.util.Optional;
import java.util.UUID;

public interface MetadataFactory {
    interface IndexMetadata {
        enum IndexType {
            KEYS,
            CUSTOM,
            COMPOSITES
        }

        IndexType indexType();

        UUID id();

        Optional<String> customClassName();
    }

    interface TableMetadata {
        UUID id();

        boolean isView();
    }

    Optional<IndexMetadata> indexMetadata(String keyspaceName, String tableName, String indexName);

    Optional<TableMetadata> tableOrViewMetadata(String keyspaceName, String tableName);
}
