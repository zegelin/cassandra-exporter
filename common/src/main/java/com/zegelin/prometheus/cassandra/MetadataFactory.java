package com.zegelin.prometheus.cassandra;

import java.util.Optional;
import java.util.UUID;

public interface MetadataFactory {
    interface IndexMetadata {
        enum Kind {
            KEYS,
            CUSTOM,
            COMPOSITES
        }

        Kind kind();

        UUID id();

        Optional<String> customClassName();
    }

    interface TableMetadata {
        UUID id();
    }

    Optional<IndexMetadata> indexMetadata(String keyspaceName, String tableName, String indexName);

    Optional<TableMetadata> tableOrViewMetadata(String keyspaceName, String tableName);
}
