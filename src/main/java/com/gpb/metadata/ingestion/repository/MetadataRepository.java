package com.gpb.metadata.ingestion.repository;

import java.util.Collection;
import java.util.Map;

import com.gpb.metadata.ingestion.cache.dto.MetadataFingerprint;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.Metadata;

public interface MetadataRepository<T extends Metadata> {

    /**
     * Lightweight snapshot used for delta detection.
     * Must not read JSON/data payloads.
     *
     * Returning Map directly avoids creating an intermediate List and then
     * converting it to another Map in the cache layer.
     */
    Map<EntityId, MetadataFingerprint> findFingerprintsByServiceName(
            String tableName,
            String serviceName
    );

    /**
     * Loads full metadata only for NEW/MODIFIED/RENAMED entities.
     */
    Map<EntityId, T> findByIds(
            String tableName,
            String serviceName,
            Collection<EntityId> ids
    );
}
