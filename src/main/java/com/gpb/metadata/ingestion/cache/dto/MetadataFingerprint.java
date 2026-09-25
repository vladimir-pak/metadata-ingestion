package com.gpb.metadata.ingestion.cache.dto;

import com.gpb.metadata.ingestion.model.EntityId;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Compact PostgreSQL snapshot used only for delta detection.
 */
@Getter
@AllArgsConstructor
public class MetadataFingerprint {
    private final EntityId id;
    private final String hashData;
    private final String fqn;
}
