package com.gpb.metadata.ingestion.cache.dto;

import com.gpb.metadata.ingestion.model.Metadata;

/**
 * Rename/move within the same EntityId.
 *
 * The old FQN is taken from the last committed Ignite state.
 * The current metadata object is loaded from PostgreSQL and is used for PUT.
 *
 * Ignite must be updated only after BOTH operations succeed logically:
 * 1) PUT current entity;
 * 2) DELETE old FQN (HTTP 404 is treated as already satisfied).
 */
public record MetadataRename<T extends Metadata>(
        String oldFqn,
        T current) {
}
