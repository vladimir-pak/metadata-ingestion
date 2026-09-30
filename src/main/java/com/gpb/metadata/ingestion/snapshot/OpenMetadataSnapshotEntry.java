package com.gpb.metadata.ingestion.snapshot;

/**
 * Minimal OpenMetadata state needed for reconciliation and project-entity checks.
 */
public record OpenMetadataSnapshotEntry(
        String id,
        String fqn,
        boolean projectEntity) {
}
