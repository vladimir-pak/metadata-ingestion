package com.gpb.metadata.ingestion.snapshot;

/**
 * Минимальное состояние Table из OpenMetadata, необходимое ingestion-процессу.
 */
public record TableSnapshotEntry(
        String id,
        boolean projectEntity
) {
}
