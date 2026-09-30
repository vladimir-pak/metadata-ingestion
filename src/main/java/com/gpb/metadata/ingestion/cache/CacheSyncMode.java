package com.gpb.metadata.ingestion.cache;

/**
 * NORMAL         - committed baseline exists in Ignite and ordinary delta is calculated.
 * RECONCILIATION - no trusted baseline exists; all current source entities are re-applied
 *                  to OpenMetadata and OpenMetadata orphans are deleted before the cache
 *                  is marked READY.
 */
public enum CacheSyncMode {
    NORMAL,
    RECONCILIATION
}
