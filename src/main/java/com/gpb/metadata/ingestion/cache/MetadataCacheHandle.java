package com.gpb.metadata.ingestion.cache;

import org.apache.ignite.client.ClientCache;

import com.gpb.metadata.ingestion.cache.dto.MetadataCacheEntry;
import com.gpb.metadata.ingestion.model.EntityId;

public record MetadataCacheHandle(
        ClientCache<EntityId, MetadataCacheEntry> cache,
        CacheSyncMode mode,
        String cacheName,
        boolean migratedFromLegacy) {
}
