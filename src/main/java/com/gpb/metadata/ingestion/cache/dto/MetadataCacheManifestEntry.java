package com.gpb.metadata.ingestion.cache.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Marker proving that the logical runtime cache contains a trusted baseline.
 *
 * The marker is written LAST, after reconciliation/migration has completed.
 * Absence of the marker means that the runtime cache must not be trusted even
 * if a physical Ignite cache with the expected name already exists.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MetadataCacheManifestEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private int cacheVersion;
    private String runtimeCacheName;
    private long initializedAtEpochMs;
    private long committedEntries;
    private String initializedBy;
}
