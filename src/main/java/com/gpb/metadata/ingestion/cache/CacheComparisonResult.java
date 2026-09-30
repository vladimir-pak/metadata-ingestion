package com.gpb.metadata.ingestion.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.gpb.metadata.ingestion.cache.dto.MetadataFingerprint;
import com.gpb.metadata.ingestion.cache.dto.MetadataRename;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.Metadata;

/**
 * Delta prepared for OpenMetadata processing.
 *
 * NORMAL:
 *   NEW/MODIFIED/RENAMED/DELETE are calculated against trusted Ignite state.
 *
 * RECONCILIATION:
 *   only compact source fingerprints are retained for the whole run. Full
 *   metadata is loaded later in bounded batches. OpenMetadata orphan detection
 *   stays in MetadataHandlerServiceImpl because that layer owns OMD access and
 *   project-entity semantics.
 */
public class CacheComparisonResult<T extends Metadata> {

    private final CacheSyncMode syncMode;

    private final Map<EntityId, T> newRecords = new LinkedHashMap<>();
    private final Map<EntityId, T> modifiedRecords = new LinkedHashMap<>();
    private final Map<EntityId, MetadataRename<T>> renamedRecords = new LinkedHashMap<>();
    private final Map<EntityId, String> deletedRecords = new LinkedHashMap<>();

    /*
     * RECONCILIATION deliberately keeps only compact fingerprints here.
     * Full metadata is loaded later in bounded batches by the handler.
     */
    private Map<EntityId, MetadataFingerprint> reconciliationFingerprints =
            Collections.emptyMap();

    public CacheComparisonResult(CacheSyncMode syncMode) {
        this.syncMode = syncMode;
    }

    public CacheSyncMode getSyncMode() {
        return syncMode;
    }

    public boolean isReconciliation() {
        return syncMode == CacheSyncMode.RECONCILIATION;
    }

    public void addNewRecord(EntityId id, T metadata) {
        newRecords.put(id, metadata);
    }

    public void addModifiedRecord(EntityId id, T metadata) {
        modifiedRecords.put(id, metadata);
    }

    public void addRenamedRecord(EntityId id, String oldFqn, T current) {
        renamedRecords.put(id, new MetadataRename<>(oldFqn, current));
    }

    public void addDeletedRecord(EntityId id, String fqn) {
        deletedRecords.put(id, fqn);
    }

    public Map<EntityId, T> getNewRecords() {
        return Collections.unmodifiableMap(newRecords);
    }

    public Map<EntityId, T> getModifiedRecords() {
        return Collections.unmodifiableMap(modifiedRecords);
    }

    public Map<EntityId, T> getPutRecords() {
        Map<EntityId, T> result = new LinkedHashMap<>(
                newRecords.size() + modifiedRecords.size()
        );
        result.putAll(newRecords);
        result.putAll(modifiedRecords);
        return result;
    }

    public Map<EntityId, MetadataRename<T>> getRenamedRecords() {
        return Collections.unmodifiableMap(renamedRecords);
    }

    public Map<EntityId, String> getDeletedRecords() {
        return Collections.unmodifiableMap(deletedRecords);
    }

    public void setReconciliationFingerprints(
            Map<EntityId, MetadataFingerprint> fingerprints) {

        reconciliationFingerprints = fingerprints == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(fingerprints);
    }

    public Map<EntityId, MetadataFingerprint> getReconciliationFingerprints() {
        return reconciliationFingerprints;
    }

    public boolean hasChanges() {
        return !newRecords.isEmpty()
                || !modifiedRecords.isEmpty()
                || !renamedRecords.isEmpty()
                || !deletedRecords.isEmpty()
                || !reconciliationFingerprints.isEmpty();
    }

    public boolean hasUpserts() {
        return !newRecords.isEmpty()
                || !modifiedRecords.isEmpty()
                || !renamedRecords.isEmpty()
                || !reconciliationFingerprints.isEmpty();
    }
}
