package com.gpb.metadata.ingestion.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.cache.Cache;

import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;

import com.gpb.metadata.ingestion.cache.CacheComparisonResult;
import com.gpb.metadata.ingestion.cache.CacheSyncMode;
import com.gpb.metadata.ingestion.cache.MetadataCacheHandle;
import com.gpb.metadata.ingestion.cache.dto.MetadataCacheEntry;
import com.gpb.metadata.ingestion.cache.dto.MetadataFingerprint;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.exceptions.DeleteThresholdExceededException;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.Metadata;
import com.gpb.metadata.ingestion.properties.MetadataCacheProperties;
import com.gpb.metadata.ingestion.repository.MetadataRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractMetadataCacheService<T extends Metadata> {

    protected final MetadataCacheCoordinator cacheCoordinator;
    protected final MetadataCacheProperties cacheProperties;
    protected final MetadataRepository<T> repository;
    protected final DbObjectType dbObjectTypeType;

    @org.springframework.beans.factory.annotation.Value("${ord.delete.threshold:70}")
    protected int deleteThreshold;

    protected AbstractMetadataCacheService(
            MetadataCacheCoordinator cacheCoordinator,
            MetadataCacheProperties cacheProperties,
            MetadataRepository<T> repository,
            DbObjectType type) {

        this.cacheCoordinator = cacheCoordinator;
        this.cacheProperties = cacheProperties;
        this.repository = repository;
        this.dbObjectTypeType = type;
    }

    /**
     * Build a synchronization plan without mutating committed Ignite state.
     *
     * NORMAL mode is deliberately memory efficient: PostgreSQL fingerprints are
     * loaded once and the Ignite baseline is streamed. We do NOT create a second
     * committed Map in heap. Each matching source fingerprint is removed from
     * the current map; the remaining keys are NEW entities.
     *
     * RECONCILIATION mode is entered only when no trusted baseline exists. Only
     * compact source fingerprints are retained here; full objects are loaded in
     * bounded batches while OpenMetadata is re-applied before READY is marked.
     */
    public CacheComparisonResult<T> synchronizeWithDatabase(
            ServiceType serviceType,
            String tableName,
            String serviceName) {

        MetadataCacheHandle handle = cacheCoordinator.prepare(
                serviceType,
                dbObjectTypeType,
                tableName,
                serviceName
        );

        Map<EntityId, MetadataFingerprint> current =
                repository.findFingerprintsByServiceName(
                        tableName,
                        serviceName
                );

        if (handle.mode() == CacheSyncMode.RECONCILIATION) {
            return buildReconciliationResult(
                    tableName,
                    serviceName,
                    current
            );
        }

        return buildNormalDelta(
                handle.cache(),
                tableName,
                serviceName,
                current,
                handle.migratedFromLegacy()
        );
    }

    /**
     * Commit only OpenMetadata-successful PUTs/renames.
     * Data is sent to Thin Client in bounded chunks to avoid oversized network
     * messages and a second large compact-entry map in heap.
     */
    public void commitSuccessfulUpserts(
            ServiceType serviceType,
            String serviceName,
            Map<EntityId, T> successful) {

        if (successful == null || successful.isEmpty()) {
            return;
        }

        ClientCache<EntityId, MetadataCacheEntry> cache =
                cacheCoordinator.runtimeCache(
                        serviceType,
                        dbObjectTypeType,
                        serviceName
                );

        int batchSize = cacheProperties.getCommitBatchSize();
        Map<EntityId, MetadataCacheEntry> batch =
                new HashMap<>(Math.min(batchSize, successful.size()));

        int committed = 0;

        for (Map.Entry<EntityId, T> entry : successful.entrySet()) {
            T metadata = entry.getValue();

            batch.put(
                    entry.getKey(),
                    new MetadataCacheEntry(
                            metadata.getHashData(),
                            metadata.getFqn()
                    )
            );

            if (batch.size() >= batchSize) {
                cache.putAll(batch);
                committed += batch.size();
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            cache.putAll(batch);
            committed += batch.size();
        }

        log.info(
                "Committed {} successful {} upserts to Ignite. " +
                "serviceType={}, service={}",
                committed,
                dbObjectTypeType,
                serviceType,
                serviceName
        );
    }

    /**
     * Commit only logically successful DELETEs in bounded removeAll batches.
     */
    public void commitSuccessfulDeletes(
            ServiceType serviceType,
            String serviceName,
            Collection<EntityId> successfulIds) {

        if (successfulIds == null || successfulIds.isEmpty()) {
            return;
        }

        ClientCache<EntityId, MetadataCacheEntry> cache =
                cacheCoordinator.runtimeCache(
                        serviceType,
                        dbObjectTypeType,
                        serviceName
                );

        int batchSize = cacheProperties.getCommitBatchSize();
        Set<EntityId> batch = new LinkedHashSet<>(batchSize);
        int committed = 0;

        for (EntityId id : successfulIds) {
            batch.add(id);

            if (batch.size() >= batchSize) {
                cache.removeAll(batch);
                committed += batch.size();
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            cache.removeAll(batch);
            committed += batch.size();
        }

        log.info(
                "Committed {} successful {} deletes to Ignite. " +
                "serviceType={}, service={}",
                committed,
                dbObjectTypeType,
                serviceType,
                serviceName
        );
    }

    /**
     * Mark cold-start reconciliation complete. This must be invoked only after
     * BOTH upsert and orphan-delete phases have completed without entity errors.
     */
    public void markReconciliationComplete(
            ServiceType serviceType,
            String serviceName,
            long committedEntries) {

        cacheCoordinator.markReady(
                serviceType,
                dbObjectTypeType,
                serviceName,
                committedEntries,
                "full-reconciliation"
        );
    }

    public void validateReconciliationDeleteThreshold(
            String serviceName,
            int openMetadataManagedCount,
            int orphanDeleteCount) {

        validateDeleteThreshold(
                serviceName,
                openMetadataManagedCount,
                orphanDeleteCount,
                "OpenMetadata reconciliation"
        );
    }

    public void destroyRuntimeCache(
            ServiceType serviceType,
            String tableName,
            String serviceName) {

        cacheCoordinator.destroy(
                serviceType,
                dbObjectTypeType,
                tableName,
                serviceName
        );
    }

    public Set<String> getAllRuntimeCaches() {
        return cacheCoordinator.getAllRuntimeCaches(dbObjectTypeType);
    }

    public DbObjectType getDbObjectType() {
        return dbObjectTypeType;
    }

    private CacheComparisonResult<T> buildReconciliationResult(
            String tableName,
            String serviceName,
            Map<EntityId, MetadataFingerprint> current) {

        CacheComparisonResult<T> result =
                new CacheComparisonResult<>(CacheSyncMode.RECONCILIATION);

        /*
         * Keep reconciliation compact. TableMetadata can contain very large
         * JSON payloads, therefore loading every full source object here can
         * cause a cold-start memory spike. MetadataHandlerServiceImpl loads
         * these IDs later in bounded reconciliation batches.
         */
        result.setReconciliationFingerprints(current);

        log.warn(
                "Full reconciliation plan {} table={} service={}: sourceEntities={}",
                dbObjectTypeType,
                tableName,
                serviceName,
                current.size()
        );

        return result;
    }

    /**
     * Loads full source metadata for a bounded set of IDs. Intended for
     * reconciliation batches; normal delta processing already performs its own
     * minimal full-load.
     */
    public Map<EntityId, T> loadFullMetadata(
            String tableName,
            String serviceName,
            Collection<EntityId> ids) {

        if (ids == null || ids.isEmpty()) {
            return Map.of();
        }

        Map<EntityId, T> loaded = repository.findByIds(
                tableName,
                serviceName,
                ids
        );

        if (loaded.size() != ids.size()) {
            for (EntityId id : ids) {
                requireLoaded(loaded, id);
            }
        }

        return loaded;
    }

    private CacheComparisonResult<T> buildNormalDelta(
            ClientCache<EntityId, MetadataCacheEntry> cache,
            String tableName,
            String serviceName,
            Map<EntityId, MetadataFingerprint> current,
            boolean migratedFromLegacy) {

        int currentCount = current.size();
        int committedCount = 0;

        Set<EntityId> modifiedIds = new LinkedHashSet<>();
        Set<EntityId> renamedIds = new LinkedHashSet<>();
        Map<EntityId, String> oldFqnByRenamedId = new LinkedHashMap<>();
        Map<EntityId, String> deleted = new LinkedHashMap<>();

        ScanQuery<EntityId, MetadataCacheEntry> query =
                new ScanQuery<EntityId, MetadataCacheEntry>()
                        .setPageSize(cacheProperties.getScanPageSize());

        try (QueryCursor<Cache.Entry<EntityId, MetadataCacheEntry>> cursor =
                     cache.query(query)) {

            for (Cache.Entry<EntityId, MetadataCacheEntry> entry : cursor) {
                committedCount++;

                EntityId id = entry.getKey();
                MetadataCacheEntry old = entry.getValue();

                /*
                 * Removal is intentional. After the scan, current contains only
                 * source entities absent from the committed baseline => NEW.
                 */
                MetadataFingerprint currentFingerprint = current.remove(id);

                if (currentFingerprint == null) {
                    deleted.put(id, old.getFqn());
                    continue;
                }

                if (!Objects.equals(
                        old.getFqn(),
                        currentFingerprint.getFqn()
                )) {
                    renamedIds.add(id);
                    oldFqnByRenamedId.put(id, old.getFqn());
                    continue;
                }

                if (!Objects.equals(
                        old.getHashData(),
                        currentFingerprint.getHashData()
                )) {
                    modifiedIds.add(id);
                }
            }
        }

        Set<EntityId> newIds = new LinkedHashSet<>(current.keySet());

        validateDeleteThreshold(
                serviceName,
                committedCount,
                deleted.size(),
                "runtime cache"
        );

        Set<EntityId> fullLoadIds = new LinkedHashSet<>(newIds);
        fullLoadIds.addAll(modifiedIds);
        fullLoadIds.addAll(renamedIds);

        Map<EntityId, T> full = repository.findByIds(
                tableName,
                serviceName,
                fullLoadIds
        );

        CacheComparisonResult<T> result =
                new CacheComparisonResult<>(CacheSyncMode.NORMAL);

        for (EntityId id : newIds) {
            result.addNewRecord(id, requireLoaded(full, id));
        }

        for (EntityId id : modifiedIds) {
            result.addModifiedRecord(id, requireLoaded(full, id));
        }

        for (EntityId id : renamedIds) {
            result.addRenamedRecord(
                    id,
                    oldFqnByRenamedId.get(id),
                    requireLoaded(full, id)
            );
        }

        deleted.forEach(result::addDeletedRecord);

        log.info(
                "Delta {} table={} service={}: committed={}, current={}, " +
                "new={}, modified={}, renamed={}, deleted={}, migratedV2={}",
                dbObjectTypeType,
                tableName,
                serviceName,
                committedCount,
                currentCount,
                newIds.size(),
                modifiedIds.size(),
                renamedIds.size(),
                deleted.size(),
                migratedFromLegacy
        );

        return result;
    }

    private T requireLoaded(
            Map<EntityId, T> full,
            EntityId id) {

        T value = full.get(id);

        if (value == null) {
            throw new IllegalStateException(
                    "Full metadata was not loaded for "
                    + dbObjectTypeType
                    + " EntityId="
                    + id
            );
        }

        return value;
    }

    private void validateDeleteThreshold(
            String serviceName,
            int baselineCount,
            int deleteCount,
            String baselineName) {

        if (baselineCount <= 0 || deleteCount <= 0) {
            return;
        }

        double deletePercent =
                ((double) deleteCount / baselineCount) * 100.0;

        if (deletePercent <= deleteThreshold) {
            return;
        }

        throw new DeleteThresholdExceededException(
                String.format(
                        "Potential false mass deletion detected. " +
                        "service=%s, objectType=%s, baseline=%s, " +
                        "baselineCount=%d, deleted=%d, deletePercent=%.2f%%, " +
                        "threshold=%d%%",
                        serviceName,
                        dbObjectTypeType,
                        baselineName,
                        baselineCount,
                        deleteCount,
                        deletePercent,
                        deleteThreshold
                )
        );
    }

    @jakarta.annotation.PostConstruct
    public void validateConfiguration() {
        if (deleteThreshold < 0 || deleteThreshold > 100) {
            throw new IllegalStateException(
                    "ord.delete.threshold must be between 0 and 100, actual="
                    + deleteThreshold
            );
        }
    }
}
