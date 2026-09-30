package com.gpb.metadata.ingestion.service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.cache.Cache;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.springframework.stereotype.Service;

import com.gpb.metadata.ingestion.cache.CacheSyncMode;
import com.gpb.metadata.ingestion.cache.MetadataCacheHandle;
import com.gpb.metadata.ingestion.cache.dto.MetadataCacheEntry;
import com.gpb.metadata.ingestion.cache.dto.MetadataCacheManifestEntry;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.properties.MetadataCacheProperties;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetadataCacheCoordinator {

    private static final int CACHE_VERSION = 3;
    private static final String MANIFEST_CACHE_NAME =
            "metadata_ingestion_cache_manifest_v1";

    private static final String V3_CACHE_NAME =
            "runtime_v3_%s_%s_%s_%s";

    private static final String V2_CACHE_NAME =
            "runtime_v2_%s_%s_%s";

    private final IgniteClient igniteClient;
    private final MetadataCacheProperties properties;

    private final Map<String, ClientCache<EntityId, MetadataCacheEntry>>
            runtimeCacheProxies = new ConcurrentHashMap<>();

    private final Map<String, ReentrantLock> identityLocks =
            new ConcurrentHashMap<>();

    private volatile ClientCache<String, MetadataCacheManifestEntry> manifestCacheProxy;

    /**
     * Resolve a trusted runtime cache.
     *
     * Order:
     * 1. READY v3 manifest + existing physical cache -> NORMAL.
     * 2. No trusted v3 + existing v2 -> streaming migration -> NORMAL.
     * 3. No trusted baseline -> clear/create v3 -> RECONCILIATION.
     *
     * The manifest is deliberately separate from the data cache: an empty cache
     * can be a valid state, therefore cache existence/size cannot prove that the
     * baseline is initialized.
     */
    public MetadataCacheHandle prepare(
            ServiceType serviceType,
            DbObjectType objectType,
            String legacyTableName,
            String serviceName) {

        String manifestKey = manifestKey(
                serviceType,
                objectType,
                serviceName
        );

        ReentrantLock lock = identityLocks.computeIfAbsent(
                manifestKey,
                ignored -> new ReentrantLock()
        );

        lock.lock();
        try {
            String v3Name = buildV3CacheName(
                    serviceType,
                    objectType,
                    serviceName
            );

            String v2Name = buildV2CacheName(
                    objectType,
                    legacyTableName,
                    serviceName
            );

            ClientCache<String, MetadataCacheManifestEntry> manifestCache =
                    manifestCache();

            Collection<String> existingNames = igniteClient.cacheNames();

            MetadataCacheManifestEntry manifest = manifestCache.get(manifestKey);
            boolean hadManifest = manifest != null;

            if (isTrustedManifest(manifest, v3Name)
                    && existingNames.contains(v3Name)) {

                return new MetadataCacheHandle(
                        runtimeCache(v3Name),
                        CacheSyncMode.NORMAL,
                        v3Name,
                        false
                );
            }

            if (manifest != null) {
                log.warn(
                        "Cache manifest is stale. key={}, expectedCache={}, " +
                        "manifestCache={}. Baseline will be rebuilt.",
                        manifestKey,
                        v3Name,
                        manifest.getRuntimeCacheName()
                );
                manifestCache.remove(manifestKey);
            }

            boolean v3AlreadyExists = existingNames.contains(v3Name);

            /*
             * Migrate v2 only when v3 has never been created. If an unmarked
             * v3 already exists, it can be either a partial crashed operation
             * or a newer baseline whose manifest was lost. Reusing old v2 in
             * that ambiguous case could roll state backwards, therefore we
             * prefer a safe full reconciliation.
             */
            if (properties.isMigrateLegacyV2()
                    && !hadManifest
                    && !v3AlreadyExists
                    && existingNames.contains(v2Name)) {

                long migrated = migrateLegacyV2(
                        v2Name,
                        v3Name
                );

                manifestCache.put(
                        manifestKey,
                        readyManifest(
                                v3Name,
                                migrated,
                                "legacy-v2-migration"
                        )
                );

                if (properties.isDestroyLegacyAfterMigration()) {
                    destroyIfExists(v2Name);
                }

                log.info(
                        "Migrated Ignite baseline v2 -> v3. serviceType={}, " +
                        "objectType={}, service={}, entries={}, oldCache={}, newCache={}",
                        serviceType,
                        objectType,
                        serviceName,
                        migrated,
                        v2Name,
                        v3Name
                );

                return new MetadataCacheHandle(
                        runtimeCache(v3Name),
                        CacheSyncMode.NORMAL,
                        v3Name,
                        true
                );
            }

            /*
             * An unmarked v3 cache can contain a partial reconciliation from a
             * crashed process. It must never be trusted. Clear it before a new
             * full reconciliation attempt.
             */
            ClientCache<EntityId, MetadataCacheEntry> v3 =
                    getOrCreateV3Cache(v3Name);
            v3.clear();

            log.warn(
                    "No trusted Ignite baseline. Full reconciliation required. " +
                    "serviceType={}, objectType={}, service={}, cache={}",
                    serviceType,
                    objectType,
                    serviceName,
                    v3Name
            );

            return new MetadataCacheHandle(
                    v3,
                    CacheSyncMode.RECONCILIATION,
                    v3Name,
                    false
            );
        } finally {
            lock.unlock();
        }
    }

    /**
     * Access the already prepared logical v3 cache without lifecycle decisions.
     * Used by success-only commit methods during the same ingestion stage.
     */
    public ClientCache<EntityId, MetadataCacheEntry> runtimeCache(
            ServiceType serviceType,
            DbObjectType objectType,
            String serviceName) {

        return getOrCreateV3Cache(
                buildV3CacheName(
                        serviceType,
                        objectType,
                        serviceName
                )
        );
    }

    /**
     * Written LAST after reconciliation PUT/DELETE processing has no errors.
     * If the process crashes before this call, the next run safely repeats the
     * full reconciliation and clears any partial untrusted cache first.
     */
    public void markReady(
            ServiceType serviceType,
            DbObjectType objectType,
            String serviceName,
            long committedEntries,
            String initializedBy) {

        String cacheName = buildV3CacheName(
                serviceType,
                objectType,
                serviceName
        );

        manifestCache().put(
                manifestKey(
                        serviceType,
                        objectType,
                        serviceName
                ),
                readyManifest(
                        cacheName,
                        committedEntries,
                        initializedBy
                )
        );

        log.info(
                "Ignite baseline marked READY. serviceType={}, objectType={}, " +
                "service={}, cache={}, committedEntries={}",
                serviceType,
                objectType,
                serviceName,
                cacheName,
                committedEntries
        );
    }

    /**
     * Explicit clean now safely invalidates the manifest as well. The next run
     * automatically enters RECONCILIATION instead of silently treating every
     * source entity as NEW against an unknown OpenMetadata state.
     */
    public void destroy(
            ServiceType serviceType,
            DbObjectType objectType,
            String legacyTableName,
            String serviceName) {

        String key = manifestKey(
                serviceType,
                objectType,
                serviceName
        );

        manifestCache().remove(key);

        String v3Name = buildV3CacheName(
                serviceType,
                objectType,
                serviceName
        );
        String v2Name = buildV2CacheName(
                objectType,
                legacyTableName,
                serviceName
        );

        runtimeCacheProxies.remove(v3Name);
        runtimeCacheProxies.remove(v2Name);

        destroyIfExists(v3Name);
        destroyIfExists(v2Name);

        log.info(
                "Destroyed Ignite cache baseline. serviceType={}, objectType={}, service={}",
                serviceType,
                objectType,
                serviceName
        );
    }

    public Set<String> getAllRuntimeCaches(DbObjectType objectType) {
        String marker = "_" + objectType.name() + "_";

        return igniteClient.cacheNames()
                .stream()
                .filter(name -> name.startsWith("runtime_v3_"))
                .filter(name -> name.contains(marker))
                .collect(Collectors.toSet());
    }

    private long migrateLegacyV2(
            String legacyName,
            String targetName) {

        ClientCache<EntityId, MetadataCacheEntry> legacy =
                igniteClient.cache(legacyName);

        ClientCache<EntityId, MetadataCacheEntry> target =
                getOrCreateV3Cache(targetName);

        target.clear();

        int batchSize = properties.getCommitBatchSize();
        Map<EntityId, MetadataCacheEntry> batch =
                new HashMap<>(batchSize);

        long migrated = 0L;

        ScanQuery<EntityId, MetadataCacheEntry> query =
                new ScanQuery<EntityId, MetadataCacheEntry>()
                        .setPageSize(properties.getScanPageSize());

        try (QueryCursor<Cache.Entry<EntityId, MetadataCacheEntry>> cursor =
                     legacy.query(query)) {

            for (Cache.Entry<EntityId, MetadataCacheEntry> entry : cursor) {
                batch.put(entry.getKey(), entry.getValue());

                if (batch.size() >= batchSize) {
                    target.putAll(batch);
                    migrated += batch.size();
                    batch.clear();
                }
            }
        }

        if (!batch.isEmpty()) {
            target.putAll(batch);
            migrated += batch.size();
        }

        return migrated;
    }

    private ClientCache<String, MetadataCacheManifestEntry> manifestCache() {
        ClientCache<String, MetadataCacheManifestEntry> local = manifestCacheProxy;
        if (local != null) {
            return local;
        }

        synchronized (this) {
            local = manifestCacheProxy;
            if (local == null) {
                local = igniteClient.getOrCreateCache(
                        new ClientCacheConfiguration()
                                .setName(MANIFEST_CACHE_NAME)
                                .setCacheMode(CacheMode.REPLICATED)
                );
                manifestCacheProxy = local;
            }
            return local;
        }
    }

    private ClientCache<EntityId, MetadataCacheEntry> getOrCreateV3Cache(
            String cacheName) {

        return runtimeCacheProxies.computeIfAbsent(
                cacheName,
                name -> igniteClient.getOrCreateCache(
                        new ClientCacheConfiguration()
                                .setName(name)
                                .setCacheMode(CacheMode.PARTITIONED)
                                .setBackups(properties.getBackups())
                )
        );
    }

    private ClientCache<EntityId, MetadataCacheEntry> runtimeCache(
            String cacheName) {

        return runtimeCacheProxies.computeIfAbsent(
                cacheName,
                igniteClient::cache
        );
    }

    private void destroyIfExists(String cacheName) {
        if (igniteClient.cacheNames().contains(cacheName)) {
            igniteClient.destroyCache(cacheName);
            runtimeCacheProxies.remove(cacheName);
        }
    }

    private boolean isTrustedManifest(
            MetadataCacheManifestEntry manifest,
            String expectedCacheName) {

        return manifest != null
                && manifest.getCacheVersion() == CACHE_VERSION
                && expectedCacheName.equals(manifest.getRuntimeCacheName());
    }

    private MetadataCacheManifestEntry readyManifest(
            String cacheName,
            long committedEntries,
            String initializedBy) {

        return new MetadataCacheManifestEntry(
                CACHE_VERSION,
                cacheName,
                System.currentTimeMillis(),
                committedEntries,
                initializedBy
        );
    }

    private String manifestKey(
            ServiceType serviceType,
            DbObjectType objectType,
            String serviceName) {

        return CACHE_VERSION
                + "|"
                + serviceType.name()
                + "|"
                + objectType.name()
                + "|"
                + serviceName;
    }

    private String buildV3CacheName(
            ServiceType serviceType,
            DbObjectType objectType,
            String serviceName) {

        String normalized = serviceName
                .replaceAll("[^A-Za-z0-9._-]", "_");

        if (normalized.length() > 80) {
            normalized = normalized.substring(0, 80);
        }

        return String.format(
                V3_CACHE_NAME,
                serviceType.name(),
                objectType.name(),
                normalized,
                shortHash(serviceName)
        );
    }

    private String buildV2CacheName(
            DbObjectType objectType,
            String tableName,
            String serviceName) {

        return String.format(
                V2_CACHE_NAME,
                objectType.name(),
                tableName,
                serviceName
        );
    }

    private String shortHash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(
                    value.getBytes(StandardCharsets.UTF_8)
            );

            StringBuilder result = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                result.append(String.format("%02x", bytes[i]));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "SHA-256 is not available",
                    e
            );
        }
    }
}
