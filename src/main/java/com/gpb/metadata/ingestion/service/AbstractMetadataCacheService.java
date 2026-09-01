package com.gpb.metadata.ingestion.service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.gpb.metadata.ingestion.cache.CacheComparisonResult;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.exceptions.DeleteThresholdExceededException;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.Metadata;
import com.gpb.metadata.ingestion.repository.MetadataRepository;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class AbstractMetadataCacheService<T extends Metadata> {

    @Qualifier("igniteInstance")
    protected final Ignite ignite;
    protected final MetadataRepository<T> repository;
    protected final DbObjectType dbObjectTypeType;

    protected final Map<String, IgniteCache<EntityId, T>> runtimeCaches = new ConcurrentHashMap<>();
    protected final String TEMP_CACHE_PREFIX = "temp_%s_";
    protected final String CACHE_NAME = "runtime_%s_";

    @Value("${ord.delete.threshold:70}")
    protected int deleteThreshold;

    /**
     * Получить или создать runtime кэш по serviceName + schemaName
     */
    protected IgniteCache<EntityId, T> getOrCreateRuntimeCache(String schemaName, String serviceName) {
        String cacheKey = schemaName + "_" + serviceName;
        String cacheName = String.format(CACHE_NAME, dbObjectTypeType.name()) + cacheKey;

        return runtimeCaches.computeIfAbsent(cacheName, name -> {
            CacheConfiguration<EntityId, T> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName(name);
            cacheCfg.setCacheMode(CacheMode.REPLICATED);
            cacheCfg.setIndexedTypes(EntityId.class, getMetadataClass());

            return ignite.getOrCreateCache(cacheCfg);
        });
    }

    /**
     * Создать временный кэш из данных БД
     */
    protected IgniteCache<EntityId, T> createTempCacheFromDatabase(String schemaName, String serviceName) {
        String tempCacheName = String.format(TEMP_CACHE_PREFIX, dbObjectTypeType.name()) +
                schemaName + "_" + serviceName + "_" + System.currentTimeMillis();

        CacheConfiguration<EntityId, T> tempCacheCfg = new CacheConfiguration<>();
        tempCacheCfg.setName(tempCacheName);
        tempCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        tempCacheCfg.setIndexedTypes(EntityId.class, getMetadataClass());

        IgniteCache<EntityId, T> tempCache = ignite.getOrCreateCache(tempCacheCfg);

        // Загружаем данные из БД
        List<T> dbData = repository.findByServiceName(schemaName, serviceName);
        Map<EntityId, T> tempData = dbData.stream()
                .collect(Collectors.toMap(
                        Metadata::getId,
                        Function.identity()
                ));

        tempCache.putAll(tempData);
        return tempCache;
    }

    /**
     * Сравнить runtime кэш с временным (из БД) и найти изменения
     */
    protected CacheComparisonResult<T> compareCaches(
            String schemaName,
            String serviceName) {

        IgniteCache<EntityId, T> runtimeCache =
                getOrCreateRuntimeCache(schemaName, serviceName);

        IgniteCache<EntityId, T> tempCache =
                createTempCacheFromDatabase(schemaName, serviceName);

        try {
            CacheComparisonResult<T> result =
                    new CacheComparisonResult<>();

            Set<EntityId> runtimeKeys =
                    getAllKeys(runtimeCache);

            Set<EntityId> tempKeys =
                    getAllKeys(tempCache);

            findNewRecords(
                    runtimeCache,
                    tempCache,
                    runtimeKeys,
                    tempKeys,
                    result
            );

            findModifiedRecords(
                    runtimeCache,
                    tempCache,
                    runtimeKeys,
                    tempKeys,
                    result
            );

            findDeletedRecords(
                    runtimeCache,
                    runtimeKeys,
                    tempKeys,
                    result
            );

            log.info(
                    "Comparison result for {} (schema={}, service={}): " +
                    "previousCount={}, currentCount={}, " +
                    "newRecords={}, modifiedRecords={}, deletedRecords={}",
                    dbObjectTypeType.getName(),
                    schemaName,
                    serviceName,
                    runtimeKeys.size(),
                    tempKeys.size(),
                    result.getNewRecords().size(),
                    result.getModifiedRecords().size(),
                    result.getDeletedRecords().size()
            );

            /*
            * ВАЖНО:
            * проверяем ДО updateRuntimeCache().
            */
            validateDeleteThreshold(
                    serviceName,
                    runtimeKeys.size(),
                    result.getDeletedRecords().size()
            );

            return result;

        } finally {
            tempCache.destroy();
        }
    }

    /**
     * Обновить runtime кэш на основе временного кэша
     */
    protected void updateRuntimeCache(String schemaName, String serviceName, CacheComparisonResult<T> changes) {
        IgniteCache<EntityId, T> runtimeCache = getOrCreateRuntimeCache(schemaName, serviceName);

        if (!changes.getDeletedRecords().isEmpty()) {
            runtimeCache.removeAll(changes.getDeletedRecords().keySet());
        }

        Map<EntityId, T> recordsToUpdate = new HashMap<>();
        recordsToUpdate.putAll(changes.getNewRecords());
        recordsToUpdate.putAll(changes.getModifiedRecords());

        if (!recordsToUpdate.isEmpty()) {
            runtimeCache.putAll(recordsToUpdate);
        }
    }

    /**
     * Полная синхронизация: сравнить и обновить
     */
    public CacheComparisonResult<T> synchronizeWithDatabase(
            String schemaName,
            String serviceName) {
        /*
        * compareCaches внутри выполняет threshold validation.
        * Если threshold превышен,
        * отсюда будет выброшено исключение.
        */
        CacheComparisonResult<T> changes =
                compareCaches(
                        schemaName,
                        serviceName
                );

        /*
        * Сюда мы попадём ТОЛЬКО если validation прошёл.
        */
        updateRuntimeCache(
                schemaName,
                serviceName,
                changes
        );

        return changes;
    }

    private void findNewRecords(
            IgniteCache<EntityId, T> runtimeCache,
            IgniteCache<EntityId, T> tempCache,
            Set<EntityId> runtimeKeys,
            Set<EntityId> tempKeys,
            CacheComparisonResult<T> result) {

        Set<EntityId> newKeys =
                new HashSet<>(tempKeys);

        newKeys.removeAll(runtimeKeys);

        for (EntityId key : newKeys) {
            T tempData = tempCache.get(key);

            if (tempData != null) {
                result.addNewRecord(
                        key,
                        tempData
                );
            }
        }
    }

    private void findModifiedRecords(
            IgniteCache<EntityId, T> runtimeCache,
            IgniteCache<EntityId, T> tempCache,
            Set<EntityId> runtimeKeys,
            Set<EntityId> tempKeys,
            CacheComparisonResult<T> result) {

        Set<EntityId> commonKeys =
                new HashSet<>(runtimeKeys);

        commonKeys.retainAll(tempKeys);

        for (EntityId key : commonKeys) {

            T runtimeData =
                    runtimeCache.get(key);

            T tempData =
                    tempCache.get(key);

            if (runtimeData != null
                    && tempData != null
                    && !Objects.equals(
                            runtimeData.getHashData(),
                            tempData.getHashData()
                    )) {

                result.addModifiedRecord(
                        key,
                        tempData
                );
            }
        }
    }

    private void findDeletedRecords(
            IgniteCache<EntityId, T> runtimeCache,
            Set<EntityId> runtimeKeys,
            Set<EntityId> tempKeys,
            CacheComparisonResult<T> result) {

        Set<EntityId> deletedKeys =
                new HashSet<>(runtimeKeys);

        deletedKeys.removeAll(tempKeys);

        for (EntityId key : deletedKeys) {

            T runtimeData =
                    runtimeCache.get(key);

            if (runtimeData != null) {
                result.addDeletedRecord(
                        key,
                        runtimeData
                );
            }
        }
    }

    private Set<EntityId> getAllKeys(IgniteCache<EntityId, T> cache) {
        Set<EntityId> keys = ConcurrentHashMap.newKeySet();

        cache.query(new ScanQuery<EntityId, T>())
                .forEach(entry -> keys.add(entry.getKey()));

        return keys;
    }

    /**
     * Удалить runtime кэш для serviceName
     */
    public void destroyRuntimeCache(String schemaName, String serviceName) {
        String cacheKey = schemaName + "_" + serviceName;
        String cacheName = String.format(CACHE_NAME, dbObjectTypeType.name()) + cacheKey;

        runtimeCaches.remove(cacheName);

        if (ignite.cache(cacheName) != null) {  // проверка существования
            ignite.destroyCache(cacheName);
        }
    }

    private void validateDeleteThreshold(
            String serviceName,
            int previousCount,
            int deleteCount) {

        /*
        * Защиту применяем только к TABLE и SCHEMA.
        */
        if (dbObjectTypeType != DbObjectType.TABLE
                && dbObjectTypeType != DbObjectType.SCHEMA) {
            return;
        }

        /*
        * Первый запуск.
        *
        * Runtime cache ещё пустой, поэтому вычислять процент
        * удаления не относительно чего.
        */
        if (previousCount == 0) {
            return;
        }

        /*
        * Ничего не удаляется.
        */
        if (deleteCount == 0) {
            return;
        }

        double deletePercent =
                ((double) deleteCount / previousCount) * 100.0;

        log.info(
                "Delete threshold validation. " +
                "DbService=\"{}\", objectType={}, " +
                "previousCount={}, deleteCount={}, " +
                "deletePercent={}%, threshold={}%",
                serviceName,
                dbObjectTypeType.name(),
                previousCount,
                deleteCount,
                String.format("%.2f", deletePercent),
                deleteThreshold
        );

        if (deletePercent <= deleteThreshold) {
            return;
        }

        String message = String.format(
                "Potential false mass deletion detected. " +
                "DbService=\"%s\", objectType=%s, " +
                "previousCount=%d, deleteCount=%d, " +
                "deletePercent=%.2f%%, threshold=%d%%. " +
                "Runtime cache will not be updated.",
                serviceName,
                dbObjectTypeType.name(),
                previousCount,
                deleteCount,
                deletePercent,
                deleteThreshold
        );

        log.error(message);

        throw new DeleteThresholdExceededException(message);
    }

    @PostConstruct
    public void validateConfiguration() {

        if (deleteThreshold < 0
                || deleteThreshold > 100) {

            throw new IllegalStateException(
                    "ord.delete.threshold must be between 0 and 100, actual="
                    + deleteThreshold
            );
        }
    }

    /**
     * Получить все runtime кэши
     */
    public Set<String> getAllRuntimeCaches() {
        return new HashSet<>(runtimeCaches.keySet()); // ключи уже cacheName
    }

    public IgniteCache<EntityId, T> getRuntimeCache(String schemaName, String serviceName) {
        return getOrCreateRuntimeCache(schemaName, serviceName);
    };

    /**
     * Абстрактный метод для получения класса метаданных
     */
    protected abstract Class<T> getMetadataClass();
}
