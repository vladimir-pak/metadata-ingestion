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

import com.gpb.metadata.ingestion.cache.CacheComparisonResult;
import com.gpb.metadata.ingestion.enums.Entity;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.Metadata;
import com.gpb.metadata.ingestion.repository.MetadataRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class AbstractMetadataCacheService<T extends Metadata> {
    
    @Qualifier("igniteInstance")
    protected final Ignite ignite;
    protected final MetadataRepository<T> repository;
    protected final Entity entityType;
    
    protected final Map<String, IgniteCache<EntityId, T>> runtimeCaches = new ConcurrentHashMap<>();
    protected final String TEMP_CACHE_PREFIX = "temp_%s_";
    protected final String CACHE_NAME = "runtime_%s_";
    
    /**
     * Получить или создать runtime кэш по serviceName
     */
    protected IgniteCache<EntityId, T> getOrCreateRuntimeCache(String serviceName) {
        return runtimeCaches.computeIfAbsent(serviceName, key -> {
            String cacheName = String.format(CACHE_NAME, entityType.name()) + serviceName;
            
            CacheConfiguration<EntityId, T> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName(cacheName);
            cacheCfg.setCacheMode(CacheMode.REPLICATED);
            cacheCfg.setIndexedTypes(EntityId.class, getMetadataClass());
            
            return ignite.getOrCreateCache(cacheCfg);
        });
    }
    
    /**
     * Создать временный кэш из данных БД
     */
    protected IgniteCache<EntityId, T> createTempCacheFromDatabase(String serviceName) {
        String tempCacheName = String.format(TEMP_CACHE_PREFIX, entityType.name()) + 
                             serviceName + "_" + System.currentTimeMillis();
        
        CacheConfiguration<EntityId, T> tempCacheCfg = new CacheConfiguration<>();
        tempCacheCfg.setName(tempCacheName);
        tempCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        tempCacheCfg.setIndexedTypes(EntityId.class, getMetadataClass());
        
        IgniteCache<EntityId, T> tempCache = ignite.getOrCreateCache(tempCacheCfg);
        
        // Загружаем данные из БД
        List<T> dbData = repository.findByServiceName(serviceName);
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
    protected CacheComparisonResult<T> compareCaches(String serviceName) {
        IgniteCache<EntityId, T> runtimeCache = getOrCreateRuntimeCache(serviceName);
        IgniteCache<EntityId, T> tempCache = createTempCacheFromDatabase(serviceName);
        
        try {
            CacheComparisonResult<T> result = new CacheComparisonResult<>();
            
            findNewRecords(runtimeCache, tempCache, result);
            findModifiedRecords(runtimeCache, tempCache, result);
            findDeletedRecords(runtimeCache, tempCache, result);

            log.info("Comparison result for {}: newRecords={}, modifiedRecords={}, deletedRecords={}",
                    entityType.getName(),
                    result.getNewRecords().size(),
                    result.getModifiedRecords().size(),
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
    protected void updateRuntimeCache(String serviceName, CacheComparisonResult<T> changes) {
        IgniteCache<EntityId, T> runtimeCache = getOrCreateRuntimeCache(serviceName);
        
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
    public CacheComparisonResult<T> synchronizeWithDatabase(String serviceName) {
        CacheComparisonResult<T> changes = compareCaches(serviceName);
        updateRuntimeCache(serviceName, changes);
        return changes;
    }
    
    private void findNewRecords(IgniteCache<EntityId, T> runtimeCache, 
                               IgniteCache<EntityId, T> tempCache,
                               CacheComparisonResult<T> result) {
        Set<EntityId> tempKeys = getAllKeys(tempCache);
        Set<EntityId> runtimeKeys = getAllKeys(runtimeCache);
        
        tempKeys.removeAll(runtimeKeys);
        
        for (EntityId key : tempKeys) {
            T tempData = tempCache.get(key);
            if (tempData != null) {
                result.addNewRecord(key, tempData);
            }
        }
    }
    
    private void findModifiedRecords(IgniteCache<EntityId, T> runtimeCache,
                                    IgniteCache<EntityId, T> tempCache,
                                    CacheComparisonResult<T> result) {
        Set<EntityId> commonKeys = new HashSet<>(getAllKeys(runtimeCache));
        commonKeys.retainAll(getAllKeys(tempCache));
        
        for (EntityId key : commonKeys) {
            T runtimeData = runtimeCache.get(key);
            T tempData = tempCache.get(key);
            
            if (runtimeData != null && tempData != null && 
                !Objects.equals(runtimeData.getHashData(), tempData.getHashData())) {
                result.addModifiedRecord(key, tempData);
            }
        }
    }
    
    private void findDeletedRecords(IgniteCache<EntityId, T> runtimeCache,
                                   IgniteCache<EntityId, T> tempCache,
                                   CacheComparisonResult<T> result) {
        Set<EntityId> runtimeKeys = getAllKeys(runtimeCache);
        Set<EntityId> tempKeys = getAllKeys(tempCache);
        
        runtimeKeys.removeAll(tempKeys);
        
        for (EntityId key : runtimeKeys) {
            T runtimeData = runtimeCache.get(key);
            if (runtimeData != null) {
                result.addDeletedRecord(key, runtimeData);
            }
        }
    }
    
    private Set<EntityId> getAllKeys(IgniteCache<EntityId, T> cache) {
        Set<EntityId> keys = ConcurrentHashMap.newKeySet(); // ← Потокобезопасный Set
    
        cache.query(new ScanQuery<EntityId, T>())
            .forEach(entry -> keys.add(entry.getKey()));
        
        return keys;
    }
    
    /**
     * Удалить runtime кэш для serviceName
     */
    public void destroyRuntimeCache(String serviceName) {
        IgniteCache<EntityId, T> cache = runtimeCaches.remove(serviceName);
        if (cache != null) {
            cache.destroy();
        }
    }
    
    /**
     * Получить все runtime кэши
     */
    public Set<String> getAllRuntimeCaches() {
        return new HashSet<>(runtimeCaches.keySet());
    }
    
    /**
     * Абстрактный метод для получения класса метаданных
     */
    protected abstract Class<T> getMetadataClass();
}
