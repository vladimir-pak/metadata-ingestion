package com.gpb.metadata.ingestion.service.impl;

import com.gpb.metadata.ingestion.cache.CacheComparisonResult;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.postgres.TableMetadata;
import com.gpb.metadata.ingestion.repository.TableMetadataCacheRepository;
import com.gpb.metadata.ingestion.service.AbstractMetadataCacheService;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TableMetadataCacheServiceImpl extends AbstractMetadataCacheService<TableMetadata> {

    private final Map<String, IgniteCache<String, EntityId>> fqnIndexCaches = new ConcurrentHashMap<>();
    private static final String FQN_INDEX_CACHE_NAME = "runtime_TABLE_FQN_IDX_%s_%s"; // schema_service

    public TableMetadataCacheServiceImpl(
            @Qualifier("igniteInstance") Ignite ignite,
            TableMetadataCacheRepository repository
    ) {
        super(ignite, repository, DbObjectType.TABLE);
    }

    @Override
    protected Class<TableMetadata> getMetadataClass() {
        return TableMetadata.class;
    }

    private IgniteCache<String, EntityId> getOrCreateFqnIndexCache(String schemaName, String serviceName) {
        String cacheName = String.format(FQN_INDEX_CACHE_NAME, schemaName, serviceName);

        return fqnIndexCaches.computeIfAbsent(cacheName, name -> {
            CacheConfiguration<String, EntityId> cfg = new CacheConfiguration<>();
            cfg.setName(name);
            cfg.setCacheMode(CacheMode.REPLICATED);
            return ignite.getOrCreateCache(cfg);
        });
    }

    public Optional<TableMetadata> findByFqn(String schemaName, String serviceName, String fqn) {
        IgniteCache<EntityId, TableMetadata> tables = getRuntimeCache(schemaName, serviceName);
        IgniteCache<String, EntityId> idx = getOrCreateFqnIndexCache(schemaName, serviceName);

        EntityId id = idx.get(fqn);
        if (id == null) return Optional.empty();
        return Optional.ofNullable(tables.get(id));
    }

    @Override
    public CacheComparisonResult<TableMetadata> synchronizeWithDatabase(String schemaName, String serviceName) {
        CacheComparisonResult<TableMetadata> changes = super.synchronizeWithDatabase(schemaName, serviceName);
        rebuildFqnIndex(schemaName, serviceName);
        return changes;
    }

    private void rebuildFqnIndex(String schemaName, String serviceName) {
        IgniteCache<EntityId, TableMetadata> tables = getRuntimeCache(schemaName, serviceName);
        IgniteCache<String, EntityId> idx = getOrCreateFqnIndexCache(schemaName, serviceName);

        idx.clear();

        for (Cache.Entry<EntityId, TableMetadata> e : tables) {
            TableMetadata t = e.getValue();
            if (t != null && t.getFqn() != null) {
                idx.put(t.getFqn(), e.getKey());
            }
        }
    }

    @Override
    public void destroyRuntimeCache(String schemaName, String serviceName) {
        super.destroyRuntimeCache(schemaName, serviceName);

        String idxName = String.format(FQN_INDEX_CACHE_NAME, schemaName, serviceName);
        fqnIndexCaches.remove(idxName);
        if (ignite.cache(idxName) != null) {
            ignite.destroyCache(idxName);
        }
    }
}
