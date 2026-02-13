package com.gpb.metadata.ingestion.service;

import org.springframework.stereotype.Service;

import com.gpb.metadata.ingestion.service.impl.DatabaseMetadataCacheServiceImpl;
import com.gpb.metadata.ingestion.service.impl.SchemaMetadataCacheServiceImpl;
import com.gpb.metadata.ingestion.service.impl.TableMetadataCacheServiceImpl;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class CacheService {
    private final DatabaseMetadataCacheServiceImpl databaseCacheService;
    private final SchemaMetadataCacheServiceImpl schemaCacheService;
    private final TableMetadataCacheServiceImpl tableCacheService;

    public void cleanCache(String schema, String serviceName) {
        databaseCacheService.destroyRuntimeCache(schema, serviceName);
        schemaCacheService.destroyRuntimeCache(schema, serviceName);
        tableCacheService.destroyRuntimeCache(schema, serviceName);
    }
}
