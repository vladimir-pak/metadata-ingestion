package com.gpb.metadata.ingestion.service;

import java.util.Map;

import org.springframework.stereotype.Service;

import com.gpb.metadata.ingestion.properties.MetadataSchemasProperties;
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

    private final MetadataSchemasProperties schemasProperties;    

    public void cleanCache(String schema, String serviceName) {
        final Map<String, String> schemaTypeMap = Map.of(
            "postgres", schemasProperties.getPostgres(),
            "mssql", schemasProperties.getMssql(),
            "oracle", schemasProperties.getOracle(),
            "sapiq", schemasProperties.getSapiq()
        );
        String schemaName = schemaTypeMap.get(schema);
        databaseCacheService.destroyRuntimeCache(schemaName, serviceName);
        schemaCacheService.destroyRuntimeCache(schemaName, serviceName);
        tableCacheService.destroyRuntimeCache(schemaName, serviceName);
    }
}
