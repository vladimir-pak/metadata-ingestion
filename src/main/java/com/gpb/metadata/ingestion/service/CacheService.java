package com.gpb.metadata.ingestion.service;

import org.springframework.stereotype.Service;

import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.properties.MetadataTablesProperties;
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
    private final MetadataTablesProperties metadataTablesProperties;

    /**
     * Safe logical reset.
     *
     * Both v3 data + manifest and matching legacy v2 caches are removed.
     * The next ingestion automatically runs FULL RECONCILIATION, so clearing
     * cache no longer silently loses DELETE baseline information.
     */
    public void cleanCache(
            ServiceType serviceType,
            String serviceName) {

        databaseCacheService.destroyRuntimeCache(
                serviceType,
                metadataTablesProperties.getTable(
                        serviceType,
                        DbObjectType.DATABASE
                ),
                serviceName
        );

        schemaCacheService.destroyRuntimeCache(
                serviceType,
                metadataTablesProperties.getTable(
                        serviceType,
                        DbObjectType.SCHEMA
                ),
                serviceName
        );

        tableCacheService.destroyRuntimeCache(
                serviceType,
                metadataTablesProperties.getTable(
                        serviceType,
                        DbObjectType.TABLE
                ),
                serviceName
        );
    }
}
