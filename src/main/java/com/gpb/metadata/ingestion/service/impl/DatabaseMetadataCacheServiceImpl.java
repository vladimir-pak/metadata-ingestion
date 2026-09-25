package com.gpb.metadata.ingestion.service.impl;

import org.springframework.stereotype.Service;

import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.DatabaseMetadata;
import com.gpb.metadata.ingestion.properties.MetadataCacheProperties;
import com.gpb.metadata.ingestion.repository.DatabaseMetadataCacheRepository;
import com.gpb.metadata.ingestion.service.AbstractMetadataCacheService;
import com.gpb.metadata.ingestion.service.MetadataCacheCoordinator;

@Service
public class DatabaseMetadataCacheServiceImpl
        extends AbstractMetadataCacheService<DatabaseMetadata> {

    public DatabaseMetadataCacheServiceImpl(
            MetadataCacheCoordinator cacheCoordinator,
            MetadataCacheProperties cacheProperties,
            DatabaseMetadataCacheRepository repository) {

        super(
                cacheCoordinator,
                cacheProperties,
                repository,
                DbObjectType.DATABASE
        );
    }
}
