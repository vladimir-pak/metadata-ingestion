package com.gpb.metadata.ingestion.service.impl;

import org.springframework.stereotype.Service;

import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.SchemaMetadata;
import com.gpb.metadata.ingestion.properties.MetadataCacheProperties;
import com.gpb.metadata.ingestion.repository.SchemaMetadataCacheRepository;
import com.gpb.metadata.ingestion.service.AbstractMetadataCacheService;
import com.gpb.metadata.ingestion.service.MetadataCacheCoordinator;

@Service
public class SchemaMetadataCacheServiceImpl
        extends AbstractMetadataCacheService<SchemaMetadata> {

    public SchemaMetadataCacheServiceImpl(
            MetadataCacheCoordinator cacheCoordinator,
            MetadataCacheProperties cacheProperties,
            SchemaMetadataCacheRepository repository) {

        super(
                cacheCoordinator,
                cacheProperties,
                repository,
                DbObjectType.SCHEMA
        );
    }
}
