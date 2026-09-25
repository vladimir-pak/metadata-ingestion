package com.gpb.metadata.ingestion.service.impl;

import org.springframework.stereotype.Service;

import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.TableMetadata;
import com.gpb.metadata.ingestion.properties.MetadataCacheProperties;
import com.gpb.metadata.ingestion.repository.TableMetadataCacheRepository;
import com.gpb.metadata.ingestion.service.AbstractMetadataCacheService;
import com.gpb.metadata.ingestion.service.MetadataCacheCoordinator;

@Service
public class TableMetadataCacheServiceImpl
        extends AbstractMetadataCacheService<TableMetadata> {

    public TableMetadataCacheServiceImpl(
            MetadataCacheCoordinator cacheCoordinator,
            MetadataCacheProperties cacheProperties,
            TableMetadataCacheRepository repository) {

        super(
                cacheCoordinator,
                cacheProperties,
                repository,
                DbObjectType.TABLE
        );
    }
}
