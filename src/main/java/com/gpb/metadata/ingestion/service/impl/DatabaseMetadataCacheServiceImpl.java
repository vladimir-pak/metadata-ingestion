package com.gpb.metadata.ingestion.service.impl;

import com.gpb.metadata.ingestion.enums.Entity;
import com.gpb.metadata.ingestion.model.DatabaseMetadata;
import com.gpb.metadata.ingestion.repository.DatabaseMetadataCacheRepository;
import com.gpb.metadata.ingestion.service.AbstractMetadataCacheService;

import org.apache.ignite.Ignite;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class DatabaseMetadataCacheServiceImpl extends AbstractMetadataCacheService<DatabaseMetadata> {
    
    public DatabaseMetadataCacheServiceImpl(
            @Qualifier("igniteInstance") Ignite ignite,
            DatabaseMetadataCacheRepository repository) {
        super(ignite, repository, Entity.DATABASE);
    }
    
    @Override
    protected Class<DatabaseMetadata> getMetadataClass() {
        return DatabaseMetadata.class;
    }
}
