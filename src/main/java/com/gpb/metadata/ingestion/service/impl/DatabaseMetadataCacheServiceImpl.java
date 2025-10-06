package com.gpb.metadata.ingestion.service.impl;

import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.postgres.DatabaseMetadata;
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
        super(ignite, repository, DbObjectType.DATABASE);
    }
    
    @Override
    protected Class<DatabaseMetadata> getMetadataClass() {
        return DatabaseMetadata.class;
    }
}
