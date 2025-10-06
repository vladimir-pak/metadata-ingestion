package com.gpb.metadata.ingestion.service.impl;

import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;
import com.gpb.metadata.ingestion.repository.SchemaMetadataCacheRepository;
import com.gpb.metadata.ingestion.service.AbstractMetadataCacheService;

import org.apache.ignite.Ignite;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class SchemaMetadataCacheServiceImpl extends AbstractMetadataCacheService<SchemaMetadata> {
    
    public SchemaMetadataCacheServiceImpl(
            @Qualifier("igniteInstance") Ignite ignite,
            SchemaMetadataCacheRepository repository) {
        super(ignite, repository, DbObjectType.SCHEMA);
    }
    
    @Override
    protected Class<SchemaMetadata> getMetadataClass() {
        return SchemaMetadata.class;
    }
}
