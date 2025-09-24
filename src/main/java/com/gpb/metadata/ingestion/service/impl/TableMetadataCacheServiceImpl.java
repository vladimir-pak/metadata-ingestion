package com.gpb.metadata.ingestion.service.impl;

import com.gpb.metadata.ingestion.enums.Entity;
import com.gpb.metadata.ingestion.model.TableMetadata;
import com.gpb.metadata.ingestion.repository.TableMetadataCacheRepository;
import com.gpb.metadata.ingestion.service.AbstractMetadataCacheService;

import org.apache.ignite.Ignite;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class TableMetadataCacheServiceImpl extends AbstractMetadataCacheService<TableMetadata> {
    
    public TableMetadataCacheServiceImpl(
            @Qualifier("igniteInstance") Ignite ignite,
            TableMetadataCacheRepository repository) {
        super(ignite, repository, Entity.TABLE);
    }
    
    @Override
    protected Class<TableMetadata> getMetadataClass() {
        return TableMetadata.class;
    }
}
