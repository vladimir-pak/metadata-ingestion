package com.gpb.metadata.ingestion.repository;

import org.springframework.stereotype.Repository;

import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;

import java.util.List;

@Repository
public interface SchemaMetadataCacheRepository extends MetadataRepository<SchemaMetadata> {

    /**
     * Поиск всех записей по serviceName
     */
    List<SchemaMetadata> findByServiceName(String serviceName);

    /**
     * Удаление всех записей по serviceName
     */
    void deleteByServiceName(String serviceName);
}
