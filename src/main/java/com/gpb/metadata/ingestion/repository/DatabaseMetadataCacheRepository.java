package com.gpb.metadata.ingestion.repository;

import org.springframework.stereotype.Repository;

import com.gpb.metadata.ingestion.model.postgres.DatabaseMetadata;

import java.util.List;

@Repository
public interface DatabaseMetadataCacheRepository extends MetadataRepository<DatabaseMetadata> {

    /**
     * Поиск всех записей по serviceName
     */
    List<DatabaseMetadata> findByServiceName(String serviceName);

    /**
     * Удаление всех записей по serviceName
     */
    void deleteByServiceName(String serviceName);
}
