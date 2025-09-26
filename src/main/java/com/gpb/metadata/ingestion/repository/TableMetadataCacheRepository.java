package com.gpb.metadata.ingestion.repository;

import org.springframework.stereotype.Repository;

import com.gpb.metadata.ingestion.model.postgres.TableMetadata;

import java.util.List;

@Repository
public interface TableMetadataCacheRepository extends MetadataRepository<TableMetadata> {

    /**
     * Поиск всех записей по serviceName
     */
    List<TableMetadata> findByServiceName(String serviceName);

    /**
     * Удаление всех записей по serviceName
     */
    void deleteByServiceName(String serviceName);
}
