package com.gpb.metadata.ingestion.repository;

import java.util.List;

import com.gpb.metadata.ingestion.model.Metadata;

public interface MetadataRepository<T extends Metadata> {
    List<T> findByServiceName(String schema, String serviceName);
    void deleteByServiceName(String schema, String serviceName);
}
