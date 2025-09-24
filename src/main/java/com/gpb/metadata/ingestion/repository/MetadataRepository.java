package com.gpb.metadata.ingestion.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.NoRepositoryBean;

import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.Metadata;

@NoRepositoryBean
public interface MetadataRepository<T extends Metadata> extends JpaRepository<T, EntityId> {
    List<T> findByServiceName(String serviceName);
}
