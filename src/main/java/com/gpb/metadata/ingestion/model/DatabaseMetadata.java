package com.gpb.metadata.ingestion.model;

import java.time.LocalDateTime;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "database_metadata", schema = "postgres_metadata")
@EntityListeners(AuditingEntityListener.class)
public class DatabaseMetadata implements Metadata {
    @EmbeddedId
    private EntityId id;
    
    @Column(name = "fqn")
    private String fqn;

    @Column(name = "name")
    private String name;

    @Column(name = "service_name")
    private String serviceName;

    @Column(name = "hash_data")
    private String hashData;

    @CreatedDate
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Override
    public EntityId getId() { return id; }

    @Override
    public String getName() { return name; }
    
    @Override
    public String getFqn() { return fqn; }

    @Override
    public String getServiceName() { return serviceName; }
    
    @Override
    public String getParentFqn() { return id.getParentFqn(); }

    @Override
    public String getHashData() { return hashData; }
}
