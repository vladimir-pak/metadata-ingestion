package com.gpb.metadata.ingestion.model;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor 
@AllArgsConstructor 
@Builder 
public class SchemaMetadata implements Metadata {

    private EntityId id;

    private String parentFqn;
    
    private String fqn;

    private String dbName;

    private String name;

    private String serviceName;

    private String hashData;

    private LocalDateTime createdAt;
}
