package com.gpb.metadata.ingestion.model;

import java.time.LocalDateTime;

import com.gpb.metadata.ingestion.model.schema.TableData;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableMetadata implements Metadata {

    private EntityId id;

    private String parentFqn;

    private String fqn;

    private String dbName;

    private String schemaName;

    private String description;

    private String name;

    private String serviceName;

    private TableData data;

    private String hashData;

    private LocalDateTime createdAt;
}
