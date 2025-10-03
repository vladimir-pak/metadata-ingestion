package com.gpb.metadata.ingestion.model.postgres;

import java.time.LocalDateTime;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.Metadata;
import com.gpb.metadata.ingestion.model.schema.TableData;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "table_metadata", schema = "postgres_metadata")
@EntityListeners(AuditingEntityListener.class)
public class TableMetadata implements Metadata {
    @EmbeddedId
    private EntityId id;

    @Column(name = "fqn")
    private String fqn;

    @Column(name = "db_name")
    private String dbName;

    @Column(name = "schema_name")
    private String schemaName;

    @Column(name = "description")
    private String description;

    @Column(name = "name")
    private String name;

    @Column(name = "service_name")
    private String serviceName;

    @Column(columnDefinition = "jsonb", nullable = false)
    @JdbcTypeCode(SqlTypes.JSON)
    private JsonNode data;

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

    public TableData getTableData() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return objectMapper.treeToValue(this.data, TableData.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting JSON data to TableData", e);
        }
    }

    public void setTableData(TableData tableData) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            this.data = objectMapper.valueToTree(tableData);
        } catch (Exception e) {
            throw new RuntimeException("Error converting TableData to JSON", e);
        }
    }
}
