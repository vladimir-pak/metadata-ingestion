package com.gpb.metadata.ingestion.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.metadata.ingestion.model.EntityId;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.metadata.ingestion.model.postgres.TableMetadata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class TableMetadataCacheRepository implements MetadataRepository<TableMetadata>{

    @Qualifier("jdbcTemplate")
    private final JdbcTemplate jdbcTemplate;

    public TableMetadataCacheRepository(@Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }    /**
     * Получить все записи по serviceName из выбранной схемы
     */
    public List<TableMetadata> findByServiceName(String schema, String serviceName) {
        String sql = String.format("""
            SELECT id, parent_fqn, fqn, db_name, schema_name, description, name, 
                   service_name, data, hash_data, created_at
            FROM %s.table_metadata
            WHERE service_name = ?
        """, schema);

        return jdbcTemplate.query(sql, this::mapRow, serviceName);
    }

    /**
     * Удалить все записи по serviceName из выбранной схемы
     */
    public void deleteByServiceName(String schema, String serviceName) {
        String sql = String.format("DELETE FROM %s.table_metadata WHERE service_name = ?", schema);
        jdbcTemplate.update(sql, serviceName);
    }

    private TableMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
        TableMetadata entity = new TableMetadata();
        EntityId id = new EntityId(
                rs.getLong("id"),
                rs.getString("parent_fqn")
        );
        entity.setId(id);
        entity.setFqn(rs.getString("fqn"));
        entity.setDbName(rs.getString("db_name"));
        entity.setSchemaName(rs.getString("schema_name"));
        entity.setDescription(rs.getString("description"));
        entity.setName(rs.getString("name"));
        entity.setServiceName(rs.getString("service_name"));

        // JSONB -> JsonNode
        String jsonData = rs.getString("data");
        if (jsonData != null) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                entity.setData(objectMapper.readTree(jsonData));
            } catch (Exception e) {
                throw new RuntimeException("Ошибка преобразования JSONB в JsonNode", e);
            }
        }

        entity.setHashData(rs.getString("hash_data"));
        entity.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
        return entity;
    }
}

