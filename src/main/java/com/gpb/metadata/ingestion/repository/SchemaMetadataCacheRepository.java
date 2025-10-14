package com.gpb.metadata.ingestion.repository;

import com.gpb.metadata.ingestion.model.EntityId;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class SchemaMetadataCacheRepository implements MetadataRepository<SchemaMetadata>{

    @Qualifier("jdbcTemplate")
    private final JdbcTemplate jdbcTemplate;

    public SchemaMetadataCacheRepository(@Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    /**
     * Получить все записи из нужной схемы по serviceName
     */
    public List<SchemaMetadata> findByServiceName(String schema, String serviceName) {
        String sql = String.format("""
            SELECT id, parent_fqn, fqn, db_name, name, service_name, hash_data, created_at
            FROM %s.schema_metadata
            WHERE service_name = ?
        """, schema);

        return jdbcTemplate.query(sql, this::mapRow, serviceName);
    }

    /**
     * Удалить все записи по serviceName из нужной схемы
     */
    public void deleteByServiceName(String schema, String serviceName) {
        String sql = String.format("DELETE FROM %s.schema_metadata WHERE service_name = ?", schema);
        jdbcTemplate.update(sql, serviceName);
    }

    private SchemaMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
        SchemaMetadata entity = new SchemaMetadata();
        EntityId id = new EntityId(
                rs.getLong("id"),
                rs.getString("parent_fqn")
        );
        entity.setId(id);
        entity.setFqn(rs.getString("fqn"));
        entity.setDbName(rs.getString("db_name"));
        entity.setName(rs.getString("name"));
        entity.setServiceName(rs.getString("service_name"));
        entity.setHashData(rs.getString("hash_data"));
        entity.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
        return entity;
    }
}
