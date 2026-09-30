package com.gpb.metadata.ingestion.repository;

import com.gpb.metadata.ingestion.cache.dto.MetadataFingerprint;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.SchemaMetadata;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
public class SchemaMetadataCacheRepository
        implements MetadataRepository<SchemaMetadata> {

    private static final int LOAD_BATCH_SIZE = 1000;

    private final JdbcTemplate jdbcTemplate;

    public SchemaMetadataCacheRepository(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Map<EntityId, MetadataFingerprint> findFingerprintsByServiceName(
            String tableName,
            String serviceName) {

        String sql = """
                SELECT id,
                       parent_fqn,
                       fqn,
                       hash_data
                  FROM %s
                 WHERE service_name = ?
                """.formatted(tableName);

        return jdbcTemplate.query(
                sql,
                rs -> {
                    Map<EntityId, MetadataFingerprint> result = new LinkedHashMap<>();

                    while (rs.next()) {
                        EntityId id = new EntityId(
                                rs.getLong("id"),
                                rs.getString("parent_fqn")
                        );

                        MetadataFingerprint fingerprint = new MetadataFingerprint(
                                id,
                                rs.getString("hash_data"),
                                rs.getString("fqn")
                        );

                        MetadataFingerprint previous = result.put(id, fingerprint);
                        if (previous != null) {
                            throw new IllegalStateException(
                                    "Duplicate metadata EntityId in " + tableName + ": " + id
                            );
                        }
                    }

                    return result;
                },
                serviceName
        );
    }

    @Override
    public Map<EntityId, SchemaMetadata> findByIds(
            String tableName,
            String serviceName,
            Collection<EntityId> ids) {

        if (ids == null || ids.isEmpty()) {
            return Map.of();
        }

        List<EntityId> all = new ArrayList<>(ids);
        Map<EntityId, SchemaMetadata> result = new LinkedHashMap<>();

        for (int from = 0; from < all.size(); from += LOAD_BATCH_SIZE) {
            List<EntityId> batch = all.subList(
                    from,
                    Math.min(from + LOAD_BATCH_SIZE, all.size())
            );

            String values = String.join(
                    ",",
                    java.util.Collections.nCopies(batch.size(), "(?, ?)")
            );

            String sql = """
                    SELECT m.id,
                           m.parent_fqn,
                           m.fqn,
                           m.db_name,
                           m.name,
                           m.service_name,
                           m.hash_data,
                           m.created_at
                      FROM %s m
                      JOIN (VALUES %s) AS v(id, parent_fqn)
                        ON m.id = v.id
                       AND m.parent_fqn IS NOT DISTINCT FROM v.parent_fqn
                     WHERE m.service_name = ?
                    """.formatted(tableName, values);

            List<Object> args = new ArrayList<>(batch.size() * 2 + 1);
            for (EntityId id : batch) {
                args.add(id.getId());
                args.add(id.getParentFqn());
            }
            args.add(serviceName);

            List<SchemaMetadata> rows = jdbcTemplate.query(
                    sql,
                    this::mapRow,
                    args.toArray()
            );

            for (SchemaMetadata row : rows) {
                SchemaMetadata previous = result.put(row.getId(), row);
                if (previous != null) {
                    throw new IllegalStateException(
                            "Duplicate full metadata EntityId in " + tableName + ": " + row.getId()
                    );
                }
            }
        }

        return result;
    }

    private SchemaMetadata mapRow(
            ResultSet rs,
            int rowNum) throws SQLException {

        SchemaMetadata entity = new SchemaMetadata();
        entity.setId(new EntityId(
                rs.getLong("id"),
                rs.getString("parent_fqn")
        ));
        entity.setParentFqn(rs.getString("parent_fqn"));
        entity.setFqn(rs.getString("fqn"));
        entity.setDbName(rs.getString("db_name"));
        entity.setName(rs.getString("name"));
        entity.setServiceName(rs.getString("service_name"));
        entity.setHashData(rs.getString("hash_data"));

        if (rs.getTimestamp("created_at") != null) {
            entity.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
        }

        return entity;
    }
}
