package com.gpb.metadata.ingestion.repository;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.snapshot.OpenMetadataSnapshot;
import com.gpb.metadata.ingestion.snapshot.OpenMetadataSnapshotEntry;

/**
 * Direct read-only snapshot access to OpenMetadata storage.
 *
 * Full service scans are used only during cold-cache reconciliation.
 * Normal table processing uses loadByFqns(...) and therefore queries only the
 * affected entities instead of loading the entire OpenMetadata table catalog.
 */
@Repository
public class OpenMetadataSnapshotRepository {

    private static final int FQN_BATCH_SIZE = 1000;

    private final JdbcTemplate ordJdbcTemplate;

    public OpenMetadataSnapshotRepository(
            @Qualifier("ordJdbcTemplate") JdbcTemplate ordJdbcTemplate) {
        this.ordJdbcTemplate = ordJdbcTemplate;
    }

    public OpenMetadataSnapshot loadByServiceName(
            DbObjectType objectType,
            String serviceName) {

        String entityTable = entityTable(objectType);
        String projectExpression = projectExpression(objectType);

        String sql = """
                select e.\"json\" ->> 'fullyQualifiedName' as fqn,
                       %s as is_project_entity,
                       e.id::text as id
                from %s e
                where e.deleted = false
                  and left(
                        e.\"json\" ->> 'fullyQualifiedName',
                        char_length(?) + 1
                      ) = ? || '.'
                """.formatted(
                projectExpression,
                entityTable
        );

        Map<String, OpenMetadataSnapshotEntry> entities =
                ordJdbcTemplate.query(
                        sql,
                        ps -> {
                            ps.setString(1, serviceName);
                            ps.setString(2, serviceName);
                        },
                        rs -> {
                            Map<String, OpenMetadataSnapshotEntry> result =
                                    new HashMap<>();

                            while (rs.next()) {
                                String fqn = rs.getString("fqn");
                                String id = rs.getString("id");

                                if (fqn == null || fqn.isBlank()) {
                                    continue;
                                }

                                boolean projectEntity = Boolean.parseBoolean(
                                        rs.getString("is_project_entity")
                                );

                                result.put(
                                        fqn,
                                        new OpenMetadataSnapshotEntry(
                                                id,
                                                fqn,
                                                projectEntity
                                        )
                                );
                            }

                            return result;
                        }
                );

        return new OpenMetadataSnapshot(entities);
    }

    public OpenMetadataSnapshot loadByFqns(
            DbObjectType objectType,
            Collection<String> fqns) {

        if (fqns == null || fqns.isEmpty()) {
            return OpenMetadataSnapshot.empty();
        }

        List<String> distinct = fqns.stream()
                .filter(fqn -> fqn != null && !fqn.isBlank())
                .distinct()
                .toList();

        if (distinct.isEmpty()) {
            return OpenMetadataSnapshot.empty();
        }

        Map<String, OpenMetadataSnapshotEntry> result = new HashMap<>();

        for (int from = 0; from < distinct.size(); from += FQN_BATCH_SIZE) {
            int to = Math.min(from + FQN_BATCH_SIZE, distinct.size());
            List<String> batch = distinct.subList(from, to);

            result.putAll(
                    loadFqnBatch(
                            objectType,
                            batch
                    )
            );
        }

        return new OpenMetadataSnapshot(result);
    }

    private Map<String, OpenMetadataSnapshotEntry> loadFqnBatch(
            DbObjectType objectType,
            List<String> fqns) {

        String entityTable = entityTable(objectType);
        String projectExpression = projectExpression(objectType);

        String placeholders = String.join(
                ",",
                java.util.Collections.nCopies(fqns.size(), "?")
        );

        String sql = """
                select e.\"json\" ->> 'fullyQualifiedName' as fqn,
                       %s as is_project_entity,
                       e.id::text as id
                from %s e
                where e.deleted = false
                  and e.\"json\" ->> 'fullyQualifiedName' in (%s)
                """.formatted(
                projectExpression,
                entityTable,
                placeholders
        );

        return ordJdbcTemplate.query(
                sql,
                ps -> {
                    for (int i = 0; i < fqns.size(); i++) {
                        ps.setString(i + 1, fqns.get(i));
                    }
                },
                rs -> {
                    Map<String, OpenMetadataSnapshotEntry> result =
                            new HashMap<>();

                    while (rs.next()) {
                        String fqn = rs.getString("fqn");
                        if (fqn == null || fqn.isBlank()) {
                            continue;
                        }

                        result.put(
                                fqn,
                                new OpenMetadataSnapshotEntry(
                                        rs.getString("id"),
                                        fqn,
                                        Boolean.parseBoolean(
                                                rs.getString("is_project_entity")
                                        )
                                )
                        );
                    }

                    return result;
                }
        );
    }

    private String entityTable(DbObjectType objectType) {
        return switch (objectType) {
            case DATABASE -> "database_entity";
            case SCHEMA -> "database_schema_entity";
            case TABLE -> "table_entity";
        };
    }

    private String projectExpression(DbObjectType objectType) {
        if (objectType == DbObjectType.TABLE) {
            return "coalesce(e.\"json\" ->> 'isProjectEntity', 'false')";
        }

        return "'false'";
    }
}
