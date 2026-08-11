package com.gpb.metadata.ingestion.repository;

import com.gpb.metadata.ingestion.snapshot.TableSnapshot;
import com.gpb.metadata.ingestion.snapshot.TableSnapshotEntry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;

@Repository
public class OpenMetadataTableSnapshotRepository {

    private static final String SQL = """
            select te."json" ->> 'fullyQualifiedName' as fqn,
                   te."json" ->> 'isProjectEntity' as is_project_entity,
                   id
            from table_entity te
            where deleted = false
              and te."json" -> 'database' ->> 'fullyQualifiedName' ilike ?
            """;

    private final JdbcTemplate ordJdbcTemplate;

    public OpenMetadataTableSnapshotRepository(
            @Qualifier("ordJdbcTemplate") JdbcTemplate ordJdbcTemplate) {
        this.ordJdbcTemplate = ordJdbcTemplate;
    }

    public TableSnapshot loadByServiceName(String serviceName) {
        String servicePattern = serviceName + ".%";

        Map<String, TableSnapshotEntry> tables = ordJdbcTemplate.query(
                SQL,
                ps -> ps.setString(1, servicePattern),
                rs -> {
                    Map<String, TableSnapshotEntry> result = new HashMap<>();
                    while (rs.next()) {
                        String fqn = rs.getString("fqn");
                        String id = rs.getString("id");

                        if (fqn == null || fqn.isBlank() || id == null || id.isBlank()) {
                            continue;
                        }

                        boolean projectEntity = Boolean.parseBoolean(
                                rs.getString("is_project_entity")
                        );

                        result.put(
                                fqn,
                                new TableSnapshotEntry(id, projectEntity)
                        );
                    }
                    return result;
                }
        );

        return new TableSnapshot(tables);
    }
}
