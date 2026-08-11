package com.gpb.metadata.ingestion.snapshot;

import java.util.Map;
import java.util.Optional;

/**
 * Read-only snapshot таблиц OpenMetadata для одного ingestion-run.
 * Ключ карты — fullyQualifiedName таблицы.
 */
public final class TableSnapshot {

    private final Map<String, TableSnapshotEntry> byFqn;

    public TableSnapshot(Map<String, TableSnapshotEntry> byFqn) {
        this.byFqn = Map.copyOf(byFqn);
    }

    public Optional<TableSnapshotEntry> find(String fqn) {
        return Optional.ofNullable(byFqn.get(fqn));
    }

    public Optional<String> findId(String fqn) {
        return find(fqn).map(TableSnapshotEntry::id);
    }

    public boolean isProjectEntity(String fqn) {
        return find(fqn)
                .map(TableSnapshotEntry::projectEntity)
                .orElse(false);
    }

    public boolean contains(String fqn) {
        return byFqn.containsKey(fqn);
    }

    public int size() {
        return byFqn.size();
    }
}
