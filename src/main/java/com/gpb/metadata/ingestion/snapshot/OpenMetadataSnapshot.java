package com.gpb.metadata.ingestion.snapshot;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class OpenMetadataSnapshot {

    private static final OpenMetadataSnapshot EMPTY =
            new OpenMetadataSnapshot(Map.of());

    private final Map<String, OpenMetadataSnapshotEntry> byFqn;

    public OpenMetadataSnapshot(Map<String, OpenMetadataSnapshotEntry> byFqn) {
        this.byFqn = Map.copyOf(byFqn);
    }

    public static OpenMetadataSnapshot empty() {
        return EMPTY;
    }

    public Optional<OpenMetadataSnapshotEntry> find(String fqn) {
        if (fqn == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(byFqn.get(fqn));
    }

    public boolean contains(String fqn) {
        return fqn != null && byFqn.containsKey(fqn);
    }

    public boolean isProjectEntity(String fqn) {
        return find(fqn)
                .map(OpenMetadataSnapshotEntry::projectEntity)
                .orElse(false);
    }

    public int size() {
        return byFqn.size();
    }

    public int managedSize() {
        int result = 0;
        for (OpenMetadataSnapshotEntry entry : byFqn.values()) {
            if (!entry.projectEntity()) {
                result++;
            }
        }
        return result;
    }

    /**
     * OpenMetadata entities that do not exist in the source snapshot.
     * Project entities are excluded because this ingestion does not own them.
     */
    public Set<String> managedOrphans(Collection<String> sourceFqns) {
        Set<String> source = sourceFqns == null
                ? Set.of()
                : Set.copyOf(sourceFqns);

        Set<String> result = new LinkedHashSet<>();

        for (OpenMetadataSnapshotEntry entry : byFqn.values()) {
            if (entry.projectEntity()) {
                continue;
            }

            if (!source.contains(entry.fqn())) {
                result.add(entry.fqn());
            }
        }

        return result;
    }

    public Set<String> fqns() {
        return Set.copyOf(byFqn.keySet());
    }
}
