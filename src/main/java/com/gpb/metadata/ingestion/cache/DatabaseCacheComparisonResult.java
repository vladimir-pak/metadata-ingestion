package com.gpb.metadata.ingestion.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.postgres.DatabaseMetadata;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabaseCacheComparisonResult extends CacheComparisonResult<DatabaseMetadata> {
    private Map<EntityId, DatabaseMetadata> newRecords = new ConcurrentHashMap<>();
    private Map<EntityId, DatabaseMetadata> modifiedRecords = new ConcurrentHashMap<>();
    private Map<EntityId, DatabaseMetadata> deletedRecords = new ConcurrentHashMap<>();
    
    public void addNewRecord(EntityId id, DatabaseMetadata metadata) {
        newRecords.put(id, metadata);
    }
    
    public void addModifiedRecord(EntityId id, DatabaseMetadata metadata) {
        modifiedRecords.put(id, metadata);
    }
    
    public void addDeletedRecord(EntityId id, DatabaseMetadata metadata) {
        deletedRecords.put(id, metadata);
    }

    public Map<EntityId, DatabaseMetadata> getNewRecords() {
        return Collections.unmodifiableMap(new HashMap<>(newRecords));
    }

    public Map<EntityId, DatabaseMetadata> getModifiedRecords() {
        return Collections.unmodifiableMap(new HashMap<>(modifiedRecords));
    }

    public Map<EntityId, DatabaseMetadata> getDeletedRecords() {
        return Collections.unmodifiableMap(new HashMap<>(deletedRecords));
    }

    public Map<EntityId, DatabaseMetadata> getPutRecords() {
        Map<EntityId, DatabaseMetadata> result = new ConcurrentHashMap<>(newRecords);
        result.putAll(modifiedRecords);
        return Collections.unmodifiableMap(result);
    }
    
    public boolean hasChanges() {
        return !newRecords.isEmpty() || !modifiedRecords.isEmpty() || !deletedRecords.isEmpty();
    }
}
