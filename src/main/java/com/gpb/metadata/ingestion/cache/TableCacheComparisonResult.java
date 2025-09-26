package com.gpb.metadata.ingestion.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.postgres.TableMetadata;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableCacheComparisonResult extends CacheComparisonResult<TableMetadata> {
    private Map<EntityId, TableMetadata> newRecords = new ConcurrentHashMap<>();
    private Map<EntityId, TableMetadata> modifiedRecords = new ConcurrentHashMap<>();
    private Map<EntityId, TableMetadata> deletedRecords = new ConcurrentHashMap<>();
    
    public void addNewRecord(EntityId id, TableMetadata metadata) {
        newRecords.put(id, metadata);
    }
    
    public void addModifiedRecord(EntityId id, TableMetadata metadata) {
        modifiedRecords.put(id, metadata);
    }
    
    public void addDeletedRecord(EntityId id, TableMetadata metadata) {
        deletedRecords.put(id, metadata);
    }

    public Map<EntityId, TableMetadata> getNewRecords() {
        return Collections.unmodifiableMap(new HashMap<>(newRecords));
    }

    public Map<EntityId, TableMetadata> getModifiedRecords() {
        return Collections.unmodifiableMap(new HashMap<>(modifiedRecords));
    }

    public Map<EntityId, TableMetadata> getDeletedRecords() {
        return Collections.unmodifiableMap(new HashMap<>(deletedRecords));
    }

    public Map<EntityId, TableMetadata> getPutRecords() {
        Map<EntityId, TableMetadata> result = new ConcurrentHashMap<>(newRecords);
        result.putAll(modifiedRecords);
        return Collections.unmodifiableMap(result);
    }
    
    public boolean hasChanges() {
        return !newRecords.isEmpty() || !modifiedRecords.isEmpty() || !deletedRecords.isEmpty();
    }
}
