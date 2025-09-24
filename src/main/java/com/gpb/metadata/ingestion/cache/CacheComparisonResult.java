package com.gpb.metadata.ingestion.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.Metadata;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class CacheComparisonResult<T extends Metadata> {
    private Map<EntityId, T> newRecords = new ConcurrentHashMap<>();
    private Map<EntityId, T> modifiedRecords = new ConcurrentHashMap<>();
    private Map<EntityId, T> deletedRecords = new ConcurrentHashMap<>();
    
    public void addNewRecord(EntityId id, T metadata) {
        newRecords.put(id, metadata);
    }
    
    public void addModifiedRecord(EntityId id, T metadata) {
        modifiedRecords.put(id, metadata);
    }
    
    public void addDeletedRecord(EntityId id, T metadata) {
        deletedRecords.put(id, metadata);
    }

    public Map<EntityId, T> getNewRecords() {
        return Collections.unmodifiableMap(new HashMap<>(newRecords));
    }

    public Map<EntityId, T> getModifiedRecords() {
        return Collections.unmodifiableMap(new HashMap<>(modifiedRecords));
    }

    public Map<EntityId, T> getDeletedRecords() {
        return Collections.unmodifiableMap(new HashMap<>(deletedRecords));
    }

    public Map<EntityId, T> getPutRecords() {
        Map<EntityId, T> result = new ConcurrentHashMap<>(newRecords);
        result.putAll(modifiedRecords);
        return Collections.unmodifiableMap(result);
    }
    
    public boolean hasChanges() {
        return !newRecords.isEmpty() || !modifiedRecords.isEmpty() || !deletedRecords.isEmpty();
    }
}
