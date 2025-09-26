package com.gpb.metadata.ingestion.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaCacheComparisonResult extends CacheComparisonResult<SchemaMetadata> {
    private Map<EntityId, SchemaMetadata> newRecords = new ConcurrentHashMap<>();
    private Map<EntityId, SchemaMetadata> modifiedRecords = new ConcurrentHashMap<>();
    private Map<EntityId, SchemaMetadata> deletedRecords = new ConcurrentHashMap<>();
    
    public void addNewRecord(EntityId id, SchemaMetadata metadata) {
        newRecords.put(id, metadata);
    }
    
    public void addModifiedRecord(EntityId id, SchemaMetadata metadata) {
        modifiedRecords.put(id, metadata);
    }
    
    public void addDeletedRecord(EntityId id, SchemaMetadata metadata) {
        deletedRecords.put(id, metadata);
    }

    public Map<EntityId, SchemaMetadata> getNewRecords() {
        return Collections.unmodifiableMap(new HashMap<>(newRecords));
    }

    public Map<EntityId, SchemaMetadata> getModifiedRecords() {
        return Collections.unmodifiableMap(new HashMap<>(modifiedRecords));
    }

    public Map<EntityId, SchemaMetadata> getDeletedRecords() {
        return Collections.unmodifiableMap(new HashMap<>(deletedRecords));
    }

    public Map<EntityId, SchemaMetadata> getPutRecords() {
        Map<EntityId, SchemaMetadata> result = new ConcurrentHashMap<>(newRecords);
        result.putAll(modifiedRecords);
        return Collections.unmodifiableMap(result);
    }
    
    public boolean hasChanges() {
        return !newRecords.isEmpty() || !modifiedRecords.isEmpty() || !deletedRecords.isEmpty();
    }
}
