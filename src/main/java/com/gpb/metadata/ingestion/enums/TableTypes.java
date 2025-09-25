package com.gpb.metadata.ingestion.enums;

import java.util.*;

public enum TableTypes {
    REGULAR("Regular"),
    EXTERNAL("External"),
    VIEW("View"),
    SECUREVIEW("SecureView"),
    MATERIALIZED_VIEW("MaterializedView"),
    ICEBERG("Iceberg"),
    LOCAL("Local");

    private final String targetType;

    TableTypes(String targetType) {
        this.targetType = targetType;
    }

    public String getTargetType() {
        return targetType;
    }

    private static final Map<String, TableTypes> LOOKUP = new HashMap<>();

    static {
        for (TableTypes type : values()) {
            LOOKUP.put(type.name(), type);
        }
    }

    public static String map(String source) {
        if (!source.isEmpty()) {
            TableTypes mapping = LOOKUP.get(source.toUpperCase());
        return mapping != null ? mapping.getTargetType() : "UNKNOWN";
        } else {
            return source;
        }
    }
}
