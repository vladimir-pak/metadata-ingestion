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
        if (source == null || source.isEmpty()) {
            return "UNKNOWN";
        }
        return Optional.ofNullable(LOOKUP.get(source.toUpperCase()))
                .map(TableTypes::getTargetType)
                .orElse("UNKNOWN");
    }
}
