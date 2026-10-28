package com.gpb.metadata.ingestion.enums;

import java.util.Set;

public enum ColumnConstraintType {

    NULL,
    NOT_NULL,
    UNIQUE,
    PRIMARY_KEY,
    UNKNOWN;

    private static final Set<ColumnConstraintType> SUPPORTED = Set.of(
            NULL, NOT_NULL, UNIQUE, PRIMARY_KEY
    );

    public static ColumnConstraintType fromString(String type) {
        if (type == null) return UNKNOWN;
        
        try {
            return ColumnConstraintType.valueOf(
                    type.trim().toUpperCase()
            );
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    public static boolean isSupported(String type) {
        return SUPPORTED.contains(
                fromString(type)
        );
    }
}
