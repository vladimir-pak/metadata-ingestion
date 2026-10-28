package com.gpb.metadata.ingestion.enums;

import java.util.Set;

public enum TableConstraintType {

    UNIQUE,
    PRIMARY_KEY,
    FOREIGN_KEY,
    SORT_KEY,
    DIST_KEY,
    UNKNOWN;
    private static final Set<TableConstraintType> SUPPORTED = Set.of(
            UNIQUE, PRIMARY_KEY, FOREIGN_KEY, SORT_KEY, DIST_KEY
    );

    public static TableConstraintType fromString(String type) {
        if (type == null) return UNKNOWN;
        try {
            return TableConstraintType.valueOf(type.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    public static boolean isSupported(String type) {
        return SUPPORTED.contains(fromString(type));
    }
}
