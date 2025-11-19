package com.gpb.metadata.ingestion.enums;

public enum DbObjectType {
    DATABASE("database"),
    SCHEMA("schema"),
    TABLE("table");

    private final String name;

    DbObjectType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static DbObjectType fromString(String name) {
        for (DbObjectType dbObjectType : DbObjectType.values()) {
            if (dbObjectType.name.equalsIgnoreCase(name)) {
                return dbObjectType;
            }
        }
        throw new IllegalArgumentException("Unknown entity: " + name);
    }
}
