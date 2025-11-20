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
}
