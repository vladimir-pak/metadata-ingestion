package com.gpb.metadata.ingestion.enums;

public enum Entity {
    DATABASE("database"),
    SCHEMA("schema"),
    TABLE("table");

    private final String name;

    Entity(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static Entity fromString(String name) {
        for (Entity entity : Entity.values()) {
            if (entity.name.equalsIgnoreCase(name)) {
                return entity;
            }
        }
        throw new IllegalArgumentException("Unknown entity: " + name);
    }
}
