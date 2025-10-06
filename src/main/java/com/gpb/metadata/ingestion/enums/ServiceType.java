package com.gpb.metadata.ingestion.enums;

public enum ServiceType {
    POSTGRES("postgres"),
    MSSQL("mssql"),
    ORACLE("oracle");

    private final String value;

    ServiceType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

