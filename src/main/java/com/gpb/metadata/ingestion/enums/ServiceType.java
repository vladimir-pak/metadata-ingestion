package com.gpb.metadata.ingestion.enums;

import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum ServiceType {
    POSTGRES("postgres"),
    MSSQL("mssql"),
    ORACLE("oracle"),
    SAPIQ("sapiq"),
    SAPASE("sapase");

    private final String value;

    ServiceType(String value) {
        this.value = value;
    }

    public String getValue() {
        if ("sapase".equals(value)) return "sapiq";
        return value;
    }

    @JsonCreator
    public static ServiceType from(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return ServiceType.valueOf(
                value.trim().toUpperCase(Locale.ROOT)
        );
    }
}