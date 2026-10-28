package com.gpb.metadata.ingestion.enums;

public enum MetadataVersion {
    V1_2_3("1.2.3"),
    V1_9_4("1.9.4");

    private final String value;

    MetadataVersion(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static MetadataVersion from(String value) {
        for (MetadataVersion version : values()) {
            if (version.value.equals(value)) {
                return version;
            }
        }

        throw new IllegalArgumentException(
            "Unsupported metadata.version: " + value
            + ". Allowed: " + MetadataVersion.values()
        );
    }
}
