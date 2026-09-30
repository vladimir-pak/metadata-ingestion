package com.gpb.metadata.ingestion.properties;

import java.util.EnumMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.enums.ServiceType;

import lombok.Data;

@Validated 
@Configuration 
@ConfigurationProperties (prefix = "metadata.tables")
@Data 
public class MetadataTablesProperties {

    private Map<ServiceType, Tables> sources =
            new EnumMap<>(ServiceType.class);

    @Data
    public static class Tables {

        private String database;
        private String schema;
        private String table;
    }

    public Tables get(ServiceType serviceType) {

        Tables tables = sources.get(serviceType);

        if (tables == null) {
            throw new IllegalArgumentException(
                    "Metadata tables are not configured for serviceType="
                            + serviceType
            );
        }

        return tables;
    }

    public String getTable(
            ServiceType serviceType,
            DbObjectType objectType) {

        Tables tables = get(serviceType);

        return switch (objectType) {
            case DATABASE -> tables.getDatabase();
            case SCHEMA -> tables.getSchema();
            case TABLE -> tables.getTable();

            default -> throw new IllegalArgumentException(
                    "Unsupported metadata object type: "
                            + objectType
            );
        };
    }
}
