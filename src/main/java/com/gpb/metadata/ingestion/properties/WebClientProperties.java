package com.gpb.metadata.ingestion.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;

@Configuration
@ConfigurationProperties(prefix = "ord.api")
@Getter
@Setter
public class WebClientProperties {
    private String baseUrl;
    private Endpoints endpoints = new Endpoints();
    
    @Getter
    @Setter
    public static class Endpoints {
        private String database = "/databases";
        private String schema = "/databaseSchemas";
        private String table = "/tables";
    }
    
    // Методы для конкретных endpoints (возвращают только путь)
    public String getDatabaseEndpoint() {
        return endpoints.database;
    }

    public String getDatabaseDeleteEndpoint() {
        return endpoints.database + "/name";
    }

    public String getSchemaEndpoint() {
        return endpoints.schema;
    }
    
    public String getSchemaDeleteEndpoint() {
        return endpoints.schema + "/name";
    }

    public String getTableEndpoint() {
        return endpoints.table;
    }

    public String getTableDeleteEndpoint() {
        return endpoints.table + "/name";
    }
}
