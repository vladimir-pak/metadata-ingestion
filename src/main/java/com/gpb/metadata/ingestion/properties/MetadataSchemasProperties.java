package com.gpb.metadata.ingestion.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "metadata.schemas")
@Data
public class MetadataSchemasProperties {
    private String postgres;
    private String mssql;
    private String oracle;
}
