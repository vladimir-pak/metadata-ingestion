package com.gpb.metadata.ingestion.config;

import com.gpb.metadata.ingestion.enums.MetadataVersion;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetadataVersionConfig {

    @Bean
    public MetadataVersion metadataVersion(
            @Value("${metadata.version}") String value) {

        return MetadataVersion.from(value);
    }
}