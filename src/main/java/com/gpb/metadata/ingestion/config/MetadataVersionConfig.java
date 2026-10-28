package com.gpb.metadata.ingestion.config;

import com.gpb.metadata.ingestion.dto.mapper.*;
import com.gpb.metadata.ingestion.enums.MetadataVersion;
import com.gpb.metadata.ingestion.service.impl.ColumnTypeMapperService;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class MetadataVersionConfig {

    @Bean
    public MetadataVersion metadataVersion(
            @Value("${metadata.version}") String value) {

        MetadataVersion version = MetadataVersion.from(value);

        log.info("OpenMetadata version: {}", version.getValue());

        return version;
    }

    @Bean
    public OpenMetadataRequestMapper openMetadataRequestMapper(
            MetadataVersion version,
            ColumnTypeMapperService columnTypeMapperService) {

        return switch (version) {

            case V1_2_3 -> {
                log.info("Using LegacyOpenMetadataMapper");

                yield new LegacyOpenMetadataMapper(
                        columnTypeMapperService
                );
            }

            case V1_9_4 -> {
                log.info("Using OpenMetadataMapper");

                yield new OpenMetadataMapper(
                        columnTypeMapperService
                );
            }
        };
    }
}