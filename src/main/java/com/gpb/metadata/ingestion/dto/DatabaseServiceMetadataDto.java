package com.gpb.metadata.ingestion.dto;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DatabaseServiceMetadataDto implements MetadataDto {
    private String name;
    private String serviceType;
    private String displayName;
    private JsonNode connection;
}
