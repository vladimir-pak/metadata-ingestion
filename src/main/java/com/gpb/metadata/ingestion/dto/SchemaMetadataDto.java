package com.gpb.metadata.ingestion.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SchemaMetadataDto implements MetadataDto {
    private String name;
    private String displayName;
    private String database;
}
