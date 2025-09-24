package com.gpb.metadata.ingestion.dto;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableMetadataDto implements MetadataDto {
    private String name;
    private String displayName;
    private String databaseSchema;
    private String description;
    private List<ColumnMetadataDto> columns;
}
