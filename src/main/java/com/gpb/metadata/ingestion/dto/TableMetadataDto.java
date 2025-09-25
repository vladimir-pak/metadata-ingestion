package com.gpb.metadata.ingestion.dto;

import java.util.List;

import com.gpb.metadata.ingestion.model.schema.TableConstraints;

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
    private String tableType;
    private Boolean isProjectEntity;
    private String viewDefinition;
    private List<ColumnMetadataDto> columns;
    private List<TableConstraints> tableConstraints;
}
