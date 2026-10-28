package com.gpb.metadata.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnMetadataDto {

    private String name;
    private String dataType;
    private String dataTypeDisplay;

    private int dataLength;
    private int precision;
    private int ordinalPosition;
    private int scale;

    private String description;
    private String constraint;
    private String arrayDataType;

}
