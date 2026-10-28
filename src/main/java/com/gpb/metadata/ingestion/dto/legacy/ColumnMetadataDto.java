package com.gpb.metadata.ingestion.dto.legacy;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ColumnMetadataDto {

    private String name;
    private String dataType;
    private String dataTypeDisplay;

    private int dataLength;
    private int precision;
    private int scale;
    private int ordinalPosition;

    private String description;
    private String constraint;
    private String arrayDataType;

}
