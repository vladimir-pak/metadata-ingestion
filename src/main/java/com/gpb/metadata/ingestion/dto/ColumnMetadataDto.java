package com.gpb.metadata.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ColumnMetadataDto {
    @JsonProperty("name")
    private String name;

    @JsonProperty("dataType")
    private String dataType;

    @JsonProperty("dataLength")
    private String dataLength;

    @JsonProperty("description")
    private String description;

    @JsonProperty("constraint")
    private String constraint;
}
