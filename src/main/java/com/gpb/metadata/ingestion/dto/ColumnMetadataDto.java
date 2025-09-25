package com.gpb.metadata.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gpb.metadata.ingestion.enums.TypesWithDataLength;

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

    @JsonProperty("dataTypeDisplay")
    private String dataTypeDisplay;

    @JsonProperty("dataLength")
    private String dataLength;

    @JsonProperty("description")
    private String description;

    @JsonProperty("ordinalPosition")
    private String ordinalPosition;

    @JsonProperty("constraint")
    private String constraint;

    /*
     * setter-заглушка с проверкой обязательности заполнения dataLength. Если пусто, то 0
     */
    public void setDataLength(String dataLength) {
        this.dataLength = TypesWithDataLength.getProcessedDataLength(this.dataType, dataLength);
    }
}
