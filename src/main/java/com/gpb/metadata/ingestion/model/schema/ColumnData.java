package com.gpb.metadata.ingestion.model.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnData {

    private String name;
    private String dataType;
    private String dataTypeDisplay;

    @Builder.Default
    @JsonSetter(nulls = Nulls.SKIP)
    private int dataLength = 0;

    @Builder.Default
    @JsonSetter(nulls = Nulls.SKIP)
    private int precision = 0;

    @Builder.Default
    @JsonSetter(nulls = Nulls.SKIP)
    private int scale = 0;

    private int ordinalPosition;

    private String description;
    private String constraint;
}
