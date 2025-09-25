package com.gpb.metadata.ingestion.model.schema;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableData {

    @JsonProperty("tableType")
    private String tableType;

    @JsonProperty("viewDefinition")
    private String viewDefinition;

    @JsonProperty("columns")
    private List<ColumnData> columns;

    @JsonProperty("tableConstraints")
    private List<TableConstraints> tableConstraints;
    
    // Для хранения всех остальных полей, которых нет в DTO
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<>();
    
    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        additionalProperties.put(name, value);
    }
}
