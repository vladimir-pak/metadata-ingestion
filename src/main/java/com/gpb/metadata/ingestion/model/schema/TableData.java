package com.gpb.metadata.ingestion.model.schema;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableData {

    private String tableType;
    private String viewDefinition;

    private List<ColumnData> columns;

    private List<TableConstraints> tableConstraints;
    
    // Для хранения всех остальных полей, которых нет в DTO
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<>();
    
    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        additionalProperties.put(name, value);
    }
}
