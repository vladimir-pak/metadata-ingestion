package com.gpb.metadata.ingestion.dto.lineage;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data 
@NoArgsConstructor 
@AllArgsConstructor 
@Builder
public class EntityReference {
    private String type; // "table"
    private String id;  // uuid
}
