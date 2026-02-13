package com.gpb.metadata.ingestion.dto.lineage;

import java.util.List;

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
public class LineageDetails {
    private List<ColumnsLineage> columnsLineage;
    private String sqlQuery;
    private LineageSource source;
}
