package com.gpb.metadata.ingestion.dto.lineage;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@NoArgsConstructor 
@AllArgsConstructor 
@Builder
public class ColumnsLineage {
    private List<String> fromColumns; // List<fqn>
    private String toColumn; // fqn
}
