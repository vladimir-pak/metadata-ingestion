package com.gpb.metadata.ingestion.model.schema;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableConstraints {
    private List<String> columns;

    private String constraintType;

    private List<String> referredColumns;
}
