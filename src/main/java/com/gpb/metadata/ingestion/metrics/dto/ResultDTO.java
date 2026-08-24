package com.gpb.metadata.ingestion.metrics.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ResultDTO {
    private String jobName;
    private MetricDTO metric;
}
