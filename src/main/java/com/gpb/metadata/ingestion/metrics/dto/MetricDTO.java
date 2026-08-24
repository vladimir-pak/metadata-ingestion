package com.gpb.metadata.ingestion.metrics.dto;

import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MetricDTO {
    private long errorCount;
    private long successCount;
    private LocalDateTime start;
    private LocalDateTime end;
}
