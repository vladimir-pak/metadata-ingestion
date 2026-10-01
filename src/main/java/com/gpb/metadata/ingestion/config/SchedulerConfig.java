package com.gpb.metadata.ingestion.config;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.gpb.metadata.ingestion.repository.MetadataIngestionMetricRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class SchedulerConfig {
    private final MetadataIngestionMetricRepository repository;

    @Scheduled(cron = "${metadata.metric.create-partition-schedule:0 0 0 1 * *}")
    public void createPartition() {
        repository.createMetricPartition();
    }
}
