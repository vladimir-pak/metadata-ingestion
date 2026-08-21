package com.gpb.metadata.ingestion.service;

import java.time.LocalDateTime;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.gpb.metadata.ingestion.enums.IngestionMetricJob;
import com.gpb.metadata.ingestion.metrics.MetricCounter;
import com.gpb.metadata.ingestion.repository.MetadataIngestionMetricRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class IngestionMetricService {

    private final MetadataIngestionMetricRepository metricRepository;

    @Value("${spring.application.name:metadata-ingestion}")
    private String appName;

    public <T> T execute(
            String serviceName,
            IngestionMetricJob jobName,
            Function<MetricCounter, T> action) {

        LocalDateTime startDttm = LocalDateTime.now();
        MetricCounter counter = new MetricCounter();

        try {
            return action.apply(counter);
        } finally {
            LocalDateTime endDttm = LocalDateTime.now();

            try {
                metricRepository.save(
                        serviceName,
                        jobName,
                        counter.getSuccessCount(),
                        counter.getErrorCount(),
                        startDttm,
                        endDttm,
                        appName
                );

                log.info(
                        "Metric saved. serviceName={}, jobName={}, success={}, errors={}, start={}, end={}",
                        serviceName,
                        jobName,
                        counter.getSuccessCount(),
                        counter.getErrorCount(),
                        startDttm,
                        endDttm
                );

            } catch (Exception e) {
                /*
                 * Ошибка сохранения метрики не должна маскировать
                 * ошибку основного ingestion.
                 */
                log.error(
                        "Failed to save ingestion metric. serviceName={}, jobName={}",
                        serviceName,
                        jobName,
                        e
                );
            }
        }
    }
}