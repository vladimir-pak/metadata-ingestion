package com.gpb.metadata.ingestion.service;

import java.util.UUID;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.gpb.metadata.ingestion.metrics.enums.IngestionMetricJob;
import com.gpb.metadata.ingestion.metrics.MetricCounter;
import com.gpb.metadata.ingestion.repository.MetadataIngestionMetricRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class IngestionMetricService {

    private final MetadataIngestionMetricRepository repository;

    @Value("${spring.application.name:metadata-ingestion}")
    private String appName;

    /**
     * Создаёт новый ingestion run и сразу
     * регистрирует все этапы как QUEUE.
     */
    public String createRun(String serviceName) {

        String runId = UUID.randomUUID().toString();

        repository.createQueuedJobs(
                runId,
                serviceName,
                appName
        );

        log.info(
                "Created ingestion run. runId={}, serviceName={}",
                runId,
                serviceName
        );

        return runId;
    }

    /**
     * Выполняет отдельный job:
     * QUEUE -> RUNNING -> DONE
     *
     * либо:
     * QUEUE -> RUNNING -> FAILED
     */
    public <T> T execute(
            String runId,
            IngestionMetricJob job,
            Function<MetricCounter, T> action) {

        MetricCounter counter = new MetricCounter();

        repository.markRunning(runId,job);

        log.info(
                "Ingestion job started. runId={}, job={}",
                runId,
                job
        );

        try {

            T result = action.apply(counter);

            repository.markDone(runId, job, counter);

            log.info(
                    "Ingestion job completed. " +
                    "runId={}, job={}, successCount={}, errorCount={}",
                    runId,
                    job,
                    counter.getSuccessCount(),
                    counter.getErrorCount()
            );

            return result;

        } catch (RuntimeException e) {

            try {

                repository.markFailed(runId, job, counter);

            } catch (RuntimeException metricException) {

                /*
                 * Не маскируем первоначальную ошибку ingestion.
                 */
                e.addSuppressed(metricException);

                log.error(
                        "Failed to update ingestion job status to FAILED. " +
                        "runId={}, job={}",
                        runId,
                        job,
                        metricException
                );
            }

            log.error(
                    "Ingestion job failed. " +
                    "runId={}, job={}, successCount={}, errorCount={}",
                    runId,
                    job,
                    counter.getSuccessCount(),
                    counter.getErrorCount(),
                    e
            );

            throw e;
        }
    }

    public void createViewParsingJob(
            String runId,
            String serviceName) {

        repository.createViewParsingJob(
                runId,
                serviceName,
                appName
        );

        log.info(
                "Created VIEW_PARSING job. runId={}, serviceName={}",
                runId,
                serviceName
        );
    }

    /**
     * Все ещё не запущенные процессы переводим
     * из QUEUE в SKIPPED.
     */
    public void skipRemaining(String runId) {

        int skipped = repository.markQueuedAsSkipped(runId);

        log.info(
                "Skipped remaining ingestion jobs. runId={}, count={}",
                runId,
                skipped
        );
    }
}