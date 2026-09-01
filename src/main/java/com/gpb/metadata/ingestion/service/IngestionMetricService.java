package com.gpb.metadata.ingestion.service;

import java.util.UUID;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.gpb.metadata.ingestion.exceptions.IngestionAlreadyRunningException;
import com.gpb.metadata.ingestion.metrics.MetricCounter;
import com.gpb.metadata.ingestion.metrics.enums.IngestionMetricJob;
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
     * Атомарно:
     *
     * 1. Блокируем запуск для serviceName.
     * 2. Проверяем активный ingestion.
     * 3. Создаём новый run.
     *
     * Advisory lock автоматически освобождается
     * после commit / rollback транзакции.
     */
    @Transactional
    public String createRunIfNotExecuting(
            String serviceName) {

        /*
         * Второй конкурентный запрос с тем же serviceName
         * будет ждать окончания этой транзакции.
         */
        repository.lockService(
                serviceName
        );

        /*
         * После получения lock обязательно повторно
         * проверяем состояние в БД.
         */
        if (
            repository.isExecuting(
                serviceName,
                appName
            )
        ) {
            log.warn(
                "Ingestion already executing. serviceName={}",
                serviceName
            );

            throw new IngestionAlreadyRunningException(
                serviceName
            );
        }

        String runId =
                UUID.randomUUID()
                        .toString();

        repository.createQueuedJobs(
            runId,
            serviceName,
            appName
        );

        log.info(
            "Created ingestion run. " +
            "runId={}, serviceName={}",
            runId,
            serviceName
        );

        return runId;
    }


    /**
     * Просто read-check.
     *
     * Можно использовать для GET/status и мониторинга,
     * но НЕ использовать для реализации эксклюзивного запуска.
     */
    public boolean isExecuting(
            String serviceName) {

        return repository.isExecuting(
            serviceName,
            appName
        );
    }

    /**
     * Выполнение отдельного job:
     *
     * QUEUE -> RUNNING -> DONE
     *
     * либо
     *
     * QUEUE -> RUNNING -> FAILED
     */
    public <T> T execute(
            String runId,
            IngestionMetricJob job,
            Function<MetricCounter, T> action) {

        MetricCounter counter =
                new MetricCounter();

        repository.markRunning(
            runId,
            job
        );

        log.info(
            "Ingestion job started. " +
            "runId={}, job={}",
            runId,
            job
        );

        try {

            T result =
                    action.apply(
                        counter
                    );

            repository.markDone(
                runId,
                job,
                counter
            );

            log.info(
                "Ingestion job completed. " +
                "runId={}, job={}, " +
                "successCount={}, errorCount={}",
                runId,
                job,
                counter.getSuccessCount(),
                counter.getErrorCount()
            );

            return result;

        } catch (RuntimeException e) {
            try {
                repository.markFailed(
                    runId,
                    job,
                    counter
                );

            } catch (
                    RuntimeException metricException
            ) {

                /*
                 * Ошибка записи метрики не должна
                 * маскировать исходную ошибку ingestion.
                 */
                e.addSuppressed(
                    metricException
                );

                log.error(
                    "Failed to update ingestion job " +
                    "status to FAILED. runId={}, job={}",
                    runId,
                    job,
                    metricException
                );
            }

            log.error(
                "Ingestion job failed. " +
                "runId={}, job={}, " +
                "successCount={}, errorCount={}",
                runId,
                job,
                counter.getSuccessCount(),
                counter.getErrorCount(),
                e
            );

            throw e;
        }
    }

    /**
     * Создаём VIEW_PARSING после успешного
     * основного metadata ingestion.
     */
    public void createViewParsingJob(
            String runId,
            String serviceName) {

        repository.createViewParsingJob(
            runId,
            serviceName,
            appName
        );

        log.info(
            "Created VIEW_PARSING job. " +
            "runId={}, serviceName={}",
            runId,
            serviceName
        );
    }

    /**
     * Оставшиеся QUEUE -> SKIPPED.
     */
    public void skipRemaining(
            String runId) {

        int skipped =
                repository.markQueuedAsSkipped(
                    runId
                );

        log.info(
            "Skipped remaining ingestion jobs. " +
            "runId={}, count={}",
            runId,
            skipped
        );
    }
}