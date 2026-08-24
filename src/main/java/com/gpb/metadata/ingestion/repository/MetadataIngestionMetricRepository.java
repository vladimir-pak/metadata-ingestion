package com.gpb.metadata.ingestion.repository;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.metadata.ingestion.metrics.MetricCounter;
import com.gpb.metadata.ingestion.metrics.enums.IngestionMetricJob;
import com.gpb.metadata.ingestion.metrics.enums.IngestionStatus;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class MetadataIngestionMetricRepository {

    @Qualifier("jdbcTemplate")
    private final JdbcTemplate jdbcTemplate;

    private static final String INSERT_JOB = """
            INSERT INTO public.metadata_ingestion (
                run_id,
                job_name,
                appname,
                status,
                service_name,
                success_count,
                error_count
            )
            VALUES (?, ?, ?, ?, ?, 0, 0)
            """;

    /**
     * Создаём все процессы конкретного ingestion
     * сразу со статусом QUEUE.
     */
    public void createQueuedJobs(
            String runId,
            String serviceName,
            String appName) {

        List<Object[]> batchArgs = Arrays.stream(IngestionMetricJob.values())
                .map(job -> new Object[]{
                        runId,
                        job.name(),
                        appName,
                        IngestionStatus.QUEUE.name(),
                        serviceName
                })
                .toList();

        jdbcTemplate.batchUpdate(
                INSERT_JOB,
                batchArgs
        );
    }

    /**
     * QUEUE -> RUNNING
     */
    public void markRunning(
            String runId,
            IngestionMetricJob job) {

        int updated = jdbcTemplate.update(
                """
                UPDATE public.metadata_ingestion
                   SET status = ?,
                       start_dttm = now()
                 WHERE run_id = ?
                   AND job_name = ?
                   AND status = ?
                """,
                IngestionStatus.RUNNING.name(),
                runId,
                job.name(),
                IngestionStatus.QUEUE.name()
        );

        checkSingleUpdate(
                updated,
                runId,
                job,
                IngestionStatus.RUNNING
        );
    }

    /**
     * RUNNING -> DONE
     */
    public void markDone(
            String runId,
            IngestionMetricJob job,
            MetricCounter counter) {

        int updated = jdbcTemplate.update(
                """
                UPDATE public.metadata_ingestion
                   SET status = ?,
                       success_count = ?,
                       error_count = ?,
                       end_dttm = now()
                 WHERE run_id = ?
                   AND job_name = ?
                   AND status = ?
                """,
                IngestionStatus.DONE.name(),
                counter.getSuccessCount(),
                counter.getErrorCount(),
                runId,
                job.name(),
                IngestionStatus.RUNNING.name()
        );

        checkSingleUpdate(
                updated,
                runId,
                job,
                IngestionStatus.DONE
        );
    }

    /**
     * RUNNING -> FAILED
     */
    public void markFailed(
            String runId,
            IngestionMetricJob job,
            MetricCounter counter) {

        int updated = jdbcTemplate.update(
                """
                UPDATE public.metadata_ingestion
                   SET status = ?,
                       success_count = ?,
                       error_count = ?,
                       end_dttm = now()
                 WHERE run_id = ?
                   AND job_name = ?
                   AND status = ?
                """,
                IngestionStatus.FAILED.name(),
                counter.getSuccessCount(),
                counter.getErrorCount(),
                runId,
                job.name(),
                IngestionStatus.RUNNING.name()
        );

        checkSingleUpdate(
                updated,
                runId,
                job,
                IngestionStatus.FAILED
        );
    }

    /**
     * После аварийного завершения текущего этапа:
     *
     * все процессы, которые ещё даже не стартовали,
     * QUEUE -> SKIPPED.
     *
     * start_dttm остаётся NULL, потому что job не запускался.
     */
    public int markQueuedAsSkipped(String runId) {

        return jdbcTemplate.update(
                """
                UPDATE public.metadata_ingestion
                   SET status = ?,
                       end_dttm = now()
                 WHERE run_id = ?
                   AND status = ?
                """,
                IngestionStatus.SKIPPED.name(),
                runId,
                IngestionStatus.QUEUE.name()
        );
    }

    private void checkSingleUpdate(
            int updated,
            String runId,
            IngestionMetricJob job,
            IngestionStatus targetStatus) {

        if (updated != 1) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot change ingestion status. " +
                            "runId=%s, job=%s, targetStatus=%s, updated=%d",
                            runId,
                            job,
                            targetStatus,
                            updated
                    )
            );
        }
    }

    /**
     * Создание партиции
     */
    public void createMetricPartition() {
        String table = "public.metadata_ingestion";

        YearMonth currentMonth = YearMonth.now();

        String partitionName = String.format(
                "public.metadata_ingestion_%s",
                currentMonth.format(DateTimeFormatter.ofPattern("yyyy_MM"))
        );

        LocalDate from = currentMonth.atDay(1);
        LocalDate to = currentMonth.plusMonths(1).atDay(1);

        String sql = String.format("""
                CREATE TABLE IF NOT EXISTS %s
                PARTITION OF %s
                FOR VALUES FROM ('%s 00:00:00')
                        TO ('%s 00:00:00')
                """,
                partitionName,
                table,
                from,
                to
        );

        jdbcTemplate.execute(sql);
    }
}
