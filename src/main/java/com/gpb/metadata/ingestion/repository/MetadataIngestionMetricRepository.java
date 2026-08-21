package com.gpb.metadata.ingestion.repository;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.gpb.metadata.ingestion.enums.IngestionMetricJob;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class MetadataIngestionMetricRepository {

    @Qualifier("jdbcTemplate")
    private final JdbcTemplate jdbcTemplate;

    private static final String INSERT_SQL = """
        INSERT INTO public.metadata_ingestion_metric (
            service_name,
            job_name,
            success_count,
            error_count,
            start_dttm,
            end_dttm,
            appname
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

    public void save(
            String serviceName,
            IngestionMetricJob jobName,
            long successCount,
            long errorCount,
            LocalDateTime startDttm,
            LocalDateTime endDttm,
            String appName) {

        jdbcTemplate.update(
                INSERT_SQL,
                serviceName,
                jobName.name(),
                successCount,
                errorCount,
                Timestamp.valueOf(startDttm),
                Timestamp.valueOf(endDttm),
                appName
        );
    }

    public void createMetricPartition() {
        String table = "public.metadata_ingestion_metric";

        YearMonth currentMonth = YearMonth.now();

        String partitionName = String.format(
                "public.metadata_ingestion_metric_%s",
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
