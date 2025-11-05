package com.gpb.metadata.ingestion.log;

import com.gpb.metadata.ingestion.logrepository.LogPartitionRepository;
import com.gpb.metadata.ingestion.service.CefLogFileService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LogScheduler {
    private final SvoiCustomLogger svoiCustomLogger;
    private final LogPartitionRepository logPartitionRepository;
    private final CefLogFileService cefLogger;

    @Scheduled(cron = "${logs-database.task-create-partition}")
    public void createPartition() {
        logPartitionRepository.createTodayPartition();
    }

    @Scheduled(cron = "${clean-database-logs.task-cleaner-schedule}")
    public void cleanPartition() {
        logPartitionRepository.dropOldPartitions();
        svoiCustomLogger.send("cleanLogs", "Clean Logs", "Cleaned old logs", SvoiSeverityEnum.ONE);
    }

    @Scheduled(cron = "${clean-database-logs.task-cleaner-schedule}")
    public void cleanupOldLogs() {
        cefLogger.rotateLogFile();
        cefLogger.cleanupOldLogs();
    }
}
