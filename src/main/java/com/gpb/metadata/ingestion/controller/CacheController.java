package com.gpb.metadata.ingestion.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.metadata.ingestion.cef.SvoiApiLog;
import com.gpb.metadata.ingestion.dto.RequestBodyDto;
import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.service.CacheService;
import com.gpb.metadata.ingestion.service.IngestionMetricService;
import com.gpb.metadata.ingestion.service.MetadataHandlerService;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/ingestion")
@RequiredArgsConstructor
@Tag(name = "ingestion", description = "API запуска приема метаданных")
@Slf4j
public class CacheController {

    private final MetadataHandlerService metadataHandlerService;
    private final CacheService cacheService;
    private final IngestionMetricService ingestionMetricService;

    @PostMapping("/start")
    @SvoiApiLog(functionName = "TriggerIngestion")
    public ResponseEntity<String> startPostgres(
            @Valid @RequestBody RequestBodyDto body,
            HttpServletRequest request) {
        return startInternal(
            body.getServiceType(), 
            body.getServiceName(),
            body.isAsync(),
            body.isSkipDeletionThreshold()
        );
    }

    @DeleteMapping("/clean")
    @SvoiApiLog(functionName = "CleanCache")
    public ResponseEntity<String> cleanCache(
        @RequestBody RequestBodyDto body, 
        HttpServletRequest request
    ) {
        cacheService.cleanCache(
                body.getServiceType(), 
                body.getServiceName()
        );
        return ResponseEntity.ok(
            String.format("Cache for %s cleaned", body.getServiceName())
        );
    }

    private ResponseEntity<String> startInternal(
            ServiceType serviceType,
            String serviceName,
            boolean async,
            boolean skipDeletionThreshold) {

        String runId = null;
        try {
            /*
            * Здесь появляются:
            * DATABASE_UPSERT QUEUE
            * SCHEMA_UPSERT   QUEUE
            * TABLE_UPSERT    QUEUE
            * TABLE_DELETE    QUEUE
            * SCHEMA_DELETE   QUEUE
            * DATABASE_DELETE QUEUE
            * 
            * Атомарная операция:
            * advisory lock -> active check -> create QUEUE jobs
            */
            runId = ingestionMetricService
                    .createRunIfNotExecuting(serviceName);

            if (async) {
                metadataHandlerService.startAsync(
                    serviceType,
                    serviceName,
                    runId,
                    skipDeletionThreshold
                );
            } else {
                metadataHandlerService.start(
                    serviceType,
                    serviceName,
                    runId,
                    skipDeletionThreshold
                );
            }

            return ResponseEntity.ok(
                    String.format(
                        "Ingestion run %s for %s from schema %s started",
                        runId,
                        serviceName,
                        serviceType
                    )
            );
        } catch (IllegalArgumentException e) {
            if (runId != null) {
                skipSafely(runId);
            }
            return ResponseEntity
                    .badRequest()
                    .body(e.getMessage());
        } catch (Exception e) {
            if (runId != null) {
                skipSafely(runId);
            }
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(
                        "Failed to start replication: "
                        + e.getMessage()
                    );
        }
    }

    private void skipSafely(String runId) {
        try {
            ingestionMetricService.skipRemaining(runId);
        } catch (Exception e) {
            log.error(
                "Failed to mark ingestion jobs as SKIPPED. runId={}",
                runId,
                e
            );
        }
    }
}
