package com.gpb.metadata.ingestion.controller;

import com.gpb.metadata.ingestion.log.SvoiCustomLogger;
import com.gpb.metadata.ingestion.properties.MetadataSchemasProperties;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.metadata.ingestion.dto.RequestBodyDto;
import com.gpb.metadata.ingestion.service.CacheService;
import com.gpb.metadata.ingestion.service.IngestionMetricService;
import com.gpb.metadata.ingestion.service.MetadataHandlerService;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/ingestion")
@RequiredArgsConstructor
@Tag(name = "ingestion", description = "API запуска приема метаданных")
@Slf4j
public class CacheController {

    private final MetadataHandlerService metadataHandlerService;
    private final MetadataSchemasProperties schemasProperties;
    private final CacheService cacheService;
    private final SvoiCustomLogger logger;
    private final IngestionMetricService ingestionMetricService;

    @PostMapping("/start/postgres")
    public ResponseEntity<String> startPostgres(@RequestBody RequestBodyDto body, HttpServletRequest request) {
        logger.logApiCall(request, "startIngestionPostgres", body);
        return startInternal(
            schemasProperties.getPostgres(), 
            body.getServiceName(),
            body.isAsync()
        );
    }

    @PostMapping("/start/oracle")
    public ResponseEntity<String> startOracle(@RequestBody RequestBodyDto body, HttpServletRequest request) {
        logger.logApiCall(request, "startIngestionOracle", body);
        return startInternal(
            schemasProperties.getOracle(), 
            body.getServiceName(),
            body.isAsync()
        );
    }

    @PostMapping("/start/mssql")
    public ResponseEntity<String> startMssql(@RequestBody RequestBodyDto body, HttpServletRequest request) {
        logger.logApiCall(request, "startIngestionMssql", body);
        return startInternal(
            schemasProperties.getMssql(), 
            body.getServiceName(),
            body.isAsync()
        );
    }

    @PostMapping("/start/sapiq")
    public ResponseEntity<String> startSapIq(@RequestBody RequestBodyDto body, HttpServletRequest request) {
        logger.logApiCall(request, "startIngestionSapIq", body);
        return startInternal(
            schemasProperties.getSapiq(), 
            body.getServiceName(),
            body.isAsync()
        );
    }

    @DeleteMapping("/clean/{schema}")
    public ResponseEntity<String> cleanCache(
        @RequestBody RequestBodyDto body, 
        HttpServletRequest request, 
        @PathVariable String schema
    ) {
        logger.logApiCall(request, "cleanCache", body);
        cacheService.cleanCache(schema, body.getServiceName());
        return ResponseEntity.ok(
            String.format("Cache for %s from schema %s finished", body.getServiceName(), schema)
        );
    }

    private ResponseEntity<String> startInternal(
            String schema,
            String serviceName,
            boolean async) {

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
                    schema,
                    serviceName,
                    runId
                );
            } else {
                metadataHandlerService.start(
                    schema,
                    serviceName,
                    runId
                );
            }

            return ResponseEntity.ok(
                    String.format(
                        "Ingestion run %s for %s from schema %s started",
                        runId,
                        serviceName,
                        schema
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
