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
import com.gpb.metadata.ingestion.service.MetadataHandlerService;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/ingestion")
@RequiredArgsConstructor
@Tag(name = "ingestion", description = "API запуска приема метаданных")
public class CacheController {

    private final MetadataHandlerService metadataHandlerService;
    private final MetadataSchemasProperties schemasProperties;
    private final CacheService cacheService;
    private final SvoiCustomLogger logger;

    @PostMapping("/start/postgres")
    public ResponseEntity<String> startPostgres(@RequestBody RequestBodyDto body, HttpServletRequest request) {
        logger.logApiCall(request, "startIngestionPostgres", body);
        return startInternal(schemasProperties.getPostgres(), body.getServiceName());
    }

    @PostMapping("/start/oracle")
    public ResponseEntity<String> startOracle(@RequestBody RequestBodyDto body, HttpServletRequest request) {
        logger.logApiCall(request, "startIngestionOracle", body);
        return startInternal(schemasProperties.getOracle(), body.getServiceName());
    }

    @PostMapping("/start/mssql")
    public ResponseEntity<String> startMssql(@RequestBody RequestBodyDto body, HttpServletRequest request) {
        logger.logApiCall(request, "startIngestionMssql", body);
        return startInternal(schemasProperties.getMssql(), body.getServiceName());
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

    private ResponseEntity<String> startInternal(String schema, String serviceName) {
        try {
            metadataHandlerService.startAsync(schema, serviceName);
            return ResponseEntity.ok(
                    String.format("Ingestion for %s from schema %s starting", serviceName, schema)
            );
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to start replication: " + e.getMessage());
        }
    }

}
