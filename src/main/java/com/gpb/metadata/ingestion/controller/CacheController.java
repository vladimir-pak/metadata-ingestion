package com.gpb.metadata.ingestion.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.metadata.ingestion.service.MetadataHandlerService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/ingestion")
@RequiredArgsConstructor
@Tag(name = "ingestion", description = "API запуска приема метаданных")
public class CacheController {

    // private DatabaseMetadataCacheServiceImpl cacheService;
    private final MetadataHandlerService metadataHandlerService;

    @PostMapping("/start/{serviceName}")
    @Operation(summary = "Запуск приема метаданных по наименованию сервиса")
    public ResponseEntity<String> start(@PathVariable String serviceName) {
        try {
            metadataHandlerService.startAsync(serviceName);
            return ResponseEntity.ok(String.format("Ingestion for %s added to queue", serviceName));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to start replication: " + e.getMessage());
        }
    }
}
