package com.gpb.metadata.ingestion.controller;

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
    public ResponseEntity<Void> synchronize(@PathVariable String serviceName) {
        metadataHandlerService.start(serviceName);
        return ResponseEntity.ok().build();
    }
    
}
