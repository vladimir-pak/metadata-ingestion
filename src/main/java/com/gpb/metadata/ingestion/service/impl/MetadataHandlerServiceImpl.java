package com.gpb.metadata.ingestion.service.impl;

import java.util.*;

import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.properties.MetadataSchemasProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.reactive.function.client.WebClient;

import com.gpb.metadata.ingestion.cache.CacheComparisonResult;
import com.gpb.metadata.ingestion.dto.mapper.MapperDto;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.postgres.DatabaseMetadata;
import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;
import com.gpb.metadata.ingestion.model.postgres.TableMetadata;
import com.gpb.metadata.ingestion.properties.JwtTokenProvider;
import com.gpb.metadata.ingestion.properties.WebClientProperties;
import com.gpb.metadata.ingestion.service.MetadataHandlerService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetadataHandlerServiceImpl implements MetadataHandlerService {

    private final DatabaseMetadataCacheServiceImpl databaseCacheService;
    private final SchemaMetadataCacheServiceImpl schemaCacheService;
    private final TableMetadataCacheServiceImpl tableCacheService;
    private final MapperDto mapperDto;


    private final WebClient webClient;
    private final JwtTokenProvider tokenProvider;
    private final WebClientProperties webClientProperties;
    private final MetadataSchemasProperties schemasProperties;

    @Value("${ord.api.max-connections:5}")
    private Integer maxConn;

    @Async
    public void startAsync(String schemaName, String serviceName) {
        start(schemaName,serviceName);
    }

    @Override
    public void start(String schemaName, String serviceName) {
        final Map<String, ServiceType> schemaTypeMap = Map.of(
                schemasProperties.getPostgres(), ServiceType.POSTGRES,
                schemasProperties.getMssql(), ServiceType.MSSQL,
                schemasProperties.getOracle(), ServiceType.ORACLE
        );

        ServiceType type = schemaTypeMap.get(schemaName);
        if (type == null) {
            throw new IllegalArgumentException("Неизвестный тип схемы: " + schemaName);
        }
        CacheComparisonResult<DatabaseMetadata> cacheDatabase =
            databaseCacheService.synchronizeWithDatabase(schemaName, serviceName);
        CacheComparisonResult<SchemaMetadata> cacheSchema = 
            schemaCacheService.synchronizeWithDatabase(schemaName, serviceName);
        CacheComparisonResult<TableMetadata> cacheTable = 
            tableCacheService.synchronizeWithDatabase(schemaName, serviceName);
        
        /*
         * Добавляем сущности в порядке очередности:
         * 1. БД
         * 2. Схемы
         * 3. Таблицы
         */
        Collection<DatabaseMetadata> putDatabases = cacheDatabase.getPutRecords().values();
        databasePutRequest(putDatabases, webClientProperties.getDatabaseEndpoint());

        Collection<SchemaMetadata> putSchemas = cacheSchema.getPutRecords().values();
        schemaPutRequest(putSchemas, webClientProperties.getSchemaEndpoint());

        Collection<TableMetadata> putTables = cacheTable.getPutRecords().values();
        tablePutRequest(putTables, webClientProperties.getTableEndpoint(), type);

        /*
         * Удаляем сущности в порядке очередности:
         * 1. Таблицы
         * 2. Схемы
         * 3. БД
         */
        Collection<TableMetadata> toDeleteTable = cacheTable.getDeletedRecords().values();
        tableDeleteRequest(toDeleteTable, webClientProperties.getTableDeleteEndpoint());

        Collection<SchemaMetadata> toDeleteSchema = cacheSchema.getDeletedRecords().values();
        schemaDeleteRequest(toDeleteSchema, webClientProperties.getSchemaDeleteEndpoint());

        Collection<DatabaseMetadata> toDeleteDatabase = cacheDatabase.getDeletedRecords().values();
        databaseDeleteRequest(toDeleteDatabase, webClientProperties.getDatabaseDeleteEndpoint());
    }

    private void databasePutRequest(Collection<DatabaseMetadata> meta, String endpoint) {
        Flux.fromIterable(meta)
                .flatMap(value -> 
                    putRequest(endpoint, mapperDto.getDto(DbObjectType.DATABASE, value, null), Void.class)
                        .doOnSuccess(response -> 
                            log.info("Успешно создано/обновлено {}: {}", DbObjectType.DATABASE.name().toLowerCase(), value.getFqn())
                        )
                        .doOnError(error -> 
                            log.error("Ошибка при создании/обновлении {}: {}", value.getFqn(), error.getMessage())
                        )
                        .onErrorResume(error -> Mono.empty()),
                    maxConn
                )
                .then() // Преобразуем в Mono<Void>
                .block(); // Ждем завершения всех
    }

    private void databaseDeleteRequest(Collection<DatabaseMetadata> meta, String endpoint) {
        Flux.fromIterable(meta)
                .flatMap(value -> 
                    deleteRequest(String.format("%s/%s", endpoint, value.getFqn()))
                        .doOnSuccess(response -> 
                            log.info("Успешно удалено {}", value.getFqn())
                        )
                        .doOnError(error -> 
                            log.error("Ошибка при удалении {}: {}", value.getFqn(), error.getMessage())
                        )
                        .onErrorResume(error -> Mono.empty()),
                    maxConn
                )
                .then() // Преобразуем в Mono<Void>
                .block(); // Ждем завершения всех
    }

    private void schemaPutRequest(Collection<SchemaMetadata> meta, String endpoint) {
        Flux.fromIterable(meta)
                .flatMap(value -> 
                    putRequest(endpoint, mapperDto.getDto(DbObjectType.SCHEMA, value, null), Void.class)
                        .doOnSuccess(response -> 
                            log.info("Успешно создано/обновлено {}: {}", DbObjectType.SCHEMA.name().toLowerCase(), value.getFqn())
                        )
                        .doOnError(error -> 
                            log.error("Ошибка при создании/обновлении {}: {}", value.getFqn(), error.getMessage())
                        )
                        .onErrorResume(error -> Mono.empty()),
                    maxConn
                )
                .then() // Преобразуем в Mono<Void>
                .block(); // Ждем завершения всех
    }

    private void schemaDeleteRequest(Collection<SchemaMetadata> meta, String endpoint) {
        Flux.fromIterable(meta)
                .flatMap(value -> 
                    deleteRequest(String.format("%s/%s", endpoint, value.getFqn()))
                        .doOnSuccess(response -> 
                            log.info("Успешно удалено {}", value.getFqn())
                        )
                        .doOnError(error -> 
                            log.error("Ошибка при удалении {}: {}", value.getFqn(), error.getMessage())
                        )
                        .onErrorResume(error -> Mono.empty()),
                    maxConn
                )
                .then() // Преобразуем в Mono<Void>
                .block(); // Ждем завершения всех
    }

    private void tablePutRequest(Collection<TableMetadata> meta, String endpoint, ServiceType serviceType) {
        Flux.fromIterable(meta)
                .flatMap(value ->
                                putRequest(endpoint, mapperDto.getDto(DbObjectType.TABLE, value, serviceType), Void.class)
                                        .doOnSuccess(response ->
                                                log.info("Успешно создано/обновлено {}: {}", DbObjectType.TABLE.name().toLowerCase(), value.getFqn())
                                        )
                                        .doOnError(error ->
                                                log.error("Ошибка при создании/обновлении {}: {}", value.getFqn(), error.getMessage())
                                        )
                                        .onErrorResume(error -> Mono.empty()),
                        maxConn
                )
                .then()
                .block();
    }

    private void tableDeleteRequest(Collection<TableMetadata> meta, String endpoint) {
        Flux.fromIterable(meta)
                .flatMap(value -> 
                    deleteRequest(String.format("%s/%s", endpoint, value.getFqn()))
                        .doOnSuccess(response -> 
                            log.info("Успешно удалено {}", value.getFqn())
                        )
                        .doOnError(error -> 
                            log.error("Ошибка при удалении {}: {}", value.getFqn(), error.getMessage())
                        )
                        .onErrorResume(error -> Mono.empty()),
                    maxConn
                )
                .then() // Преобразуем в Mono<Void>
                .block(); // Ждем завершения всех
    }

    // PUT запрос с динамическим получением токена
    private <T> Mono<T> putRequest(String endpoint, Object requestBody, Class<T> responseType) {
        return webClient.put()
                .uri(uriBuilder -> uriBuilder
                        .path(endpoint)
                        .build())
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenProvider.getToken())
                .bodyValue(requestBody)
                .exchangeToMono(response -> {
                    if (response.statusCode().isError()) {
                        return response.bodyToMono(String.class)
                                .flatMap(errorBody -> 
                                    Mono.error(new RuntimeException("HTTP " + response.statusCode() + " - " + errorBody))
                                );
                    } else {
                        return response.bodyToMono(responseType);
                    }
                });
    }
    
    // DELETE запрос с динамическим получением токена
    private Mono<Void> deleteRequest(String endpoint) {
        log.info("Endpoint for deletion: {}", endpoint);
        return webClient.delete()
                .uri(uriBuilder -> uriBuilder
                        .path(endpoint)
                        .queryParam("recursive", "true")
                        .build())
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + tokenProvider.getToken())
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), response -> 
                    Mono.error(new HttpClientErrorException(response.statusCode())))
                .onStatus(status -> status.is5xxServerError(), response -> 
                    Mono.error(new HttpServerErrorException(response.statusCode())))
                .bodyToMono(Void.class);
    }
}
