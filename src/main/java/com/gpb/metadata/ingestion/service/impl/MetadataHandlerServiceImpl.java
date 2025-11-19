package com.gpb.metadata.ingestion.service.impl;

import java.net.InetAddress;
import java.net.URI;
import java.util.*;

import com.gpb.metadata.ingestion.config.KeycloakConfig;
import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.log.SvoiCustomLogger;
import com.gpb.metadata.ingestion.properties.MetadataSchemasProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.gpb.metadata.ingestion.cache.CacheComparisonResult;
import com.gpb.metadata.ingestion.dto.mapper.MapperDto;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.postgres.DatabaseMetadata;
import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;
import com.gpb.metadata.ingestion.model.postgres.TableMetadata;
import com.gpb.metadata.ingestion.properties.WebClientProperties;
import com.gpb.metadata.ingestion.service.KeycloakAuthService;
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
    private final SvoiCustomLogger svoiCustomLogger;

    private final WebClient webClient;
    private final WebClientProperties webClientProperties;
    private final MetadataSchemasProperties schemasProperties;
    private final KeycloakConfig keycloakConfig;

    private final KeycloakAuthService keycloakAuthService;
    private String token;

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

        token = keycloakAuthService.getValidAccessToken();
        
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

    private <T> Mono<T> putRequest(String endpoint, Object requestBody, Class<T> responseType) {
        OrdaHost orda = parseOrdaHost();
        long start = System.currentTimeMillis();
        String username = keycloakConfig.getUsername();

        return webClient.put()
                .uri(endpoint)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .bodyValue(requestBody)
                .exchangeToMono(response -> {

                    long duration = System.currentTimeMillis() - start;

                    if (response.statusCode().isError()) {

                        return response.bodyToMono(String.class)
                                .flatMap(err -> {
                                    svoiCustomLogger.logOrdaRequest(
                                            endpoint,
                                            "PUT",
                                            response.statusCode().value(),
                                            duration,
                                            err,
                                            orda.dns(),
                                            orda.ip(),
                                            orda.port(),
                                            username
                                    );
                                    return Mono.error(new RuntimeException(err));
                                });
                    }

                    svoiCustomLogger.logOrdaRequest(
                            endpoint,
                            "PUT",
                            response.statusCode().value(),
                            duration,
                            null,
                            orda.dns(),
                            orda.ip(),
                            orda.port(),
                            username
                    );

                    return response.bodyToMono(responseType);
                });
    }

    private Mono<Void> deleteRequest(String endpoint) {

        OrdaHost orda = parseOrdaHost();
        long start = System.currentTimeMillis();
        String username = keycloakConfig.getUsername();

        return webClient.delete()
                .uri(uriBuilder -> uriBuilder
                        .path(endpoint)
                        .queryParam("recursive", "true")
                        .build())
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .exchangeToMono(response -> {

                    long duration = System.currentTimeMillis() - start;

                    if (response.statusCode().isError()) {
                        return response.bodyToMono(String.class)
                                .flatMap(err -> {
                                    svoiCustomLogger.logOrdaRequest(
                                            endpoint,
                                            "DELETE",
                                            response.statusCode().value(),
                                            duration,
                                            err,
                                            orda.dns(),
                                            orda.ip(),
                                            orda.port(),
                                            username
                                    );
                                    return Mono.error(new RuntimeException(err));
                                });
                    }

                    svoiCustomLogger.logOrdaRequest(
                            endpoint,
                            "DELETE",
                            response.statusCode().value(),
                            duration,
                            null,
                            orda.dns(),
                            orda.ip(),
                            orda.port(),
                            username
                    );

                    return Mono.empty();
                });
    }

    private record OrdaHost(String dns, String ip, int port) {}

    private OrdaHost parseOrdaHost() {
        try {
            URI uri = new URI(webClientProperties.getBaseUrl());
            String dns = uri.getHost();
            int port = uri.getPort() == -1 ? 443 : uri.getPort();
            String ip = InetAddress.getByName(dns).getHostAddress();
            return new OrdaHost(dns, ip, port);
        } catch (Exception e) {
            return new OrdaHost("unknown", "unknown", 443);
        }
    }
}
