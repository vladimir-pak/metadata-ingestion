package com.gpb.metadata.ingestion.service.impl;

import java.util.*;
import java.util.stream.Collectors;

import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.exceptions.OrdaNotFoundException;
import com.gpb.metadata.ingestion.properties.MetadataSchemasProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import com.gpb.metadata.ingestion.cache.CacheComparisonResult;
import com.gpb.metadata.ingestion.dto.DatabaseServiceMetadataDto;
import com.gpb.metadata.ingestion.dto.lineage.AddLineageRequest;
import com.gpb.metadata.ingestion.dto.mapper.MapperDto;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.postgres.DatabaseMetadata;
import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;
import com.gpb.metadata.ingestion.model.postgres.TableMetadata;
import com.gpb.metadata.ingestion.model.schema.TableData;
import com.gpb.metadata.ingestion.properties.WebClientProperties;
import com.gpb.metadata.ingestion.service.KeycloakAuthService;
import com.gpb.metadata.ingestion.service.MetadataHandlerService;
import com.gpb.metadata.ingestion.utils.OrdaClient;
import com.gpb.metadata.ingestion.utils.ViewLineageRequestBuilder;

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
    // Парсер и сборщик Lineage для View
    private final ViewLineageRequestBuilder viewRequestBuilder;

    private final WebClientProperties webClientProperties;
    private final MetadataSchemasProperties schemasProperties;

    private final KeycloakAuthService keycloakAuthService;
    private final OrdaClient ordaClient;

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

        /**
         * Проверяем наличие DatabaseService в ОРДе
         * Если сервиса нет, то создаем
         */
        String token = keycloakAuthService.getValidAccessToken();
        if (token == null) {
            log.error("ORD access_token is not resolved");
            return;
        }
        boolean isExists = ordaClient.checkEntityExists(
                (webClientProperties.getDatabaseServiceEndpoint() + "/name/" + serviceName),
                token);
        if (!isExists) {
            log.info("Creating databaseService: {}", serviceName);
            String dbServiceUrl = webClientProperties.getDatabaseServiceEndpoint();
            DatabaseServiceMetadataDto dbServiceDto = DatabaseServiceMetadataDto.builder()
                    .name(serviceName)
                    .displayName(serviceName)
                    .serviceType(
                        type.getValue().substring(0, 1).toUpperCase() + 
                                type.getValue().substring(1))
                    .build();
            log.info("DTO creating DB: name={}; serviceType={}", dbServiceDto.getName(), dbServiceDto.getServiceType());
            Mono<String> resp = ordaClient.putRequest(dbServiceUrl, dbServiceDto, token, String.class);
            String respString = resp.block();
            log.info("Response for creating DB: {}", respString);
        }
        
        /*
         * Добавляем сущности в порядке очередности:
         * 1. БД
         * 2. Схемы
         * 3. Таблицы
         */
        Collection<DatabaseMetadata> putDatabases = cacheDatabase.getPutRecords().values();
        int dbError = databasePutRequest(putDatabases, webClientProperties.getDatabaseEndpoint());
        log.info("DbService \"{}\". Databases to PUT: {}. With errors: {}.",
                serviceName,
                putDatabases.size(),
                dbError);

        Collection<SchemaMetadata> putSchemas = cacheSchema.getPutRecords().values();
        int schemaError = schemaPutRequest(putSchemas, webClientProperties.getSchemaEndpoint());
        log.info("DbService \"{}\". Schemas to PUT: {}. With errors: {}.",
                serviceName,
                putSchemas.size(),
                schemaError);

        Collection<TableMetadata> putTables = cacheTable.getPutRecords().values();
        int tableError = tablePutRequest(putTables, webClientProperties.getTableEndpoint(), type);
        log.info("DbService \"{}\". Tables to PUT: {}. With errors: {}.",
                serviceName,
                putTables.size(),
                tableError);

        Collection<TableMetadata> viewTables = putTables.stream()
            .filter(table -> {
                TableData tableData = table.getTableData();
                return tableData != null && "VIEW".equals(tableData.getTableType());
            })
            .collect(Collectors.toList());

        viewLineageRequest(viewTables, webClientProperties.getLineageEndpoint(), schemaName);
        
        /*
         * Удаляем сущности в порядке очередности:
         * 1. Таблицы
         * 2. Схемы
         * 3. БД
         */
        Collection<TableMetadata> toDeleteTable = cacheTable.getDeletedRecords().values();
        int tableErrorDel = tableDeleteRequest(toDeleteTable, webClientProperties.getTableDeleteEndpoint());
        log.info("DbService \"{}\". Tables to DEL: {}. With errors: {}.",
                serviceName,
                toDeleteTable.size(),
                tableErrorDel);

        Collection<SchemaMetadata> toDeleteSchema = cacheSchema.getDeletedRecords().values();
        int schemaErrorDel = schemaDeleteRequest(toDeleteSchema, webClientProperties.getSchemaDeleteEndpoint());
        log.info("DbService \"{}\". Schemas to DEL: {}. With errors: {}.",
                serviceName,
                toDeleteSchema.size(),
                schemaErrorDel);

        Collection<DatabaseMetadata> toDeleteDatabase = cacheDatabase.getDeletedRecords().values();
        int dbErrorDel = databaseDeleteRequest(toDeleteDatabase, webClientProperties.getDatabaseDeleteEndpoint());
        log.info("DbService \"{}\". Databases to DEL: {}. With errors: {}.",
                serviceName,
                toDeleteDatabase.size(),
                dbErrorDel);
    }

    private int databasePutRequest(Collection<DatabaseMetadata> meta, String endpoint) {
        String token = keycloakAuthService.getValidAccessToken();
        if (token == null) {
            log.error("ORD access_token is not resolved");
            return meta.size(); // считаем, что все упали
        }

        return Flux.fromIterable(meta)
                .flatMap(value ->
                        ordaClient.putRequest(
                                        endpoint,
                                        mapperDto.getDto(DbObjectType.DATABASE, value, null),
                                        token,
                                        Void.class)
                                .doOnSuccess(response ->
                                        log.info("Успешно создано/обновлено {}: {}",
                                                DbObjectType.DATABASE.name().toLowerCase(), value.getFqn())
                                )
                                .doOnError(error ->
                                        log.error("Ошибка при создании/обновлении {}: {}",
                                                value.getFqn(), error.getMessage())
                                )
                                .map(r -> 0)                // успех = 0 ошибок
                                .onErrorReturn(1),         // ошибка = 1
                        maxConn
                )
                .reduce(0, Integer::sum)   // суммируем все ошибки
                .block();                  // ждем и получаем int
    }

    private int databaseDeleteRequest(Collection<DatabaseMetadata> meta, String endpoint) {
        String token = keycloakAuthService.getValidAccessToken();
        if (token == null) {
            log.error("ORD access_token is not resolved");
            return meta.size();
        }
        return Flux.fromIterable(meta)
                .flatMap(value -> 
                    ordaClient.deleteRequest(
                            String.format("%s/%s", endpoint, value.getFqn()),
                            token)
                        .doOnSuccess(response -> 
                            log.info("Успешно удалено {}", value.getFqn())
                        )
                        .doOnError(error -> 
                            log.error("Ошибка при удалении {}: {}", value.getFqn(), error.getMessage())
                        )
                        // .onErrorResume(error -> Mono.empty()),
                        .map(r -> 0)
                        .onErrorReturn(1),
                    maxConn
                )
                // .then() // Преобразуем в Mono<Void>
                .reduce(0, Integer::sum)
                .block(); // Ждем завершения всех
    }

    private int schemaPutRequest(Collection<SchemaMetadata> meta, String endpoint) {
        String token = keycloakAuthService.getValidAccessToken();
        if (token == null) {
            log.error("ORD access_token is not resolved");
            return meta.size();
        }
        return Flux.fromIterable(meta)
                .flatMap(value -> 
                    ordaClient.putRequest(
                            endpoint, 
                            mapperDto.getDto(DbObjectType.SCHEMA, value, null), 
                            token,
                            Void.class)
                        .doOnSuccess(response -> 
                            log.info("Успешно создано/обновлено {}: {}", DbObjectType.SCHEMA.name().toLowerCase(), value.getFqn())
                        )
                        .doOnError(error -> 
                            log.error("Ошибка при создании/обновлении {}: {}", value.getFqn(), error.getMessage())
                        )
                        // .onErrorResume(error -> Mono.empty()),
                        .map(r -> 0)
                        .onErrorReturn(1),
                    maxConn
                )
                // .then() // Преобразуем в Mono<Void>
                .reduce(0, Integer::sum)
                .block(); // Ждем завершения всех
    }

    private int schemaDeleteRequest(Collection<SchemaMetadata> meta, String endpoint) {
        String token = keycloakAuthService.getValidAccessToken();
        if (token == null) {
            log.error("ORD access_token is not resolved");
            return meta.size();
        }
        return Flux.fromIterable(meta)
                .flatMap(value -> 
                    ordaClient.deleteRequest(
                            String.format("%s/%s", endpoint, value.getFqn()),
                            token)
                        .doOnSuccess(response -> 
                            log.info("Успешно удалено {}", value.getFqn())
                        )
                        .doOnError(error -> 
                            log.error("Ошибка при удалении {}: {}", value.getFqn(), error.getMessage())
                        )
                        // .onErrorResume(error -> Mono.empty()),
                        .map(r -> 0)
                        .onErrorReturn(1),
                    maxConn
                )
                // .then() // Преобразуем в Mono<Void>
                .reduce(0, Integer::sum)
                .block(); // Ждем завершения всех
    }

    private int tablePutRequest(Collection<TableMetadata> meta, String endpoint, ServiceType serviceType) {
        String token = keycloakAuthService.getValidAccessToken();
        if (token == null) {
            log.error("ORD access_token is not resolved");
            return meta.size();
        }
        return Flux.fromIterable(meta)
            .flatMap(value -> {
                String url = String.format("%s/name/%s", endpoint, value.getFqn());

                Mono<Boolean> shouldPutMono = ordaClient.getIsProjectEntity(url, token)
                    .map(isProject -> {
                        if (isProject) {
                            log.info("Пропуск проектной сущности {} (isProjectEntity=true)", value.getFqn());
                            return false; // PUT НЕ надо
                        }
                        return true; // isProjectEntity=false => PUT надо
                    })
                    .onErrorResume(OrdaNotFoundException.class, e -> {
                        // 404 => PUT надо
                        return Mono.just(true);
                    })
                    .onErrorResume(e -> {
                        // прочие ошибки => PUT НЕ надо
                        log.error("GET isProjectEntity ошибка для {}, PUT пропущен: {}", value.getFqn(), e.getMessage());
                        return Mono.just(false);
                    });

                return shouldPutMono.flatMap(shouldPut -> {
                    if (!shouldPut) return Mono.empty();

                    Object body = mapperDto.getDto(DbObjectType.TABLE, value, serviceType);
                    if (body == null) {
                        return Mono.just(1);
                    }

                    return ordaClient.putRequest(
                            endpoint, 
                            body,
                            token,
                            Void.class)
                        .doOnSuccess(r -> log.info("Успешно создано/обновлено table: {}", value.getFqn()))
                        .doOnError(e -> log.error("Ошибка PUT {}: {}", value.getFqn(), e.getMessage()))
                        // .onErrorResume(e -> Mono.empty());
                        .map(r -> 0)
                        .onErrorReturn(1);
                });
            }, maxConn)
            // .then()
            .reduce(0, Integer::sum)
            .block();
    }

    private int tableDeleteRequest(Collection<TableMetadata> meta, String endpoint) {
        String token = keycloakAuthService.getValidAccessToken();
        if (token == null) {
            log.error("ORD access_token is not resolved");
            return meta.size();
        }
        return Flux.fromIterable(meta)
            .filterWhen(value ->
                ordaClient.getIsProjectEntity(
                        String.format("%s/%s", endpoint, value.getFqn()),
                        token)
                    .map(isProject -> !isProject) // true => удаляем только если НЕ проектная
                    .doOnNext(shouldDelete -> {
                        if (!shouldDelete) {
                            log.info("Пропуск удаления {} (isProjectEntity=true)", value.getFqn());
                        }
                    })
                    .onErrorResume(e -> {
                        // 404 и любые ошибки => НЕ удаляем
                        if (e instanceof OrdaNotFoundException) {
                            log.info("Не найдено (404), удаление пропускаем: {}", value.getFqn());
                        } else {
                            log.error("GET isProjectEntity ошибка, удаление пропускаем {}: {}", value.getFqn(), e.getMessage());
                        }
                        return Mono.just(false);
                    })
            )
            .flatMap(value ->
                ordaClient.deleteRequest(
                        String.format("%s/%s", endpoint, value.getFqn()),
                        token)
                    .doOnSuccess(v -> log.info("Успешно удалено {}", value.getFqn()))
                    .doOnError(e -> log.error("Ошибка при удалении {}: {}", value.getFqn(), e.getMessage()))
                    // .onErrorResume(e -> Mono.empty()),
                    .map(r -> 0)
                    .onErrorReturn(1),
                maxConn
            )
            // .then()
            .reduce(0, Integer::sum)
            .block();
    }

    private void viewLineageRequest(Collection<TableMetadata> meta, String endpoint, String dbType) {
        List<AddLineageRequest> requests = new ArrayList<>();

        for (TableMetadata view : meta) {
            // Lineage request
            try {
                List<AddLineageRequest> viewRequests = viewRequestBuilder.buildEdgesForView(view, dbType);
                requests.addAll(viewRequests);
            } catch (RuntimeException e) {
                log.error("Error while parsing viewDefinition: {}. {}", view.getFqn(), e.getMessage());
                continue;
            }            
        }

        String token = keycloakAuthService.getValidAccessToken();
        if (token == null) {
            log.error("ORD access_token is not resolved");
            return;
        }
        Flux.fromIterable(requests)
                .flatMap(value -> 
                    ordaClient.putRequest(
                            endpoint, 
                            value, 
                            token,
                            Void.class)
                        .doOnSuccess(response -> 
                            log.info("Успешно создано/обновлено ViewLineage: {}", 
                                    value.getEdge().getToEntity().getId())
                        )
                        .doOnError(error -> 
                            log.error("Ошибка при создании/обновлении ViewLineage {}: {}", 
                                    value.getEdge().getToEntity().getId(), error.getMessage())
                        )
                        .onErrorResume(error -> Mono.empty()),
                    maxConn
                )
                .then() // Преобразуем в Mono<Void>
                .block(); // Ждем завершения всех
    }
}
