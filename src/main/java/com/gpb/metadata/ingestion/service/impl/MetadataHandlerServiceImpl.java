package com.gpb.metadata.ingestion.service.impl;

import java.util.*;

import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.exceptions.TokenRefreshException;
import com.gpb.metadata.ingestion.metrics.enums.IngestionMetricJob;
import com.gpb.metadata.ingestion.metrics.MetricCounter;
import com.gpb.metadata.ingestion.properties.MetadataSchemasProperties;
import com.gpb.metadata.ingestion.repository.OpenMetadataTableSnapshotRepository;
import com.gpb.metadata.ingestion.snapshot.TableSnapshot;
import com.gpb.metadata.ingestion.snapshot.TableSnapshotEntry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.metadata.ingestion.cache.CacheComparisonResult;
import com.gpb.metadata.ingestion.dto.DatabaseServiceMetadataDto;
import com.gpb.metadata.ingestion.dto.mapper.MapperDto;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.postgres.DatabaseMetadata;
import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;
import com.gpb.metadata.ingestion.model.postgres.TableMetadata;
import com.gpb.metadata.ingestion.properties.WebClientProperties;
import com.gpb.metadata.ingestion.service.IngestionMetricService;
import com.gpb.metadata.ingestion.service.MetadataHandlerService;
import com.gpb.metadata.ingestion.utils.OrdaClient;

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

    private final WebClientProperties webClientProperties;
    private final MetadataSchemasProperties schemasProperties;

    private final OrdaClient ordaClient;
    private final OpenMetadataTableSnapshotRepository tableSnapshotRepository;

    private final IngestionMetricService ingestionMetricService;

    @Value("${ord.api.max-connections:5}")
    private Integer maxConn;

    @Async
    @Override
    public void startAsync(
            String schemaName,
            String serviceName,
            String runId) {
        start(
                schemaName,
                serviceName,
                runId
        );
    }

    @Override
    public void start(
            String schemaName,
            String serviceName,
            String runId) {
        try {
            executeIngestion(
                    schemaName,
                    serviceName,
                    runId
            );
        } catch (RuntimeException e) {
            /*
             * Текущий job уже был переведён execute(...) в FAILED.
             * Оставшиеся QUEUE, становится SKIPPED.
             */
            try {
                ingestionMetricService.skipRemaining(
                        runId
                );
            } catch (RuntimeException metricException) {
                e.addSuppressed(metricException);
                log.error(
                        "Failed to mark remaining jobs as SKIPPED. runId={}",
                        runId,
                        metricException
                );
            }

            log.error(
                    "Metadata ingestion failed. runId={}, serviceName={}",
                    runId,
                    serviceName,
                    e
            );
            throw e;
        }
    }

    private void executeIngestion(
            String schemaName,
            String serviceName,
            String runId) {
        /*
         * ==========================================================
         * DATABASE UPSERT
         * ==========================================================
         */
        DatabaseUpsertContext databaseContext =
                ingestionMetricService.execute(
                        runId,
                        IngestionMetricJob.DATABASE_UPSERT,
                        metric -> {
                            ServiceType type = resolveServiceType(schemaName);

                            CacheComparisonResult<DatabaseMetadata>
                                    cacheDatabase =
                                        databaseCacheService.synchronizeWithDatabase(
                                            schemaName,
                                            serviceName
                                        );

                            ensureDatabaseService(serviceName, type);

                            Collection<DatabaseMetadata> putDatabases =
                                    cacheDatabase
                                            .getPutRecords()
                                            .values();

                            int dbError =
                                    databasePutRequest(
                                            putDatabases,
                                            webClientProperties.getDatabaseEndpoint(),
                                            metric
                                    );

                            log.info(
                                    "DbService \"{}\". " +
                                    "Databases to PUT: {}. " +
                                    "With errors: {}.",
                                    serviceName,
                                    putDatabases.size(),
                                    dbError
                            );

                            return new DatabaseUpsertContext(
                                    type,
                                    cacheDatabase
                            );
                        }
                );

        /*
         * ==========================================================
         * SCHEMA UPSERT
         * ==========================================================
         */
        CacheComparisonResult<SchemaMetadata> cacheSchema =
                ingestionMetricService.execute(
                        runId,
                        IngestionMetricJob.SCHEMA_UPSERT,
                        metric -> {
                            CacheComparisonResult<SchemaMetadata> cache =
                                    schemaCacheService.synchronizeWithDatabase(
                                        schemaName,
                                        serviceName
                                    );

                            Collection<SchemaMetadata> putSchemas =
                                    cache
                                        .getPutRecords()
                                        .values();

                            int schemaError =
                                    schemaPutRequest(
                                            putSchemas,
                                            webClientProperties.getSchemaEndpoint(),
                                            metric
                                    );

                            log.info(
                                    "DbService \"{}\". " +
                                    "Schemas to PUT: {}. " +
                                    "With errors: {}.",
                                    serviceName,
                                    putSchemas.size(),
                                    schemaError
                            );
                            return cache;
                        }
                );

        /*
         * ==========================================================
         * TABLE UPSERT
         * ==========================================================
         */
        TableUpsertContext tableContext =
                ingestionMetricService.execute(
                        runId,
                        IngestionMetricJob.TABLE_UPSERT,
                        metric -> {
                            CacheComparisonResult<TableMetadata> cache =
                                    tableCacheService.synchronizeWithDatabase(
                                        schemaName,
                                        serviceName
                                    );

                            TableSnapshot snapshotBeforePut =
                                    tableSnapshotRepository
                                            .loadByServiceName(serviceName);

                            log.info(
                                    "DbService \"{}\". " +
                                    "Loaded Table snapshot before PUT: {} entities.",
                                    serviceName,
                                    snapshotBeforePut.size()
                            );

                            Collection<TableMetadata> putTables =
                                    cache
                                        .getPutRecords()
                                        .values();

                            int tableError =
                                    tablePutRequest(
                                            putTables,
                                            webClientProperties.getTableEndpoint(),
                                            databaseContext.serviceType(),
                                            snapshotBeforePut,
                                            metric
                                    );

                            log.info(
                                    "DbService \"{}\". " +
                                    "Tables to PUT: {}. " +
                                    "With errors: {}.",
                                    serviceName,
                                    putTables.size(),
                                    tableError
                            );

                            /*
                             * После PUT перечитываем snapshot.
                             */
                            TableSnapshot snapshotAfterPut =
                                    tableSnapshotRepository.loadByServiceName(serviceName);

                            log.info(
                                    "DbService \"{}\". " +
                                    "Reloaded Table snapshot after PUT: {} entities.",
                                    serviceName,
                                    snapshotAfterPut.size()
                            );

                            return new TableUpsertContext(
                                    cache,
                                    snapshotAfterPut
                            );
                        }
                );

        /*
         * ==========================================================
         * TABLE DELETE
         * ==========================================================
         */
        ingestionMetricService.execute(
                runId,
                IngestionMetricJob.TABLE_DELETE,
                metric -> {
                    Collection<TableMetadata> toDeleteTable =
                            tableContext
                                    .cache()
                                    .getDeletedRecords()
                                    .values();

                    int tableErrorDel =
                            tableDeleteRequest(
                                    toDeleteTable,
                                    webClientProperties.getTableDeleteEndpoint(),
                                    tableContext.snapshotAfterPut(),
                                    metric
                            );

                    log.info(
                            "DbService \"{}\". " +
                            "Tables to DEL: {}. " +
                            "With errors: {}.",
                            serviceName,
                            toDeleteTable.size(),
                            tableErrorDel
                    );
                    return null;
                }
        );

        /*
         * ==========================================================
         * SCHEMA DELETE
         * ==========================================================
         */
        ingestionMetricService.execute(
                runId,
                IngestionMetricJob.SCHEMA_DELETE,
                metric -> {
                    Collection<SchemaMetadata> toDeleteSchema =
                            cacheSchema
                                    .getDeletedRecords()
                                    .values();

                    int schemaErrorDel =
                            schemaDeleteRequest(
                                    toDeleteSchema,
                                    webClientProperties.getSchemaDeleteEndpoint(),
                                    metric
                            );

                    log.info(
                            "DbService \"{}\". " +
                            "Schemas to DEL: {}. " +
                            "With errors: {}.",
                            serviceName,
                            toDeleteSchema.size(),
                            schemaErrorDel
                    );
                    return null;
                }
        );

        /*
         * ==========================================================
         * DATABASE DELETE
         * ==========================================================
         */
        ingestionMetricService.execute(
                runId,
                IngestionMetricJob.DATABASE_DELETE,
                metric -> {
                    Collection<DatabaseMetadata> toDeleteDatabase =
                            databaseContext
                                    .cache()
                                    .getDeletedRecords()
                                    .values();

                    int dbErrorDel =
                            databaseDeleteRequest(
                                    toDeleteDatabase,
                                    webClientProperties.getDatabaseDeleteEndpoint(),
                                    metric
                            );

                    log.info(
                            "DbService \"{}\". " +
                            "Databases to DEL: {}. " +
                            "With errors: {}.",
                            serviceName,
                            toDeleteDatabase.size(),
                            dbErrorDel
                    );
                    return null;
                }
        );

        // Создаем задачу на парсинг view
        ingestionMetricService.createViewParsingJob(runId, serviceName);
    }

    private ServiceType resolveServiceType(
            String schemaName) {

        final Map<String, ServiceType> schemaTypeMap = Map.of(
                schemasProperties.getPostgres(),
                ServiceType.POSTGRES,

                schemasProperties.getMssql(),
                ServiceType.MSSQL,

                schemasProperties.getOracle(),
                ServiceType.ORACLE,

                schemasProperties.getSapiq(),
                ServiceType.SAPIQ
        );

        ServiceType type =
                schemaTypeMap.get(schemaName);

        if (type == null) {
            throw new IllegalArgumentException(
                    "Неизвестный тип схемы: "
                    + schemaName
            );
        }

        return type;
    }

    private void ensureDatabaseService(
            String serviceName,
            ServiceType type) {
        boolean isExists =
                ordaClient.checkEntityExists(
                        webClientProperties
                                .getDatabaseServiceEndpoint()
                                + "/name/"
                                + serviceName
                );

        if (isExists) {
            return;
        }

        ObjectMapper mapper = new ObjectMapper();

        log.info(
                "Creating databaseService: {}",
                serviceName
        );

        String dbServiceUrl =
                webClientProperties
                        .getDatabaseServiceEndpoint();

        String serviceType =
                type.getValue()
                        .substring(0, 1)
                        .toUpperCase()
                +
                type.getValue()
                        .substring(1);

        ObjectNode connection =
                mapper.createObjectNode();

        ObjectNode connectionConfig =
                connection.putObject("config");

        connectionConfig.put(
                "type",
                serviceType
        );

        DatabaseServiceMetadataDto dbServiceDto =
                DatabaseServiceMetadataDto
                        .builder()
                        .name(serviceName)
                        .displayName(serviceName)
                        .serviceType(serviceType)
                        .connection(connection)
                        .build();

        log.info(
                "DTO creating DB: name={}; serviceType={}",
                dbServiceDto.getName(),
                dbServiceDto.getServiceType()
        );

        String response =
                ordaClient.putRequest(
                        dbServiceUrl,
                        dbServiceDto,
                        String.class
                )
                .block();

        log.info(
                "Response for creating DB: {}",
                response
        );
    }

    /*
     * ==========================================================
     * DATABASE PUT
     * ==========================================================
     */

    private int databasePutRequest(
            Collection<DatabaseMetadata> meta,
            String endpoint,
            MetricCounter metric) {

        return Flux.fromIterable(meta)
                .flatMap(
                        value -> trackRequest(
                                ordaClient.putRequest(
                                        endpoint,
                                        mapperDto.getDto(
                                                DbObjectType.DATABASE,
                                                value,
                                                null
                                        ),
                                        Void.class
                                ),
                                metric,
                                () -> log.info(
                                        "Успешно создано/обновлено database: {}",
                                        value.getFqn()
                                ),
                                error -> log.error(
                                        "Ошибка при создании/обновлении {}: {}",
                                        value.getFqn(),
                                        error.getMessage()
                                )
                        ),
                        maxConn
                )
                .reduce(
                        0,
                        Integer::sum
                )
                .block();
    }


    /*
     * ==========================================================
     * DATABASE DELETE
     * ==========================================================
     */
    private int databaseDeleteRequest(
            Collection<DatabaseMetadata> meta,
            String endpoint,
            MetricCounter metric) {

        return Flux.fromIterable(meta)
                .flatMap(
                        value -> trackRequest(
                                ordaClient.deleteRequest(
                                        String.format(
                                                "%s/%s",
                                                endpoint,
                                                value.getFqn()
                                        ),
                                        true
                                ),
                                metric,
                                () -> log.info(
                                        "Успешно удалено {}",
                                        value.getFqn()
                                ),
                                error -> log.error(
                                        "Ошибка при удалении {}: {}",
                                        value.getFqn(),
                                        error.getMessage()
                                )
                        ),
                        maxConn
                )
                .reduce(
                        0,
                        Integer::sum
                )
                .block();
    }

    /*
     * ==========================================================
     * SCHEMA PUT
     * ==========================================================
     */
    private int schemaPutRequest(
            Collection<SchemaMetadata> meta,
            String endpoint,
            MetricCounter metric) {

        return Flux.fromIterable(meta)
                .flatMap(
                        value -> trackRequest(
                                ordaClient.putRequest(
                                        endpoint,
                                        mapperDto.getDto(
                                                DbObjectType.SCHEMA,
                                                value,
                                                null
                                        ),
                                        Void.class
                                ),
                                metric,
                                () -> log.info(
                                        "Успешно создано/обновлено schema: {}",
                                        value.getFqn()
                                ),
                                error -> log.error(
                                        "Ошибка при создании/обновлении {}: {}",
                                        value.getFqn(),
                                        error.getMessage()
                                )
                        ),
                        maxConn
                )
                .reduce(
                        0,
                        Integer::sum
                )
                .block();
    }

    /*
     * ==========================================================
     * SCHEMA DELETE
     * ==========================================================
     */
    private int schemaDeleteRequest(
            Collection<SchemaMetadata> meta,
            String endpoint,
            MetricCounter metric) {

        return Flux.fromIterable(meta)
                .flatMap(
                        value -> trackRequest(
                                ordaClient.deleteRequest(
                                        String.format(
                                                "%s/%s",
                                                endpoint,
                                                value.getFqn()
                                        ),
                                        true
                                ),
                                metric,
                                () -> log.info(
                                        "Успешно удалено {}",
                                        value.getFqn()
                                ),
                                error -> log.error(
                                        "Ошибка при удалении {}: {}",
                                        value.getFqn(),
                                        error.getMessage()
                                )
                        ),
                        maxConn
                )
                .reduce(
                        0,
                        Integer::sum
                )
                .block();
    }

    /*
     * ==========================================================
     * TABLE PUT
     * ==========================================================
     */
    private int tablePutRequest(
            Collection<TableMetadata> meta,
            String endpoint,
            ServiceType serviceType,
            TableSnapshot tableSnapshot,
            MetricCounter metric) {

        return Flux.fromIterable(meta)
                .flatMap(
                        value -> {
                            TableSnapshotEntry existing =
                                    tableSnapshot
                                            .find(value.getFqn())
                                            .orElse(null);

                            if (
                                existing != null
                                && existing.projectEntity()
                            ) {
                                log.info(
                                        "Пропуск проектной сущности {} " +
                                        "(isProjectEntity=true)",
                                        value.getFqn()
                                );
                                return Mono.empty();
                            }

                            Object body =
                                    mapperDto.getDto(
                                            DbObjectType.TABLE,
                                            value,
                                            serviceType
                                    );

                            if (body == null) {
                                metric.error();
                                log.error(
                                        "Не удалось сформировать DTO для table {}",
                                        value.getFqn()
                                );
                                return Mono.just(1);
                            }

                            return trackRequest(
                                    ordaClient.putRequest(
                                            endpoint,
                                            body,
                                            Void.class
                                    ),
                                    metric,
                                    () -> log.info(
                                            "Успешно создано/обновлено table: {}",
                                            value.getFqn()
                                    ),
                                    error -> log.error(
                                            "Ошибка PUT {}: {}",
                                            value.getFqn(),
                                            error.getMessage()
                                    )
                            );
                        },
                        maxConn
                )
                .reduce(
                        0,
                        Integer::sum
                )
                .block();
    }

    /*
     * ==========================================================
     * TABLE DELETE
     * ==========================================================
     */
    private int tableDeleteRequest(
            Collection<TableMetadata> meta,
            String endpoint,
            TableSnapshot tableSnapshot,
            MetricCounter metric) {

        return Flux.fromIterable(meta)
                .filter(
                        value -> {
                            TableSnapshotEntry existing =
                                    tableSnapshot
                                            .find(value.getFqn())
                                            .orElse(null);

                            if (existing == null) {
                                log.info(
                                        "Удаление пропущено: таблица {} " +
                                        "отсутствует в OMD snapshot",
                                        value.getFqn()
                                );
                                return false;
                            }

                            if (existing.projectEntity()) {
                                log.info(
                                        "Пропуск удаления {} " +
                                        "(isProjectEntity=true)",
                                        value.getFqn()
                                );
                                return false;
                            }
                            return true;
                        }
                )
                .flatMap(
                        value -> trackRequest(
                                ordaClient.deleteRequest(
                                        String.format(
                                                "%s/%s",
                                                endpoint,
                                                value.getFqn()
                                        ),
                                        true
                                ),
                                metric,
                                () -> log.info(
                                        "Успешно удалено {}",
                                        value.getFqn()
                                ),
                                error -> log.error(
                                        "Ошибка при удалении {}: {}",
                                        value.getFqn(),
                                        error.getMessage()
                                )
                        ),
                        maxConn
                )
                .reduce(
                        0,
                        Integer::sum
                )
                .block();
    }

    /*
     * ==========================================================
     * ERROR PROCESSING
     * ==========================================================
     */
    private Mono<Integer> countEntityError(
            Throwable error) {
        if (isCriticalError(error)) {
            return Mono.error(error);
        }
        return Mono.just(1);
    }

    private boolean isCriticalError(
            Throwable error) {
        Throwable current = error;

        while (current != null) {
            if (current instanceof TokenRefreshException) {
                return true;
            }
            current = current.getCause();
        }

        return false;
    }

    /*
     * ==========================================================
     * REQUEST METRICS
     * ==========================================================
     */
    private <T> Mono<Integer> trackRequest(
            Mono<T> request,
            MetricCounter metric,
            Runnable successAction,
            java.util.function.Consumer<Throwable> errorAction) {
        return request
                .doOnSuccess(
                        response -> {
                            metric.success();
                            successAction.run();
                        }
                )
                .doOnError(
                        error -> {
                            metric.error();
                            errorAction.accept(error);
                        }
                )
                .thenReturn(0)
                .onErrorResume(
                        this::countEntityError
                );
    }

    /*
     * Нужны, чтобы сохранить данные между последовательными job.
     */
    private record DatabaseUpsertContext(
            ServiceType serviceType,
            CacheComparisonResult<DatabaseMetadata> cache) {
    }

    private record TableUpsertContext(
            CacheComparisonResult<TableMetadata> cache,
            TableSnapshot snapshotAfterPut) {
    }
}
