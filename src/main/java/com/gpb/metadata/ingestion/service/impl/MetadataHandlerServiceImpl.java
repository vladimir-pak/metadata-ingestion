package com.gpb.metadata.ingestion.service.impl;

import java.util.*;
import java.util.stream.Collectors;

import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.exceptions.TokenRefreshException;
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
import com.gpb.metadata.ingestion.dto.lineage.AddLineageRequest;
import com.gpb.metadata.ingestion.dto.mapper.MapperDto;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.model.postgres.DatabaseMetadata;
import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;
import com.gpb.metadata.ingestion.model.postgres.TableMetadata;
import com.gpb.metadata.ingestion.model.schema.TableData;
import com.gpb.metadata.ingestion.properties.WebClientProperties;
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

    private final OrdaClient ordaClient;
    private final OpenMetadataTableSnapshotRepository tableSnapshotRepository;


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
                schemasProperties.getOracle(), ServiceType.ORACLE,
                schemasProperties.getSapiq(), ServiceType.SAPIQ
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
        boolean isExists = ordaClient.checkEntityExists(
                webClientProperties.getDatabaseServiceEndpoint() + "/name/" + serviceName
        );

        if (!isExists) {
            ObjectMapper mapper = new ObjectMapper();
            log.info("Creating databaseService: {}", serviceName);
            String dbServiceUrl = webClientProperties.getDatabaseServiceEndpoint();
            String serviceType = type.getValue().substring(0, 1).toUpperCase() + 
                    type.getValue().substring(1);

            ObjectNode connection = mapper.createObjectNode();
            ObjectNode connectionConfig = connection.putObject("config");
            connectionConfig.put("type", serviceType);

            DatabaseServiceMetadataDto dbServiceDto = DatabaseServiceMetadataDto.builder()
                    .name(serviceName)
                    .displayName(serviceName)
                    .serviceType(serviceType)
                    .connection(connection)
                    .build();
            log.info("DTO creating DB: name={}; serviceType={}", dbServiceDto.getName(), dbServiceDto.getServiceType());
            Mono<String> resp = ordaClient.putRequest(dbServiceUrl, dbServiceDto, String.class);
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

        TableSnapshot tableSnapshot = tableSnapshotRepository.loadByServiceName(serviceName);
        log.info("DbService \"{}\". Loaded Table snapshot before PUT: {} entities.",
                serviceName,
                tableSnapshot.size());

        Collection<TableMetadata> putTables = cacheTable.getPutRecords().values();
        int tableError = tablePutRequest(
                putTables,
                webClientProperties.getTableEndpoint(),
                type,
                tableSnapshot
        );
        log.info("DbService \"{}\". Tables to PUT: {}. With errors: {}.",
                serviceName,
                putTables.size(),
                tableError);

        // После PUT перечитываем snapshot одним SQL-запросом.
        // Так в snapshot появляются id новых таблиц, необходимые для lineage.
        tableSnapshot = tableSnapshotRepository.loadByServiceName(serviceName);
        log.info("DbService \"{}\". Reloaded Table snapshot after PUT: {} entities.",
                serviceName,
                tableSnapshot.size());

        Collection<TableMetadata> viewTables = putTables.stream()
            .filter(table -> {
                TableData tableData = table.getTableData();
                return tableData != null && "VIEW".equals(tableData.getTableType());
            })
            .collect(Collectors.toList());

        viewLineageRequest(
                viewTables,
                webClientProperties.getLineageEndpoint(),
                schemaName,
                tableSnapshot
        );
        
        /*
         * Удаляем сущности в порядке очередности:
         * 1. Таблицы
         * 2. Схемы
         * 3. БД
         */
        Collection<TableMetadata> toDeleteTable = cacheTable.getDeletedRecords().values();
        int tableErrorDel = tableDeleteRequest(
                toDeleteTable,
                webClientProperties.getTableDeleteEndpoint(),
                tableSnapshot
        );
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
        return Flux.fromIterable(meta)
                .flatMap(value ->
                                ordaClient.putRequest(
                                                endpoint,
                                                mapperDto.getDto(DbObjectType.DATABASE, value, null),
                                                Void.class
                                        )
                                        .doOnSuccess(response ->
                                                log.info("Успешно создано/обновлено {}: {}",
                                                        DbObjectType.DATABASE.name().toLowerCase(),
                                                        value.getFqn())
                                        )
                                        .doOnError(error ->
                                                log.error("Ошибка при создании/обновлении {}: {}",
                                                        value.getFqn(),
                                                        error.getMessage())
                                        )
                                        .thenReturn(0)
                                        .onErrorResume(this::countEntityError),
                        maxConn
                )
                .reduce(0, Integer::sum)
                .block();
    }

    private int databaseDeleteRequest(Collection<DatabaseMetadata> meta, String endpoint) {
        return Flux.fromIterable(meta)
                .flatMap(value ->
                                ordaClient.deleteRequest(
                                                String.format("%s/%s", endpoint, value.getFqn())
                                        )
                                        .doOnSuccess(response ->
                                                log.info("Успешно удалено {}", value.getFqn())
                                        )
                                        .doOnError(error ->
                                                log.error("Ошибка при удалении {}: {}",
                                                        value.getFqn(),
                                                        error.getMessage())
                                        )
                                        .thenReturn(0)
                                        .onErrorResume(this::countEntityError),
                        maxConn
                )
                .reduce(0, Integer::sum)
                .block();
    }

    private int schemaPutRequest(Collection<SchemaMetadata> meta, String endpoint) {
        return Flux.fromIterable(meta)
                .flatMap(value ->
                                ordaClient.putRequest(
                                                endpoint,
                                                mapperDto.getDto(DbObjectType.SCHEMA, value, null),
                                                Void.class
                                        )
                                        .doOnSuccess(response ->
                                                log.info("Успешно создано/обновлено {}: {}",
                                                        DbObjectType.SCHEMA.name().toLowerCase(),
                                                        value.getFqn())
                                        )
                                        .doOnError(error ->
                                                log.error("Ошибка при создании/обновлении {}: {}",
                                                        value.getFqn(),
                                                        error.getMessage())
                                        )
                                        .thenReturn(0)
                                        .onErrorResume(this::countEntityError),
                        maxConn
                )
                .reduce(0, Integer::sum)
                .block();
    }

    private int schemaDeleteRequest(Collection<SchemaMetadata> meta, String endpoint) {
        return Flux.fromIterable(meta)
                .flatMap(value ->
                                ordaClient.deleteRequest(
                                                String.format("%s/%s", endpoint, value.getFqn())
                                        )
                                        .doOnSuccess(response ->
                                                log.info("Успешно удалено {}", value.getFqn())
                                        )
                                        .doOnError(error ->
                                                log.error("Ошибка при удалении {}: {}",
                                                        value.getFqn(),
                                                        error.getMessage())
                                        )
                                        .thenReturn(0)
                                        .onErrorResume(this::countEntityError),
                        maxConn
                )
                .reduce(0, Integer::sum)
                .block();
    }

    private int tablePutRequest(
            Collection<TableMetadata> meta,
            String endpoint,
            ServiceType serviceType,
            TableSnapshot tableSnapshot) {

        return Flux.fromIterable(meta)
                .flatMap(value -> {
                    TableSnapshotEntry existing = tableSnapshot.find(value.getFqn()).orElse(null);
                    if (existing != null && existing.projectEntity()) {
                        log.info(
                                "Пропуск проектной сущности {} (isProjectEntity=true)",
                                value.getFqn()
                        );
                        return Mono.empty();
                    }

                    Object body = mapperDto.getDto(
                            DbObjectType.TABLE,
                            value,
                            serviceType
                    );
                    if (body == null) {
                        return Mono.just(1);
                    }

                    return ordaClient.putRequest(
                                    endpoint,
                                    body,
                                    Void.class
                            )
                            .doOnSuccess(response ->
                                    log.info(
                                            "Успешно создано/обновлено table: {}",
                                            value.getFqn()
                                    )
                            )
                            .doOnError(error ->
                                    log.error(
                                            "Ошибка PUT {}: {}",
                                            value.getFqn(),
                                            error.getMessage()
                                    )
                            )
                            .thenReturn(0)
                            .onErrorResume(this::countEntityError);
                }, maxConn)
                .reduce(0, Integer::sum)
                .block();
    }

    private int tableDeleteRequest(
            Collection<TableMetadata> meta,
            String endpoint,
            TableSnapshot tableSnapshot) {

        return Flux.fromIterable(meta)
                .filter(value -> {
                    TableSnapshotEntry existing = tableSnapshot.find(value.getFqn()).orElse(null);
                    if (existing == null) {
                        log.info(
                                "Удаление пропущено: таблица {} отсутствует в OMD snapshot",
                                value.getFqn()
                        );
                        return false;
                    }

                    if (existing.projectEntity()) {
                        log.info(
                                "Пропуск удаления {} (isProjectEntity=true)",
                                value.getFqn()
                        );
                        return false;
                    }

                    return true;
                })
                .flatMap(value ->
                                ordaClient.deleteRequest(
                                                String.format("%s/%s", endpoint, value.getFqn())
                                        )
                                        .doOnSuccess(response ->
                                                log.info("Успешно удалено {}", value.getFqn())
                                        )
                                        .doOnError(error ->
                                                log.error(
                                                        "Ошибка при удалении {}: {}",
                                                        value.getFqn(),
                                                        error.getMessage()
                                                )
                                        )
                                        .thenReturn(0)
                                        .onErrorResume(this::countEntityError),
                        maxConn
                )
                .reduce(0, Integer::sum)
                .block();
    }

    private void viewLineageRequest(
            Collection<TableMetadata> meta,
            String endpoint,
            String dbType,
            TableSnapshot tableSnapshot) {
        List<AddLineageRequest> requests = new ArrayList<>();

        for (TableMetadata view : meta) {
            try {
                List<AddLineageRequest> viewRequests = viewRequestBuilder.buildEdgesForView(
                        view,
                        dbType,
                        tableSnapshot
                );
                if (viewRequests.isEmpty()) {
                    continue;
                }
                log.info("viewRequest. fromEntity: {}, toEntity: {}",
                        viewRequests.get(0).getEdge().getFromEntity().getId(),
                        viewRequests.get(0).getEdge().getToEntity().getId());
                requests.addAll(viewRequests);
            } catch (RuntimeException e) {
                log.error("Error while parsing viewDefinition: {}. {}", view.getFqn(), e.getMessage());
            }
        }

        Flux.fromIterable(requests)
                .flatMap(value ->
                                ordaClient.putRequest(
                                                endpoint,
                                                value,
                                                Void.class
                                        )
                                        .doOnSuccess(response ->
                                                log.info("Успешно создано/обновлено ViewLineage: {}",
                                                        value.getEdge().getToEntity().getId())
                                        )
                                        .doOnError(error ->
                                                log.error("Ошибка при создании/обновлении ViewLineage {}: {}",
                                                        value.getEdge().getToEntity().getId(),
                                                        error.getMessage())
                                        )
                                        .onErrorResume(error -> {
                                            if (isCriticalError(error)) {
                                                return Mono.error(error);
                                            }
                                            return Mono.empty();
                                        }),
                        maxConn
                )
                .then()
                .block();
    }



    private Mono<Integer> countEntityError(Throwable error) {
        if (isCriticalError(error)) {
            return Mono.error(error);
        }
        return Mono.just(1);
    }

    private boolean isCriticalError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof TokenRefreshException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
