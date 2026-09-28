package com.gpb.metadata.ingestion.service.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gpb.metadata.ingestion.cache.CacheComparisonResult;
import com.gpb.metadata.ingestion.cache.dto.MetadataFingerprint;
import com.gpb.metadata.ingestion.cache.dto.MetadataRename;
import com.gpb.metadata.ingestion.dto.DatabaseServiceMetadataDto;
import com.gpb.metadata.ingestion.dto.mapper.MapperDto;
import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.exceptions.TokenRefreshException;
import com.gpb.metadata.ingestion.metrics.MetricCounter;
import com.gpb.metadata.ingestion.metrics.enums.IngestionMetricJob;
import com.gpb.metadata.ingestion.model.DatabaseMetadata;
import com.gpb.metadata.ingestion.model.EntityId;
import com.gpb.metadata.ingestion.model.Metadata;
import com.gpb.metadata.ingestion.model.SchemaMetadata;
import com.gpb.metadata.ingestion.model.TableMetadata;
import com.gpb.metadata.ingestion.properties.MetadataCacheProperties;
import com.gpb.metadata.ingestion.properties.MetadataTablesProperties;
import com.gpb.metadata.ingestion.properties.WebClientProperties;
import com.gpb.metadata.ingestion.repository.OpenMetadataSnapshotRepository;
import com.gpb.metadata.ingestion.service.AbstractMetadataCacheService;
import com.gpb.metadata.ingestion.service.IngestionMetricService;
import com.gpb.metadata.ingestion.service.MetadataHandlerService;
import com.gpb.metadata.ingestion.snapshot.OpenMetadataSnapshot;
import com.gpb.metadata.ingestion.snapshot.OpenMetadataSnapshotEntry;
import com.gpb.metadata.ingestion.utils.OrdaClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
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
    private final ObjectMapper objectMapper;
    private final WebClientProperties webClientProperties;
    private final OrdaClient ordaClient;
    private final OpenMetadataSnapshotRepository openMetadataSnapshotRepository;
    private final IngestionMetricService ingestionMetricService;
    private final MetadataTablesProperties metadataTablesProperties;
    private final MetadataCacheProperties metadataCacheProperties;

    @Value("${ord.api.concurrency:10}")
    private Integer maxConn;

    @Async
    @Override
    public void startAsync(
            ServiceType serviceType,
            String serviceName,
            String runId,
            boolean skipDeletionThreshold) {

        start(serviceType, serviceName, runId, skipDeletionThreshold);
    }

    @Async
    @Override
    public void startAsync(
            ServiceType serviceType,
            String serviceName,
            String runId) {

        start(serviceType, serviceName, runId, false);
    }

    @Override
    public void start(
            ServiceType serviceType,
            String serviceName,
            String runId) {
        start(
                serviceType,
                serviceName,
                runId,
                false
        );
    }

    @Override
    public void start(
            ServiceType serviceType,
            String serviceName,
            String runId,
            boolean skipDeletionThreshold) {

        try {
            executeIngestion(
                    serviceType,
                    serviceName,
                    runId,
                    skipDeletionThreshold
            );
        } catch (RuntimeException e) {
            try {
                ingestionMetricService.skipRemaining(runId);
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
            ServiceType serviceType,
            String serviceName,
            String runId,
            boolean skipDeletionThreshold) {

        String databaseTableName = metadataTablesProperties.getTable(
                serviceType,
                DbObjectType.DATABASE
        );

        /* ==========================================================
         * DATABASE UPSERT / RENAME / RECONCILIATION PREPARE
         * ========================================================== */
        DatabaseUpsertContext databaseContext = ingestionMetricService.execute(
                runId,
                IngestionMetricJob.DATABASE_UPSERT,
                metric -> {
                    CacheComparisonResult<DatabaseMetadata> delta =
                            databaseCacheService.synchronizeWithDatabase(
                                    serviceType,
                                    databaseTableName,
                                    serviceName
                            );

                    ensureDatabaseService(serviceName, serviceType);

                    ReconciliationPlan reconciliationPlan =
                            prepareReconciliationPlan(
                                    delta,
                                    DbObjectType.DATABASE,
                                    serviceName,
                                    databaseCacheService::validateReconciliationDeleteThreshold,
                                    skipDeletionThreshold
                            );

                    if (delta.isReconciliation()) {
                        ReconciliationUpsertResult reconciliationUpserts =
                                reconciliationMetadataUpserts(
                                        databaseCacheService,
                                        databaseTableName,
                                        serviceType,
                                        serviceName,
                                        delta,
                                        webClientProperties.getDatabaseEndpoint(),
                                        DbObjectType.DATABASE,
                                        null,
                                        metric
                                );

                        logReconciliationUpsertResult(
                                serviceName,
                                "DATABASE RECONCILIATION PUT",
                                delta.getReconciliationFingerprints().size(),
                                reconciliationUpserts
                        );

                        return new DatabaseUpsertContext(
                                serviceType,
                                delta,
                                new ReconciliationState(
                                        true,
                                        reconciliationPlan.orphanFqns(),
                                        reconciliationUpserts.errorCount() == 0,
                                        reconciliationUpserts.committedCount()
                                )
                        );
                    }

                    Map<EntityId, DatabaseMetadata> regular =
                            delta.getPutRecords();
                    Map<EntityId, MetadataRename<DatabaseMetadata>> renamed =
                            delta.getRenamedRecords();

                    BatchResult regularResult = metadataPutRequest(
                            regular,
                            webClientProperties.getDatabaseEndpoint(),
                            DbObjectType.DATABASE,
                            null,
                            metric
                    );

                    BatchResult renameResult = metadataRenameRequest(
                            renamed,
                            webClientProperties.getDatabaseEndpoint(),
                            webClientProperties.getDatabaseDeleteEndpoint(),
                            DbObjectType.DATABASE,
                            null,
                            metric
                    );

                    Map<EntityId, DatabaseMetadata> successful =
                            collectSuccessfulUpserts(
                                    regular,
                                    regularResult.commitIds(),
                                    renamed,
                                    renameResult.commitIds()
                            );

                    databaseCacheService.commitSuccessfulUpserts(
                            serviceType,
                            serviceName,
                            successful
                    );

                    logBatchResult(
                            serviceName,
                            "DATABASE PUT",
                            regular.size(),
                            regularResult
                    );
                    logBatchResult(
                            serviceName,
                            "DATABASE RENAME",
                            renamed.size(),
                            renameResult
                    );

                    return new DatabaseUpsertContext(
                            serviceType,
                            delta,
                            new ReconciliationState(
                                    reconciliationPlan.required(),
                                    reconciliationPlan.orphanFqns(),
                                    regularResult.errorCount() == 0
                                            && renameResult.errorCount() == 0,
                                    successful.size()
                            )
                    );
                }
        );

        String schemaTableName = metadataTablesProperties.getTable(
                serviceType,
                DbObjectType.SCHEMA
        );

        /* ==========================================================
         * SCHEMA UPSERT / RENAME / RECONCILIATION PREPARE
         * ========================================================== */
        SchemaUpsertContext schemaContext = ingestionMetricService.execute(
                runId,
                IngestionMetricJob.SCHEMA_UPSERT,
                metric -> {
                    CacheComparisonResult<SchemaMetadata> delta =
                            schemaCacheService.synchronizeWithDatabase(
                                    serviceType,
                                    schemaTableName,
                                    serviceName
                            );

                    ReconciliationPlan reconciliationPlan =
                            prepareReconciliationPlan(
                                    delta,
                                    DbObjectType.SCHEMA,
                                    serviceName,
                                    schemaCacheService::validateReconciliationDeleteThreshold,
                                    skipDeletionThreshold
                            );

                    if (delta.isReconciliation()) {
                        ReconciliationUpsertResult reconciliationUpserts =
                                reconciliationMetadataUpserts(
                                        schemaCacheService,
                                        schemaTableName,
                                        serviceType,
                                        serviceName,
                                        delta,
                                        webClientProperties.getSchemaEndpoint(),
                                        DbObjectType.SCHEMA,
                                        null,
                                        metric
                                );

                        logReconciliationUpsertResult(
                                serviceName,
                                "SCHEMA RECONCILIATION PUT",
                                delta.getReconciliationFingerprints().size(),
                                reconciliationUpserts
                        );

                        return new SchemaUpsertContext(
                                delta,
                                new ReconciliationState(
                                        true,
                                        reconciliationPlan.orphanFqns(),
                                        reconciliationUpserts.errorCount() == 0,
                                        reconciliationUpserts.committedCount()
                                )
                        );
                    }

                    Map<EntityId, SchemaMetadata> regular =
                            delta.getPutRecords();
                    Map<EntityId, MetadataRename<SchemaMetadata>> renamed =
                            delta.getRenamedRecords();

                    BatchResult regularResult = metadataPutRequest(
                            regular,
                            webClientProperties.getSchemaEndpoint(),
                            DbObjectType.SCHEMA,
                            null,
                            metric
                    );

                    BatchResult renameResult = metadataRenameRequest(
                            renamed,
                            webClientProperties.getSchemaEndpoint(),
                            webClientProperties.getSchemaDeleteEndpoint(),
                            DbObjectType.SCHEMA,
                            null,
                            metric
                    );

                    Map<EntityId, SchemaMetadata> successful =
                            collectSuccessfulUpserts(
                                    regular,
                                    regularResult.commitIds(),
                                    renamed,
                                    renameResult.commitIds()
                            );

                    schemaCacheService.commitSuccessfulUpserts(
                            serviceType,
                            serviceName,
                            successful
                    );

                    logBatchResult(
                            serviceName,
                            "SCHEMA PUT",
                            regular.size(),
                            regularResult
                    );
                    logBatchResult(
                            serviceName,
                            "SCHEMA RENAME",
                            renamed.size(),
                            renameResult
                    );

                    return new SchemaUpsertContext(
                            delta,
                            new ReconciliationState(
                                    reconciliationPlan.required(),
                                    reconciliationPlan.orphanFqns(),
                                    regularResult.errorCount() == 0
                                            && renameResult.errorCount() == 0,
                                    successful.size()
                            )
                    );
                }
        );

        String tableName = metadataTablesProperties.getTable(
                serviceType,
                DbObjectType.TABLE
        );

        /* ==========================================================
         * TABLE UPSERT / RENAME / RECONCILIATION PREPARE
         * ========================================================== */
        TableUpsertContext tableContext = ingestionMetricService.execute(
                runId,
                IngestionMetricJob.TABLE_UPSERT,
                metric -> {
                    CacheComparisonResult<TableMetadata> delta =
                            tableCacheService.synchronizeWithDatabase(
                                    serviceType,
                                    tableName,
                                    serviceName
                            );

                    ReconciliationPlan reconciliationPlan =
                            prepareReconciliationPlan(
                                    delta,
                                    DbObjectType.TABLE,
                                    serviceName,
                                    tableCacheService::validateReconciliationDeleteThreshold,
                                    skipDeletionThreshold
                            );

                    if (delta.isReconciliation()) {
                        ReconciliationUpsertResult reconciliationUpserts =
                                reconciliationTableUpserts(
                                        tableCacheService,
                                        tableName,
                                        serviceType,
                                        serviceName,
                                        delta,
                                        webClientProperties.getTableEndpoint(),
                                        databaseContext.serviceType(),
                                        reconciliationPlan.snapshot(),
                                        metric
                                );

                        logReconciliationUpsertResult(
                                serviceName,
                                "TABLE RECONCILIATION PUT",
                                delta.getReconciliationFingerprints().size(),
                                reconciliationUpserts
                        );

                        return new TableUpsertContext(
                                delta,
                                new ReconciliationState(
                                        true,
                                        reconciliationPlan.orphanFqns(),
                                        reconciliationUpserts.errorCount() == 0,
                                        reconciliationUpserts.committedCount()
                                ),
                                reconciliationUpserts.requestSuccessCount()
                        );
                    }

                    Map<EntityId, TableMetadata> regular =
                            delta.getPutRecords();
                    Map<EntityId, MetadataRename<TableMetadata>> renamed =
                            delta.getRenamedRecords();

                    OpenMetadataSnapshot snapshotBeforePut =
                            reconciliationPlan.required()
                                    ? reconciliationPlan.snapshot()
                                    : loadTableSnapshotForPut(
                                            regular,
                                            renamed
                                    );

                    BatchResult regularResult = tablePutRequest(
                            regular,
                            webClientProperties.getTableEndpoint(),
                            databaseContext.serviceType(),
                            snapshotBeforePut,
                            metric
                    );

                    BatchResult renameResult = tableRenameRequest(
                            renamed,
                            webClientProperties.getTableEndpoint(),
                            webClientProperties.getTableDeleteEndpoint(),
                            databaseContext.serviceType(),
                            snapshotBeforePut,
                            metric
                    );

                    Map<EntityId, TableMetadata> successful =
                            collectSuccessfulUpserts(
                                    regular,
                                    regularResult.commitIds(),
                                    renamed,
                                    renameResult.commitIds()
                            );

                    tableCacheService.commitSuccessfulUpserts(
                            serviceType,
                            serviceName,
                            successful
                    );

                    logBatchResult(
                            serviceName,
                            "TABLE PUT",
                            regular.size(),
                            regularResult
                    );
                    logBatchResult(
                            serviceName,
                            "TABLE RENAME",
                            renamed.size(),
                            renameResult
                    );

                    return new TableUpsertContext(
                            delta,
                            new ReconciliationState(
                                    reconciliationPlan.required(),
                                    reconciliationPlan.orphanFqns(),
                                    regularResult.errorCount() == 0
                                            && renameResult.errorCount() == 0,
                                    successful.size()
                            ),
                            regularResult.requestSuccessCount()
                                    + renameResult.requestSuccessCount()
                    );
                }
        );

        /* ==========================================================
         * TABLE DELETE + RECONCILIATION FINALIZE
         * ========================================================== */
        ingestionMetricService.execute(
                runId,
                IngestionMetricJob.TABLE_DELETE,
                metric -> {
                    Map<EntityId, String> toDelete =
                            tableContext.delta().getDeletedRecords();

                    OpenMetadataSnapshot snapshotBeforeDelete =
                            tableContext.reconciliation().required()
                                    ? OpenMetadataSnapshot.empty()
                                    : openMetadataSnapshotRepository.loadByFqns(
                                            DbObjectType.TABLE,
                                            toDelete.values()
                                    );

                    BatchResult normalDeleteResult = tableDeleteRequest(
                            toDelete,
                            webClientProperties.getTableDeleteEndpoint(),
                            snapshotBeforeDelete,
                            metric
                    );

                    tableCacheService.commitSuccessfulDeletes(
                            serviceType,
                            serviceName,
                            normalDeleteResult.commitIds()
                    );

                    FqnBatchResult orphanDeleteResult =
                            tableOrphanDeleteRequest(
                                    tableContext.reconciliation()
                                            .orphanFqns(),
                                    webClientProperties.getTableDeleteEndpoint(),
                                    metric
                            );

                    finalizeReconciliation(
                            tableCacheService,
                            serviceType,
                            serviceName,
                            tableContext.reconciliation(),
                            normalDeleteResult,
                            orphanDeleteResult
                    );

                    logBatchResult(
                            serviceName,
                            "TABLE DELETE",
                            toDelete.size(),
                            normalDeleteResult
                    );
                    logFqnBatchResult(
                            serviceName,
                            "TABLE RECONCILIATION ORPHAN DELETE",
                            tableContext.reconciliation()
                                .orphanFqns()
                                .size(),
                            orphanDeleteResult
                    );

                    return null;
                }
        );

        /* ==========================================================
         * SCHEMA DELETE + RECONCILIATION FINALIZE
         * ========================================================== */
        ingestionMetricService.execute(
                runId,
                IngestionMetricJob.SCHEMA_DELETE,
                metric -> {
                    Map<EntityId, String> toDelete =
                            schemaContext.delta().getDeletedRecords();

                    BatchResult normalDeleteResult = metadataDeleteRequest(
                            toDelete,
                            webClientProperties.getSchemaDeleteEndpoint(),
                            DbObjectType.SCHEMA,
                            metric
                    );

                    schemaCacheService.commitSuccessfulDeletes(
                            serviceType,
                            serviceName,
                            normalDeleteResult.commitIds()
                    );

                    FqnBatchResult orphanDeleteResult =
                            metadataOrphanDeleteRequest(
                                    schemaContext.reconciliation()
                                            .orphanFqns(),
                                    webClientProperties.getSchemaDeleteEndpoint(),
                                    DbObjectType.SCHEMA,
                                    metric
                            );

                    finalizeReconciliation(
                            schemaCacheService,
                            serviceType,
                            serviceName,
                            schemaContext.reconciliation(),
                            normalDeleteResult,
                            orphanDeleteResult
                    );

                    logBatchResult(
                            serviceName,
                            "SCHEMA DELETE",
                            toDelete.size(),
                            normalDeleteResult
                    );
                    logFqnBatchResult(
                            serviceName,
                            "SCHEMA RECONCILIATION ORPHAN DELETE",
                            schemaContext.reconciliation()
                                .orphanFqns()
                                .size(),
                            orphanDeleteResult
                    );

                    return null;
                }
        );

        /* ==========================================================
         * DATABASE DELETE + RECONCILIATION FINALIZE
         * ========================================================== */
        ingestionMetricService.execute(
                runId,
                IngestionMetricJob.DATABASE_DELETE,
                metric -> {
                    Map<EntityId, String> toDelete =
                            databaseContext.delta().getDeletedRecords();

                    BatchResult normalDeleteResult = metadataDeleteRequest(
                            toDelete,
                            webClientProperties.getDatabaseDeleteEndpoint(),
                            DbObjectType.DATABASE,
                            metric
                    );

                    databaseCacheService.commitSuccessfulDeletes(
                            serviceType,
                            serviceName,
                            normalDeleteResult.commitIds()
                    );

                    FqnBatchResult orphanDeleteResult =
                            metadataOrphanDeleteRequest(
                                    databaseContext.reconciliation()
                                            .orphanFqns(),
                                    webClientProperties.getDatabaseDeleteEndpoint(),
                                    DbObjectType.DATABASE,
                                    metric
                            );

                    finalizeReconciliation(
                            databaseCacheService,
                            serviceType,
                            serviceName,
                            databaseContext.reconciliation(),
                            normalDeleteResult,
                            orphanDeleteResult
                    );

                    logBatchResult(
                            serviceName,
                            "DATABASE DELETE",
                            toDelete.size(),
                            normalDeleteResult
                    );
                    logFqnBatchResult(
                            serviceName,
                            "DATABASE RECONCILIATION ORPHAN DELETE",
                            databaseContext.reconciliation()
                                .orphanFqns()
                                .size(),
                            orphanDeleteResult
                    );

                    return null;
                }
        );

        if (tableContext.successfulUpserts() > 0) {
            ingestionMetricService.createViewParsingJob(
                    runId,
                    serviceName
            );
        }
    }

    /* ==========================================================
     * RECONCILIATION
     * ========================================================== */
    private ReconciliationPlan prepareReconciliationPlan(
            CacheComparisonResult<? extends Metadata> delta,
            DbObjectType objectType,
            String serviceName,
            ReconciliationThresholdValidator thresholdValidator,
            boolean skipDeleteThresholdValidation) {

        if (!delta.isReconciliation()) {
            return ReconciliationPlan.normal();
        }

        OpenMetadataSnapshot snapshot =
                openMetadataSnapshotRepository.loadByServiceName(
                        objectType,
                        serviceName
                );

        Set<String> sourceFqns =
                currentFqns(delta);

        Set<String> orphanFqns =
                snapshot.managedOrphans(
                        sourceFqns
                );

        if (skipDeleteThresholdValidation) {

            log.warn(
                    "Reconciliation delete threshold validation BYPASSED. "
                            + "objectType={}, service={}, "
                            + "omdManaged={}, orphans={}",
                    objectType,
                    serviceName,
                    snapshot.managedSize(),
                    orphanFqns.size()
            );

        } else {

            thresholdValidator.validate(
                    serviceName,
                    snapshot.managedSize(),
                    orphanFqns.size()
            );
        }

        log.warn(
                "FULL RECONCILIATION {} service={}: "
                        + "source={}, omdManaged={}, "
                        + "omdTotal={}, orphans={}, "
                        + "thresholdValidationSkipped={}",
                objectType,
                serviceName,
                sourceFqns.size(),
                snapshot.managedSize(),
                snapshot.size(),
                orphanFqns.size(),
                skipDeleteThresholdValidation
        );

        return new ReconciliationPlan(
                true,
                Collections.unmodifiableSet(
                        orphanFqns
                ),
                snapshot
        );
    }

    private <T extends Metadata> void finalizeReconciliation(
            AbstractMetadataCacheService<T> cacheService,
            ServiceType serviceType,
            String serviceName,
            ReconciliationState state,
            BatchResult normalDeleteResult,
            FqnBatchResult orphanDeleteResult) {

        if (!state.required()) {
            return;
        }

        boolean deletePhaseSuccessful =
                normalDeleteResult.errorCount() == 0
                && orphanDeleteResult.errorCount() == 0;

        if (!state.upsertPhaseSuccessful() || !deletePhaseSuccessful) {
            log.warn(
                    "Reconciliation remains UNINITIALIZED. serviceType={}, " +
                    "objectType={}, service={}, upsertOk={}, deleteOk={}",
                    serviceType,
                    cacheService.getDbObjectType(),
                    serviceName,
                    state.upsertPhaseSuccessful(),
                    deletePhaseSuccessful
            );
            return;
        }

        cacheService.markReconciliationComplete(
                serviceType,
                serviceName,
                state.committedUpserts()
        );
    }

    private Set<String> currentFqns(
            CacheComparisonResult<? extends Metadata> delta) {

        Set<String> result = new LinkedHashSet<>();

        if (delta.isReconciliation()) {
            for (MetadataFingerprint fingerprint
                    : delta.getReconciliationFingerprints().values()) {

                String fqn = fingerprint.getFqn();
                if (fqn != null && !fqn.isBlank()) {
                    result.add(fqn);
                }
            }
            return result;
        }

        for (Metadata metadata : delta.getPutRecords().values()) {
            String fqn = metadata.getFqn();
            if (fqn != null && !fqn.isBlank()) {
                result.add(fqn);
            }
        }

        for (MetadataRename<? extends Metadata> rename
                : delta.getRenamedRecords().values()) {

            String fqn = rename.current().getFqn();
            if (fqn != null && !fqn.isBlank()) {
                result.add(fqn);
            }
        }

        return result;
    }

    /**
     * Reconciliation intentionally loads full metadata in bounded chunks.
     * Only compact fingerprints live for the whole run; large TableMetadata
     * JSON payloads are eligible for GC after every batch.
     */
    private <T extends Metadata> ReconciliationUpsertResult
            reconciliationMetadataUpserts(
                    AbstractMetadataCacheService<T> cacheService,
                    String tableName,
                    ServiceType serviceType,
                    String serviceName,
                    CacheComparisonResult<T> delta,
                    String endpoint,
                    DbObjectType objectType,
                    ServiceType mapperServiceType,
                    MetricCounter metric) {

        ReconciliationAccumulator accumulator = new ReconciliationAccumulator();
        LinkedHashSet<EntityId> batchIds = new LinkedHashSet<>(
                metadataCacheProperties.getReconciliationBatchSize()
        );

        for (EntityId id : delta.getReconciliationFingerprints().keySet()) {
            batchIds.add(id);

            if (batchIds.size() >= metadataCacheProperties.getReconciliationBatchSize()) {
                processReconciliationMetadataBatch(
                        cacheService,
                        tableName,
                        serviceType,
                        serviceName,
                        batchIds,
                        endpoint,
                        objectType,
                        mapperServiceType,
                        metric,
                        accumulator
                );
                batchIds.clear();
            }
        }

        if (!batchIds.isEmpty()) {
            processReconciliationMetadataBatch(
                    cacheService,
                    tableName,
                    serviceType,
                    serviceName,
                    batchIds,
                    endpoint,
                    objectType,
                    mapperServiceType,
                    metric,
                    accumulator
            );
        }

        return accumulator.toResult();
    }

    private <T extends Metadata> void processReconciliationMetadataBatch(
            AbstractMetadataCacheService<T> cacheService,
            String tableName,
            ServiceType serviceType,
            String serviceName,
            Collection<EntityId> batchIds,
            String endpoint,
            DbObjectType objectType,
            ServiceType mapperServiceType,
            MetricCounter metric,
            ReconciliationAccumulator accumulator) {

        Map<EntityId, T> full = cacheService.loadFullMetadata(
                tableName,
                serviceName,
                batchIds
        );

        BatchResult result = metadataPutRequest(
                full,
                endpoint,
                objectType,
                mapperServiceType,
                metric
        );

        Map<EntityId, T> successful = selectSuccessful(
                full,
                result.commitIds()
        );

        cacheService.commitSuccessfulUpserts(
                serviceType,
                serviceName,
                successful
        );

        accumulator.add(result, successful.size());
    }

    private ReconciliationUpsertResult reconciliationTableUpserts(
            TableMetadataCacheServiceImpl cacheService,
            String tableName,
            ServiceType serviceType,
            String serviceName,
            CacheComparisonResult<TableMetadata> delta,
            String endpoint,
            ServiceType mapperServiceType,
            OpenMetadataSnapshot snapshot,
            MetricCounter metric) {

        ReconciliationAccumulator accumulator = new ReconciliationAccumulator();
        LinkedHashSet<EntityId> batchIds = new LinkedHashSet<>(
                metadataCacheProperties.getReconciliationBatchSize()
        );

        for (EntityId id : delta.getReconciliationFingerprints().keySet()) {
            batchIds.add(id);

            if (batchIds.size() >= metadataCacheProperties.getReconciliationBatchSize()) {
                processReconciliationTableBatch(
                        cacheService,
                        tableName,
                        serviceType,
                        serviceName,
                        batchIds,
                        endpoint,
                        mapperServiceType,
                        snapshot,
                        metric,
                        accumulator
                );
                batchIds.clear();
            }
        }

        if (!batchIds.isEmpty()) {
            processReconciliationTableBatch(
                    cacheService,
                    tableName,
                    serviceType,
                    serviceName,
                    batchIds,
                    endpoint,
                    mapperServiceType,
                    snapshot,
                    metric,
                    accumulator
            );
        }

        return accumulator.toResult();
    }

    private void processReconciliationTableBatch(
            TableMetadataCacheServiceImpl cacheService,
            String tableName,
            ServiceType serviceType,
            String serviceName,
            Collection<EntityId> batchIds,
            String endpoint,
            ServiceType mapperServiceType,
            OpenMetadataSnapshot snapshot,
            MetricCounter metric,
            ReconciliationAccumulator accumulator) {

        Map<EntityId, TableMetadata> full = cacheService.loadFullMetadata(
                tableName,
                serviceName,
                batchIds
        );

        BatchResult result = tablePutRequest(
                full,
                endpoint,
                mapperServiceType,
                snapshot,
                metric
        );

        Map<EntityId, TableMetadata> successful = selectSuccessful(
                full,
                result.commitIds()
        );

        cacheService.commitSuccessfulUpserts(
                serviceType,
                serviceName,
                successful
        );

        accumulator.add(result, successful.size());
    }

    private <T extends Metadata> Map<EntityId, T> selectSuccessful(
            Map<EntityId, T> source,
            Collection<EntityId> successfulIds) {

        if (source.isEmpty() || successfulIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<EntityId, T> result = new LinkedHashMap<>(successfulIds.size());
        for (EntityId id : successfulIds) {
            T metadata = source.get(id);
            if (metadata != null) {
                result.put(id, metadata);
            }
        }
        return result;
    }

    private void logReconciliationUpsertResult(
            String serviceName,
            String operation,
            int total,
            ReconciliationUpsertResult result) {

        log.info(
                "DbService \"{}\". {}: total={}, requestSuccess={}, " +
                "alreadySatisfied={}, errors={}, skipped={}, committedToIgnite={}",
                serviceName,
                operation,
                total,
                result.requestSuccessCount(),
                result.alreadySatisfiedCount(),
                result.errorCount(),
                result.skippedCount(),
                result.committedCount()
        );
    }

    /* ==========================================================
     * DATABASE SERVICE
     * ========================================================== */
    private void ensureDatabaseService(
            String serviceName,
            ServiceType type) {

        boolean exists = ordaClient.checkEntityExists(
                webClientProperties.getDatabaseServiceEndpoint()
                        + "/name/"
                        + serviceName
        );

        if (exists) {
            return;
        }

        log.debug("Creating databaseService: {}", serviceName);

        String serviceType = type.getValue()
                .substring(0, 1)
                .toUpperCase(Locale.ROOT)
                + type.getValue().substring(1);

        ObjectNode connection = objectMapper.createObjectNode();
        connection.putObject("config")
                .put("type", serviceType);

        DatabaseServiceMetadataDto dbServiceDto =
                DatabaseServiceMetadataDto.builder()
                        .name(serviceName)
                        .displayName(serviceName)
                        .serviceType(serviceType)
                        .connection(connection)
                        .build();

        String response = ordaClient.putRequest(
                        webClientProperties.getDatabaseServiceEndpoint(),
                        dbServiceDto,
                        String.class
                )
                .block();

        log.debug(
                "DatabaseService created. name={}, response={}",
                serviceName,
                response
        );
    }

    /* ==========================================================
     * GENERIC DATABASE / SCHEMA PUT
     * ========================================================== */
    private <T extends Metadata> BatchResult metadataPutRequest(
            Map<EntityId, T> metadata,
            String endpoint,
            DbObjectType objectType,
            ServiceType serviceType,
            MetricCounter metric) {

        return executeBatch(
                metadata,
                entry -> {
                    EntityId id = entry.getKey();
                    T value = entry.getValue();

                    Object body = mapperDto.getDto(
                            objectType,
                            value,
                            serviceType
                    );

                    if (body == null) {
                        metric.error();
                        log.error(
                                "Cannot build DTO. type={}, fqn={}",
                                objectType,
                                value.getFqn()
                        );
                        return Mono.just(EntityOutcome.failed(id));
                    }

                    return trackRequest(
                            id,
                            ordaClient.putRequest(
                                    endpoint,
                                    body,
                                    Void.class
                            ),
                            metric,
                            () -> log.debug(
                                    "Successfully created/updated {}: {}",
                                    objectType.name().toLowerCase(Locale.ROOT),
                                    value.getFqn()
                            ),
                            error -> log.error(
                                    "PUT {} failed. fqn={}, error={}",
                                    objectType,
                                    value.getFqn(),
                                    error.getMessage()
                            )
                    );
                }
        );
    }

    /* ==========================================================
     * GENERIC DATABASE / SCHEMA RENAME
     * ========================================================== */
    private <T extends Metadata> BatchResult metadataRenameRequest(
            Map<EntityId, MetadataRename<T>> metadata,
            String putEndpoint,
            String deleteEndpoint,
            DbObjectType objectType,
            ServiceType serviceType,
            MetricCounter metric) {

        return executeBatch(
                metadata,
                entry -> {
                    EntityId id = entry.getKey();
                    MetadataRename<T> rename = entry.getValue();
                    T current = rename.current();

                    Object body = mapperDto.getDto(
                            objectType,
                            current,
                            serviceType
                    );

                    if (body == null) {
                        metric.error();
                        log.error(
                                "Cannot build rename DTO. type={}, oldFqn={}, newFqn={}",
                                objectType,
                                rename.oldFqn(),
                                current.getFqn()
                        );
                        return Mono.just(EntityOutcome.failed(id));
                    }

                    Mono<Void> operation = ordaClient.putRequest(
                                    putEndpoint,
                                    body,
                                    Void.class
                            )
                            .then(
                                    ordaClient.deleteRequest(
                                            deleteEndpoint + "/" + rename.oldFqn(),
                                            true
                                    )
                            );

                    return trackRequest(
                            id,
                            operation,
                            metric,
                            () -> log.debug(
                                    "Successfully renamed {}: {} -> {}",
                                    objectType.name().toLowerCase(Locale.ROOT),
                                    rename.oldFqn(),
                                    current.getFqn()
                            ),
                            error -> log.error(
                                    "RENAME {} failed. oldFqn={}, newFqn={}, error={}",
                                    objectType,
                                    rename.oldFqn(),
                                    current.getFqn(),
                                    error.getMessage()
                            )
                    );
                }
        );
    }

    /* ==========================================================
     * GENERIC DATABASE / SCHEMA DELETE
     * ========================================================== */
    private BatchResult metadataDeleteRequest(
            Map<EntityId, String> metadata,
            String endpoint,
            DbObjectType objectType,
            MetricCounter metric) {

        return executeBatch(
                metadata,
                entry -> {
                    EntityId id = entry.getKey();
                    String fqn = entry.getValue();

                    return trackRequest(
                            id,
                            ordaClient.deleteRequest(
                                    endpoint + "/" + fqn,
                                    true
                            ),
                            metric,
                            () -> log.debug(
                                    "Successfully deleted {}: {}",
                                    objectType.name().toLowerCase(Locale.ROOT),
                                    fqn
                            ),
                            error -> log.error(
                                    "DELETE {} failed. fqn={}, error={}",
                                    objectType,
                                    fqn,
                                    error.getMessage()
                            )
                    );
                }
        );
    }

    private FqnBatchResult metadataOrphanDeleteRequest(
            Collection<String> fqns,
            String endpoint,
            DbObjectType objectType,
            MetricCounter metric) {

        return executeFqnBatch(
                fqns,
                fqn -> trackFqnRequest(
                        ordaClient.deleteRequest(
                                endpoint + "/" + fqn,
                                true
                        ),
                        metric,
                        () -> log.info(
                                "Reconciliation deleted orphan {}: {}",
                                objectType.name().toLowerCase(Locale.ROOT),
                                fqn
                        ),
                        error -> log.error(
                                "Reconciliation orphan DELETE {} failed. fqn={}, error={}",
                                objectType,
                                fqn,
                                error.getMessage()
                        )
                )
        );
    }

    /* ==========================================================
     * TABLE PUT
     * ========================================================== */
    private BatchResult tablePutRequest(
            Map<EntityId, TableMetadata> metadata,
            String endpoint,
            ServiceType serviceType,
            OpenMetadataSnapshot snapshot,
            MetricCounter metric) {

        return executeBatch(
                metadata,
                entry -> {
                    EntityId id = entry.getKey();
                    TableMetadata value = entry.getValue();

                    OpenMetadataSnapshotEntry existing =
                            findSnapshotEntry(
                                    snapshot,
                                    value.getFqn()
                            );

                    if (existing != null && existing.projectEntity()) {
                        log.info(
                                "Skip TABLE PUT {} (isProjectEntity=true)",
                                value.getFqn()
                        );
                        return Mono.just(EntityOutcome.skipped(id));
                    }

                    Object body = mapperDto.getDto(
                            DbObjectType.TABLE,
                            value,
                            serviceType
                    );

                    if (body == null) {
                        metric.error();
                        log.error(
                                "Cannot build TABLE DTO. fqn={}",
                                value.getFqn()
                        );
                        return Mono.just(EntityOutcome.failed(id));
                    }

                    return trackRequest(
                            id,
                            ordaClient.putRequest(
                                    endpoint,
                                    body,
                                    Void.class
                            ),
                            metric,
                            () -> log.debug(
                                    "Successfully created/updated table: {}",
                                    value.getFqn()
                            ),
                            error -> log.error(
                                    "TABLE PUT failed. fqn={}, error={}",
                                    value.getFqn(),
                                    error.getMessage()
                            )
                    );
                }
        );
    }

    /* ==========================================================
     * TABLE RENAME
     * ========================================================== */
    private BatchResult tableRenameRequest(
            Map<EntityId, MetadataRename<TableMetadata>> metadata,
            String putEndpoint,
            String deleteEndpoint,
            ServiceType serviceType,
            OpenMetadataSnapshot snapshot,
            MetricCounter metric) {

        return executeBatch(
                metadata,
                entry -> {
                    EntityId id = entry.getKey();
                    MetadataRename<TableMetadata> rename = entry.getValue();
                    TableMetadata current = rename.current();

                    OpenMetadataSnapshotEntry oldEntity =
                            findSnapshotEntry(
                                    snapshot,
                                    rename.oldFqn()
                            );
                    OpenMetadataSnapshotEntry newEntity =
                            findSnapshotEntry(
                                    snapshot,
                                    current.getFqn()
                            );

                    if ((oldEntity != null && oldEntity.projectEntity())
                            || (newEntity != null && newEntity.projectEntity())) {

                        log.info(
                                "Skip TABLE RENAME {} -> {} (isProjectEntity=true)",
                                rename.oldFqn(),
                                current.getFqn()
                        );
                        return Mono.just(EntityOutcome.skipped(id));
                    }

                    Object body = mapperDto.getDto(
                            DbObjectType.TABLE,
                            current,
                            serviceType
                    );

                    if (body == null) {
                        metric.error();
                        log.error(
                                "Cannot build TABLE rename DTO. oldFqn={}, newFqn={}",
                                rename.oldFqn(),
                                current.getFqn()
                        );
                        return Mono.just(EntityOutcome.failed(id));
                    }

                    Mono<Void> operation = ordaClient.putRequest(
                                    putEndpoint,
                                    body,
                                    Void.class
                            )
                            .then(
                                    ordaClient.deleteRequest(
                                            deleteEndpoint + "/" + rename.oldFqn(),
                                            true
                                    )
                            );

                    return trackRequest(
                            id,
                            operation,
                            metric,
                            () -> log.debug(
                                    "Successfully renamed table: {} -> {}",
                                    rename.oldFqn(),
                                    current.getFqn()
                            ),
                            error -> log.error(
                                    "TABLE RENAME failed. oldFqn={}, newFqn={}, error={}",
                                    rename.oldFqn(),
                                    current.getFqn(),
                                    error.getMessage()
                            )
                    );
                }
        );
    }

    /* ==========================================================
     * TABLE DELETE
     * ========================================================== */
    private BatchResult tableDeleteRequest(
            Map<EntityId, String> metadata,
            String endpoint,
            OpenMetadataSnapshot snapshot,
            MetricCounter metric) {

        return executeBatch(
                metadata,
                entry -> {
                    EntityId id = entry.getKey();
                    String fqn = entry.getValue();

                    OpenMetadataSnapshotEntry existing =
                            findSnapshotEntry(snapshot, fqn);

                    if (existing == null) {
                        log.debug(
                                "TABLE DELETE already satisfied: {} is absent in OMD snapshot",
                                fqn
                        );
                        return Mono.just(EntityOutcome.alreadySatisfied(id));
                    }

                    if (existing.projectEntity()) {
                        log.info(
                                "Skip TABLE DELETE {} (isProjectEntity=true)",
                                fqn
                        );
                        return Mono.just(EntityOutcome.skipped(id));
                    }

                    return trackRequest(
                            id,
                            ordaClient.deleteRequest(
                                    endpoint + "/" + fqn,
                                    true
                            ),
                            metric,
                            () -> log.debug(
                                    "Successfully deleted table: {}",
                                    fqn
                            ),
                            error -> log.error(
                                    "TABLE DELETE failed. fqn={}, error={}",
                                    fqn,
                                    error.getMessage()
                            )
                    );
                }
        );
    }

    private FqnBatchResult tableOrphanDeleteRequest(
            Collection<String> fqns,
            String endpoint,
            MetricCounter metric) {

        /*
         * fqns already come from OpenMetadataSnapshot.managedOrphans(),
         * therefore project entities are excluded before this method.
         */
        return executeFqnBatch(
                fqns,
                fqn -> trackFqnRequest(
                            ordaClient.deleteRequest(
                                    endpoint + "/" + fqn,
                                    true
                            ),
                            metric,
                            () -> log.info(
                                    "Reconciliation deleted orphan table: {}",
                                    fqn
                            ),
                            error -> log.error(
                                    "Reconciliation orphan TABLE DELETE failed. fqn={}, error={}",
                                    fqn,
                                    error.getMessage()
                            )
                    )
        );
    }

    /* ==========================================================
     * TARGETED TABLE SNAPSHOT
     * ========================================================== */
    private OpenMetadataSnapshot loadTableSnapshotForPut(
            Map<EntityId, TableMetadata> regular,
            Map<EntityId, MetadataRename<TableMetadata>> renamed) {

        Set<String> fqns = new LinkedHashSet<>();

        regular.values()
                .stream()
                .map(TableMetadata::getFqn)
                .filter(fqn -> fqn != null && !fqn.isBlank())
                .forEach(fqns::add);

        for (MetadataRename<TableMetadata> rename : renamed.values()) {
            if (rename.oldFqn() != null && !rename.oldFqn().isBlank()) {
                fqns.add(rename.oldFqn());
            }

            String currentFqn = rename.current().getFqn();
            if (currentFqn != null && !currentFqn.isBlank()) {
                fqns.add(currentFqn);
            }
        }

        return openMetadataSnapshotRepository.loadByFqns(
                DbObjectType.TABLE,
                fqns
        );
    }

    private OpenMetadataSnapshotEntry findSnapshotEntry(
            OpenMetadataSnapshot snapshot,
            String fqn) {

        if (snapshot == null || fqn == null) {
            return null;
        }

        return snapshot.find(fqn).orElse(null);
    }

    /* ==========================================================
     * BATCH EXECUTION
     * ========================================================== */
    private <T> BatchResult executeBatch(
            Map<EntityId, T> metadata,
            Function<Map.Entry<EntityId, T>, Mono<EntityOutcome>> operation) {

        if (metadata == null || metadata.isEmpty()) {
            return BatchResult.empty();
        }

        BatchAccumulator accumulator = Flux.fromIterable(metadata.entrySet())
                .flatMap(
                        operation,
                        requestConcurrency()
                )
                .collect(
                        BatchAccumulator::new,
                        BatchAccumulator::add
                )
                .block();

        return accumulator == null
                ? BatchResult.empty()
                : accumulator.toResult();
    }

    private FqnBatchResult executeFqnBatch(
            Collection<String> fqns,
            Function<String, Mono<OutcomeType>> operation) {

        if (fqns == null || fqns.isEmpty()) {
            return FqnBatchResult.empty();
        }

        FqnBatchAccumulator accumulator = Flux.fromIterable(fqns)
                .flatMap(
                        operation,
                        requestConcurrency()
                )
                .collect(
                        FqnBatchAccumulator::new,
                        FqnBatchAccumulator::add
                )
                .block();

        return accumulator == null
                ? FqnBatchResult.empty()
                : accumulator.toResult();
    }

    /* ==========================================================
     * REQUEST TRACKING
     * ========================================================== */
    private <T> Mono<EntityOutcome> trackRequest(
            EntityId id,
            Mono<T> request,
            MetricCounter metric,
            Runnable successAction,
            java.util.function.Consumer<Throwable> errorAction) {

        return request
                .then(Mono.fromSupplier(() -> {
                    metric.success();
                    successAction.run();
                    return EntityOutcome.success(id);
                }))
                .onErrorResume(error -> {
                    metric.error();
                    errorAction.accept(error);

                    if (isCriticalError(error)) {
                        return Mono.error(error);
                    }

                    return Mono.just(EntityOutcome.failed(id));
                });
    }

    private <T> Mono<OutcomeType> trackFqnRequest(
            Mono<T> request,
            MetricCounter metric,
            Runnable successAction,
            java.util.function.Consumer<Throwable> errorAction) {

        return request
                .then(Mono.fromSupplier(() -> {
                    metric.success();
                    successAction.run();
                    return OutcomeType.REQUEST_SUCCESS;
                }))
                .onErrorResume(error -> {
                    metric.error();
                    errorAction.accept(error);

                    if (isCriticalError(error)) {
                        return Mono.error(error);
                    }

                    return Mono.just(OutcomeType.FAILED);
                });
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

    /* ==========================================================
     * CACHE COMMIT HELPERS
     * ========================================================== */
    private <T extends Metadata> Map<EntityId, T> collectSuccessfulUpserts(
            Map<EntityId, T> regular,
            Set<EntityId> successfulRegularIds,
            Map<EntityId, MetadataRename<T>> renamed,
            Set<EntityId> successfulRenameIds) {

        int expectedSize = sizeOf(successfulRegularIds)
                + sizeOf(successfulRenameIds);

        if (expectedSize == 0) {
            return Map.of();
        }

        Map<EntityId, T> result = new LinkedHashMap<>(expectedSize);

        if (successfulRegularIds != null) {
            for (EntityId id : successfulRegularIds) {
                T value = regular.get(id);
                if (value == null) {
                    throw new IllegalStateException(
                            "Successful regular EntityId not found in delta: " + id
                    );
                }
                result.put(id, value);
            }
        }

        if (successfulRenameIds != null) {
            for (EntityId id : successfulRenameIds) {
                MetadataRename<T> rename = renamed.get(id);
                if (rename == null) {
                    throw new IllegalStateException(
                            "Successful renamed EntityId not found in delta: " + id
                    );
                }
                result.put(id, rename.current());
            }
        }

        return result;
    }

    private static int sizeOf(Set<?> values) {
        return values == null ? 0 : values.size();
    }

    /* ==========================================================
     * LOGGING
     * ========================================================== */
    private void logBatchResult(
            String serviceName,
            String operation,
            int total,
            BatchResult result) {

        log.info(
                "DbService \"{}\". {}: total={}, requestSuccess={}, " +
                "alreadySatisfied={}, errors={}, skipped={}, committedToIgnite={}.",
                serviceName,
                operation,
                total,
                result.requestSuccessCount(),
                result.alreadySatisfiedCount(),
                result.errorCount(),
                result.skippedCount(),
                result.commitIds().size()
        );
    }

    private void logFqnBatchResult(
            String serviceName,
            String operation,
            int total,
            FqnBatchResult result) {

        if (total == 0) {
            return;
        }

        log.info(
                "DbService \"{}\". {}: total={}, requestSuccess={}, errors={}, skipped={}.",
                serviceName,
                operation,
                total,
                result.requestSuccessCount(),
                result.errorCount(),
                result.skippedCount()
        );
    }

    private int requestConcurrency() {
        return maxConn == null || maxConn < 1
                ? 1
                : maxConn;
    }

    /* ==========================================================
     * INTERNAL RESULT MODEL
     * ========================================================== */
    private enum OutcomeType {
        REQUEST_SUCCESS,
        ALREADY_SATISFIED,
        FAILED,
        SKIPPED
    }

    private static final class ReconciliationAccumulator {
        private int requestSuccessCount;
        private int alreadySatisfiedCount;
        private int errorCount;
        private int skippedCount;
        private int committedCount;

        private void add(BatchResult result, int committed) {
            requestSuccessCount += result.requestSuccessCount();
            alreadySatisfiedCount += result.alreadySatisfiedCount();
            errorCount += result.errorCount();
            skippedCount += result.skippedCount();
            committedCount += committed;
        }

        private ReconciliationUpsertResult toResult() {
            return new ReconciliationUpsertResult(
                    requestSuccessCount,
                    alreadySatisfiedCount,
                    errorCount,
                    skippedCount,
                    committedCount
            );
        }
    }

    private record ReconciliationUpsertResult(
            int requestSuccessCount,
            int alreadySatisfiedCount,
            int errorCount,
            int skippedCount,
            int committedCount) {
    }

    private record EntityOutcome(
            EntityId id,
            OutcomeType type) {

        private static EntityOutcome success(EntityId id) {
            return new EntityOutcome(id, OutcomeType.REQUEST_SUCCESS);
        }

        private static EntityOutcome alreadySatisfied(EntityId id) {
            return new EntityOutcome(id, OutcomeType.ALREADY_SATISFIED);
        }

        private static EntityOutcome failed(EntityId id) {
            return new EntityOutcome(id, OutcomeType.FAILED);
        }

        private static EntityOutcome skipped(EntityId id) {
            return new EntityOutcome(id, OutcomeType.SKIPPED);
        }
    }

    private static final class BatchAccumulator {
        private final Set<EntityId> commitIds = new LinkedHashSet<>();
        private int requestSuccess;
        private int alreadySatisfied;
        private int errors;
        private int skipped;

        private void add(EntityOutcome outcome) {
            switch (outcome.type()) {
                case REQUEST_SUCCESS -> {
                    requestSuccess++;
                    commitIds.add(outcome.id());
                }
                case ALREADY_SATISFIED -> {
                    alreadySatisfied++;
                    commitIds.add(outcome.id());
                }
                case FAILED -> errors++;
                case SKIPPED -> skipped++;
            }
        }

        private BatchResult toResult() {
            return new BatchResult(
                    Collections.unmodifiableSet(
                            new LinkedHashSet<>(commitIds)
                    ),
                    requestSuccess,
                    alreadySatisfied,
                    errors,
                    skipped
            );
        }
    }

    private record BatchResult(
            Set<EntityId> commitIds,
            int requestSuccessCount,
            int alreadySatisfiedCount,
            int errorCount,
            int skippedCount) {

        private static BatchResult empty() {
            return new BatchResult(
                    Set.of(),
                    0,
                    0,
                    0,
                    0
            );
        }
    }

    private static final class FqnBatchAccumulator {
        private int requestSuccess;
        private int errors;
        private int skipped;

        private void add(OutcomeType outcome) {
            switch (outcome) {
                case REQUEST_SUCCESS, ALREADY_SATISFIED -> requestSuccess++;
                case FAILED -> errors++;
                case SKIPPED -> skipped++;
            }
        }

        private FqnBatchResult toResult() {
            return new FqnBatchResult(
                    requestSuccess,
                    errors,
                    skipped
            );
        }
    }

    private record FqnBatchResult(
            int requestSuccessCount,
            int errorCount,
            int skippedCount) {

        private static FqnBatchResult empty() {
            return new FqnBatchResult(0, 0, 0);
        }
    }

    private record ReconciliationPlan(
            boolean required,
            Set<String> orphanFqns,
            OpenMetadataSnapshot snapshot) {

        private static ReconciliationPlan normal() {
            return new ReconciliationPlan(
                    false,
                    Set.of(),
                    OpenMetadataSnapshot.empty()
            );
        }
    }

    private record ReconciliationState(
            boolean required,
            Set<String> orphanFqns,
            boolean upsertPhaseSuccessful,
            int committedUpserts) {
    }

    @FunctionalInterface
    private interface ReconciliationThresholdValidator {
        void validate(
                String serviceName,
                int openMetadataManagedCount,
                int orphanDeleteCount
        );
    }

    private record DatabaseUpsertContext(
            ServiceType serviceType,
            CacheComparisonResult<DatabaseMetadata> delta,
            ReconciliationState reconciliation) {
    }

    private record SchemaUpsertContext(
            CacheComparisonResult<SchemaMetadata> delta,
            ReconciliationState reconciliation) {
    }

    private record TableUpsertContext(
            CacheComparisonResult<TableMetadata> delta,
            ReconciliationState reconciliation,
            int successfulUpserts) {
    }
}
