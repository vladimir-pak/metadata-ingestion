package com.gpb.metadata.ingestion.utils;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import com.gpb.metadata.ingestion.dto.lineage.LineageSource;
import com.gpb.metadata.ingestion.model.postgres.TableMetadata;
import com.gpb.metadata.ingestion.model.schema.ColumnData;
import com.gpb.metadata.ingestion.model.schema.TableData;
import com.gpb.metadata.ingestion.properties.WebClientProperties;
import com.gpb.metadata.ingestion.service.KeycloakAuthService;
import com.gpb.metadata.ingestion.service.impl.TableMetadataCacheServiceImpl;
import com.gpb.metadata.ingestion.dto.lineage.AddLineageRequest;
import com.gpb.metadata.ingestion.dto.lineage.ColumnsLineage;
import com.gpb.metadata.ingestion.dto.lineage.EntityReference;
import com.gpb.metadata.ingestion.dto.lineage.LineageDetails;
import com.gpb.metadata.ingestion.dto.lineage.LineageEdge;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class ViewLineageRequestBuilder {

    private final OrdaClient ordaClient;
    private final ViewSqlLineageParser parser;

    private final KeycloakAuthService keycloakAuthService;
    private final WebClientProperties webClientProperties;

    private final TableMetadataCacheServiceImpl tableCacheService;

    // кэш только для OM id (FQN -> Optional<uuid>)
    private final Map<String, Optional<String>> idCache = new ConcurrentHashMap<>();

    private static final List<String> DEFAULT_SCHEMA_FALLBACK =
            List.of("pg_catalog", "information_schema");

    public List<AddLineageRequest> buildEdgesForView(TableMetadata viewDTO, String dbType) {
        String token = keycloakAuthService.getValidAccessToken();
        if (token == null) {
            throw new RuntimeException("ORD access_token is not resolved");
        }

        // viewFqn = service.db.schema.view  (parentFqn уже = service.db.schema)
        String viewFqn = String.format("%s.%s", viewDTO.getParentFqn(), normalizeIdent(viewDTO.getName()));

        String viewId = resolveTableIdCached(viewFqn, token).orElse(null);
        if (viewId == null) {
            log.warn("View {} отсутствует в OMD", viewFqn);
            return List.of();
        }

        // Метаданные view из cache (для toColumn FQN)
        TableMetadata viewMeta = tableCacheService
                .findByFqn(dbType, viewDTO.getServiceName(), viewFqn)
                .orElse(null);

        // name(lower) -> fqn
        Map<String, String> viewColumnFqns = indexColumnFqns(viewMeta);

        String sql = Optional.ofNullable(viewDTO.getTableData())
                .map(TableData::getViewDefinition)
                .orElse(null);

        if (sql == null || sql.isBlank()) {
            log.warn("View {}: viewDefinition пустой, пропускаем lineage", viewFqn);
            return List.of();
        }

        ViewSqlLineageParser.ParsedLineage parsed = parser.parse(sql);

        // alias/name(normalized lower) -> resolved upstream tableFqn (service.db.schema.table)
        Map<String, String> aliasToResolvedFqn = new HashMap<>();

        // tableFqn -> (colName(lower) -> colFqn)
        Map<String, Map<String, String>> upstreamColumnIndexByTableFqn = new HashMap<>();

        // 1) резолвим alias->tableFqn + прогреваем колонковые индексы (из cache)
        for (var e : parsed.aliasToTable().entrySet()) {
            String aliasKey = normKey(e.getKey());
            ViewSqlLineageParser.TableRef tr = e.getValue();

            String upstreamTableFqn = resolveUpstreamTableFqn(viewDTO, tr, dbType);
            if (upstreamTableFqn == null) continue;

            aliasToResolvedFqn.put(aliasKey, upstreamTableFqn);

            TableMetadata upstreamMeta = tableCacheService
                    .findByFqn(dbType, viewDTO.getServiceName(), upstreamTableFqn)
                    .orElse(null);

            upstreamColumnIndexByTableFqn.put(upstreamTableFqn, indexColumnFqns(upstreamMeta));
        }

        // 2) строим columnsLineage (общий список, затем фильтруем на каждый edge)
        List<ColumnsLineage> columnsLineage = buildColumnsLineage(
                viewFqn,
                viewColumnFqns,
                parsed,
                aliasToResolvedFqn,
                upstreamColumnIndexByTableFqn,
                viewDTO,
                dbType
        );

        // 3) table-level edges: для каждой upstream таблицы создаём edge.
        List<AddLineageRequest> out = new ArrayList<>();

        for (var upstreamRef : parsed.upstreamTables()) {
            String upstreamFqn = resolveUpstreamTableFqn(viewDTO, upstreamRef, dbType);
            if (upstreamFqn == null) continue;

            // фильтруем column-lineage только по этой upstream таблице
            List<ColumnsLineage> filtered = columnsLineage.stream()
                    .map(cl -> {
                        List<String> from = Optional.ofNullable(cl.getFromColumns())
                                .orElse(List.of())
                                .stream()
                                .filter(fc -> fc != null && fc.startsWith(upstreamFqn + "."))
                                .toList();

                        return from.isEmpty() ? null : ColumnsLineage.builder()
                                .toColumn(cl.getToColumn())
                                .fromColumns(from)
                                .build();
                    })
                    .filter(Objects::nonNull)
                    .toList();

            String upstreamId = resolveTableIdCached(upstreamFqn, token).orElse(null);
            if (upstreamId == null) {
                log.warn("Upstream {} отсутствует в OMD", upstreamFqn);
                continue;
            }

            out.add(AddLineageRequest.builder()
                    .edge(LineageEdge.builder()
                            .fromEntity(EntityReference.builder().type("table").id(upstreamId).build())
                            .toEntity(EntityReference.builder().type("table").id(viewId).build())
                            .lineageDetails(LineageDetails.builder()
                                    .source(LineageSource.ViewLineage)
                                    .sqlQuery(sql)
                                    .columnsLineage(filtered.isEmpty() ? null : filtered)
                                    .build())
                            .build())
                    .build());
        }

        return out;
    }

    private List<ColumnsLineage> buildColumnsLineage(
            String viewFqn,
            Map<String, String> viewCols, // name(lower)->fqn
            ViewSqlLineageParser.ParsedLineage parsed,
            Map<String, String> aliasToResolvedTableFqn, // alias(lower)->tableFqn
            Map<String, Map<String, String>> upstreamColsByTableFqn, // tableFqn -> (col(lower)->colFqn)
            TableMetadata viewDTO,
            String dbType
    ) {
        List<ColumnsLineage> out = new ArrayList<>();

        for (var mapping : parsed.columnMappings()) {
            String toName = normalizeIdent(mapping.toColumn());
            if (toName == null || toName.isBlank()) continue;

            String toFqn = viewCols.getOrDefault(toName.toLowerCase(Locale.ROOT), viewFqn + "." + toName);

            List<String> fromFqns = new ArrayList<>();

            for (var cr : mapping.from()) {
                String holderRaw = normalizeIdent(cr.tableAliasOrName()); // alias or table name or null
                String colName = normalizeIdent(cr.column());
                if (colName == null || colName.isBlank()) continue;

                String upstreamTableFqn = null;

                if (holderRaw != null && !holderRaw.isBlank()) {
                    // 1) пробуем alias/name в aliasToResolvedFqn
                    String holderKey = holderRaw.toLowerCase(Locale.ROOT);
                    upstreamTableFqn = aliasToResolvedTableFqn.get(holderKey);

                    // 2) если не нашли — трактуем holder как имя таблицы без схемы
                    if (upstreamTableFqn == null) {
                        upstreamTableFqn = resolveUpstreamTableFqn(
                                viewDTO,
                                new ViewSqlLineageParser.TableRef(null, holderRaw),
                                dbType
                        );
                    }
                } else {
                    // table не указан в column ref.
                    // Если upstream ровно 1 — можно предположить её
                    if (parsed.upstreamTables().size() == 1) {
                        var only = parsed.upstreamTables().iterator().next();
                        upstreamTableFqn = resolveUpstreamTableFqn(viewDTO, only, dbType);
                    }
                }

                if (upstreamTableFqn == null) continue;

                Map<String, String> upstreamCols = upstreamColsByTableFqn.getOrDefault(upstreamTableFqn, Map.of());

                String fromFqn = upstreamCols.get(colName.toLowerCase(Locale.ROOT));
                if (fromFqn == null) {
                    // fallback: строим fqn вручную
                    fromFqn = upstreamTableFqn + "." + colName;
                }

                fromFqns.add(fromFqn);
            }

            if (!fromFqns.isEmpty()) {
                // уникализируем, чтобы не дублировать fromColumns
                List<String> uniq = new ArrayList<>(new LinkedHashSet<>(fromFqns));

                out.add(ColumnsLineage.builder()
                        .toColumn(toFqn)
                        .fromColumns(uniq)
                        .build());
            }
        }

        return out;
    }

    private String resolveUpstreamTableFqn(TableMetadata viewDTO, ViewSqlLineageParser.TableRef ref, String dbType) {
        String tableName = normalizeIdent(ref.name());
        String schema = normalizeIdent(ref.schema());

        if (tableName == null || tableName.isBlank()) return null;

        // если схема указана — service.db.<schema>.<table>
        if (schema != null && !schema.isBlank()) {
            String fqn = String.format("%s.%s.%s.%s",
                    viewDTO.getServiceName(),
                    viewDTO.getDbName(),
                    schema,
                    tableName);

            return tableCacheService
                    .findByFqn(dbType, viewDTO.getServiceName(), fqn)
                    .map(TableMetadata::getFqn)
                    .orElse(null);
        }

        // иначе: schema view -> pg_catalog -> information_schema
        List<String> candidates = new ArrayList<>(1 + DEFAULT_SCHEMA_FALLBACK.size());

        // ВАЖНО: viewDTO.getSchemaName() должен быть именно "schema", не FQN.
        candidates.add(normalizeIdent(viewDTO.getSchemaName()));
        candidates.addAll(DEFAULT_SCHEMA_FALLBACK);

        for (String schemaCandidateRaw : candidates) {
            String schemaCandidate = normalizeIdent(schemaCandidateRaw);
            if (schemaCandidate == null || schemaCandidate.isBlank()) continue;

            String fqn = String.format("%s.%s.%s.%s",
                    viewDTO.getServiceName(),
                    viewDTO.getDbName(),
                    schemaCandidate,
                    tableName);

            Optional<TableMetadata> tm = tableCacheService.findByFqn(dbType, viewDTO.getServiceName(), fqn);
            if (tm.isPresent()) return tm.get().getFqn();
        }

        return null;
    }

    private Optional<String> resolveTableIdCached(String fqn, @NonNull String token) {
        String endpoint = webClientProperties.getTableEndpoint() + "/name/" + fqn;
        return idCache.computeIfAbsent(fqn, key -> ordaClient.resolveTableId(endpoint, token));
    }

    /**
     * Индекс колонок таблицы: colName(lower)->colFqn
     */
    private static Map<String, String> indexColumnFqns(TableMetadata meta) {
        if (meta == null) return Map.of();

        TableData td;
        try {
            td = meta.getTableData();
        } catch (Exception e) {
            return Map.of();
        }

        List<ColumnData> cols = Optional.ofNullable(td.getColumns()).orElse(List.of());
        if (cols.isEmpty()) return Map.of();

        String tableFqn = meta.getFqn();
        Map<String, String> idx = new HashMap<>(cols.size() * 2);

        for (ColumnData c : cols) {
            String col = normalizeIdent(c.getName());
            if (col != null && !col.isBlank()) {
                idx.put(col.toLowerCase(Locale.ROOT), tableFqn + "." + col);
            }
        }

        return idx;
    }

    /**
     * Нормализация идентификатора: trim, снять двойные кавычки.
     */
    private static String normalizeIdent(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        return s;
    }

    /**
     * Нормализованный ключ для alias/name map: normalizeIdent + lower-case.
     */
    private static String normKey(String s) {
        String n = normalizeIdent(s);
        return (n == null) ? null : n.toLowerCase(Locale.ROOT);
    }
}
