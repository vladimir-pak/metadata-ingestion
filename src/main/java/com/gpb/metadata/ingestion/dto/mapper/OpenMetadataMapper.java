package com.gpb.metadata.ingestion.dto.mapper;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import com.gpb.metadata.ingestion.enums.*;
import com.gpb.metadata.ingestion.model.schema.TableConstraints;
import com.gpb.metadata.ingestion.service.impl.ColumnTypeMapperService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import com.gpb.metadata.ingestion.dto.ColumnMetadataDto;
import com.gpb.metadata.ingestion.dto.DatabaseMetadataDto;
import com.gpb.metadata.ingestion.dto.SchemaMetadataDto;
import com.gpb.metadata.ingestion.dto.TableMetadataDto;
import com.gpb.metadata.ingestion.model.DatabaseMetadata;
import com.gpb.metadata.ingestion.model.Metadata;
import com.gpb.metadata.ingestion.model.SchemaMetadata;
import com.gpb.metadata.ingestion.model.TableMetadata;
import com.gpb.metadata.ingestion.model.schema.TableData;


@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(
    prefix = "metadata",
    name = "version",
    havingValue = "1.9.4"
)
public class OpenMetadataMapper implements OpenMetadataRequestMapper  {

    private final ColumnTypeMapperService columnTypeMapperService;

    public Object getDto(
            DbObjectType dbObjectType, 
            Metadata metadata, 
            ServiceType serviceType) {
        return switch (dbObjectType) {

            case DATABASE -> mapToDatabaseDto(
                    (DatabaseMetadata) metadata
            );

            case SCHEMA -> mapToSchemaDto(
                    (SchemaMetadata) metadata
            );

            case TABLE -> mapToTableDto(
                    (TableMetadata) metadata, 
                    serviceType
            );
        };
    }

    private DatabaseMetadataDto mapToDatabaseDto(
            DatabaseMetadata meta) {
        return DatabaseMetadataDto.builder()
                .name(meta.getName())
                .displayName(meta.getName())
                .service(meta.getServiceName())
                .build();
    }

    private SchemaMetadataDto mapToSchemaDto(SchemaMetadata meta) {
        SchemaMetadataDto dto = SchemaMetadataDto.builder()
                .name(meta.getName())
                .displayName(meta.getName())
                .database(meta.getParentFqn())
                .build();
        if (meta.getDbName().contains(".")) {
            String dbFqn = String.format("%s.\"%s\"", meta.getServiceName(), meta.getDbName());
            dto.setDatabase(dbFqn);
        }
        return dto;
    }

    private TableMetadataDto mapToTableDto(
            TableMetadata meta, 
            ServiceType serviceType) {

        TableData tableData = meta.getData();

        String processedTableType = 
                TableTypes.map(
                        tableData.getTableType()
                );

        if (tableData.getColumns() == null) {
            log.error(
                    "Ошибка при обработке Table {}: columns is null", 
                    meta.getFqn()
            );
            return null;
        }

        List<ColumnMetadataDto> columns = tableData.getColumns().stream()
                .map(column -> {
                    String processedDataType = 
                            columnTypeMapperService.map(
                                    serviceType, 
                                    column.getDataType()
                            );

                    int processedDataLength = column.getDataLength();
                    int precision =  column.getPrecision();
                    int scale = column.getScale();
                    String constraint = column.getConstraint();

                    if (constraint != null) {
                        constraint = constraint.trim();

                        if ("NULLABLE".equalsIgnoreCase(constraint)
                                || constraint.isBlank()) {

                            constraint = null;

                        } else if (!ColumnConstraintType.isSupported(constraint)) {

                            log.warn(
                                "Unsupported ColumnConstraint '{}' for column '{}', table '{}'",
                                constraint,
                                column.getName(),
                                meta.getFqn()
                            );

                            constraint = null;

                        } else {
                            constraint = constraint.toUpperCase(Locale.ROOT);
                        }
                    }

                    return ColumnMetadataDto.builder()
                            .name(column.getName())
                            .dataType(processedDataType)
                            .arrayDataType(
                                    resolveArrayType(
                                            column.getDataType(), 
                                            processedDataType, 
                                            serviceType
                                    )
                            )
                            .dataTypeDisplay(column.getDataTypeDisplay())
                            .dataLength(processedDataLength)
                            .precision(precision)
                            .scale(scale)
                            .description(column.getDescription())
                            .constraint(constraint)
                            .ordinalPosition(column.getOrdinalPosition())
                            .build();
                })
                .collect(Collectors.toList());

        //Фильтрация поддерживаемых констрейнтов перед отправкой в Орду
        List<TableConstraints> filteredConstraints = Optional.ofNullable(tableData.getTableConstraints())
                .orElseGet(List::of)
                .stream()
                .filter(c -> {
                    boolean keep = TableConstraintType.isSupported(c.getConstraintType());
                    if (!keep) {
                        log.warn("TableConstraint '{}' пропущен — не поддерживается Ордой",
                                c.getConstraintType());
                    }
                    if (
                        "FOREIGN_KEY".equals(c.getConstraintType())
                                && c.getReferredColumns() == null
                    ) {
                        log.debug("TableConstraint '{}' пропущен — не найдены ссылки на внешние ключи",
                                c.getConstraintType());
                        return false;
                    }
                    return keep;
                })
                .collect(Collectors.toList());

        TableMetadataDto dto = TableMetadataDto.builder()
                .name(meta.getName())
                .displayName(meta.getName())
                .tableType(processedTableType)
                .isProjectEntity(false)
                .schemaDefinition(tableData.getViewDefinition())
                .databaseSchema(meta.getParentFqn())
                .description(meta.getDescription())
                .columns(columns)
                .tableConstraints(filteredConstraints)
                .build();
        
        String dbName = meta.getDbName();
        String schemaName = meta.getSchemaName();

        if (meta.getDbName().contains(".")) {
            dbName = String.format("\"%s\"", meta.getDbName());
        }

        if (meta.getSchemaName().contains(".")) {
            schemaName = String.format("\"%s\"", meta.getSchemaName());
        }
        
        dto.setDatabaseSchema(
                String.format(
                        "%s.%s.%s", 
                        meta.getServiceName(), 
                        dbName, 
                        schemaName
                )
        );

        return dto;
    }

    private String resolveArrayType(
            String sourceType,
            String processedType,
            ServiceType serviceType) {

        if (sourceType == null || !"ARRAY".equalsIgnoreCase(processedType)) {
            return null; // только для ARRAY
        }

        if (sourceType.endsWith("[]")) {
            String elementType = 
                    sourceType.substring(
                            0, 
                            sourceType.length() - 2
                    );
            return columnTypeMapperService.map(serviceType, elementType);
        }

        return null;
    }
}
