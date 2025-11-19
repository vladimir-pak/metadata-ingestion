package com.gpb.metadata.ingestion.dto.mapper;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.gpb.metadata.ingestion.enums.*;
import com.gpb.metadata.ingestion.model.schema.TableConstraints;
import com.gpb.metadata.ingestion.service.impl.ColumnTypeMapperService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import com.gpb.metadata.ingestion.dto.ColumnMetadataDto;
import com.gpb.metadata.ingestion.dto.DatabaseMetadataDto;
import com.gpb.metadata.ingestion.dto.SchemaMetadataDto;
import com.gpb.metadata.ingestion.dto.TableMetadataDto;
import com.gpb.metadata.ingestion.model.Metadata;
import com.gpb.metadata.ingestion.model.postgres.DatabaseMetadata;
import com.gpb.metadata.ingestion.model.postgres.SchemaMetadata;
import com.gpb.metadata.ingestion.model.postgres.TableMetadata;
import com.gpb.metadata.ingestion.model.schema.TableData;


@Slf4j
@Configuration
@RequiredArgsConstructor
public class MapperDto {

    private final ColumnTypeMapperService columnTypeMapperService;

    public Object getDto(DbObjectType dbObjectType, Metadata metadata, ServiceType serviceType) {
        return switch (dbObjectType) {
            case DATABASE -> mapToDatabaseDto((DatabaseMetadata) metadata);
            case SCHEMA -> mapToSchemaDto((SchemaMetadata) metadata);
            case TABLE -> mapToTableDto((TableMetadata) metadata, serviceType);
        };
    }

    public DatabaseMetadataDto mapToDatabaseDto(DatabaseMetadata meta) {
        return DatabaseMetadataDto.builder()
                .name(meta.getName())
                .displayName(meta.getName())
                .service(meta.getServiceName())
                .build();
    }

    public SchemaMetadataDto mapToSchemaDto(SchemaMetadata meta) {
        return SchemaMetadataDto.builder()
                .name(meta.getName())
                .displayName(meta.getName())
                .database(meta.getParentFqn())
                .build();
    }

    public TableMetadataDto mapToTableDto(TableMetadata meta, ServiceType serviceType) {
        TableData tableData = meta.getTableData();
        String processedTableType = TableTypes.map(tableData.getTableType());

        List<ColumnMetadataDto> columns = tableData.getColumns().stream()
                .map(column -> {
                    String processedDataType = columnTypeMapperService.map(serviceType, column.getDataType());

                    String processedDataLength = TypesWithDataLength.getProcessedDataLength(
                            processedDataType,
                            column.getDataLength()
                    );

                    String constraint = column.getConstraint();
                    if ("NULLABLE".equalsIgnoreCase(constraint)) {
                        constraint = null;
                    }

                    return ColumnMetadataDto.builder()
                            .name(column.getName())
                            .dataType(processedDataType)
                            .arrayDataType(resolveArrayType(column.getDataType(), processedDataType))
                            .dataTypeDisplay(column.getDataTypeDisplay())
                            .dataLength(processedDataLength)
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
                    boolean keep = ConstraintType.isSupported(c.getConstraintType());
                    if (!keep) {
                        log.debug("Constraint '{}' пропущен — не поддерживается Ордой",
                                c.getConstraintType());
                    }
                    return keep;
                })
                .collect(Collectors.toList());

        return TableMetadataDto.builder()
                .name(meta.getName())
                .displayName(meta.getName())
                .tableType(processedTableType)
                .isProjectEntity(false)
                .viewDefinition(tableData.getViewDefinition())
                .databaseSchema(meta.getParentFqn())
                .description(meta.getDescription())
                .columns(columns)
                .tableConstraints(filteredConstraints)
                .build();
    }

    private String resolveArrayType(String sourceType, String processedType) {
        if (sourceType == null || !"ARRAY".equalsIgnoreCase(processedType)) {
            return null; // только для ARRAY
        }

        if (sourceType.endsWith("[]")) {
            return sourceType.substring(0, sourceType.length() - 2).toUpperCase();
        }

        return null;
    }
}
