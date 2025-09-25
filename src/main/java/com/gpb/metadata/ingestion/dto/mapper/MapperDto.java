package com.gpb.metadata.ingestion.dto.mapper;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.context.annotation.Configuration;

import com.gpb.metadata.ingestion.dto.ColumnMetadataDto;
import com.gpb.metadata.ingestion.dto.DatabaseMetadataDto;
import com.gpb.metadata.ingestion.dto.SchemaMetadataDto;
import com.gpb.metadata.ingestion.dto.TableMetadataDto;
import com.gpb.metadata.ingestion.enums.Entity;
import com.gpb.metadata.ingestion.enums.PostgresColumnType;
import com.gpb.metadata.ingestion.enums.TableTypes;
import com.gpb.metadata.ingestion.enums.TypesWithDataLength;
import com.gpb.metadata.ingestion.model.DatabaseMetadata;
import com.gpb.metadata.ingestion.model.Metadata;
import com.gpb.metadata.ingestion.model.SchemaMetadata;
import com.gpb.metadata.ingestion.model.TableMetadata;
import com.gpb.metadata.ingestion.model.schema.TableData;

@Configuration
public class MapperDto {
    public Object getDto(Entity entity, Metadata metadata) {
        return switch (entity) {
            case DATABASE -> mapToDatabaseDto((DatabaseMetadata) metadata);
            case SCHEMA -> mapToSchemaDto((SchemaMetadata) metadata);
            case TABLE -> mapToTableDto((TableMetadata) metadata);
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
    
    public TableMetadataDto mapToTableDto(TableMetadata meta) {
        TableData tableData = meta.getTableData();
        // Преобразуем tableType
        String processedTableType = TableTypes.map(tableData.getTableType());

        // Собираем атрибуты таблицы
        List<ColumnMetadataDto> columns = tableData.getColumns().stream()
                .map(column -> {
                    // Преобразуем dataType через enum
                    String processedDataType = PostgresColumnType.map(column.getDataType());
                    // Предобработка dataLength с проверкой на обязательность заполнения с учетом dataType
                    String processedDataLength = TypesWithDataLength.getProcessedDataLength(
                            processedDataType,
                            column.getDataLength()
                        );
                    
                    return ColumnMetadataDto.builder()
                        .name(column.getName())
                        .dataType(processedDataType)  // Обработанное значение
                        .dataTypeDisplay(column.getDataTypeDisplay())
                        .dataLength(processedDataLength)
                        .description(column.getDescription())
                        .constraint(column.getConstraint())
                        .ordinalPosition(column.getOrdinalPosition())
                        .build();
                })
                .collect(Collectors.toList());

        // Собираем ограничения (constraints) таблицы

        return TableMetadataDto.builder()
                .name(meta.getName())
                .displayName(meta.getName())
                .tableType(processedTableType)
                .isProjectEntity(false)
                .viewDefinition(tableData.getViewDefinition())
                .databaseSchema(meta.getParentFqn())
                .description(meta.getDescription())
                .columns(columns)
                .tableConstraints(tableData.getTableConstraints())
                .build();
    }
}
