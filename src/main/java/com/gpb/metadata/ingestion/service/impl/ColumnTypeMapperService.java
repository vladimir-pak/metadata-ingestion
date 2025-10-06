package com.gpb.metadata.ingestion.service.impl;

import com.gpb.metadata.ingestion.enums.*;
import org.springframework.stereotype.Component;

@Component
public class ColumnTypeMapperService {
    public String map(ServiceType type, String sourceType) {
        if (type == null || sourceType == null) return "UNKNOWN";

        return switch (type) {
            case MSSQL -> MssqlOrdaColumnType.map(sourceType);
            case POSTGRES -> PostgresColumnType.map(sourceType);
            case ORACLE -> OracleOrdaColumnType.map(sourceType);
        };
    }
}
