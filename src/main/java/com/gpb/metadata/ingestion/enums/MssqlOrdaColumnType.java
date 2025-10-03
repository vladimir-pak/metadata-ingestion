package com.gpb.metadata.ingestion.enums;

import java.util.HashMap;
import java.util.Map;

public enum MssqlOrdaColumnType implements OrdaColumnTypeMapper {
    BIGINT("BIGINT"),
    INT("INT"),
    SMALLINT("SMALLINT"),
    TINYINT("TINYINT"),

    BIT("BOOLEAN"),

    DECIMAL("DECIMAL"),
    NUMERIC("NUMERIC"),
    MONEY("NUMBER"),
    SMALLMONEY("NUMBER"),

    FLOAT("FLOAT"),
    REAL("FLOAT"),

    DATE("DATE"),
    TIME("TIME"),
    DATETIME("DATETIME"),
    DATETIME2("DATETIME"),
    DATETIMEOFFSET("DATETIME"),
    SMALLDATETIME("DATETIME"),

    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    NCHAR("CHAR"),
    NVARCHAR("VARCHAR"),
    TEXT("TEXT"),
    NTEXT("TEXT"),

    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    IMAGE("BLOB"),

    UNIQUEIDENTIFIER("UUID"),

    XML("XML"),
    SQL_VARIANT("VARBINARY"),

    GEOGRAPHY("GEOGRAPHY"),
    GEOMETRY("GEOMETRY"),

    UNKNOWN("UNKNOWN");

    private final String targetType;

    MssqlOrdaColumnType(String targetType) {
        this.targetType = targetType;
    }

    public String getTargetType() {
        return targetType;
    }

    private static final Map<String, MssqlOrdaColumnType> LOOKUP = new HashMap<>();

    static {
        for (MssqlOrdaColumnType type : values()) {
            LOOKUP.put(type.name(), type);
        }
    }

    public static String map(String source) {
        if (source == null) return "UNKNOWN";
        MssqlOrdaColumnType mapping = LOOKUP.get(source.toUpperCase());
        return mapping != null ? mapping.getTargetType() : "UNKNOWN";
    }
}

