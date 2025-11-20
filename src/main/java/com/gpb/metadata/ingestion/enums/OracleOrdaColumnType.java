package com.gpb.metadata.ingestion.enums;

import java.util.HashMap;
import java.util.Map;

public enum OracleOrdaColumnType implements OrdaColumnTypeMapper {
    // Числовые типы
    NUMBER("NUMBER"),
    FLOAT("FLOAT"),
    BINARY_FLOAT("FLOAT"),
    BINARY_DOUBLE("DOUBLE"),

    // Строковые типы
    CHAR("CHAR"),
    NCHAR("CHAR"),
    VARCHAR("VARCHAR"),
    VARCHAR2("VARCHAR"),
    NVARCHAR2("VARCHAR"),
    LONG("LONG"),
    CLOB("CLOB"),
    NCLOB("CLOB"),
    BLOB("BLOB"),

    // Дата/время
    DATE("DATE"),
    TIMESTAMP("TIMESTAMP"),
    TIMESTAMP_WITH_TIME_ZONE("TIMESTAMP"),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE("TIMESTAMP"),
    INTERVAL_YEAR_TO_MONTH("INTERVAL"),
    INTERVAL_DAY_TO_SECOND("INTERVAL"),

    // Двоичные типы
    RAW("BINARY"),
    LONG_RAW("BINARY"),
    BFILE("BINARY"),

    // Пространственные и XML
    XMLTYPE("XML"),
    ROWID("VARCHAR"),
    UROWID("VARCHAR"),

    // Прочие
    BOOLEAN("BOOLEAN"),
    JSON("JSON"),

    UNKNOWN("UNKNOWN");

    private final String targetType;

    OracleOrdaColumnType(String targetType) {
        this.targetType = targetType;
    }

    public String getTargetType() {
        return targetType;
    }

    private static final Map<String, OracleOrdaColumnType> LOOKUP = new HashMap<>();

    static {
        for (OracleOrdaColumnType type : values()) {
            LOOKUP.put(type.name(), type);
        }
    }

    public static String map(String source) {
        if (source == null) return "UNKNOWN";
        OracleOrdaColumnType mapping = LOOKUP.get(source.toUpperCase());
        return mapping != null ? mapping.getTargetType() : "UNKNOWN";
    }
}

