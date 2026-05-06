package com.gpb.metadata.ingestion.enums;

import java.util.HashMap;
import java.util.Map;

public enum SapIqColumnType implements OrdaColumnTypeMapper {

    // Character / string
    CHAR("CHAR"),
    CHARACTER("CHAR"),
    NCHAR("CHAR"),
    VARCHAR("VARCHAR"),
    NVARCHAR("VARCHAR"),
    CHAR_VARYING("VARCHAR"),
    CHARACTER_VARYING("VARCHAR"),
    LONG_VARCHAR("VARCHAR"),
    LONG_NVARCHAR("VARCHAR"),
    TEXT("TEXT"),
    CLOB("CLOB"),

    // Binary
    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    BINARY_VARYING("VARBINARY"),
    LONG_BINARY("BINARY"),
    LONG_VARBINARY("BINARY"),
    IMAGE("IMAGE"),
    BLOB("BLOB"),

    // Boolean / bit
    BIT("BOOLEAN"),
    BOOLEAN("BOOLEAN"),

    // Integer
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    INT("INT"),
    INTEGER("INT"),
    BIGINT("BIGINT"),

    UNSIGNED_TINYINT("TINYINT"),
    UNSIGNED_SMALLINT("INT"),
    UNSIGNED_INT("BIGINT"),
    UNSIGNED_INTEGER("BIGINT"),
    UNSIGNED_BIGINT("NUMERIC"),

    // Exact numeric
    DEC("DECIMAL"),
    DECIMAL("DECIMAL"),
    NUMERIC("NUMERIC"),
    NUMBER("NUMBER"),
    MONEY("NUMBER"),
    SMALLMONEY("NUMBER"),

    // Approximate numeric
    REAL("FLOAT"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DOUBLE_PRECISION("DOUBLE"),

    // Date / time
    DATE("DATE"),
    TIME("TIME"),
    TIMESTAMP("TIMESTAMP"),
    DATETIME("DATETIME"),
    BIGTIME("TIME"),
    BIGDATETIME("DATETIME"),

    // SAP / Sybase special
    UNIQUEIDENTIFIER("UUID"),
    UNIQUE_IDENTIFIER("UUID"),
    UUID("UUID"),

    // Compatibility / fallback
    XML("XML"),
    JSON("JSON"),
    NULL("NULL"),
    UNKNOWN("UNKNOWN");

    private final String targetType;

    SapIqColumnType(String targetType) {
        this.targetType = targetType;
    }

    @Override
    public String getTargetType() {
        return targetType;
    }

    private static final Map<String, SapIqColumnType> LOOKUP = new HashMap<>();

    static {
        for (SapIqColumnType type : values()) {
            LOOKUP.put(type.name(), type);
            LOOKUP.put(type.name().replace("_", " "), type);
        }

        // Character aliases
        putAlias("char", CHAR);
        putAlias("character", CHARACTER);
        putAlias("nchar", NCHAR);
        putAlias("national char", NCHAR);
        putAlias("national character", NCHAR);

        putAlias("varchar", VARCHAR);
        putAlias("nvarchar", NVARCHAR);
        putAlias("national varchar", NVARCHAR);
        putAlias("national character varying", NVARCHAR);

        putAlias("char varying", CHAR_VARYING);
        putAlias("character varying", CHARACTER_VARYING);
        putAlias("long varchar", LONG_VARCHAR);
        putAlias("long nvarchar", LONG_NVARCHAR);

        // Binary aliases
        putAlias("binary", BINARY);
        putAlias("varbinary", VARBINARY);
        putAlias("binary varying", BINARY_VARYING);
        putAlias("long binary", LONG_BINARY);
        putAlias("long varbinary", LONG_VARBINARY);
        putAlias("image", IMAGE);
        putAlias("blob", BLOB);

        // Boolean / bit
        putAlias("bit", BIT);
        putAlias("boolean", BOOLEAN);

        // Integer aliases
        putAlias("tinyint", TINYINT);
        putAlias("smallint", SMALLINT);
        putAlias("int", INT);
        putAlias("integer", INTEGER);
        putAlias("bigint", BIGINT);

        putAlias("unsigned tinyint", UNSIGNED_TINYINT);
        putAlias("unsigned smallint", UNSIGNED_SMALLINT);
        putAlias("unsigned int", UNSIGNED_INT);
        putAlias("unsigned integer", UNSIGNED_INTEGER);
        putAlias("unsigned bigint", UNSIGNED_BIGINT);

        // Exact numeric aliases
        putAlias("dec", DEC);
        putAlias("decimal", DECIMAL);
        putAlias("numeric", NUMERIC);
        putAlias("number", NUMBER);
        putAlias("money", MONEY);
        putAlias("smallmoney", SMALLMONEY);
        putAlias("small money", SMALLMONEY);

        // Approximate numeric aliases
        putAlias("real", REAL);
        putAlias("float", FLOAT);
        putAlias("double", DOUBLE);
        putAlias("double precision", DOUBLE_PRECISION);

        // Date / time aliases
        putAlias("date", DATE);
        putAlias("time", TIME);
        putAlias("timestamp", TIMESTAMP);
        putAlias("datetime", DATETIME);
        putAlias("bigtime", BIGTIME);
        putAlias("bigdatetime", BIGDATETIME);
        putAlias("big time", BIGTIME);
        putAlias("big datetime", BIGDATETIME);

        // Special aliases
        putAlias("uniqueidentifier", UNIQUEIDENTIFIER);
        putAlias("unique identifier", UNIQUE_IDENTIFIER);
        putAlias("uuid", UUID);

        putAlias("xml", XML);
        putAlias("json", JSON);
        putAlias("null", NULL);
    }

    public static String map(String source) {
        if (source == null || source.isBlank()) {
            return "UNKNOWN";
        }

        SapIqColumnType mapping = LOOKUP.get(normalize(source));
        return mapping != null ? mapping.getTargetType() : "UNKNOWN";
    }

    private static void putAlias(String alias, SapIqColumnType type) {
        LOOKUP.put(normalize(alias), type);
    }

    private static String normalize(String source) {
        String normalized = source.trim().toUpperCase();

        // VARCHAR(255) -> VARCHAR
        // NUMERIC(19,4) -> NUMERIC
        // UNSIGNED INT(10) -> UNSIGNED INT
        int bracketIndex = normalized.indexOf('(');
        if (bracketIndex >= 0) {
            normalized = normalized.substring(0, bracketIndex).trim();
        }

        // убираем кавычки, если тип внезапно пришёл quoted
        normalized = normalized
                .replace("\"", "")
                .replace("'", "")
                .replace("[", "")
                .replace("]", "");

        // все варианты разделителей приводим к "_":
        // LONG VARCHAR      -> LONG_VARCHAR
        // LONG    VARCHAR   -> LONG_VARCHAR
        // LONG-VARCHAR      -> LONG_VARCHAR
        normalized = normalized
                .replace("-", "_")
                .replaceAll("\\s+", "_");

        // на случай двойных подчёркиваний
        normalized = normalized.replaceAll("_+", "_");

        return normalized;
    }
}
