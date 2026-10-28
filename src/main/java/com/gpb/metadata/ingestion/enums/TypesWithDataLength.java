package com.gpb.metadata.ingestion.enums;

/*
 * Класс с типами данных ОРДы, для которых dataLength должен быть заполнен
 */
public enum TypesWithDataLength {
    VARCHAR,
    CHAR,
    BINARY,
    VARBINARY,
    UUID;
    
    public static boolean isSupportedType(String dataType) {
        if (dataType == null) return false;
        try {
            valueOf(dataType.toUpperCase());
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
