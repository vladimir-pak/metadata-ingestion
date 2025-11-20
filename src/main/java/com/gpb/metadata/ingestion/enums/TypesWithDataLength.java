package com.gpb.metadata.ingestion.enums;

/**
 * Класс с типами данных ОРДы, для которых dataLength должен быть заполнен
 */
public enum TypesWithDataLength {
    VARCHAR,
    CHAR,
    BINARY,
    VARBINARY;
    
    public static String getProcessedDataLength(String dataType, String dataLength) {
        if (!isSupportedType(dataType)) {
            return dataLength;
        }
        return dataLength == null ? "0" : dataLength;
    }
    
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
