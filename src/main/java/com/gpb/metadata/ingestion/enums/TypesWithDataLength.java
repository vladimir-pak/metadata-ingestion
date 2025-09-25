package com.gpb.metadata.ingestion.enums;

/*
 * Класс с типами данных ОРДы, для которых dataLength должен быть заполнен
 */
public enum TypesWithDataLength {
    VARCHAR,
    CHAR,
    BINARY,
    VARBINARY;
    
    public static String getProcessedDataLength(String dataType, String dataLength) {
        // Если тип не поддерживается или dataType null, возвращаем исходное значение
        if (!isSupportedType(dataType)) {
            return dataLength;
        }
        // Если тип поддерживается и dataLength null, возвращаем "0"
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
