package com.gpb.metadata.ingestion.dto;

import lombok.Data;

@Data
public class RequestBodyDto {
    private String serviceName;
    private String dbType;
}
