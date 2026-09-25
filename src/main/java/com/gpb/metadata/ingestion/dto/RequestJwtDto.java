package com.gpb.metadata.ingestion.dto;

import lombok.Data;

@Data
public class RequestJwtDto {
    private String secret;
    private String service;
}
