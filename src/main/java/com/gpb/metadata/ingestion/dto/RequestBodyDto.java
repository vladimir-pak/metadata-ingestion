package com.gpb.metadata.ingestion.dto;

import com.gpb.metadata.ingestion.enums.ServiceType;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class RequestBodyDto {
    
    @NotBlank 
    private String serviceName;

    @NotNull 
    private ServiceType serviceType;
    
    private boolean async;
}
