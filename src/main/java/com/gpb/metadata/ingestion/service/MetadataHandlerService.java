package com.gpb.metadata.ingestion.service;

public interface MetadataHandlerService {
    public void startAsync(String schemaName ,String serviceName);
    public void start(String schemaName ,String serviceName);
}
