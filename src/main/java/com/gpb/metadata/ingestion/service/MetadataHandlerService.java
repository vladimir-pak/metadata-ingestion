package com.gpb.metadata.ingestion.service;

public interface MetadataHandlerService {
    public void startAsync(String serviceName);
    public void start(String serviceName);
}
