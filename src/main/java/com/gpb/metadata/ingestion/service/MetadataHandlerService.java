package com.gpb.metadata.ingestion.service;

public interface MetadataHandlerService {
    void start(
            String schemaName,
            String serviceName,
            String runId
    );

    void startAsync(
            String schemaName,
            String serviceName,
            String runId
    );
}
