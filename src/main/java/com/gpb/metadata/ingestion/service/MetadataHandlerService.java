package com.gpb.metadata.ingestion.service;

import com.gpb.metadata.ingestion.enums.ServiceType;

public interface MetadataHandlerService {
    void start(
            ServiceType serviceType,
            String serviceName,
            String runId,
            boolean skipDeletionThreshold
    );

    void start(
            ServiceType serviceType,
            String serviceName,
            String runId
    );

    void startAsync(
            ServiceType serviceType,
            String serviceName,
            String runId,
            boolean skipDeletionThreshold
    );

    void startAsync(
            ServiceType serviceType,
            String serviceName,
            String runId
    );
}
