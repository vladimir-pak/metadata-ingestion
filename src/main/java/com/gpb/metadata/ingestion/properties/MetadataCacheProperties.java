package com.gpb.metadata.ingestion.properties;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import lombok.Data;

@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "metadata.cache")
public class MetadataCacheProperties {

    /** Scan page sent by Ignite Thin Client. */
    @Min(128)
    @Max(65_536)
    private int scanPageSize = 4096;

    /** Maximum number of compact entries sent in one putAll/removeAll call. */
    @Min(1)
    @Max(50_000)
    private int commitBatchSize = 2000;

    /** Number of backup copies for the PARTITIONED runtime cache. */
    @Min(0)
    @Max(3)
    private int backups = 1;

    /**
     * Number of full source objects loaded at once during cold reconciliation.
     * Table metadata may contain large JSON payloads, therefore keep this
     * bounded independently from the compact cache commit batch.
     */
    @Min(1)
    @Max(10_000)
    private int reconciliationBatchSize = 500;

    /** Automatically migrate the trusted runtime_v2 cache on first v3 access. */
    private boolean migrateLegacyV2 = false;

    /**
     * Keep v2 after a successful migration by default. It is cheap insurance
     * during rollout. Enable destruction only after v3 has been stable.
     */
    private boolean destroyLegacyAfterMigration = false;
}
