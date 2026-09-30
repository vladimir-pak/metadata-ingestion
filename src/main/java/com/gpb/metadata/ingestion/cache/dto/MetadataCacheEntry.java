package com.gpb.metadata.ingestion.cache.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Compact committed state in Ignite.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class MetadataCacheEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private String hashData;
    private String fqn;
}
