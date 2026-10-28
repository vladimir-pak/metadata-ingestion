package com.gpb.metadata.ingestion.dto.mapper;

import com.gpb.metadata.ingestion.enums.DbObjectType;
import com.gpb.metadata.ingestion.enums.ServiceType;
import com.gpb.metadata.ingestion.model.Metadata;

public interface OpenMetadataRequestMapper {

    Object getDto(
        DbObjectType objectType,
        Metadata metadata,
        ServiceType serviceType
    );
}
