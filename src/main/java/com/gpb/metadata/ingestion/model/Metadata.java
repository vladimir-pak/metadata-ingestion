package com.gpb.metadata.ingestion.model;

public interface Metadata {
    EntityId getId();
    String getHashData();
    String getName();
    String getFqn();
    String getServiceName();
    String getParentFqn();
}
