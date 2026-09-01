package com.gpb.metadata.ingestion.exceptions;

public class DeleteThresholdExceededException extends RuntimeException {

    public DeleteThresholdExceededException(String message) {
        super(message);
    }
}