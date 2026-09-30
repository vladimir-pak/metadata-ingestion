package com.gpb.metadata.ingestion.jwt.exception;

public class JwtInvalidSecretException extends RuntimeException {

    public JwtInvalidSecretException() {
        super("Wrong secret");
    }
}