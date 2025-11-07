package com.gpb.metadata.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KeycloakTokenResponse {
    @JsonProperty("access_token")
    private String accessToken;

    @JsonProperty("refresh_token")
    private String refreshToken;

    @JsonProperty("expires_in")
    private long expiresIn;

    @JsonProperty("refresh_expires_in")
    private long refreshExpiresIn;

    @JsonProperty("token_type")
    private String tokenType;

    private long createdAt = System.currentTimeMillis();

    public boolean isAccessTokenExpired() {
        return System.currentTimeMillis() >= createdAt + (expiresIn - 5) * 1000;
    }

    public boolean isRefreshTokenExpired() {
        return System.currentTimeMillis() >= createdAt + (refreshExpiresIn - 5) * 1000;
    }
}
