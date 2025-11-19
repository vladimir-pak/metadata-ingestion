package com.gpb.metadata.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "keycloak")
@Data
public class KeycloakConfig {
    private String serverUrl;
    private String clientId;
    private String clientSecret;
    private String username;
    private String password;
}
