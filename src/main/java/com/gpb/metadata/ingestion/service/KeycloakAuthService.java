package com.gpb.metadata.ingestion.service;

import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.gpb.metadata.ingestion.config.KeycloakConfig;
import com.gpb.metadata.ingestion.dto.KeycloakTokenResponse;

@Service
public class KeycloakAuthService {
    private final KeycloakConfig props;
    private final RestTemplate restTemplate = new RestTemplate();

    private KeycloakTokenResponse currentToken;

    public KeycloakAuthService(KeycloakConfig props) {
        this.props = props;
    }

    /** Основной метод: вернуть гарантированно действующий токен */
    public synchronized String getValidAccessToken() {
        if (currentToken == null) {
            currentToken = fetchToken();
        } else if (currentToken.isAccessTokenExpired()) {
            if (!currentToken.isRefreshTokenExpired()) {
                currentToken = refreshToken(currentToken.getRefreshToken());
            } else {
                currentToken = fetchToken();
            }
        }
        return currentToken.getAccessToken();
    }

    /** Первый вход по логину и паролю */
    private KeycloakTokenResponse fetchToken() {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "password");
        form.add("client_id", props.getClientId());
        if (props.getClientSecret() != null)
            form.add("client_secret", props.getClientSecret());
        form.add("username", props.getUsername());
        form.add("password", props.getPassword());

        return postForToken(form);
    }

    /** Обновление по refresh_token */
    private KeycloakTokenResponse refreshToken(String refreshToken) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "refresh_token");
        form.add("client_id", props.getClientId());
        if (props.getClientSecret() != null)
            form.add("client_secret", props.getClientSecret());
        form.add("refresh_token", refreshToken);

        try {
            return postForToken(form);
        } catch (Exception e) {
            return fetchToken();
        }
    }

    private KeycloakTokenResponse postForToken(MultiValueMap<String, String> form) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<>(form, headers);

        ResponseEntity<KeycloakTokenResponse> response = restTemplate.exchange(
                props.getServerUrl(),
                HttpMethod.POST,
                entity,
                KeycloakTokenResponse.class
        );

        KeycloakTokenResponse token = response.getBody();
        if (token != null) {
            token.setCreatedAt(System.currentTimeMillis());
        }
        return token;
    }
}
