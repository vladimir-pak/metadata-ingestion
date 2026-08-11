package com.gpb.metadata.ingestion.service;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.stereotype.Component;

import com.gpb.metadata.ingestion.exceptions.TokenRefreshException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Централизованное хранилище access token для запросов в OpenMetadata.
 *
 * Проверку срока жизни и обычный refresh выполняет KeycloakAuthService.
 * Принудительный refresh после HTTP 401 дополнительно синхронизирован здесь,
 * чтобы при нескольких параллельных 401 только один поток обращался в Keycloak.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrdaTokenProvider {

    private final KeycloakAuthService keycloakAuthService;

    private final AtomicReference<String> tokenRef = new AtomicReference<>();
    private final Object refreshMonitor = new Object();

    public String getToken() {
        // KeycloakAuthService сам проверяет expires_in и синхронно выполняет
        // refresh только при необходимости. Здесь сохраняем фактически выданный
        // токен, чтобы после 401 отличить уже обновлённый токен от rejectedToken.
        return loadValidToken();
    }

    /**
     * Обновляет токен после HTTP 401.
     *
     * @param rejectedToken токен, с которым конкретный HTTP-запрос получил 401
     * @return новый токен либо токен, который уже успел обновить другой поток
     */
    public String refreshAfterUnauthorized(String rejectedToken) {
        synchronized (refreshMonitor) {
            String currentToken = tokenRef.get();

            // Пока запрос с rejectedToken обрабатывал 401, другой поток мог уже
            // получить новый токен. В таком случае повторный refresh не нужен.
            if (hasText(currentToken) && !Objects.equals(currentToken, rejectedToken)) {
                log.debug("ORD access token was already refreshed by another request");
                return currentToken;
            }

            return forceRefreshToken();
        }
    }

    private String loadValidToken() {
        try {
            String token = keycloakAuthService.getValidAccessToken();
            return store(token);
        } catch (Exception e) {
            throw asTokenRefreshException(e);
        }
    }

    private String forceRefreshToken() {
        try {
            String token = keycloakAuthService.forceRefreshAccessToken();
            log.debug("ORD access token successfully refreshed after HTTP 401");
            return store(token);
        } catch (Exception e) {
            throw asTokenRefreshException(e);
        }
    }

    private String store(String token) {
        if (!hasText(token)) {
            throw new TokenRefreshException(
                    new IllegalStateException("ORD access_token is not resolved")
            );
        }

        tokenRef.set(token);
        return token;
    }

    private TokenRefreshException asTokenRefreshException(Exception e) {
        if (e instanceof TokenRefreshException tokenRefreshException) {
            return tokenRefreshException;
        }
        return new TokenRefreshException(e);
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
