package com.gpb.metadata.ingestion.utils;

import java.net.InetAddress;
import java.net.URI;
import java.util.function.Function;

import org.springframework.http.HttpHeaders;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.gpb.metadata.ingestion.cef.SvoiLogger;
import com.gpb.metadata.ingestion.config.KeycloakConfig;
import com.gpb.metadata.ingestion.exceptions.OrdaAuthException;
import com.gpb.metadata.ingestion.exceptions.TokenRefreshException;
import com.gpb.metadata.ingestion.properties.WebClientProperties;
import com.gpb.metadata.ingestion.service.OrdaTokenProvider;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
public class OrdaClient {

    private final SvoiLogger svoiCustomLogger;
    private final WebClient webClient;
    private final WebClientProperties webClientProperties;
    private final KeycloakConfig keycloakConfig;
    private final OrdaTokenProvider tokenProvider;

    private volatile OrdaHost ordaHost;

    @PostConstruct
    void init() {
        this.ordaHost = parseOrdaHost();
    }

    /**
     * Executes a request with the current access token.
     * On the first HTTP 401, refreshes the token and retries exactly once.
     * A second 401 is promoted to TokenRefreshException and is considered fatal
     * for the current ingestion job.
     */
    private <T> Mono<T> withAuth(Function<String, Mono<T>> request) {
        return Mono.fromCallable(tokenProvider::getToken)
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(token ->
                        Mono.defer(() -> request.apply(token))
                                .onErrorResume(
                                        OrdaAuthException.class,
                                        firstAuthError ->
                                                Mono.fromCallable(() ->
                                                                tokenProvider.refreshAfterUnauthorized(token)
                                                        )
                                                        .subscribeOn(Schedulers.boundedElastic())
                                                        .flatMap(refreshedToken ->
                                                                Mono.defer(() -> request.apply(refreshedToken))
                                                                        .onErrorMap(
                                                                                OrdaAuthException.class,
                                                                                TokenRefreshException::new
                                                                        )
                                                        )
                                )
                );
    }

    public <T> Mono<T> putRequest(
            @NonNull String endpoint,
            @NonNull Object requestBody,
            @NonNull Class<T> responseType) {

        return withAuth(token ->
                putRequestInternal(
                        endpoint,
                        requestBody,
                        token,
                        responseType
                )
        );
    }

    private <T> Mono<T> putRequestInternal(
            String endpoint,
            Object requestBody,
            String token,
            Class<T> responseType) {

        return Mono.defer(() -> {
            OrdaHost orda = currentOrdaHost();
            long start = System.currentTimeMillis();
            String username = keycloakConfig.getUsername();

            return webClient.put()
                    .uri(endpoint)
                    .header(
                            HttpHeaders.AUTHORIZATION,
                            "Bearer " + token
                    )
                    .bodyValue(requestBody)
                    .exchangeToMono(response -> {
                        long duration = System.currentTimeMillis() - start;
                        int status = response.statusCode().value();

                        if (response.statusCode().isError()) {
                            return response.bodyToMono(String.class)
                                    .defaultIfEmpty("")
                                    .flatMap(body -> {
                                        logOrda(
                                                endpoint,
                                                "PUT",
                                                status,
                                                duration,
                                                body,
                                                orda,
                                                username
                                        );

                                        if (status == 401) {
                                            return Mono.error(
                                                    new OrdaAuthException(
                                                            "access_token is not valid or expired"
                                                    )
                                            );
                                        }

                                        return Mono.error(
                                                new RuntimeException(
                                                        "OpenMetadata PUT failed. status=" +
                                                        status + ", endpoint=" + endpoint +
                                                        ", body=" + body
                                                )
                                        );
                                    });
                        }

                        logOrda(
                                endpoint,
                                "PUT",
                                status,
                                duration,
                                null,
                                orda,
                                username
                        );

                        if (responseType == Void.class) {
                            return response.releaseBody().then(Mono.<T>empty());
                        }

                        return response.bodyToMono(responseType);
                    });
        });
    }

    /**
     * Idempotent DELETE.
     *
     * HTTP 404 means that the desired final state is already reached and is
     * therefore returned as logical success. This is critical when OMD DELETE
     * succeeded but the process crashed before Ignite commit.
     */
    public Mono<Void> deleteRequest(
            @NonNull String endpoint,
            boolean recursive) {

        return withAuth(token ->
                deleteRequestInternal(
                        endpoint,
                        token,
                        recursive
                )
        );
    }

    private Mono<Void> deleteRequestInternal(
            String endpoint,
            String token,
            boolean recursive) {

        return Mono.defer(() -> {
            OrdaHost orda = currentOrdaHost();
            long start = System.currentTimeMillis();
            String username = keycloakConfig.getUsername();

            return webClient.delete()
                    .uri(uriBuilder -> uriBuilder
                            .path(endpoint)
                            .queryParam("recursive", recursive)
                            .build()
                    )
                    .header(
                            HttpHeaders.AUTHORIZATION,
                            "Bearer " + token
                    )
                    .exchangeToMono(response -> {
                        long duration = System.currentTimeMillis() - start;
                        int status = response.statusCode().value();

                        if (status == 404) {
                            return response.bodyToMono(String.class)
                                    .defaultIfEmpty("")
                                    .flatMap(body -> {
                                        logOrda(
                                                endpoint,
                                                "DELETE",
                                                status,
                                                duration,
                                                body,
                                                orda,
                                                username
                                        );
                                        return Mono.empty();
                                    });
                        }

                        if (response.statusCode().isError()) {
                            return response.bodyToMono(String.class)
                                    .defaultIfEmpty("")
                                    .flatMap(body -> {
                                        logOrda(
                                                endpoint,
                                                "DELETE",
                                                status,
                                                duration,
                                                body,
                                                orda,
                                                username
                                        );

                                        if (status == 401) {
                                            return Mono.error(
                                                    new OrdaAuthException(
                                                            "access_token is not valid or expired"
                                                    )
                                            );
                                        }

                                        return Mono.error(
                                                new RuntimeException(
                                                        "OpenMetadata DELETE failed. status=" +
                                                        status + ", endpoint=" + endpoint +
                                                        ", body=" + body
                                                )
                                        );
                                    });
                        }

                        logOrda(
                                endpoint,
                                "DELETE",
                                status,
                                duration,
                                null,
                                orda,
                                username
                        );

                        return response.releaseBody().then();
                    });
        });
    }

    public boolean checkEntityExists(@NonNull String endpoint) {
        return Boolean.TRUE.equals(
                withAuth(token ->
                        checkEntityExistsInternal(
                                endpoint,
                                token
                        )
                ).block()
        );
    }

    private Mono<Boolean> checkEntityExistsInternal(
            String endpoint,
            String token) {

        return Mono.defer(() -> {
            OrdaHost orda = currentOrdaHost();
            long start = System.currentTimeMillis();
            String username = keycloakConfig.getUsername();

            return webClient.get()
                    .uri(endpoint)
                    .header(
                            HttpHeaders.AUTHORIZATION,
                            "Bearer " + token
                    )
                    .exchangeToMono(response -> {
                        long duration = System.currentTimeMillis() - start;
                        int status = response.statusCode().value();

                        return response.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(body -> {
                                    if (status == 404) {
                                        logOrda(
                                                endpoint,
                                                "GET",
                                                status,
                                                duration,
                                                body,
                                                orda,
                                                username
                                        );
                                        return Mono.just(false);
                                    }

                                    if (status == 401) {
                                        logOrda(
                                                endpoint,
                                                "GET",
                                                status,
                                                duration,
                                                body,
                                                orda,
                                                username
                                        );
                                        return Mono.error(
                                                new OrdaAuthException(
                                                        "access_token is not valid or expired"
                                                )
                                        );
                                    }

                                    if (response.statusCode().isError()) {
                                        logOrda(
                                                endpoint,
                                                "GET",
                                                status,
                                                duration,
                                                body,
                                                orda,
                                                username
                                        );
                                        return Mono.error(
                                                new RuntimeException(
                                                        "OpenMetadata GET failed. status=" +
                                                        status + ", endpoint=" + endpoint +
                                                        ", body=" + body
                                                )
                                        );
                                    }

                                    logOrda(
                                            endpoint,
                                            "GET",
                                            status,
                                            duration,
                                            null,
                                            orda,
                                            username
                                    );

                                    return Mono.just(true);
                                });
                    });
        });
    }

    private void logOrda(
            String endpoint,
            String method,
            int status,
            long duration,
            String error,
            OrdaHost orda,
            String username) {

        svoiCustomLogger.logOrdaRequest(
                endpoint,
                method,
                status,
                duration,
                error,
                orda.dns(),
                orda.ip(),
                orda.port(),
                username
        );
    }

    private OrdaHost currentOrdaHost() {
        OrdaHost current = ordaHost;
        return current != null
                ? current
                : new OrdaHost("unknown", "unknown", 443);
    }

    private OrdaHost parseOrdaHost() {
        try {
            URI uri = new URI(webClientProperties.getBaseUrl());
            String dns = uri.getHost();

            int port = uri.getPort();
            if (port < 0) {
                port = "http".equalsIgnoreCase(uri.getScheme())
                        ? 80
                        : 443;
            }

            String ip = dns == null
                    ? "unknown"
                    : InetAddress.getByName(dns).getHostAddress();

            return new OrdaHost(
                    dns == null ? "unknown" : dns,
                    ip,
                    port
            );
        } catch (Exception e) {
            return new OrdaHost(
                    "unknown",
                    "unknown",
                    443
            );
        }
    }

    private record OrdaHost(
            String dns,
            String ip,
            int port) {
    }
}
