package com.gpb.metadata.ingestion.utils;

import java.net.InetAddress;
import java.net.URI;
import java.util.function.Function;

import org.springframework.http.HttpHeaders;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.gpb.metadata.ingestion.config.KeycloakConfig;
import com.gpb.metadata.ingestion.exceptions.OrdaAuthException;
import com.gpb.metadata.ingestion.exceptions.TokenRefreshException;
import com.gpb.metadata.ingestion.log.SvoiCustomLogger;
import com.gpb.metadata.ingestion.properties.WebClientProperties;
import com.gpb.metadata.ingestion.service.OrdaTokenProvider;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
public class OrdaClient {

    private final SvoiCustomLogger svoiCustomLogger;
    private final WebClient webClient;
    private final WebClientProperties webClientProperties;
    private final KeycloakConfig keycloakConfig;
    private final OrdaTokenProvider tokenProvider;

    /**
     * Выполняет запрос с текущим access token. При первом HTTP 401 токен
     * принудительно обновляется, после чего исходный запрос повторяется ровно
     * один раз. Повторный 401 считается критической ошибкой авторизации.
     */
    private <T> Mono<T> withAuth(Function<String, Mono<T>> request) {
        return Mono.fromCallable(tokenProvider::getToken)
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(token ->
                        Mono.defer(() -> request.apply(token))
                                .onErrorResume(OrdaAuthException.class, firstAuthError ->
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
                putRequestInternal(endpoint, requestBody, token, responseType)
        );
    }

    private <T> Mono<T> putRequestInternal(
            String endpoint,
            Object requestBody,
            String token,
            Class<T> responseType) {

        return Mono.defer(() -> {
            OrdaHost orda = parseOrdaHost();
            long start = System.currentTimeMillis();
            String username = keycloakConfig.getUsername();

            return webClient.put()
                    .uri(endpoint)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                    .bodyValue(requestBody)
                    .exchangeToMono(response -> {
                        long duration = System.currentTimeMillis() - start;

                        if (response.statusCode().isError()) {
                            return response.bodyToMono(String.class)
                                    .defaultIfEmpty("")
                                    .flatMap(err -> {
                                        svoiCustomLogger.logOrdaRequest(
                                                endpoint,
                                                "PUT",
                                                response.statusCode().value(),
                                                duration,
                                                err,
                                                orda.dns(),
                                                orda.ip(),
                                                orda.port(),
                                                username
                                        );

                                        if (response.statusCode().value() == 401) {
                                            return Mono.error(
                                                    new OrdaAuthException(
                                                            "access_token is not valid or expired"
                                                    )
                                            );
                                        }

                                        return Mono.error(new RuntimeException(err));
                                    });
                        }

                        svoiCustomLogger.logOrdaRequest(
                                endpoint,
                                "PUT",
                                response.statusCode().value(),
                                duration,
                                null,
                                orda.dns(),
                                orda.ip(),
                                orda.port(),
                                username
                        );

                        return response.bodyToMono(responseType);
                    });
        });
    }

    public Mono<Void> deleteRequest(@NonNull String endpoint) {
        return withAuth(token -> deleteRequestInternal(endpoint, token));
    }

    private Mono<Void> deleteRequestInternal(String endpoint, String token) {
        return Mono.defer(() -> {
            OrdaHost orda = parseOrdaHost();
            long start = System.currentTimeMillis();
            String username = keycloakConfig.getUsername();

            return webClient.delete()
                    .uri(uriBuilder -> uriBuilder
                            .path(endpoint)
                            .build())
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                    .exchangeToMono(response -> {
                        long duration = System.currentTimeMillis() - start;

                        if (response.statusCode().isError()) {
                            return response.bodyToMono(String.class)
                                    .defaultIfEmpty("")
                                    .flatMap(err -> {
                                        svoiCustomLogger.logOrdaRequest(
                                                endpoint,
                                                "DELETE",
                                                response.statusCode().value(),
                                                duration,
                                                err,
                                                orda.dns(),
                                                orda.ip(),
                                                orda.port(),
                                                username
                                        );

                                        if (response.statusCode().value() == 401) {
                                            return Mono.error(
                                                    new OrdaAuthException(
                                                            "access_token is not valid or expired"
                                                    )
                                            );
                                        }

                                        return Mono.error(new RuntimeException(err));
                                    });
                        }

                        svoiCustomLogger.logOrdaRequest(
                                endpoint,
                                "DELETE",
                                response.statusCode().value(),
                                duration,
                                null,
                                orda.dns(),
                                orda.ip(),
                                orda.port(),
                                username
                        );

                        return Mono.empty();
                    });
        });
    }

    public boolean checkEntityExists(@NonNull String endpoint) {
        return Boolean.TRUE.equals(
                withAuth(token -> checkEntityExistsInternal(endpoint, token)).block()
        );
    }

    private Mono<Boolean> checkEntityExistsInternal(String endpoint, String token) {
        return Mono.defer(() -> {
            OrdaHost orda = parseOrdaHost();
            long start = System.currentTimeMillis();
            String username = keycloakConfig.getUsername();

            return webClient.get()
                    .uri(endpoint)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                    .exchangeToMono(response -> {
                        long duration = System.currentTimeMillis() - start;

                        return response.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(body -> {
                                    if (response.statusCode().isError()) {
                                        svoiCustomLogger.logOrdaRequest(
                                                endpoint,
                                                "GET",
                                                response.statusCode().value(),
                                                duration,
                                                body,
                                                orda.dns(),
                                                orda.ip(),
                                                orda.port(),
                                                username
                                        );

                                        if (response.statusCode().value() == 401) {
                                            return Mono.error(
                                                    new OrdaAuthException(
                                                            "access_token is not valid or expired"
                                                    )
                                            );
                                        }

                                        return Mono.just(false);
                                    }

                                    svoiCustomLogger.logOrdaRequest(
                                            endpoint,
                                            "GET",
                                            response.statusCode().value(),
                                            duration,
                                            null,
                                            orda.dns(),
                                            orda.ip(),
                                            orda.port(),
                                            username
                                    );

                                    return Mono.just(response.statusCode().is2xxSuccessful());
                                });
                    });
        });
    }

    private record OrdaHost(String dns, String ip, int port) {}

    private OrdaHost parseOrdaHost() {
        try {
            URI uri = new URI(webClientProperties.getBaseUrl());
            String dns = uri.getHost();
            int port = uri.getPort() == -1 ? 443 : uri.getPort();
            String ip = InetAddress.getByName(dns).getHostAddress();
            return new OrdaHost(dns, ip, port);
        } catch (Exception e) {
            return new OrdaHost("unknown", "unknown", 443);
        }
    }
}
