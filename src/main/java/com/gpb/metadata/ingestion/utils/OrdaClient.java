package com.gpb.metadata.ingestion.utils;

import java.net.InetAddress;
import java.net.URI;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpb.metadata.ingestion.config.KeycloakConfig;
import com.gpb.metadata.ingestion.exceptions.OrdaNotFoundException;
import com.gpb.metadata.ingestion.log.SvoiCustomLogger;
import com.gpb.metadata.ingestion.properties.WebClientProperties;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class OrdaClient {
    private final SvoiCustomLogger svoiCustomLogger;

    private final WebClient webClient;
    private final WebClientProperties webClientProperties;
    private final KeycloakConfig keycloakConfig;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${ord.api.max-connections:5}")
    private Integer maxConn;

    public <T> Mono<T> putRequest(
            @NonNull String endpoint, 
            @NonNull Object requestBody, 
            @NonNull String token,
            @NonNull Class<T> responseType) {
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
    }

    public Mono<Void> deleteRequest(@NonNull String endpoint, @NonNull String token) {

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
    }

    public boolean checkEntityExists(@NonNull String endpoint, @NonNull String token) {
        OrdaHost orda = parseOrdaHost();
        long start = System.currentTimeMillis();
        String username = keycloakConfig.getUsername();

        try {
            Mono<Boolean> mono = webClient.get()
                .uri(endpoint)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .exchangeToMono(response -> {
                    long duration = System.currentTimeMillis() - start;

                    return response.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .flatMap(body -> {
                            if (response.statusCode().isError()) {
                                svoiCustomLogger.logOrdaRequest(
                                    endpoint, "GET", response.statusCode().value(), duration,
                                    body, orda.dns(), orda.ip(), orda.port(), username
                                );
                                return Mono.just(false);
                            }

                            // лог успешного GET
                            svoiCustomLogger.logOrdaRequest(
                                endpoint, "GET", response.statusCode().value(), duration,
                                null, orda.dns(), orda.ip(), orda.port(), username
                            );

                            if (response.statusCode().is2xxSuccessful()) {
                                return Mono.just(true);
                            }
                            return Mono.just(false);
                        });
                });
            return mono.block();
        } catch (RuntimeException ex) {
            throw ex;
        }
    }

    public Mono<Boolean> getIsProjectEntity(@NonNull String endpoint, @NonNull String token) {
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
                        int code = response.statusCode().value();

                        if (code == 404) {
                            svoiCustomLogger.logOrdaRequest(
                                endpoint, "GET", code, duration,
                                body, orda.dns(), orda.ip(), orda.port(), username
                            );
                            return Mono.error(new OrdaNotFoundException("Entity not found: " + endpoint));
                        }

                        if (!response.statusCode().is2xxSuccessful()) {
                            svoiCustomLogger.logOrdaRequest(
                                endpoint, "GET", code, duration,
                                body, orda.dns(), orda.ip(), orda.port(), username
                            );
                            return Mono.error(new RuntimeException("GET failed (" + code + "): " + endpoint));
                        }

                        // 2xx
                        svoiCustomLogger.logOrdaRequest(
                            endpoint, "GET", code, duration,
                            null, orda.dns(), orda.ip(), orda.port(), username
                        );

                        try {
                            JsonNode node = objectMapper.readTree(body);
                            String raw = node.path("isProjectEntity").asText(null);
                            boolean flag = raw != null && Boolean.parseBoolean(raw.trim());
                            return Mono.just(flag);
                        } catch (Exception e) {
                            // если тело не парсится — считаем НЕ проектной
                            return Mono.just(false);
                        }
                    });
            });
    }

    public Optional<String> resolveTableId(@NonNull String endpoint, @NonNull String token) {
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
                        int code = response.statusCode().value();

                        if (code == 404) {
                            svoiCustomLogger.logOrdaRequest(
                                endpoint, "GET", code, duration,
                                body, orda.dns(), orda.ip(), orda.port(), username
                            );
                            return Mono.just(Optional.<String>empty());
                        }

                        if (!response.statusCode().is2xxSuccessful()) {
                            svoiCustomLogger.logOrdaRequest(
                                endpoint, "GET", code, duration,
                                body, orda.dns(), orda.ip(), orda.port(), username
                            );
                            return Mono.error(new RuntimeException("GET failed (" + code + "): " + endpoint));
                        }

                        // 2xx
                        svoiCustomLogger.logOrdaRequest(
                            endpoint, "GET", code, duration,
                            null, orda.dns(), orda.ip(), orda.port(), username
                        );

                        try {
                            JsonNode node = objectMapper.readTree(body);
                            JsonNode idNode = node.path("id");

                            if (idNode.isMissingNode() || idNode.isNull() || idNode.asText().isBlank()) {
                                return Mono.just(Optional.<String>empty());
                            }

                            return Mono.just(Optional.of(idNode.asText()));
                        } catch (Exception e) {
                            return Mono.error(new RuntimeException("Failed to parse id from response", e));
                        }
                    });
            })
            .block();
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
