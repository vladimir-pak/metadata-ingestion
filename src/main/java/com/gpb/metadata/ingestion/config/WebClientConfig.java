package com.gpb.metadata.ingestion.config;

import com.gpb.metadata.ingestion.properties.WebClientProperties;
import com.gpb.metadata.ingestion.rest.jwt.JwtTokenProvider;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;

@Configuration
@EnableConfigurationProperties(WebClientProperties.class)
public class WebClientConfig {

    private final WebClientProperties webClientProperties;
    private final ResourceLoader resourceLoader;
    
    public WebClientConfig(WebClientProperties webClientProperties, ResourceLoader resourceLoader) {
        this.webClientProperties = webClientProperties;
        this.resourceLoader = resourceLoader;
    }

    @Bean
    public WebClient webClient(WebClient.Builder webClientBuilder, JwtTokenProvider tokenProvider) throws Exception {
        HttpClient httpClient = createHttpClient();
        
        return webClientBuilder
                .baseUrl(webClientProperties.getBaseUrl())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }

    private HttpClient createHttpClient() throws Exception {
        HttpClient httpClient = HttpClient.create();
        
        if (!webClientProperties.getSsl().isEnabled() || 
            !webClientProperties.getBaseUrl().startsWith("https")) {
            return httpClient;
        }
        
        return httpClient.secure(sslContextSpec -> {
            try {
                SslContext sslContext = createSslContext();
                sslContextSpec.sslContext(sslContext);
            } catch (Exception e) {
                throw new RuntimeException("Failed to configure SSL context", e);
            }
        });
    }

    private SslContext createSslContext() throws Exception {
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();

        if (webClientProperties.getSsl().isInsecureTrustManager()) {
            return sslContextBuilder
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
        }

        if (webClientProperties.getSsl().getTrustStorePath() != null) {
            TrustManagerFactory trustManagerFactory = createTrustManagerFactory();
            sslContextBuilder.trustManager(trustManagerFactory);
        }

        if (webClientProperties.getSsl().getKeyStorePath() != null) {
            KeyManagerFactory keyManagerFactory = createKeyManagerFactory();
            sslContextBuilder.keyManager(keyManagerFactory);
        }

        return sslContextBuilder.build();
    }

    private KeyManagerFactory createKeyManagerFactory() throws Exception {
        String keyStorePath = webClientProperties.getSsl().getKeyStorePath();
        String keyStorePassword = webClientProperties.getSsl().getKeyStorePassword();
        String keyStoreType = webClientProperties.getSsl().getKeyStoreType();
        String keyPassword = webClientProperties.getSsl().getKeyPassword();

        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        try (InputStream keyStoreStream = getResourceStream(keyStorePath)) {
            keyStore.load(keyStoreStream, keyStorePassword.toCharArray());
        }

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyPassword != null ? keyPassword.toCharArray() : keyStorePassword.toCharArray());
        return keyManagerFactory;
    }

    private TrustManagerFactory createTrustManagerFactory() throws Exception {
        String trustStorePath = webClientProperties.getSsl().getTrustStorePath();
        String trustStorePassword = webClientProperties.getSsl().getTrustStorePassword();

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream trustStoreStream = getResourceStream(trustStorePath)) {
            trustStore.load(trustStoreStream, 
                trustStorePassword != null ? trustStorePassword.toCharArray() : null);
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        return trustManagerFactory;
    }

    private InputStream getResourceStream(String resourcePath) throws IOException {
        Resource resource = resourceLoader.getResource(resourcePath);
        if (!resource.exists()) {
            throw new IOException("Resource not found: " + resourcePath);
        }
        return resource.getInputStream();
    }
}