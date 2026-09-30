package com.gpb.metadata.ingestion.properties;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "ignite.client")
public class IgniteClientProperties {

    private List<String> addresses = new ArrayList<>();

    private String username;
    private String password;

    private boolean partitionAwarenessEnabled = true;

    private Ssl ssl = new Ssl();

    @Data
    public static class Ssl {

        private boolean enabled = true;

        private String trustStorePath;
        private String trustStorePassword;
        private String trustStoreType = "JKS";

        /*
         * Нужен только если на Ignite server:
         *
         * ssl.client-auth=true
         */
        private String keyStorePath;
        private String keyStorePassword;
        private String keyStoreType = "JKS";

        private String keyAlgorithm = "SunX509";

        private boolean trustAll = false;
    }
}
