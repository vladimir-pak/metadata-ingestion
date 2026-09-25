package com.gpb.metadata.ingestion.config;

import com.gpb.metadata.ingestion.properties.IgniteClientProperties;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(IgniteClientProperties.class)
@Slf4j
public class IgniteClientConfig {

    private final IgniteClientProperties props;

    @Bean(destroyMethod = "close")
    public IgniteClient igniteClient() {

        validate();

        ClientConfiguration cfg = new ClientConfiguration()
                .setAddresses(
                        props.getAddresses().toArray(new String[0])
                )
                .setUserName(props.getUsername())
                .setUserPassword(props.getPassword())
                .setPartitionAwarenessEnabled(
                        props.isPartitionAwarenessEnabled()
                );

        configureSsl(cfg);

        log.info(
                "Starting Ignite thin client. addresses={}, " +
                "partitionAwareness={}, ssl={}",
                props.getAddresses(),
                props.isPartitionAwarenessEnabled(),
                props.getSsl().isEnabled()
        );

        IgniteClient client = Ignition.startClient(cfg);

        log.info("Ignite thin client connected");

        return client;
    }

    private void configureSsl(ClientConfiguration cfg) {

        IgniteClientProperties.Ssl ssl = props.getSsl();

        if (!ssl.isEnabled()) {
            cfg.setSslMode(SslMode.DISABLED);
            return;
        }

        cfg.setSslMode(SslMode.REQUIRED)
                .setSslProtocol(SslProtocol.TLS)
                .setSslTrustCertificateKeyStorePath(
                        ssl.getTrustStorePath()
                )
                .setSslTrustCertificateKeyStorePassword(
                        ssl.getTrustStorePassword()
                )
                .setSslTrustCertificateKeyStoreType(
                        ssl.getTrustStoreType()
                )
                .setSslKeyAlgorithm(
                        ssl.getKeyAlgorithm()
                )
                .setSslTrustAll(
                        ssl.isTrustAll()
                );

        /*
         * Client keystore нужен только для mutual TLS.
         *
         * Если server:
         *
         * ignite.ssl.client-auth=true
         *
         * клиент обязан предоставить сертификат.
         */
        if (hasText(ssl.getKeyStorePath())) {

            cfg.setSslClientCertificateKeyStorePath(
                            ssl.getKeyStorePath()
                    )
                    .setSslClientCertificateKeyStorePassword(
                            ssl.getKeyStorePassword()
                    )
                    .setSslClientCertificateKeyStoreType(
                            ssl.getKeyStoreType()
                    );
        }
    }

    private void validate() {

        if (props.getAddresses() == null
                || props.getAddresses().isEmpty()) {

            throw new IllegalStateException(
                    "ignite.client.addresses must not be empty"
            );
        }

        requireText(
                props.getUsername(),
                "ignite.client.username"
        );

        requireText(
                props.getPassword(),
                "ignite.client.password"
        );

        if (props.getSsl().isEnabled()) {

            requireText(
                    props.getSsl().getTrustStorePath(),
                    "ignite.client.ssl.trust-store-path"
            );

            requireText(
                    props.getSsl().getTrustStorePassword(),
                    "ignite.client.ssl.trust-store-password"
            );

            if (hasText(props.getSsl().getKeyStorePath())) {

                requireText(
                        props.getSsl().getKeyStorePassword(),
                        "ignite.client.ssl.key-store-password"
                );
            }
        }
    }

    private static void requireText(
            String value,
            String propertyName) {

        if (!hasText(value)) {
            throw new IllegalStateException(
                    "Required property is not set: "
                    + propertyName
            );
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}