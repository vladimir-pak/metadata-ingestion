package com.gpb.metadata.ingestion.config;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class OrdDbConfig {

    @Bean(name = "ordDataSourceProperties")
    @ConfigurationProperties("ord.datasource")
    public DataSourceProperties ordDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "ordDataSource")
    @ConfigurationProperties("ord.datasource.hikari")
    public HikariDataSource ordDataSource(
            @Qualifier("ordDataSourceProperties") DataSourceProperties properties) {

        return properties
                .initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
    }

    @Bean(name = "ordJdbcTemplate")
    public JdbcTemplate ordJdbcTemplate(
            @Qualifier("ordDataSource") DataSource dataSource) {

        return new JdbcTemplate(dataSource);
    }
}
