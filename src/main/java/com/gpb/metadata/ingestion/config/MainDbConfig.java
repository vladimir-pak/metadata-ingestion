package com.gpb.metadata.ingestion.config;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class MainDbConfig {

    @Bean(name = "mainDataSourceProperties")
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties mainDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "mainDataSource")
    @Primary
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariDataSource mainDataSource(
            @Qualifier("mainDataSourceProperties") DataSourceProperties properties) {

        return properties
                .initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
    }

    @Bean(name = "jdbcTemplate")
    @Primary
    public JdbcTemplate jdbcTemplate(
            @Qualifier("mainDataSource") DataSource dataSource) {

        return new JdbcTemplate(dataSource);
    }
}
