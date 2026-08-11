package com.gpb.metadata.ingestion.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class OrdDbConfig {

    @Bean(name = "ordDataSource")
    public DataSource ordDataSource(
            @Value("${ord.datasource.url}") String url,
            @Value("${ord.datasource.username}") String username,
            @Value("${ord.datasource.password}") String password) {

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }

    @Bean(name = "ordJdbcTemplate")
    public JdbcTemplate ordJdbcTemplate(
            @Qualifier("ordDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
