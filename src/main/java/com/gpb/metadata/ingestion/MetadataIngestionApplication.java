package com.gpb.metadata.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableJpaRepositories
public class MetadataIngestionApplication {

	public static void main(String[] args) {
		SpringApplication.run(MetadataIngestionApplication.class, args);
	}

}
