package com.gpb.metadata.ingestion;

import com.gpb.metadata.ingestion.cef.SvoiLogger;
import com.gpb.metadata.ingestion.cef.enums.SvoiSeverityEnum;
import com.gpb.metadata.ingestion.cef.model.Log;
import com.gpb.metadata.ingestion.cef.repository.LogPartitionRepository;
import com.gpb.metadata.ingestion.cef.repository.LogRepository;
import com.gpb.metadata.ingestion.repository.MetadataIngestionMetricRepository;
import com.gpb.metadata.ingestion.utils.Utils;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
@RequiredArgsConstructor
@EnableJpaRepositories
@SpringBootApplication
@EnableScheduling
@EnableAsync
public class MetadataIngestionApplication {
	private final SvoiLogger svoiLogger;
	private final LogPartitionRepository logPartitionRepository;
	private final MetadataIngestionMetricRepository metadataMetricRepository;
	private final LogRepository logRepository;
	private final ConfigurableEnvironment configurableEnvironment;
	
	@PostConstruct
	public void startupApplication() {
		logPartitionRepository.createTodayPartition();
		metadataMetricRepository.createMetricPartition();
		svoiLogger.sendInternal(
				"startService", 
				"Start Service", 
				"Started service", 
				SvoiSeverityEnum.ONE
		);

		checkConfigChanges();
	}
	private void checkConfigChanges() {
		String props = Utils.getSources(configurableEnvironment.getPropertySources());
		String propsHash = Utils.getHash(props, "SHA-256");
		String localHostName = getHostName();

		Log logEntity = logRepository.findLatestByType(
				"checkConfig", 
				localHostName
		);
		if (logEntity == null) {
			svoiLogger.sendInternal(
					"checkConfig", 
					"Check Config", 
					propsHash, 
					SvoiSeverityEnum.ONE
			);
		} else {
			String prevHash = StringUtils.trim(
					StringUtils.substringBetween(
							logEntity.getLog(), 
							"msg=", 
							"deviceProcessName="
					)
			);
			if (!StringUtils.equals(prevHash, propsHash)) {
				svoiLogger.sendInternal(
						"checkConfig", 
						"Check Config", 
						propsHash, 
						SvoiSeverityEnum.ONE
				);
			}
		}
	}

	private String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return InetAddress.getLoopbackAddress().getHostName();
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(
				MetadataIngestionApplication.class, 
				args
		);
	}
	@PreDestroy
	public void shutdownApplication() {
		svoiLogger.sendInternal(
				"stopService", 
				"Stop Service", 
				"Stopped service", 
				SvoiSeverityEnum.ONE
		);
	}
}
