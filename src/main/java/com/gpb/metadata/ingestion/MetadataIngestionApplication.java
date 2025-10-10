package com.gpb.metadata.ingestion;

import com.gpb.metadata.ingestion.log.SvoiCustomLogger;
import com.gpb.metadata.ingestion.log.SvoiSeverityEnum;
import com.gpb.metadata.ingestion.logrepository.Log;
import com.gpb.metadata.ingestion.logrepository.LogPartitionRepository;
import com.gpb.metadata.ingestion.logrepository.LogRepository;
import com.gpb.metadata.ingestion.utils.Utils;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
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
	private final SvoiCustomLogger svoiCustomLogger;
	private final LogPartitionRepository logPartitionRepository;
	private final LogRepository logRepository;
	private final ConfigurableEnvironment configurableEnvironment;
	private static ConfigurableApplicationContext applicationContext;
	@PostConstruct
	public void startupApplication() {
		logPartitionRepository.createTodayPartition();
		svoiCustomLogger.send("startService", "Start Service", "Started service", SvoiSeverityEnum.ONE);

		checkConfigChanges();
	}
	private void checkConfigChanges() {
		String props = Utils.getSources(configurableEnvironment.getPropertySources());
		String propsHash = Utils.getHash(props, "SHA-256");
		String localHostName = getHostName();

		Log logEntity = logRepository.findLatestByType("checkConfig", localHostName);
		if (logEntity == null) {
			svoiCustomLogger.send("checkConfig", "Check Config", propsHash, SvoiSeverityEnum.ONE);
		} else {
			String prevHash = StringUtils.trim(
					StringUtils.substringBetween(logEntity.getLog(), "msg=", "deviceProcessName=")
			);
			if (!StringUtils.equals(prevHash, propsHash)) {
				svoiCustomLogger.send("checkConfig", "Check Config", propsHash, SvoiSeverityEnum.ONE);
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
		SpringApplication.run(MetadataIngestionApplication.class, args);
	}
	@PreDestroy
	public void shutdownApplication() {
		svoiCustomLogger.send("stopService", "Stop Service", "Stopped service", SvoiSeverityEnum.ONE);
	}

	public static void restart() {
		ApplicationArguments args = applicationContext.getBean(ApplicationArguments.class);
		Thread thread = new Thread(() -> {
			applicationContext.close();
			applicationContext = SpringApplication.run(MetadataIngestionApplication.class, args.getSourceArgs());
		});
		thread.setDaemon(false);
		thread.start();
	}
}
