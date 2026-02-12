package com.gpb.metadata.ingestion.config;

import java.util.Arrays;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
public class IgniteConfig {

    @Value("${ignite.persistence.storagePath:/ignite-storage/db}")
    private String persistenceStoragePath;

    @Value("${ignite.persistence.walPath:/ignite-storage/wal}")
    private String persistenceWalPath;

    @Value("${ignite.persistence.walArchivePath:/ignite-storage/wal-archive}")
    private String persistenceWalArchivePath;

    @Value("${ignite.persistence.workDir:/ignite/work}")
    private String workDir;

    @Bean(name = "igniteInstance", destroyMethod = "close")
    public Ignite igniteInstance() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Основные настройки
        cfg.setIgniteInstanceName("metadata-ingestion-cache");
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setClientMode(false);
        cfg.setWorkDirectory(workDir);

        // Конфигурация хранилища
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        // Создаем регион памяти с persistence
//        DataRegionConfiguration dataRegionConfig = new DataRegionConfiguration();
//        dataRegionConfig.setName("Default_Region");
//        dataRegionConfig.setInitialSize(1L * 1024 * 1024 * 1024); // 1 GB
//        dataRegionConfig.setMaxSize(4L * 1024 * 1024 * 1024); // 4 GB
//        dataRegionConfig.setPersistenceEnabled(true); // Включаем persistence

        DataRegionConfiguration dataRegionConfig = new DataRegionConfiguration()
                .setName("Default_Region")
                .setInitialSize(256L * 1024 * 1024) // 256 MB
                .setMaxSize(512L * 1024 * 1024)     // 512 MB
                .setPersistenceEnabled(true);



        // Устанавливаем регион по умолчанию
        storageCfg.setDefaultDataRegionConfiguration(dataRegionConfig);

        // Папка для хранения данных Ignite
        storageCfg.setStoragePath(persistenceStoragePath);
        storageCfg.setWalPath(persistenceWalPath);
        storageCfg.setWalArchivePath(persistenceWalArchivePath);

        cfg.setDataStorageConfiguration(storageCfg);

        // Discovery SPI (локальный кластер)
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500"));
        discoverySpi.setIpFinder(ipFinder);
        discoverySpi.setJoinTimeout(3000);
        cfg.setDiscoverySpi(discoverySpi);

        // Communication SPI
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setLocalPort(47100);
        cfg.setCommunicationSpi(commSpi);

        cfg.setNetworkTimeout(5000);
        cfg.setMetricsLogFrequency(0);

        Ignite ignite = Ignition.start(cfg);

        // ВАЖНО: активация кластера при persistence
        ignite.cluster().state(ClusterState.ACTIVE);

        return ignite;
    }
}
