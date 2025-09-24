package com.gpb.metadata.ingestion.config;

import java.util.Arrays;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
public class IgniteConfig {

    @Bean(name = "igniteInstance", destroyMethod = "close")
    public Ignite igniteInstance() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        
        // Основные настройки
        cfg.setIgniteInstanceName("metadata-ingestion-cache");
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setClientMode(false); // Лучше использовать false для standalone
        
        // ПРАВИЛЬНАЯ конфигурация памяти для Ignite 2.17.0
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        
        // Создаем регион памяти (правильный синтаксис)
        DataRegionConfiguration dataRegionConfig = new DataRegionConfiguration();
        dataRegionConfig.setName("Default_Region");
        dataRegionConfig.setInitialSize(100L * 1024 * 1024); // 100 MB (long)
        dataRegionConfig.setMaxSize(512L * 1024 * 1024); // 512 MB (long)
        dataRegionConfig.setPersistenceEnabled(false); // Чисто in-memory
        
        // Устанавливаем регион по умолчанию (ИСПРАВЛЕНО)
        storageCfg.setDefaultDataRegionConfiguration(dataRegionConfig);
        cfg.setDataStorageConfiguration(storageCfg);
        
        // Упрощенная настройка discovery
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        
        // Используем локальный хост (ИСПРАВЛЕНО)
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500"));
        discoverySpi.setIpFinder(ipFinder);
        discoverySpi.setJoinTimeout(3000);
        cfg.setDiscoverySpi(discoverySpi);
        
        // Настройка коммуникации
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setLocalPort(47100);
        cfg.setCommunicationSpi(commSpi);
        
        // Настройки таймаутов
        cfg.setNetworkTimeout(5000);
        
        // Отключаем лишние функции
        cfg.setMetricsLogFrequency(0);
        
        return Ignition.start(cfg);
    }
}
