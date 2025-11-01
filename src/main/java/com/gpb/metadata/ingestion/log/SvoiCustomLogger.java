package com.gpb.metadata.ingestion.log;


import com.gpb.metadata.ingestion.logrepository.Log;
import com.gpb.metadata.ingestion.logrepository.LogRepository;
import com.gpb.metadata.ingestion.properties.LogsDatabaseProperties;
import com.gpb.metadata.ingestion.properties.SysProperties;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@Slf4j
public class SvoiCustomLogger {
    private final SysProperties sysProperties;
    private final LogsDatabaseProperties logsDatabaseProperties;
    private final LogRepository logRepository;
    private final SvoiJournalFactory svoiJournalFactory = new SvoiJournalFactory();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");

    @Autowired
    public SvoiCustomLogger(SysProperties sysProperties,
                            LogsDatabaseProperties logsDatabaseProperties,
                            LogRepository logRepository) {
        this.sysProperties = sysProperties;
        this.logsDatabaseProperties = logsDatabaseProperties;
        this.logRepository = logRepository;
    }

    public void logApiCall(HttpServletRequest request, String message) {
        try {
            String clientIp = request.getRemoteAddr();
            String clientHost = request.getRemoteHost();
            int clientPort = request.getRemotePort();

            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            journal.setShost(clientHost);
            journal.setSrc(clientIp);
            journal.setSpt(clientPort);

            send("metadataSyncCall", "API Request", message, SvoiSeverityEnum.ONE, journal);

        } catch (Exception e) {
            log.error("Ошибка при логировании вызова API", e);
        }
    }

    public void send(String deviceEventClassID, String name, String message, SvoiSeverityEnum severity, SvoiJournal journal) {
        String localHostName;
        String localHostAddress;
        try {
            localHostName = InetAddress.getLocalHost().getHostName();
            localHostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            localHostName = InetAddress.getLoopbackAddress().getHostName();
            localHostAddress = InetAddress.getLoopbackAddress().getHostAddress();
        }

        journal.setDeviceProduct(sysProperties.getName());
        journal.setDeviceVersion(sysProperties.getVersion());
        journal.setDpt(sysProperties.getDpt());
        journal.setDntdom(sysProperties.getDntdom());
        journal.setDeviceEventClassID(deviceEventClassID);
        journal.setName(name);
        journal.setMessage(message);
        journal.setDhost(localHostName);
        journal.setDvchost(localHostName);
        journal.setDst(localHostAddress);
        journal.setDuser(sysProperties.getUser());
        journal.setSuser(sysProperties.getUser());
        journal.setApp("");
        journal.setDmac(getMacAddress());
        journal.setSeverity(severity);

        try (
                MDC.MDCCloseable hostClosable = MDC.putCloseable("host", journal.getHostForSvoi());
                MDC.MDCCloseable logTypeClosable = MDC.putCloseable("log_type", "audit_log");
        ) {
            log.info(StringUtils.replace(journal.toString(), "OmniPlatform", "ORD"));
        }

        if (!logsDatabaseProperties.isEnabled())
            return;

        try {
            LocalDateTime created = LocalDateTime.parse(journal.getStart(), formatter);
            logRepository.save(new Log(created,
                    StringUtils.replace(journal.toString(), "OmniPlatform", "ORD"),
                    deviceEventClassID));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }


    }

    public void send(String deviceEventClassID, String name, String message, SvoiSeverityEnum severity) {
        String localHostName = "";
        String localHostAddress = "";
        try {
            localHostName = InetAddress.getLocalHost().getHostName();
            localHostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            localHostName = InetAddress.getLoopbackAddress().getHostName();
            localHostAddress = InetAddress.getLoopbackAddress().getHostAddress();
        }
        SvoiJournal svoiJournal = svoiJournalFactory.getJournalSource();
        svoiJournal.setDeviceProduct(sysProperties.getName());
        svoiJournal.setDeviceVersion(sysProperties.getVersion());
        svoiJournal.setDpt(sysProperties.getDpt());
        svoiJournal.setDntdom(sysProperties.getDntdom());
        svoiJournal.setDeviceEventClassID(deviceEventClassID);
        svoiJournal.setName(name);
        svoiJournal.setMessage(message);
        svoiJournal.setDhost(localHostName);
        svoiJournal.setDvchost(localHostName);
        svoiJournal.setDst(localHostAddress);
        svoiJournal.setDuser(sysProperties.getUser());
        svoiJournal.setSuser(sysProperties.getUser());
        svoiJournal.setApp("");
        svoiJournal.setDmac(getMacAddress());
        svoiJournal.setSeverity(severity);
        try (
                MDC.MDCCloseable hostClosable = MDC.putCloseable("host", svoiJournal.getHostForSvoi());
                MDC.MDCCloseable logTypeClosable = MDC.putCloseable("log_type", "audit_log");
        ) {
            log.info(StringUtils.replace(svoiJournal.toString(), "OmniPlatform", "ORD"));
        }
        if (!logsDatabaseProperties.isEnabled())
            return;
        try {
            LocalDateTime created = LocalDateTime.parse(svoiJournal.getStart(), formatter);
            logRepository.save(new Log(created, StringUtils.replace(svoiJournal.toString(), "OmniPlatform", "ORD"), deviceEventClassID));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    private String getMacAddress() {
        List<String> addresses = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            NetworkInterface networkInterface;
            while (networkInterfaceEnumeration.hasMoreElements()) {
                networkInterface = networkInterfaceEnumeration.nextElement();
                byte[] mac = networkInterface.getHardwareAddress();
                if (mac == null)
                    return String.join(":", addresses);
                for (byte b : mac) {
                    addresses.add(String.format("%02X", b));
                }
            }
        } catch (SocketException e) {
            log.error(e.getMessage(), e);
        }
        return String.join(":", addresses);
    }
}
