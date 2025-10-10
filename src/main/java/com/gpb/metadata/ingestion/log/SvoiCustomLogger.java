package com.gpb.metadata.ingestion.log;


import com.gpb.metadata.ingestion.logrepository.Log;
import com.gpb.metadata.ingestion.logrepository.LogRepository;
import com.gpb.metadata.ingestion.properties.LogsDatabaseProperties;
import com.gpb.metadata.ingestion.properties.SysProperties;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
@Slf4j
public class SvoiCustomLogger {
    private final SysProperties sysProperties;
    private final LogsDatabaseProperties logsDatabaseProperties;
    private final LogRepository logRepository;
    private final SvoiJournalFactory svoiJournalFactory = new SvoiJournalFactory();
    private final SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy HH:mm:ss", Locale.getDefault());

    @Autowired
    public SvoiCustomLogger(SysProperties sysProperties,
                            LogsDatabaseProperties logsDatabaseProperties,
                            LogRepository logRepository) {
        this.sysProperties = sysProperties;
        this.logsDatabaseProperties = logsDatabaseProperties;
        this.logRepository = logRepository;
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
            log.info(StringUtils.replace(svoiJournal.toString(), "OmniPlatform", "CCP"));
        }
        if (!logsDatabaseProperties.isEnabled())
            return;
        Date created = new Date();
        try {
            created = format.parse(svoiJournal.getStart());
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
        }
        logRepository.save(new Log(created, StringUtils.replace(svoiJournal.toString(), "OmniPlatform", "CCP"), deviceEventClassID));
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
