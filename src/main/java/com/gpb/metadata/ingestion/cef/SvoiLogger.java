package com.gpb.metadata.ingestion.cef;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import com.gpb.metadata.ingestion.cef.enums.SvoiSeverityEnum;
import com.gpb.metadata.ingestion.cef.model.Log;
import com.gpb.metadata.ingestion.cef.model.SvoiJournal;
import com.gpb.metadata.ingestion.cef.properties.LogsDatabaseProperties;
import com.gpb.metadata.ingestion.cef.properties.SysProperties;
import com.gpb.metadata.ingestion.cef.repository.LogRepository;
import com.gpb.metadata.ingestion.dto.RequestBodyDto;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Класс - логгер по стандартам СВОИ (CEF)
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SvoiLogger {
    private final SysProperties sysProperties;
    private final LogsDatabaseProperties logsDatabaseProperties;
    private final LogRepository logRepository;
    private final SvoiJournalFactory svoiJournalFactory;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");

    public void sendInternal(String deviceEventClassID,
                             String name,
                             String message,
                             SvoiSeverityEnum severity) {

        SvoiJournal journal = svoiJournalFactory.getJournalSource();
        send(deviceEventClassID, name, message, severity, journal);
    }

    public void sendApiRequest(
        String endpoint, 
        String message, 
        String user,
        String ip,
        String dns,
        int port
    ) {
        SvoiJournal journal = svoiJournalFactory.getJournalSource();
        journal.setShost(dns);
        journal.setSrc(ip);
        journal.setSuser(user);
        journal.setSpt(port);
        message += " " + endpoint;
        send(
                "apiCall", 
                "API Request", 
                message, 
                SvoiSeverityEnum.ONE, 
                journal
        );
    }

    public void logApiCall(HttpServletRequest request, String message, RequestBodyDto dto) {
        try {
            SvoiJournal journal = prepareJournalFromRequest(request);

            String extendedMessage = message;
            if (dto != null && dto.getServiceName() != null) {
                extendedMessage += " serviceName: " + dto.getServiceName() + ";";
            }

            send("apiCall",
                    "Metadata Synchronization Request",
                    extendedMessage,
                    SvoiSeverityEnum.ONE,
                    journal);

        } catch (Exception e) {
            log.error("Ошибка при логировании вызова API", e);
        }
    }

    public void logOrdaRequest(String endpoint,
                               String method,
                               int status,
                               long durationMs,
                               String error,
                               String ordaDns,
                               String ordaIp,
                               int ordaPort,
                               String ordaUser) {

        try {
            SvoiJournal journal = prepareJournalBase();

            journal.setDhost(ordaDns);
            journal.setDvchost(ordaDns);
            journal.setDst(ordaIp);
            journal.setDpt(ordaPort);
            journal.setDuser(ordaUser);
            journal.setApp("https");

            String msg;
            if (error == null) {
                msg = String.format(
                        "ordaApiCall method: %s; endpoint: %s; status: %d; duration: %dms;",
                        method, endpoint, status, durationMs
                );
            } else {
                msg = String.format(
                        "ordaApiCall method: %s; endpoint: %s; status: %d; duration: %dms; error: %s;",
                        method, endpoint, status, durationMs, error
                );
            }

            send(
                    "ordaApiCall",
                    "ORD API Request",
                    msg,
                    error == null ? SvoiSeverityEnum.ONE : SvoiSeverityEnum.FIVE,
                    journal
            );

        } catch (Exception ex) {
            log.error("Ошибка при логировании запроса в ОРД", ex);
        }
    }

    public void logAuth(String ip, String username) {
        try {
            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            journal.setSrc(ip);
            journal.setShost(ip);
            journal.setSuser(username);

            String message = String.format(
                    "Authenticated user: %s; ip: %s;",
                    username != null ? username : "unknown",
                    ip
            );

            send("authSuccess", "Authentication success", message,
                    SvoiSeverityEnum.FIVE, journal);

        } catch (Exception ex) {
            log.error("Ошибка при логировании неверных учётных данных", ex);
        }
    }

    public void logBadCredentials(String ip, String username, String endpoint) {
        try {
            SvoiJournal journal = prepareJournalBase();
            journal.setSrc(ip);
            journal.setShost(ip);
            journal.setSuser(username);

            String message = String.format(
                    "authFailed invalidCredentials user: %s; endpoint: %s; ip: %s;",
                    username != null ? username : "unknown",
                    endpoint,
                    ip
            );

            send("authFailed", "Invalid Login or Password", message,
                    SvoiSeverityEnum.FIVE, journal);

        } catch (Exception ex) {
            log.error("Ошибка при логировании неверных учётных данных", ex);
        }
    }

    private void send(String deviceEventClassID,
                      String name,
                      String message,
                      SvoiSeverityEnum severity,
                      SvoiJournal journal) {

        defaultArgs(journal, deviceEventClassID, name, message, severity);

        try (MDC.MDCCloseable a = MDC.putCloseable("host", journal.getHostForSvoi());
             MDC.MDCCloseable b = MDC.putCloseable("log_type", "audit_log")) {

            log.info(journalToString(journal));
        }

        if (!logsDatabaseProperties.isEnabled()) return;

        try {
            LocalDateTime created = LocalDateTime.parse(journal.getStart(), FORMATTER);

            logRepository.save(new Log(
                    created,
                    journalToString(journal),
                    deviceEventClassID
            ));
        } catch (Exception e) {
            log.error("Ошибка при сохранении лога в БД", e);
        }
    }

    private void defaultArgs(SvoiJournal journal,
            String deviceEventClassID,
            String name,
            String message,
            SvoiSeverityEnum severity) {

        journal.setDeviceProduct(sysProperties.getName());
        journal.setDeviceVersion(sysProperties.getVersion());
        journal.setDntdom(sysProperties.getDntdom());
        journal.setDeviceEventClassID(deviceEventClassID);
        journal.setName(name);
        journal.setMessage(message);
        if (journal.getDuser() == null) {
            journal.setDuser(sysProperties.getUser());
        }
        if (journal.getSuser() == null) {
            journal.setSuser(sysProperties.getUser());
        }
        if (journal.getApp() == null) {
            journal.setApp("TCP");
        }
        journal.setDmac(getMacAddress());
        journal.setSeverity(severity);
        if (journal.getSpt() == null) {
            journal.setSpt(sysProperties.getDpt());
        }
        if (journal.getDpt() == null) {
            journal.setDpt(sysProperties.getDpt());
        }
    }

    private SvoiJournal prepareJournalFromRequest(HttpServletRequest request) {
        SvoiJournal journal = prepareJournalBase();

        journal.setSrc(request.getRemoteAddr());
        journal.setShost(request.getRemoteHost());
        journal.setSpt(request.getRemotePort());
        journal.setApp("https");

        return journal;
    }

    private SvoiJournal prepareJournalBase() {
        SvoiJournal journal = svoiJournalFactory.getJournalSource();

        HostInfo host = getLocalHostInfo();

        journal.setSrc(host.ip());
        journal.setShost(host.name());
        journal.setDhost(host.name());
        journal.setDst(host.ip());
        journal.setDvchost(host.name());
        journal.setDpt(sysProperties.getDpt());

        return journal;
    }

    private HostInfo getLocalHostInfo() {
        try {
            InetAddress inet = InetAddress.getLocalHost();
            return new HostInfo(inet.getHostName(), inet.getHostAddress());
        } catch (Exception e) {
            InetAddress inet = InetAddress.getLoopbackAddress();
            return new HostInfo(inet.getHostName(), inet.getHostAddress());
        }
    }

    private record HostInfo(String name, String ip) {
    }

    private String journalToString(SvoiJournal journal) {
        return journal.toString();
    }

    private String getMacAddress() {
        List<String> macs = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                byte[] mac = interfaces.nextElement().getHardwareAddress();
                if (mac == null) continue;
                for (byte b : mac) macs.add(String.format("%02X", b));
            }
        } catch (Exception ignored) {
        }
        return String.join(":", macs);
    }
}
