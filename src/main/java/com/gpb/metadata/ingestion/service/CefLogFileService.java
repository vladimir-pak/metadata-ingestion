package com.gpb.metadata.ingestion.service;

import com.gpb.metadata.ingestion.properties.CefLoggingProperties;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
public class CefLogFileService {

    private static final Logger log = LoggerFactory.getLogger(CefLogFileService.class);

    private final CefLoggingProperties properties;

    private static final DateTimeFormatter FILE_TS =
            DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
    private static final DateTimeFormatter FILE_DATE =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final ZoneId ZONE = ZoneId.systemDefault();

    public Path getDailyLogPath() {
        String date = LocalDate.now(ZONE).format(FILE_DATE);
        return Paths.get(properties.getPath() + ".jdata.log." + date);
    }

    public void writeToFile(LocalDateTime created, String cefLog) {
        Path path = getDailyLogPath();
        try {
            Files.createDirectories(path.getParent());
            String line = created.format(FILE_TS) + " " + cefLog + System.lineSeparator();
            Files.writeString(path, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Ошибка при записи CEF-лога в файл {}", path, e);
        }
    }

    public void cleanupOldLogs() {
        Path dir = Paths.get(properties.getPath()).getParent();
        if (dir == null || !Files.exists(dir)) return;

        LocalDate threshold = LocalDate.now(ZONE).minusDays(properties.getRetentionDays());
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "cef.jdata.log.*")) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                if (name.length() < 24) continue;
                try {
                    String datePart = name.substring(14, 24);
                    LocalDate fileDate = LocalDate.parse(datePart, FILE_DATE);
                    if (fileDate.isBefore(threshold)) {
                        Files.deleteIfExists(p);
                        log.info("Удалён старый лог: {}", name);
                    }
                } catch (Exception ignored) {
                    log.warn("Не удалось разобрать дату из имени файла: {}", name);
                }
            }
        } catch (IOException e) {
            log.error("Ошибка при очистке старых логов", e);
        }
    }
}
