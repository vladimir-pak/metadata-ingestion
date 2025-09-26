# Metadata Ingestion Application

Микросервис для отправки метаданных в АС ОРДА на базе OpenMetadata.

## Архитектура решения

Приложение работает в связке с:
- **OpenMetadata** - приемник метаданных
- **PostgreSQL** - база данных с реплицированными метаданными из различных источников
- **Apache Ignite** - система кэширования in-memory

Репликация метаданных в PostgreSQL осуществляется сторонним микросервисом.

## Настройка Apache Ignite

### Persistence Storage
Настроено постоянное хранилище для кэша в виде локального хранения на сервере.

**Важно:** Обязателен для заполнения раздел `ignite.persistence` с указанием путей хранения кэша.

При перезапуске приложения ранее сформированный кэш будет автоматически восстановлен.

## Бизнес-логика работы приложения

1. **Формирование основного кэша**
   - Приложение извлекает метаданные из БД PostgreSQL
   - Создает in-memory кэш в Apache Ignite (если отсутствует ранее сформированный)
   - Кэш сохраняется с использованием persistence storage

2. **Создание временного кэша**
   - Формируется временный кэш, привязанный к контексту выполнения

3. **Сверка метаданных**
   - Осуществляется сравнение между основным in-memory кэшем и временным
   - Выявляются расхождения (добавления, изменения, удаления)

4. **Синхронизация с OpenMetadata**
   - Найденные расхождения отправляются в OpenMetadata:
     - `PUT` - для создания/изменения сущностей
     - `DELETE` - для удаления сущностей

5. **Обновление основного кэша**
   - In-memory кэш обновляется с учетом выполненных изменений

## Запуск приложения

```bash
java \
  --add-opens=java.base/jdk.internal.access=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
  --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
  --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
  --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.math=ALL-UNNAMED \
  --add-opens=java.sql/java.sql=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.time=ALL-UNNAMED \
  --add-opens=java.base/java.text=ALL-UNNAMED \
  --add-opens=java.management/sun.management=ALL-UNNAMED \
  --add-opens=java.desktop/java.awt.font=ALL-UNNAMED \
  -jar metadata-ingestion-0.0.2-SNAPSHOT.jar