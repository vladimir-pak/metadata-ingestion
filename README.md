# Metadata Ingestion Service

Сервис синхронизации технических метаданных из локального PostgreSQL staging-слоя в OpenMetadata (ORD).

Сервис не извлекает метаданные напрямую из Oracle/PostgreSQL/MSSQL/SAP IQ. Он работает поверх уже подготовленных snapshot-таблиц `metadata_replication.*`, рассчитывает дельту относительно последнего подтверждённого состояния в Apache Ignite, выполняет идемпотентные PUT/DELETE в OpenMetadata и обновляет Ignite только после успешных операций.

Поддерживаемые типы источников:

- PostgreSQL / Greenplum — `POSTGRES`
- Microsoft SQL Server — `MSSQL`
- Oracle — `ORACLE`
- SAP IQ — `SAPIQ`

Основные типы метаданных:

- `DATABASE`
- `SCHEMA`
- `TABLE` — включает обычные таблицы, views и materialized views

---

## Содержание

1. [Назначение](#назначение)
2. [Архитектура](#архитектура)
3. [Основной поток обработки](#основной-поток-обработки)
4. [NORMAL delta synchronization](#normal-delta-synchronization)
5. [FULL RECONCILIATION](#full-reconciliation)
6. [Apache Ignite](#apache-ignite)
7. [Защита от массового удаления](#защита-от-массового-удаления)
8. [Staging metadata tables](#staging-metadata-tables)
9. [Маппинг в OpenMetadata](#маппинг-в-openmetadata)
10. [REST API](#rest-api)
11. [Авторизация входящего API](#авторизация-входящего-api)
12. [Авторизация в OpenMetadata](#авторизация-в-openmetadata)
13. [Метрики ingestion](#метрики-ingestion)
14. [Конфигурация](#конфигурация)
15. [Миграции БД](#миграции-бд)
16. [Логирование и аудит](#логирование-и-аудит)
17. [Отказоустойчивость и идемпотентность](#отказоустойчивость-и-идемпотентность)
18. [Очистка Ignite cache](#очистка-ignite-cache)
19. [Запуск](#запуск)
20. [Диагностика](#диагностика)
21. [Расширение сервиса](#расширение-сервиса)

---

## Назначение

Сервис решает следующие задачи:

1. Читает актуальный snapshot технических метаданных из PostgreSQL.
2. Сравнивает его с последним **успешно применённым** состоянием в Ignite.
3. Определяет:
   - новые сущности;
   - изменённые сущности;
   - переименованные / перемещённые сущности;
   - удалённые сущности.
4. Отправляет изменения в OpenMetadata.
5. Коммитит в Ignite только операции, которые логически успешно завершились в OpenMetadata.
6. При отсутствии доверенного Ignite baseline выполняет полную reconciliation с реальным состоянием OpenMetadata.
7. Защищает от потенциально ошибочного массового удаления.
8. Ведёт run/job-метрики и аудит внешних вызовов.

Ключевой принцип:

> Ignite хранит не состояние источника, а последнее состояние, которое сервис считает успешно применённым в OpenMetadata.

Это позволяет повторять незавершённые операции после рестарта или сетевой ошибки без потери дельты.

---

# Архитектура

```text
┌───────────────────────────────────────────────────────────────────────┐
│ Metadata Replication / staging                                      │
│                                                                       │
│ metadata_replication.database_metadata_*                             │
│ metadata_replication.schema_metadata_*                               │
│ metadata_replication.table_metadata_*                                │
└───────────────────────────────┬───────────────────────────────────────┘
                                │ JDBC / fingerprints + bounded full load
                                ▼
┌───────────────────────────────────────────────────────────────────────┐
│ Metadata Ingestion Service                                           │
│                                                                       │
│ MetadataCacheService                                                 │
│       │                                                               │
│       ├── PostgreSQL snapshot                                        │
│       ├── Ignite runtime baseline                                    │
│       └── delta / reconciliation                                     │
│                                                                       │
│ MetadataHandlerServiceImpl                                           │
│       │                                                               │
│       ├── DATABASE                                                    │
│       ├── SCHEMA                                                      │
│       └── TABLE                                                       │
└───────────────┬───────────────────────────────┬───────────────────────┘
                │                               │
                │ Thin Client                   │ REST + Bearer token
                ▼                               ▼
┌────────────────────────────┐     ┌────────────────────────────────────┐
│ Apache Ignite              │     │ OpenMetadata / ORD                 │
│                            │     │                                    │
│ runtime_v3_*               │     │ databaseServices                  │
│ manifest                   │     │ databases                         │
│ {EntityId -> hash,fqn}     │     │ databaseSchemas                   │
└────────────────────────────┘     │ tables                             │
                                   └────────────────────────────────────┘

                ┌─────────────────────────────────────┐
                │ Keycloak                            │
                │ access_token / refresh_token        │
                └─────────────────────────────────────┘
```

Внутренние PostgreSQL таблицы дополнительно используются для:

- Spring Security пользователей;
- JWT registry;
- ingestion metrics;
- CEF/audit logging.

---

# Основной поток обработки

Запуск выполняется через:

```http
POST /api/ingestion/start
```

Пример:

```json
{
  "serviceName": "dwh-prod",
  "serviceType": "POSTGRES",
  "async": true,
  "skipDeletionThreshold": false
}
```

`serviceType` регистронезависимый благодаря `@JsonCreator`:

```text
POSTGRES
postgres
Postgres
```

будут преобразованы в `ServiceType.POSTGRES`.

После запроса сервис:

1. Через `IngestionMetricService.createRunIfNotExecuting()` создаёт новый `runId`.
2. Берёт PostgreSQL transaction advisory lock по `serviceName`.
3. Проверяет, что для сервиса нет другого активного ingestion.
4. Создаёт jobs со статусом `QUEUE`.
5. Последовательно обрабатывает:

```text
DATABASE_UPSERT
        ↓
SCHEMA_UPSERT
        ↓
TABLE_UPSERT
        ↓
TABLE_DELETE
        ↓
SCHEMA_DELETE
        ↓
DATABASE_DELETE
```

После успешных table upsert при необходимости создаётся отдельный job:

```text
VIEW_PARSING
```

Он предназначен для другого сервиса и не блокирует следующий metadata ingestion.

Порядок удаления обратный порядку создания и соответствует зависимости сущностей OpenMetadata:

```text
TABLE → SCHEMA → DATABASE
```

---

# NORMAL delta synchronization

NORMAL режим используется, когда Ignite содержит доверенный v3 baseline и manifest имеет статус готового состояния.

Для каждого типа сущности сервис сначала читает из PostgreSQL только компактные fingerprints:

```text
EntityId
hash_data
fqn
```

Большой JSON таблиц на этом этапе не загружается.

Затем выполняется streaming scan Ignite cache.

## Определение изменений

Для одинакового `EntityId`:

### Без изменений

```text
old.fqn  == current.fqn
old.hash == current.hash
```

Никакого REST-вызова не выполняется.

### MODIFIED

```text
old.fqn  == current.fqn
old.hash != current.hash
```

Сущность попадает в PUT.

### RENAMED / MOVED

```text
EntityId одинаковый
old.fqn != current.fqn
```

Сервис выполняет:

```text
PUT new/current entity
        ↓
DELETE old FQN
        ↓
commit new state to Ignite
```

Ignite обновляется только после логически успешного завершения обеих операций.

### DELETE

Entity присутствует в Ignite baseline, но отсутствует в текущем PostgreSQL snapshot.

### NEW

После scan Ignite в source map остаются EntityId, которых не было в baseline.

## Полная metadata загружается только для изменений

После расчёта fingerprints сервис вызывает `findByIds()` только для:

```text
NEW
MODIFIED
RENAMED
```

Для `TableMetadata` это особенно важно, поскольку `data` может содержать большой JSON с колонками и constraints.

Repository выполняет bounded load пакетами по 1000 EntityId.

---

# FULL RECONCILIATION

RECONCILIATION используется, если отсутствует доверенный Ignite baseline.

Типичные причины:

- первый запуск сервиса;
- cache был очищен;
- manifest отсутствует;
- manifest указывает на отсутствующий physical cache;
- найден недоверенный/частичный v3 cache после аварийного завершения;
- rollout после изменения cache generation.

## Почему одного существования Ignite cache недостаточно

Пустой cache может быть корректным состоянием, а непустой cache может оказаться частично записанным после падения процесса.

Поэтому используется отдельный manifest cache:

```text
metadata_ingestion_cache_manifest_v1
```

Manifest записывается **последним**, только после успешного завершения reconciliation.

Без доверенного manifest runtime cache не считается baseline.

## Reconciliation flow

```text
PostgreSQL source fingerprints
        ↓
OpenMetadata snapshot by service
        ↓
source FQN vs managed OMD FQN
        ↓
find orphans
        ↓
delete-threshold validation
        ↓
re-apply current source metadata in bounded batches
        ↓
delete managed orphans
        ↓
if PUT + DELETE phases successful
        ↓
mark Ignite manifest READY
```

В памяти на весь reconciliation остаются только compact fingerprints.

Полные сущности загружаются пакетами:

```yaml
metadata:
  cache:
    reconciliation-batch-size: 5000
```

Это защищает JVM от пикового потребления памяти при больших `TableMetadata.data`.

## Project entities

При работе с tables учитывается `isProjectEntity` из OpenMetadata snapshot.

Проектные сущности:

- не перезаписываются обычным TABLE PUT;
- не удаляются TABLE DELETE;
- исключаются из managed orphan deletion.

---

# Apache Ignite

Сервис использует Apache Ignite Thin Client.

В Ignite хранится компактное состояние:

```java
EntityId -> MetadataCacheEntry {
    hashData,
    fqn
}
```

Полные DTO/JSON в Ignite не хранятся.

## Runtime cache v3

Имя строится в виде:

```text
runtime_v3_<SERVICE_TYPE>_<OBJECT_TYPE>_<NORMALIZED_SERVICE>_<HASH>
```

Например:

```text
runtime_v3_POSTGRES_TABLE_dwh-prod_a1b2c3d4e5f67890
```

`serviceName` нормализуется, а короткий SHA-256 hash устраняет риск коллизий после нормализации или обрезания имени.

## Manifest key

```text
3|<SERVICE_TYPE>|<OBJECT_TYPE>|<SERVICE_NAME>
```

Пример:

```text
3|POSTGRES|TABLE|dwh-prod
```

## Cache lifecycle

Порядок выбора режима:

```text
READY v3 manifest + physical v3 cache
        → NORMAL

иначе legacy v2 exists + migration enabled
        → migrate v2 → v3 → NORMAL

иначе
        → clear/create v3 → RECONCILIATION
```

## Legacy v2 migration

Настройки:

```yaml
metadata:
  cache:
    migrate-legacy-v2: false
    destroy-legacy-after-migration: false
```

`migrate-legacy-v2` следует использовать как временный rollout switch.

Рекомендуемый порядок:

1. Включить migration.
2. Дождаться появления READY v3 manifest для активных сервисов.
3. Отключить migration.
4. После стабилизации при необходимости удалить v2 caches.

---

# Защита от массового удаления

Для NORMAL delta и FULL RECONCILIATION действует safety threshold:

```yaml
ord:
  delete:
    threshold: 70
```

Процент считается как:

```text
deletePercent = deleteCount / baselineCount * 100
```

Если:

```text
deletePercent > threshold
```

бросается `DeleteThresholdExceededException`.

Пример:

```text
baseline = 10000
planned deletes = 8000
threshold = 70%

80% > 70% → ingestion останавливается
```

Это защищает от ситуаций, когда upstream snapshot случайно оказался неполным.

## Временный bypass для reconciliation

REST request поддерживает:

```json
{
  "skipDeletionThreshold": true
}
```

Параметр предназначен для контролируемого запуска, когда массовое удаление ожидаемо.

Важно:

> `skipDeletionThreshold=true` не отключает DELETE. Он только пропускает проверку `validateReconciliationDeleteThreshold` в reconciliation flow.

Использовать этот параметр следует только при подтверждённом корректном source snapshot.

В обычном режиме рекомендуется:

```json
"skipDeletionThreshold": false
```

---

# Staging metadata tables

Источник данных задаётся через `metadata.tables.sources`.

```yaml
metadata:
  tables:
    sources:
      postgres:
        database: metadata_replication.database_metadata_postgres
        schema: metadata_replication.schema_metadata_postgres
        table: metadata_replication.table_metadata_postgres

      mssql:
        database: metadata_replication.database_metadata_mssql
        schema: metadata_replication.schema_metadata_mssql
        table: metadata_replication.table_metadata_mssql

      oracle:
        database: metadata_replication.database_metadata_oracle
        schema: metadata_replication.schema_metadata_oracle
        table: metadata_replication.table_metadata_oracle

      sapiq:
        database: metadata_replication.database_metadata_sapiq
        schema: metadata_replication.schema_metadata_sapiq
        table: metadata_replication.table_metadata_sapiq
```

## DATABASE

Ожидаемые поля:

```text
id
fqn
service_name
name
hash_data
created_at
```

Внутренний `EntityId`:

```text
(id, service_name)
```

## SCHEMA

Ожидаемые поля:

```text
id
parent_fqn
fqn
db_name
name
service_name
hash_data
created_at
```

Внутренний `EntityId`:

```text
(id, parent_fqn)
```

Пример:

```text
service_name = pg-prod
 db_name     = dwh
 parent_fqn  = pg-prod.dwh
 fqn         = pg-prod.dwh.public
```

## TABLE

Ожидаемые поля:

```text
id
parent_fqn
fqn
db_name
schema_name
description
name
service_name
data
hash_data
created_at
```

`data` содержит JSON, который десериализуется в `TableData`.

Типовая структура:

```json
{
  "tableType": "REGULAR",
  "viewDefinition": null,
  "columns": [
    {
      "name": "id",
      "dataType": "BIGINT",
      "dataTypeDisplay": "bigint",
      "dataLength": "0",
      "precision": "0",
      "scale": "0",
      "constraint": "NOT_NULL",
      "ordinalPosition": 1,
      "description": null
    }
  ],
  "tableConstraints": [
    {
      "columns": ["id"],
      "constraintType": "PRIMARY_KEY"
    }
  ]
}
```

Внутренний `EntityId`:

```text
(id, parent_fqn)
```

## Требование уникальности EntityId

Внутри одного source snapshot один `EntityId` должен соответствовать ровно одной сущности.

Duplicate `EntityId` считается ошибкой source/staging data и приводит к исключению, а не к silent overwrite.

---

# Маппинг в OpenMetadata

`MapperDto` преобразует внутренние модели в API DTO OpenMetadata.

## Database

```text
name        <- meta.name
displayName <- meta.name
service     <- meta.serviceName
```

## Schema

```text
name        <- meta.name
displayName <- meta.name
database    <- meta.parentFqn
```

Если `dbName` содержит точку, FQN строится с quoted database name:

```text
service."db.with.dot"
```

## Table

Формируются:

```text
name
displayName
tableType
isProjectEntity=false
viewDefinition
databaseSchema
description
columns
tableConstraints
```

`databaseSchema` строится как:

```text
<serviceName>.<dbName>.<schemaName>
```

Если DB/schema содержит точку, соответствующий компонент заключается в двойные кавычки.

## Column types

Типы колонок нормализуются через `ColumnTypeMapperService` с учётом СУБД:

- PostgreSQL
- MSSQL
- Oracle
- SAP IQ

Для `ARRAY` дополнительно определяется `arrayDataType`, когда исходный тип имеет вид:

```text
integer[]
text[]
...
```

## Constraints

Перед отправкой constraints фильтруются через `ConstraintType.isSupported()`.

Для `FOREIGN_KEY` constraint также требуется наличие `referredColumns`.

Неподдерживаемые constraints пропускаются и логируются на `DEBUG`.

---

# REST API

Базовый порт по умолчанию:

```text
9900
```

## Запуск ingestion

```http
POST /api/ingestion/start
Content-Type: application/json
Authorization: Basic ...
```

или:

```http
Authorization: Bearer <jwt>
```

Body:

```json
{
  "serviceName": "dwh-prod",
  "serviceType": "POSTGRES",
  "async": true,
  "skipDeletionThreshold": false
}
```

Поля:

| Поле | Тип | Обязательное | Описание |
|---|---|---:|---|
| `serviceName` | string | да | Имя DatabaseService / logical source |
| `serviceType` | enum | да | `POSTGRES`, `MSSQL`, `ORACLE`, `SAPIQ` |
| `async` | boolean | нет | Запуск через `@Async` |
| `skipDeletionThreshold` | boolean | нет | Bypass reconciliation delete threshold |

Пример synchronous запуска:

```bash
curl -X POST 'http://localhost:9900/api/ingestion/start' \
  -u admin:password \
  -H 'Content-Type: application/json' \
  -d '{
        "serviceName":"dwh-prod",
        "serviceType":"POSTGRES",
        "async":false,
        "skipDeletionThreshold":false
      }'
```

Пример Bearer:

```bash
curl -X POST 'http://localhost:9900/api/ingestion/start' \
  -H 'Authorization: Bearer <JWT>' \
  -H 'Content-Type: application/json' \
  -d '{
        "serviceName":"dwh-prod",
        "serviceType":"POSTGRES",
        "async":true
      }'
```

## Очистка cache

```http
DELETE /api/ingestion/clean
```

Body:

```json
{
  "serviceName": "dwh-prod",
  "serviceType": "POSTGRES"
}
```

Очистка удаляет:

- v3 runtime cache;
- manifest entry;
- соответствующий legacy v2 cache.

Следующий ingestion автоматически перейдёт в FULL RECONCILIATION.

## Получение JWT

```http
POST /api/token/create
Content-Type: application/json
```

```json
{
  "secret": "<jwt.secret>",
  "service": "dwh-prod"
}
```

В registry допускается только один активный токен на service.

При попытке создать второй активный токен возвращается `409 Conflict`.

При неверном secret возвращается `401 Unauthorized`.

## Отзыв JWT

```http
DELETE /api/token/revoke/{service}
```

Пример:

```bash
curl -X DELETE \
  'http://localhost:9900/api/token/revoke/dwh-prod'
```

---

# Авторизация входящего API

`/api/ingestion/**` поддерживает два альтернативных механизма:

```text
HTTP Basic
    OR
Bearer JWT
```

## Basic

Пользователи хранятся в:

```text
metadata_ingestion.users
```

Пароли — BCrypt.

`CustomUserDetailsService` создаёт Spring Security principal с ролью `USER`.

## Bearer JWT

JWT:

- подписывается HS256;
- содержит `subject = metadata.ingestion-spring`;
- содержит `jti`;
- содержит `service` claim;
- имеет `issuedAt` и `expiration`;
- дополнительно проверяется по registry в PostgreSQL.

Таблица:

```text
metadata_ingestion.jwt_token_registry
```

Bearer authentication работает как альтернативный способ Spring Security authentication, а не как отдельный дополнительный обязательный filter.

Логика должна быть:

```text
Bearer header есть
        → validate JWT
        → установить Authentication в SecurityContext

Bearer header отсутствует / используется Basic
        → JWT filter пропускает запрос
        → BasicAuthenticationFilter выполняет Basic auth
```

Невалидный явно переданный Bearer token возвращает `401`.

---

# Авторизация в OpenMetadata

Входящая авторизация сервиса и авторизация самого сервиса в OpenMetadata — разные механизмы.

Для OpenMetadata используется Keycloak.

Конфигурация:

```yaml
keycloak:
  server-url: https://auth.example.com/realms/myrealm/protocol/openid-connect/token
  client-id: my-client
  client-secret: my-secret
  username: service-user
  password: service-pass
```

`KeycloakAuthService`:

1. Получает access token через password grant.
2. Хранит текущий token в памяти.
3. Проверяет expiration.
4. Использует refresh token при необходимости.
5. При неудачном refresh получает новый token через login.

`OrdaTokenProvider` дополнительно синхронизирует forced refresh после HTTP 401.

При нескольких параллельных REST-вызовах только один поток выполняет refresh, остальные переиспользуют уже обновлённый token.

## Retry после 401

`OrdaClient` выполняет:

```text
request with current token
        ↓
HTTP 401 ?
  no  → return result
  yes
        ↓
refresh token once
        ↓
retry exactly once
        ↓
second 401 → TokenRefreshException
```

`TokenRefreshException` считается critical error и прерывает текущий ingestion job.

---

# Метрики ingestion

Основная таблица:

```text
public.metadata_ingestion
```

Таблица partitioned by:

```text
log_dttm
```

Статусы:

```text
QUEUE
RUNNING
DONE
FAILED
SKIPPED
```

Jobs:

```text
DATABASE_UPSERT
SCHEMA_UPSERT
TABLE_UPSERT
TABLE_DELETE
SCHEMA_DELETE
DATABASE_DELETE
VIEW_PARSING
```

Основные поля:

```text
run_id
job_name
appname
status
service_name
success_count
error_count
start_dttm
end_dttm
log_dttm
```

## Защита от параллельного запуска

Перед созданием run выполняется:

```sql
pg_advisory_xact_lock(
    hashtext('metadata-ingestion'),
    hashtext(service_name)
)
```

После получения lock проверяется наличие активных jobs со статусом:

```text
QUEUE
RUNNING
```

`VIEW_PARSING` намеренно исключён из этой проверки, поскольку выполняется другим сервисом.

Таким образом для одного `serviceName` одновременно допускается только один основной ingestion.

## Ошибка job

Если stage падает:

```text
current RUNNING → FAILED
remaining QUEUE → SKIPPED
```

---

# Конфигурация

Ниже приведены основные параметры текущего `application.yaml`.

## Server

```yaml
server:
  port: 9900
  tomcat:
    max-connections: 100
    accept-count: 100
    threads:
      max: 50
```

## Main PostgreSQL datasource

```yaml
spring:
  datasource:
    url: ${DB_URL:jdbc:postgresql://localhost:5432/meta_base}
    username: ${DB_USER:postgres}
    password: ${DB_PASSWORD:...}
    driverClassName: org.postgresql.Driver
    hikari:
      maximum-pool-size: 20
      connection-timeout: 30000
      idle-timeout: 600000
      max-lifetime: 1800000
      pool-name: TargetDBPool
```

Используется для:

- staging metadata;
- Ignite/cache synchronization source;
- users;
- JWT registry;
- ingestion metrics;
- Flyway migrations.

## ORD PostgreSQL datasource

```yaml
ord:
  datasource:
    url: ${DB_URL:jdbc:postgresql://localhost:5432/meta_base}
    username: ${DB_USER:postgres}
    password: ${DB_PASSWORD:...}
```

Создаётся отдельный:

```text
ordDataSource
ordJdbcTemplate
```

## OpenMetadata REST

```yaml
ord:
  api:
    baseUrl: http://localhost:8585/api/v1
    endpoints:
      database: /databases
      schema: /databaseSchemas
      table: /tables
    concurrency: 10
```

`ord.api.concurrency` ограничивает количество параллельных Reactor REST operations.

## Delete threshold

```yaml
ord:
  delete:
    threshold: 70
```

Допустимый диапазон:

```text
0..100
```

Некорректное значение приводит к ошибке при старте bean.

## Ignite

```yaml
ignite:
  client:
    addresses:
      - ${IGNITE_ADDRESS_1:ignite01:10800}
      - ${IGNITE_ADDRESS_2:ignite02:10800}

    username: ${IGNITE_USERNAME}
    password: ${IGNITE_PASSWORD}

    partition-awareness-enabled: true

    ssl:
      enabled: true
      trust-store-path: ${IGNITE_TRUSTSTORE:/opt/app/certs/ignite-truststore.jks}
      trust-store-password: ${IGNITE_TRUSTSTORE_PASSWORD}
      trust-store-type: JKS

      key-store-path: ${IGNITE_KEYSTORE:/opt/app/certs/metadata-ingestion.jks}
      key-store-password: ${IGNITE_KEYSTORE_PASSWORD}
      key-store-type: JKS

      key-algorithm: SunX509
      trust-all: false
```

Client keystore необходим только при server-side mutual TLS / client-auth.

В текущей конфигурации store type — `JKS`.

## Cache tuning

```yaml
metadata:
  cache:
    scan-page-size: 4096
    commit-batch-size: 2000
    backups: 1
    reconciliation-batch-size: 5000
    migrate-legacy-v2: false
    destroy-legacy-after-migration: false
```

Назначение:

| Параметр | Назначение |
|---|---|
| `scan-page-size` | page size Ignite `ScanQuery` |
| `commit-batch-size` | размер `putAll/removeAll` |
| `backups` | число backup copies runtime PARTITIONED cache |
| `reconciliation-batch-size` | число full metadata objects в одном reconciliation batch |
| `migrate-legacy-v2` | временно разрешить v2→v3 migration |
| `destroy-legacy-after-migration` | удалить v2 после успешной migration |

## JWT

```yaml
jwt:
  secret: ${JWT_SECRET:...}
  expirationHours: 24000
```

В production secret необходимо передавать через secret-management/environment и не хранить в Git.

## Management endpoints

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,info,prometheus
```

Доступны Spring Boot Actuator endpoints для health/metrics/Prometheus.

---

# Миграции БД

Flyway включён:

```yaml
spring:
  flyway:
    enabled: true
    locations: classpath:db/migration
```

## V1 — users

Создаёт:

```text
metadata_ingestion.users
```

с полями:

```text
id
username
password
enabled
```

## V2 — ingestion metrics

Создаёт partitioned table:

```text
public.metadata_ingestion
```

и default partition.

## V3 — JWT registry

Создаёт:

```text
metadata_ingestion.jwt_token_registry
```

с индексами по expiration и active service.

---

# Логирование и аудит

Приложение использует обычный application log и CEF/SVOI audit.

## Application log

```yaml
logging:
  file:
    name: logs/metadata-ingestion.log
```

Rolling policy:

```yaml
max-file-size: 50MB
max-history: 7
```

## CEF

CEF события используются для:

- service startup/shutdown;
- API calls через `@SvoiApiLog`;
- JWT generate/revoke/auth failures;
- OpenMetadata HTTP requests;
- проверки изменения конфигурации.

При старте приложения вычисляется SHA-256 hash effective configuration. При изменении hash создаётся audit event `checkConfig`.

## Audit DB cleanup

```yaml
clean-database-logs:
  task-cleaner-schedule: 0 0 1 * * ?
  clean-period: 3
```

---

# Отказоустойчивость и идемпотентность

## Success-only Ignite commit

Ignite изменяется только после успешной операции OpenMetadata.

### PUT

```text
OMD PUT success
    → Ignite putAll

OMD PUT failed
    → Ignite unchanged
```

### DELETE

```text
OMD DELETE success
    → Ignite removeAll

OMD DELETE failed
    → Ignite unchanged
```

На следующем запуске незавершённая операция снова попадёт в delta.

## HTTP 404 на DELETE

DELETE реализован идемпотентно.

Если OpenMetadata возвращает:

```text
404 Not Found
```

это означает, что требуемое конечное состояние уже достигнуто.

Операция считается логически успешной и соответствующий Ignite entry может быть удалён.

Это важно для сценария:

```text
OMD DELETE succeeded
        ↓
process crashed before Ignite commit
        ↓
next run retries DELETE
        ↓
OMD returns 404
        ↓
logical success
        ↓
Ignite commit completed
```

## Rename

Rename считается завершённым только после:

```text
PUT new FQN
+
DELETE old FQN
```

После этого новый `hash/fqn` коммитится в Ignite.

## Crash during reconciliation

Manifest записывается только в конце.

Если процесс падает до `markReady()`:

```text
physical v3 cache may contain partial state
manifest is absent
        ↓
next run does not trust cache
        ↓
cache cleared
        ↓
FULL RECONCILIATION repeats
```

---

# Очистка Ignite cache

Endpoint:

```http
DELETE /api/ingestion/clean
```

использует `CacheService.cleanCache()` для всех трёх типов:

```text
DATABASE
SCHEMA
TABLE
```

Очистка является logical reset, а не просто удалением данных cache.

Удаляется и manifest, поэтому следующий запуск знает, что baseline отсутствует, и не пытается вычислять обычную delta относительно неизвестного состояния.

---

# Запуск

## Требуемые внешние компоненты

Для работы сервиса необходимы:

- Java runtime для Spring Boot приложения;
- PostgreSQL;
- Apache Ignite с Thin Client endpoint;
- OpenMetadata / ORD REST API;
- Keycloak token endpoint;
- подготовленные `metadata_replication.*` staging tables.

## Основные environment variables

Пример:

```bash
export DB_URL='jdbc:postgresql://postgres:5432/meta_base'
export DB_USER='metadata_ingestion'
export DB_PASSWORD='***'

export IGNITE_ADDRESS_1='ignite01:10800'
export IGNITE_ADDRESS_2='ignite02:10800'
export IGNITE_USERNAME='metadata-ingestion'
export IGNITE_PASSWORD='***'

export IGNITE_TRUSTSTORE='/opt/app/certs/ignite-truststore.jks'
export IGNITE_TRUSTSTORE_PASSWORD='***'
export IGNITE_KEYSTORE='/opt/app/certs/metadata-ingestion.jks'
export IGNITE_KEYSTORE_PASSWORD='***'
```

Keycloak/JWT secrets также рекомендуется передавать через deployment secret store.

## Startup checks

На старте выполняются:

- Flyway migrations;
- создание audit log partition;
- создание monthly ingestion metric partition;
- Ignite Thin Client validation/connection;
- configuration hash audit;
- создание startup audit event.

Если обязательные Ignite SSL/auth properties отсутствуют, приложение завершается с ошибкой конфигурации.

---

# Диагностика

## `Potential false mass deletion detected`

Причина:

```text
planned deletes / baseline > ord.delete.threshold
```

Проверить:

1. Полноту staging snapshot.
2. `serviceName`.
3. Правильность `metadata.tables.sources.*`.
4. Состояние Ignite baseline.
5. OpenMetadata snapshot при reconciliation.

Только если массовое удаление действительно ожидаемо, можно запустить reconciliation с:

```json
"skipDeletionThreshold": true
```

## После очистки cache начался FULL RECONCILIATION

Это ожидаемое поведение.

`cleanCache` намеренно удаляет manifest, поскольку после очистки нельзя доверять baseline.

## Ignite SSL errors

Проверить:

```text
trust-store-path
trust-store-password
trust-store-type
key-store-path
key-store-password
key-store-type
```

Для JKS:

```yaml
trust-store-type: JKS
key-store-type: JKS
```

Для mutual TLS client keystore должен содержать private key + certificate chain.

## OpenMetadata возвращает 401

`OrdaClient` автоматически выполняет один forced token refresh и один retry.

Если второй запрос также возвращает 401, ingestion считается критически ошибочным.

Проверить:

- Keycloak credentials;
- client-id/client-secret;
- access rights service user;
- OpenMetadata auth configuration;
- clock synchronization.

## OpenMetadata возвращает `database must not be null`

Для SCHEMA проверить, что при чтении staging metadata заполнено:

```text
SchemaMetadata.parentFqn
```

и `MapperDto` формирует:

```json
{
  "database": "<service>.<database>"
}
```

Заполненная колонка `parent_fqn` в PostgreSQL должна быть перенесена в Java model repository mapper.

## Bearer JWT даёт 401, а Basic работает

Проверить текущую security chain:

1. JWT filter должен обслуживать `/api/ingestion/**`.
2. При валидном JWT filter должен создать `Authentication` и записать его в `SecurityContext`.
3. Отсутствие Bearer header не должно отклонять запрос — Basic должен получить возможность аутентифицировать его.
4. JWT filter должен быть зарегистрирован внутри Spring Security chain до `BasicAuthenticationFilter`, а не выполняться дважды как servlet filter.

## Duplicate EntityId

Ошибка означает, что staging snapshot содержит две сущности с одинаковым logical ID + parent FQN.

Такую ошибку не следует скрывать `putIfAbsent`/last-write-wins: необходимо исправить upstream replication или идентификаторы.

---

# Расширение сервиса

## Добавление новой СУБД

Минимально требуется:

1. Добавить значение в `ServiceType`.
2. Добавить таблицы в:

```yaml
metadata.tables.sources.<new-type>
```

3. Добавить mapping column types в `ColumnTypeMapperService` / соответствующий mapper enum.
4. Убедиться, что staging tables соответствуют общему контракту DATABASE/SCHEMA/TABLE.
5. При необходимости расширить OpenMetadata DTO normalization.

Cache/reconciliation infrastructure от конкретной СУБД не зависит.

## Добавление нового типа metadata object

Потребуются изменения в:

- `DbObjectType`;
- staging configuration;
- repository;
- cache service;
- handler order;
- OpenMetadata mapping;
- metrics jobs при необходимости.

## Производительность

Основные решения, уже используемые для больших объёмов:

- fingerprints вместо full metadata для delta detection;
- streaming `ScanQuery` Ignite;
- bounded `putAll/removeAll`;
- bounded full-load SQL batches;
- bounded reconciliation batches;
- compact Ignite entries `{hash, fqn}`;
- `flatMap(..., concurrency)` для REST;
- HikariCP;
- отдельный manifest вместо полного cache inspection;
- full table JSON загружается только для реально изменившихся сущностей.

При настройке больших инсталляций в первую очередь подбираются:

```text
ord.api.concurrency
metadata.cache.scan-page-size
metadata.cache.commit-batch-size
metadata.cache.reconciliation-batch-size
Hikari maximum-pool-size
```

---

# Краткая эксплуатационная схема

```text
1. Upstream replication обновляет metadata_replication.*

2. POST /api/ingestion/start

3. Service acquires advisory lock

4. For DATABASE / SCHEMA / TABLE:
      PostgreSQL fingerprints
              +
      trusted Ignite baseline
              ↓
      NORMAL delta
          or
      FULL RECONCILIATION

5. PUT current state to OpenMetadata

6. DELETE obsolete state from OpenMetadata

7. Commit only successful operations to Ignite

8. If reconciliation fully successful:
      mark manifest READY

9. Update ingestion metrics

10. If table upserts occurred:
      create VIEW_PARSING job
```

---

# Безопасность

Для production deployment рекомендуется:

- не хранить DB/Keycloak/JWT/Ignite passwords в `application.yaml`;
- передавать secrets через environment / Vault / Kubernetes Secret;
- использовать TLS к Ignite;
- использовать HTTPS до Keycloak/OpenMetadata и перед reverse proxy;
- ограничить сетевой доступ к `/api/token/**`;
- регулярно отзывать неиспользуемые JWT;
- не использовать `skipDeletionThreshold=true` как постоянную настройку;
- контролировать `/actuator` на уровне reverse proxy/security policy;
- заменить bootstrap Basic user/password в production migration/setup процессе.

---

# Основные классы

| Класс | Назначение |
|---|---|
| `CacheController` | REST API запуска ingestion и очистки cache |
| `MetadataHandlerServiceImpl` | orchestration DATABASE/SCHEMA/TABLE PUT/DELETE/reconciliation |
| `AbstractMetadataCacheService` | NORMAL delta, threshold, success-only Ignite commit |
| `MetadataCacheCoordinator` | v3 cache lifecycle, manifest, legacy migration |
| `DatabaseMetadataCacheRepository` | database fingerprints/full load |
| `SchemaMetadataCacheRepository` | schema fingerprints/full load |
| `TableMetadataCacheRepository` | table fingerprints/full JSON load |
| `OpenMetadataSnapshotRepository` | snapshot фактического состояния OpenMetadata |
| `MapperDto` | mapping internal metadata → OpenMetadata DTO |
| `ColumnTypeMapperService` | vendor-specific type normalization |
| `OrdaClient` | reactive REST client с token refresh/retry |
| `KeycloakAuthService` | access/refresh token lifecycle |
| `OrdaTokenProvider` | concurrency-safe forced refresh after 401 |
| `JwtAuthFilter` | incoming Bearer JWT authentication |
| `JwtUtil` | JWT generate/validate/revoke |
| `IngestionMetricService` | run/job lifecycle |
| `MetadataIngestionMetricRepository` | PostgreSQL metrics, advisory lock, partitions |
| `CacheService` | safe logical reset of runtime caches |

---

## Итог

Сервис построен вокруг двух гарантий:

1. **Идемпотентность:** повторный запуск должен безопасно довести OpenMetadata и Ignite до согласованного состояния.
2. **Безопасность baseline:** Ignite считается доверенным только после подтверждённого успешного применения состояния в OpenMetadata.

За счёт этого обычные запуски работают как быстрая fingerprint delta, а первый запуск, потеря cache или аварийный partial state автоматически переходят в безопасную full reconciliation.
