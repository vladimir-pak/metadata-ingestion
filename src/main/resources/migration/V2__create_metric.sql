CREATE TABLE IF NOT EXISTS public.metadata_ingestion_metric (
    id           SERIAL NOT NULL,
    service_name VARCHAR(50) NOT NULL,
    job_name     VARCHAR(100) NOT NULL,
    success_count INT8 NULL,
    error_count INT8 NULL,
    start_dttm   TIMESTAMP NOT NULL,
    end_dttm     TIMESTAMP NOT NULL,
    appname      VARCHAR(100) NOT NULL,
    log_dttm     TIMESTAMP DEFAULT now() NOT NULL,
    CONSTRAINT metadata_ingestion_metric_pk
        PRIMARY KEY (id, log_dttm)
)
PARTITION BY RANGE (log_dttm);

CREATE TABLE IF NOT EXISTS public.metadata_ingestion_metric_default
    PARTITION OF public.metadata_ingestion_metric
    DEFAULT;