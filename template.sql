-- PL/pgSQL generator: create 100 schemas × 20 tables (5-8 columns each) using many Postgres data types
-- Save as: plpgsql_generator_create_schemas_and_tables.sql
-- Run with: psql -d your_database -f plpgsql_generator_create_schemas_and_tables.sql

-- NOTE: this script tries to create the pgcrypto extension to get gen_random_uuid().
-- If you cannot create extensions, remove the UUID default or replace with uuid_generate_v4()

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO
$$
DECLARE
    types text[] := ARRAY[
        'BOOLEAN',
        'UUID',
        'DOUBLE PRECISION',
        'INTEGER',
        'CHARACTER(5)',
        'DATE',
        'JSON',
        'CHARACTER VARYING(100)',
        'TIMESTAMP WITHOUT TIME ZONE',
        'TIMESTAMP WITH TIME ZONE',
        'TEXT',
        'NAME',
        'SMALLINT',
        'NUMERIC(12,4)',
        'OID',
        'BIGINT'
    ];

    schema_idx  int;
    table_idx   int;
    col_count   int;
    col_idx     int;
    global_counter int := 0;
    sch text;
    tbl text;
    sql text;
    colname text;
    coltype text;
BEGIN
    FOR schema_idx IN 1..100 LOOP
        sch := 'schema_' || to_char(schema_idx, '000');
        sql := format('CREATE SCHEMA IF NOT EXISTS %I;', sch);
        EXECUTE sql;

        FOR table_idx IN 1..20 LOOP
            tbl := 'table_' || to_char(table_idx, '000');
            -- number of columns per table: 5..8
            col_count := 5 + floor(random() * 4)::int; -- 0..3 -> 5..8

            sql := format('CREATE TABLE IF NOT EXISTS %I.%I (', sch, tbl);
            -- add a primary key to every table
            sql := sql || 'id SERIAL PRIMARY KEY';

            FOR col_idx IN 1..col_count LOOP
                global_counter := global_counter + 1;
                coltype := types[(global_counter - 1) % array_length(types,1) + 1];
                colname := 'col_' || to_char(col_idx, '00');

                sql := sql || ', ' || quote_ident(colname) || ' ' || coltype;

                -- sensible defaults for some types to make the tables easier to populate later
                IF coltype = 'UUID' THEN
                    sql := sql || ' DEFAULT gen_random_uuid()';
                ELSIF coltype = 'BOOLEAN' THEN
                    sql := sql || ' DEFAULT false';
                ELSIF coltype LIKE 'CHARACTER%' OR coltype LIKE 'CHARACTER VARYING%' OR coltype = 'NAME' OR coltype = 'TEXT' THEN
                    sql := sql || ' DEFAULT ''sample''';
                ELSIF coltype = 'INTEGER' OR coltype = 'SMALLINT' OR coltype = 'BIGINT' OR coltype = 'OID' THEN
                    sql := sql || ' DEFAULT 0';
                ELSIF coltype = 'DOUBLE PRECISION' OR coltype LIKE 'NUMERIC%' THEN
                    sql := sql || ' DEFAULT 0';
                ELSIF coltype = 'DATE' THEN
                    sql := sql || ' DEFAULT CURRENT_DATE';
                ELSIF coltype LIKE 'TIMESTAMP%' THEN
                    sql := sql || ' DEFAULT CURRENT_TIMESTAMP';
                ELSIF coltype = 'JSON' THEN
                    sql := sql || ' DEFAULT ''{}''::json';
                END IF;
            END LOOP;

            sql := sql || ');';
            EXECUTE sql;
        END LOOP;
    END LOOP;
END
$$ LANGUAGE plpgsql;

-- Optional: a simple row-count summary (uncomment to run after creation)
-- SELECT nspname AS schema_name, relname AS table_name
-- FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
-- WHERE relkind = 'r' AND nspname LIKE 'schema_%'
-- ORDER BY nspname, relname;
