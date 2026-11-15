-- Esquema curated AgroDatos (PostgreSQL 15+)
-- Ejecutar UNA VEZ tras crear la base de datos:
--   psql -h localhost -U agro -d agro -f sql/01_schema.sql

CREATE TABLE IF NOT EXISTS humedad_diaria_lote (
    lote_id                      VARCHAR(20) NOT NULL,
    fecha                        DATE NOT NULL,
    n_lecturas                   BIGINT,
    n_valores_imputados          BIGINT,
    suelo_unidad                 VARCHAR(5),
    calidad_humedad              VARCHAR(20),
    humedad_suelo_10cm_prom      DOUBLE PRECISION,
    humedad_suelo_10cm_min       DOUBLE PRECISION,
    humedad_suelo_10cm_max       DOUBLE PRECISION,
    humedad_suelo_30cm_prom      DOUBLE PRECISION,
    humedad_suelo_30cm_min       DOUBLE PRECISION,
    humedad_suelo_30cm_max       DOUBLE PRECISION,
    humedad_suelo_50cm_prom      DOUBLE PRECISION,
    humedad_suelo_50cm_min       DOUBLE PRECISION,
    humedad_suelo_50cm_max       DOUBLE PRECISION,
    temperatura_prom             DOUBLE PRECISION,
    temperatura_min              DOUBLE PRECISION,
    temperatura_max              DOUBLE PRECISION,
    humedad_aire_prom            DOUBLE PRECISION,
    precipitacion_acum_mm        DOUBLE PRECISION,
    n_lecturas_lluvia            BIGINT,
    pct_dia_cubierto             DOUBLE PRECISION,
    PRIMARY KEY (lote_id, fecha)
);
CREATE INDEX IF NOT EXISTS ix_hdl_fecha_lote ON humedad_diaria_lote (fecha, lote_id);
CREATE INDEX IF NOT EXISTS ix_hdl_lote_fecha ON humedad_diaria_lote (lote_id, fecha);

CREATE TABLE IF NOT EXISTS alertas (
    lote_id       VARCHAR(20) NOT NULL,
    timestamp     TIMESTAMP   NOT NULL,
    fecha         DATE        NOT NULL,
    imputado      BOOLEAN,
    tipo_alerta   VARCHAR(30) NOT NULL,
    variable      VARCHAR(30),
    valor         DOUBLE PRECISION,
    umbral        DOUBLE PRECISION,
    PRIMARY KEY (lote_id, timestamp, tipo_alerta)
);
CREATE INDEX IF NOT EXISTS ix_alertas_lote_ts ON alertas (lote_id, timestamp);
CREATE INDEX IF NOT EXISTS ix_alertas_fecha_lote ON alertas (fecha, lote_id);
CREATE INDEX IF NOT EXISTS ix_alertas_tipo ON alertas (tipo_alerta);

CREATE TABLE IF NOT EXISTS alertas_diarias (
    lote_id              VARCHAR(20) NOT NULL,
    fecha                DATE        NOT NULL,
    tipo_alerta          VARCHAR(30) NOT NULL,
    variable             VARCHAR(30),
    umbral               DOUBLE PRECISION,
    n_lecturas_en_alerta BIGINT,
    valor_min            DOUBLE PRECISION,
    valor_max            DOUBLE PRECISION,
    primera_lectura      TIMESTAMP,
    ultima_lectura       TIMESTAMP,
    PRIMARY KEY (lote_id, fecha, tipo_alerta)
);
CREATE INDEX IF NOT EXISTS ix_ald_fecha_lote ON alertas_diarias (fecha, lote_id);
CREATE INDEX IF NOT EXISTS ix_ald_lote_fecha ON alertas_diarias (lote_id, fecha);