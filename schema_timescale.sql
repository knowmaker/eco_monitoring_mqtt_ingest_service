BEGIN;

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Reset schema objects before recreation.
DROP TABLE IF EXISTS gas_sensors CASCADE;
DROP TABLE IF EXISTS gas_state CASCADE;
DROP TABLE IF EXISTS dust_state CASCADE;
DROP TABLE IF EXISTS meteo_state CASCADE;
DROP TABLE IF EXISTS ivtm_state CASCADE;
DROP TABLE IF EXISTS device_state CASCADE;
DROP TABLE IF EXISTS plc_state CASCADE;
DROP TABLE IF EXISTS monitoring_posts CASCADE;

-- Monitoring posts metadata (one row per stationary post, many rows per mobile post by coordinates).
CREATE TABLE IF NOT EXISTS monitoring_posts (
    id BIGSERIAL PRIMARY KEY,
    serial TEXT NOT NULL,
    latitude DOUBLE PRECISION CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
    is_stationary BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_monitoring_posts_stationary_serial
    ON monitoring_posts (serial)
    WHERE is_stationary;

CREATE UNIQUE INDEX IF NOT EXISTS uq_monitoring_posts_mobile_serial_coords
    ON monitoring_posts (serial, latitude, longitude)
    WHERE NOT is_stationary;

CREATE INDEX IF NOT EXISTS idx_monitoring_posts_serial
    ON monitoring_posts (serial);

-- PLC-level packet state (regular table).
CREATE TABLE IF NOT EXISTS plc_state (
    id BIGSERIAL PRIMARY KEY,
    monitoring_post_id BIGINT NOT NULL
        REFERENCES monitoring_posts (id),
    aggregation_period_min SMALLINT NOT NULL CHECK (aggregation_period_min IN (5, 20)),
    plc_timestamp_ms BIGINT NOT NULL CHECK (plc_timestamp_ms > 0),
    device_name TEXT,
    modbus_status TEXT CHECK (modbus_status IS NULL OR modbus_status IN ('OK', 'BAD')),
    modbus_status_time_ms BIGINT CHECK (modbus_status_time_ms IS NULL OR modbus_status_time_ms > 0),
    rs485_status TEXT CHECK (rs485_status IS NULL OR rs485_status IN ('OK', 'BAD')),
    rs485_status_time_ms BIGINT CHECK (rs485_status_time_ms IS NULL OR rs485_status_time_ms > 0),
    mem_total BIGINT CHECK (mem_total IS NULL OR mem_total >= 0),
    mem_free BIGINT CHECK (mem_free IS NULL OR mem_free >= 0),
    mem_used BIGINT CHECK (mem_used IS NULL OR mem_used >= 0),
    cpu_temp DOUBLE PRECISION,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (monitoring_post_id, aggregation_period_min, plc_timestamp_ms)
);

CREATE INDEX IF NOT EXISTS idx_plc_state_post_ts
    ON plc_state (monitoring_post_id, plc_timestamp_ms DESC);

CREATE INDEX IF NOT EXISTS idx_plc_state_period_ts
    ON plc_state (aggregation_period_min, plc_timestamp_ms DESC);

-- Common per-device status for one packet (regular table).
CREATE TABLE IF NOT EXISTS device_state (
    id BIGSERIAL PRIMARY KEY,
    plc_state_id BIGINT NOT NULL
        REFERENCES plc_state (id)
        ON DELETE CASCADE,
    device_type TEXT NOT NULL CHECK (device_type IN ('gas', 'dust', 'meteo', 'ivtm')),
    device_name TEXT,
    ping TEXT CHECK (ping IS NULL OR ping IN ('OK', 'BAD')),
    ping_time_ms BIGINT CHECK (ping_time_ms IS NULL OR ping_time_ms > 0),
    device_timestamp_ms BIGINT CHECK (device_timestamp_ms IS NULL OR device_timestamp_ms > 0),
    number_reboot_count INTEGER CHECK (number_reboot_count IS NULL OR number_reboot_count >= 0),
    number_reboot_time_ms BIGINT CHECK (number_reboot_time_ms IS NULL OR number_reboot_time_ms >= 0),
    UNIQUE (plc_state_id, device_type)
);

CREATE INDEX IF NOT EXISTS idx_device_state_type_ping
    ON device_state (device_type, ping, plc_state_id DESC);

-- Gas-specific fields (regular table).
CREATE TABLE IF NOT EXISTS gas_state (
    device_state_id BIGINT NOT NULL
        REFERENCES device_state (id)
        ON DELETE CASCADE,
    device_timestamp_ms BIGINT NOT NULL CHECK (device_timestamp_ms > 0),
    board_temperature DOUBLE PRECISION,
    calibration_set_time_ms BIGINT CHECK (calibration_set_time_ms IS NULL OR calibration_set_time_ms >= 0),
    calibration_value DOUBLE PRECISION,
    calibration_time_start_ms BIGINT CHECK (calibration_time_start_ms IS NULL OR calibration_time_start_ms >= 0),
    calibration_time_end_ms BIGINT CHECK (calibration_time_end_ms IS NULL OR calibration_time_end_ms >= 0),
    calibration_warning INTEGER,
    calibration_status TEXT,
    PRIMARY KEY (device_state_id, device_timestamp_ms)
);

CREATE INDEX IF NOT EXISTS idx_gas_state_device_ts
    ON gas_state (device_timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS gas_sensors (
    device_state_id BIGINT NOT NULL,
    device_timestamp_ms BIGINT NOT NULL CHECK (device_timestamp_ms > 0),
    sensor_id INTEGER NOT NULL CHECK (sensor_id > 0),
    substance_code TEXT,
    value DOUBLE PRECISION,
    scale_dimension TEXT,
    signal BIGINT,
    sensor_status TEXT,
    PRIMARY KEY (device_state_id, device_timestamp_ms, sensor_id),
    FOREIGN KEY (device_state_id, device_timestamp_ms)
        REFERENCES gas_state (device_state_id, device_timestamp_ms)
        ON DELETE CASCADE
);

SELECT create_hypertable(
    'gas_sensors',
    'device_timestamp_ms',
    if_not_exists => TRUE,
    chunk_time_interval => 604800000
);

CREATE INDEX IF NOT EXISTS idx_gas_sensors_substance
    ON gas_sensors (substance_code, device_timestamp_ms DESC);

-- Dust-specific fields (hypertable by device_timestamp_ms).
CREATE TABLE IF NOT EXISTS dust_state (
    device_state_id BIGINT NOT NULL
        REFERENCES device_state (id)
        ON DELETE CASCADE,
    device_timestamp_ms BIGINT NOT NULL CHECK (device_timestamp_ms > 0),
    humidity DOUBLE PRECISION,
    temp DOUBLE PRECISION,
    pm1_concentration DOUBLE PRECISION,
    pm2_concentration DOUBLE PRECISION,
    pm10_concentration DOUBLE PRECISION,
    tsp_concentration DOUBLE PRECISION,
    status BOOLEAN,
    PRIMARY KEY (device_state_id, device_timestamp_ms)
);

SELECT create_hypertable(
    'dust_state',
    'device_timestamp_ms',
    if_not_exists => TRUE,
    chunk_time_interval => 604800000
);

CREATE INDEX IF NOT EXISTS idx_dust_state_device_ts
    ON dust_state (device_timestamp_ms DESC);

-- Meteo-specific fields (hypertable by device_timestamp_ms).
CREATE TABLE IF NOT EXISTS meteo_state (
    device_state_id BIGINT NOT NULL
        REFERENCES device_state (id)
        ON DELETE CASCADE,
    device_timestamp_ms BIGINT NOT NULL CHECK (device_timestamp_ms > 0),
    atm_press DOUBLE PRECISION,
    air_temp DOUBLE PRECISION,
    air_hum DOUBLE PRECISION,
    hor_win_dir DOUBLE PRECISION,
    hor_win_spd DOUBLE PRECISION,
    PRIMARY KEY (device_state_id, device_timestamp_ms)
);

SELECT create_hypertable(
    'meteo_state',
    'device_timestamp_ms',
    if_not_exists => TRUE,
    chunk_time_interval => 604800000
);

CREATE INDEX IF NOT EXISTS idx_meteo_state_device_ts
    ON meteo_state (device_timestamp_ms DESC);

-- IVTM-specific fields (hypertable by device_timestamp_ms).
CREATE TABLE IF NOT EXISTS ivtm_state (
    device_state_id BIGINT NOT NULL
        REFERENCES device_state (id)
        ON DELETE CASCADE,
    device_timestamp_ms BIGINT NOT NULL CHECK (device_timestamp_ms > 0),
    sensor_ivtm_hum DOUBLE PRECISION,
    sensor_ivtm_temp DOUBLE PRECISION,
    sensor_ivtm_error TEXT[],
    PRIMARY KEY (device_state_id, device_timestamp_ms)
);

SELECT create_hypertable(
    'ivtm_state',
    'device_timestamp_ms',
    if_not_exists => TRUE,
    chunk_time_interval => 604800000
);

CREATE INDEX IF NOT EXISTS idx_ivtm_state_device_ts
    ON ivtm_state (device_timestamp_ms DESC);

COMMIT;
