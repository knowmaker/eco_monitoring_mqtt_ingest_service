-- TimescaleDB aggregation setup for existing production data.
-- Safe to re-run: continuous aggregates are recreated, retention policies are reset.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1) Current time in milliseconds for BIGINT time hypertables.
CREATE OR REPLACE FUNCTION public.now_ms()
RETURNS BIGINT
LANGUAGE SQL
STABLE
AS $$
  SELECT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
$$;

SELECT set_integer_now_func('public.gas_sensors', 'public.now_ms');
SELECT set_integer_now_func('public.dust_state',  'public.now_ms');
SELECT set_integer_now_func('public.meteo_state', 'public.now_ms');
SELECT set_integer_now_func('public.ivtm_state',  'public.now_ms');

-- 2) Recreate hourly and daily continuous aggregates.
-- Daily aggregates are used by the UI month mode: one point per day inside the selected month.
-- Day buckets are shifted to Europe/Moscow midnight for the local UI date.
DROP MATERIALIZED VIEW IF EXISTS public.cagg_gas_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.cagg_ivtm_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.cagg_meteo_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.cagg_dust_daily CASCADE;

DROP MATERIALIZED VIEW IF EXISTS public.cagg_gas_hourly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.cagg_ivtm_hourly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.cagg_meteo_hourly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.cagg_dust_hourly CASCADE;

CREATE MATERIALIZED VIEW public.cagg_dust_hourly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(3600000::BIGINT, d.device_timestamp_ms) AS bucket_ms,
  p.monitoring_post_id,
  AVG(d.pm1_concentration)  AS pm1_avg,
  AVG(d.pm2_concentration)  AS pm2_avg,
  AVG(d.pm10_concentration) AS pm10_avg,
  AVG(d.tsp_concentration)  AS tsp_avg
FROM public.dust_state d
JOIN public.device_state ds ON ds.id = d.device_state_id
JOIN public.plc_state p     ON p.id = ds.plc_state_id
WHERE ds.device_type = 'dust'
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW public.cagg_meteo_hourly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(3600000::BIGINT, m.device_timestamp_ms) AS bucket_ms,
  p.monitoring_post_id,
  AVG(m.atm_press) AS atm_press_avg,
  AVG(m.air_temp)  AS air_temp_avg,
  AVG(m.air_hum)   AS air_hum_avg,
  CASE
    WHEN sqrt(
      power(AVG(sin(radians(m.hor_win_dir))), 2) +
      power(AVG(cos(radians(m.hor_win_dir))), 2)
    ) < 1e-6 THEN NULL
    WHEN degrees(atan2(AVG(sin(radians(m.hor_win_dir))), AVG(cos(radians(m.hor_win_dir))))) < 0.0
      THEN degrees(atan2(AVG(sin(radians(m.hor_win_dir))), AVG(cos(radians(m.hor_win_dir))))) + 360.0
    ELSE degrees(atan2(AVG(sin(radians(m.hor_win_dir))), AVG(cos(radians(m.hor_win_dir)))))
  END AS hor_win_dir_avg,
  AVG(m.hor_win_spd) AS hor_win_spd_avg
FROM public.meteo_state m
JOIN public.device_state ds ON ds.id = m.device_state_id
JOIN public.plc_state p     ON p.id = ds.plc_state_id
WHERE ds.device_type = 'meteo'
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW public.cagg_ivtm_hourly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(3600000::BIGINT, i.device_timestamp_ms) AS bucket_ms,
  p.monitoring_post_id,
  AVG(i.sensor_ivtm_hum)  AS sensor_ivtm_hum_avg,
  AVG(i.sensor_ivtm_temp) AS sensor_ivtm_temp_avg
FROM public.ivtm_state i
JOIN public.device_state ds ON ds.id = i.device_state_id
JOIN public.plc_state p     ON p.id = ds.plc_state_id
WHERE ds.device_type = 'ivtm'
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW public.cagg_gas_hourly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(3600000::BIGINT, g.device_timestamp_ms) AS bucket_ms,
  p.monitoring_post_id,
  COALESCE(g.substance_code, 'UNKNOWN') AS substance_code,
  AVG(g.value) AS value_avg
FROM public.gas_sensors g
JOIN public.device_state ds ON ds.id = g.device_state_id
JOIN public.plc_state p     ON p.id = ds.plc_state_id
WHERE ds.device_type = 'gas'
GROUP BY 1, 2, 3
WITH NO DATA;

CREATE MATERIALIZED VIEW public.cagg_dust_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(86400000::BIGINT, d.device_timestamp_ms, 75600000::BIGINT) AS bucket_ms,
  p.monitoring_post_id,
  AVG(d.pm1_concentration)  AS pm1_avg,
  AVG(d.pm2_concentration)  AS pm2_avg,
  AVG(d.pm10_concentration) AS pm10_avg,
  AVG(d.tsp_concentration)  AS tsp_avg
FROM public.dust_state d
JOIN public.device_state ds ON ds.id = d.device_state_id
JOIN public.plc_state p     ON p.id = ds.plc_state_id
WHERE ds.device_type = 'dust'
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW public.cagg_meteo_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(86400000::BIGINT, m.device_timestamp_ms, 75600000::BIGINT) AS bucket_ms,
  p.monitoring_post_id,
  AVG(m.atm_press) AS atm_press_avg,
  AVG(m.air_temp)  AS air_temp_avg,
  AVG(m.air_hum)   AS air_hum_avg,
  CASE
    WHEN sqrt(
      power(AVG(sin(radians(m.hor_win_dir))), 2) +
      power(AVG(cos(radians(m.hor_win_dir))), 2)
    ) < 1e-6 THEN NULL
    WHEN degrees(atan2(AVG(sin(radians(m.hor_win_dir))), AVG(cos(radians(m.hor_win_dir))))) < 0.0
      THEN degrees(atan2(AVG(sin(radians(m.hor_win_dir))), AVG(cos(radians(m.hor_win_dir))))) + 360.0
    ELSE degrees(atan2(AVG(sin(radians(m.hor_win_dir))), AVG(cos(radians(m.hor_win_dir)))))
  END AS hor_win_dir_avg,
  AVG(m.hor_win_spd) AS hor_win_spd_avg
FROM public.meteo_state m
JOIN public.device_state ds ON ds.id = m.device_state_id
JOIN public.plc_state p     ON p.id = ds.plc_state_id
WHERE ds.device_type = 'meteo'
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW public.cagg_ivtm_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(86400000::BIGINT, i.device_timestamp_ms, 75600000::BIGINT) AS bucket_ms,
  p.monitoring_post_id,
  AVG(i.sensor_ivtm_hum)  AS sensor_ivtm_hum_avg,
  AVG(i.sensor_ivtm_temp) AS sensor_ivtm_temp_avg
FROM public.ivtm_state i
JOIN public.device_state ds ON ds.id = i.device_state_id
JOIN public.plc_state p     ON p.id = ds.plc_state_id
WHERE ds.device_type = 'ivtm'
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW public.cagg_gas_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(86400000::BIGINT, g.device_timestamp_ms, 75600000::BIGINT) AS bucket_ms,
  p.monitoring_post_id,
  COALESCE(g.substance_code, 'UNKNOWN') AS substance_code,
  AVG(g.value) AS value_avg
FROM public.gas_sensors g
JOIN public.device_state ds ON ds.id = g.device_state_id
JOIN public.plc_state p     ON p.id = ds.plc_state_id
WHERE ds.device_type = 'gas'
GROUP BY 1, 2, 3
WITH NO DATA;

-- 3) Indexes on aggregates.
CREATE INDEX IF NOT EXISTS idx_cagg_dust_hourly_post_bucket
  ON public.cagg_dust_hourly (monitoring_post_id, bucket_ms DESC);

CREATE INDEX IF NOT EXISTS idx_cagg_meteo_hourly_post_bucket
  ON public.cagg_meteo_hourly (monitoring_post_id, bucket_ms DESC);

CREATE INDEX IF NOT EXISTS idx_cagg_ivtm_hourly_post_bucket
  ON public.cagg_ivtm_hourly (monitoring_post_id, bucket_ms DESC);

CREATE INDEX IF NOT EXISTS idx_cagg_gas_hourly_post_substance_bucket
  ON public.cagg_gas_hourly (monitoring_post_id, substance_code, bucket_ms DESC);

CREATE INDEX IF NOT EXISTS idx_cagg_dust_daily_post_bucket
  ON public.cagg_dust_daily (monitoring_post_id, bucket_ms DESC);

CREATE INDEX IF NOT EXISTS idx_cagg_meteo_daily_post_bucket
  ON public.cagg_meteo_daily (monitoring_post_id, bucket_ms DESC);

CREATE INDEX IF NOT EXISTS idx_cagg_ivtm_daily_post_bucket
  ON public.cagg_ivtm_daily (monitoring_post_id, bucket_ms DESC);

CREATE INDEX IF NOT EXISTS idx_cagg_gas_daily_post_substance_bucket
  ON public.cagg_gas_daily (monitoring_post_id, substance_code, bucket_ms DESC);

-- 4) Auto refresh policies.
-- Hourly aggregates: every 5 minutes, recalculate last 1 day excluding latest 1 hour.
SELECT add_continuous_aggregate_policy(
  'public.cagg_dust_hourly',
  start_offset => 86400000::BIGINT,
  end_offset => 3600000::BIGINT,
  schedule_interval => INTERVAL '5 minutes'
);

SELECT add_continuous_aggregate_policy(
  'public.cagg_meteo_hourly',
  start_offset => 86400000::BIGINT,
  end_offset => 3600000::BIGINT,
  schedule_interval => INTERVAL '5 minutes'
);

SELECT add_continuous_aggregate_policy(
  'public.cagg_ivtm_hourly',
  start_offset => 86400000::BIGINT,
  end_offset => 3600000::BIGINT,
  schedule_interval => INTERVAL '5 minutes'
);

SELECT add_continuous_aggregate_policy(
  'public.cagg_gas_hourly',
  start_offset => 86400000::BIGINT,
  end_offset => 3600000::BIGINT,
  schedule_interval => INTERVAL '5 minutes'
);

-- Daily aggregates for month mode: every hour, recalculate last 30 days excluding current day.
-- Older aggregate buckets are preserved and are not refreshed after raw data retention removes source rows.
SELECT add_continuous_aggregate_policy(
  'public.cagg_dust_daily',
  start_offset => 2592000000::BIGINT,
  end_offset => 86400000::BIGINT,
  schedule_interval => INTERVAL '1 hour'
);

SELECT add_continuous_aggregate_policy(
  'public.cagg_meteo_daily',
  start_offset => 2592000000::BIGINT,
  end_offset => 86400000::BIGINT,
  schedule_interval => INTERVAL '1 hour'
);

SELECT add_continuous_aggregate_policy(
  'public.cagg_ivtm_daily',
  start_offset => 2592000000::BIGINT,
  end_offset => 86400000::BIGINT,
  schedule_interval => INTERVAL '1 hour'
);

SELECT add_continuous_aggregate_policy(
  'public.cagg_gas_daily',
  start_offset => 2592000000::BIGINT,
  end_offset => 86400000::BIGINT,
  schedule_interval => INTERVAL '1 hour'
);

-- 5) Raw cleanup for plc_state/device_state cascade chain.
-- Aggregates do not get retention policies and are preserved indefinitely.
CREATE INDEX IF NOT EXISTS idx_plc_state_ts_only ON public.plc_state (plc_timestamp_ms);

CREATE OR REPLACE FUNCTION public.cleanup_raw_state_older_than_30d()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  deleted_count BIGINT;
BEGIN
  DELETE FROM public.plc_state
  WHERE plc_timestamp_ms < public.now_ms() - 2592000000::BIGINT;

  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END
$$;

CREATE OR REPLACE PROCEDURE public.cleanup_raw_state_older_than_30d_job(job_id INT, config JSONB)
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM public.cleanup_raw_state_older_than_30d();
END
$$;

DO $$
DECLARE
  old_job_id INTEGER;
BEGIN
  FOR old_job_id IN
    SELECT job_id
    FROM timescaledb_information.jobs
    WHERE proc_schema = 'public'
      AND proc_name IN (
        'cleanup_raw_state_older_than_14d_job',
        'cleanup_raw_state_older_than_30d_job'
      )
  LOOP
    PERFORM delete_job(old_job_id);
  END LOOP;
END
$$;

SELECT add_job(
  'public.cleanup_raw_state_older_than_30d_job',
  INTERVAL '1 hour',
  config => '{}'::jsonb
);

-- Optional: run once manually after setup.
-- SELECT public.cleanup_raw_state_older_than_30d();
