import logging
from typing import Any, Dict, Optional

import psycopg2

SUPPORTED_DEVICE_TYPES = ("gas", "dust", "meteo", "ivtm")


def normalize_epoch_ms(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    # Seconds-based timestamp -> convert to milliseconds.
    if numeric < 10_000_000_000:
        return int(numeric * 1000)
    return int(numeric)


def to_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def extract_device_timestamp_ms(device_payload: Dict[str, Any], fallback_ms: int) -> int:
    for key in ("timeStamp", "timestamp", "timeStampDevice"):
        ts = normalize_epoch_ms(device_payload.get(key))
        if ts is not None:
            return ts
    return fallback_ms


class MqttIngestDbWriter:
    def __init__(self, db_dsn: str) -> None:
        self.db_dsn = db_dsn
        self.db_connection = None

    def _ensure_connection(self) -> None:
        if self.db_connection is None or self.db_connection.closed:
            self.db_connection = psycopg2.connect(self.db_dsn)
            self.db_connection.autocommit = False

    def close(self) -> None:
        if self.db_connection is not None and not self.db_connection.closed:
            self.db_connection.close()

    def write_payload(self, payload: Dict[str, Any], aggregation_period_min: int) -> None:
        self._ensure_connection()
        serial = str(payload.get("serial") or "UNKNOWN_SERIAL")
        plc_timestamp_ms = normalize_epoch_ms(payload.get("timeStamp"))
        if plc_timestamp_ms is None:
            raise ValueError("Payload has no valid top-level timeStamp.")

        try:
            with self.db_connection.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO plc_state (
                        serial,
                        aggregation_period_min,
                        plc_timestamp_ms,
                        device_name,
                        latitude,
                        longitude,
                        modbus_status,
                        modbus_status_time_ms,
                        rs485_status,
                        rs485_status_time_ms,
                        mem_total,
                        mem_free,
                        mem_used,
                        cpu_temp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (serial, aggregation_period_min, plc_timestamp_ms)
                    DO UPDATE SET
                        device_name = EXCLUDED.device_name,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        modbus_status = EXCLUDED.modbus_status,
                        modbus_status_time_ms = EXCLUDED.modbus_status_time_ms,
                        rs485_status = EXCLUDED.rs485_status,
                        rs485_status_time_ms = EXCLUDED.rs485_status_time_ms,
                        mem_total = EXCLUDED.mem_total,
                        mem_free = EXCLUDED.mem_free,
                        mem_used = EXCLUDED.mem_used,
                        cpu_temp = EXCLUDED.cpu_temp,
                        received_at = NOW()
                    RETURNING id
                    """,
                    (
                        serial,
                        aggregation_period_min,
                        plc_timestamp_ms,
                        payload.get("deviceName"),
                        to_float(payload.get("latitude")),
                        to_float(payload.get("longitude")),
                        payload.get("modbusStatus"),
                        normalize_epoch_ms(payload.get("modbusStatusTime")),
                        payload.get("rs485Status"),
                        normalize_epoch_ms(payload.get("rs485StatusTime")),
                        to_int(payload.get("memTotal")),
                        to_int(payload.get("memFree")),
                        to_int(payload.get("memUsed")),
                        to_float(payload.get("cpuTemp")),
                    ),
                )
                plc_id = cur.fetchone()[0]

                for device_type in SUPPORTED_DEVICE_TYPES:
                    device_payload = payload.get(device_type)
                    if not isinstance(device_payload, dict):
                        continue

                    number_reboot = device_payload.get("numberReboot")
                    reboot_count = None
                    reboot_time_ms = None
                    if isinstance(number_reboot, dict):
                        reboot_count = to_int(number_reboot.get("countReboot"))
                        reboot_time_ms = normalize_epoch_ms(number_reboot.get("time"))

                    device_timestamp_ms = extract_device_timestamp_ms(device_payload, plc_timestamp_ms)

                    cur.execute(
                        """
                        INSERT INTO device_state (
                            plc_state_id,
                            device_type,
                            device_name,
                            ping,
                            ping_time_ms,
                            device_timestamp_ms,
                            number_reboot_count,
                            number_reboot_time_ms
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (plc_state_id, device_type)
                        DO UPDATE SET
                            device_name = EXCLUDED.device_name,
                            ping = EXCLUDED.ping,
                            ping_time_ms = EXCLUDED.ping_time_ms,
                            device_timestamp_ms = EXCLUDED.device_timestamp_ms,
                            number_reboot_count = EXCLUDED.number_reboot_count,
                            number_reboot_time_ms = EXCLUDED.number_reboot_time_ms
                        RETURNING id
                        """,
                        (
                            plc_id,
                            device_type,
                            device_payload.get("deviceName"),
                            device_payload.get("ping"),
                            normalize_epoch_ms(device_payload.get("pingTime")),
                            device_timestamp_ms,
                            reboot_count,
                            reboot_time_ms,
                        ),
                    )
                    device_state_id = cur.fetchone()[0]

                    if device_type == "gas":
                        self._upsert_gas(cur, device_state_id, device_timestamp_ms, device_payload)
                    elif device_type == "dust":
                        self._upsert_dust(cur, device_state_id, device_timestamp_ms, device_payload)
                    elif device_type == "meteo":
                        self._upsert_meteo(cur, device_state_id, device_timestamp_ms, device_payload)
                    elif device_type == "ivtm":
                        self._upsert_ivtm(cur, device_state_id, device_timestamp_ms, device_payload)

            self.db_connection.commit()
            logging.info(
                "Saved payload: serial=%s plc_ts_ms=%s aggregation=%s",
                serial,
                plc_timestamp_ms,
                aggregation_period_min,
            )
        except Exception:
            self.db_connection.rollback()
            raise

    def _upsert_gas(
        self, cur, device_state_id: int, device_timestamp_ms: int, device_payload: Dict[str, Any]
    ) -> None:
        calibration = device_payload.get("calibration") if isinstance(device_payload.get("calibration"), dict) else {}
        cur.execute(
            """
            INSERT INTO gas_state (
                device_state_id,
                device_timestamp_ms,
                board_temperature,
                calibration_set_time_ms,
                calibration_value,
                calibration_time_start_ms,
                calibration_time_end_ms,
                calibration_warning,
                calibration_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_state_id, device_timestamp_ms)
            DO UPDATE SET
                board_temperature = EXCLUDED.board_temperature,
                calibration_set_time_ms = EXCLUDED.calibration_set_time_ms,
                calibration_value = EXCLUDED.calibration_value,
                calibration_time_start_ms = EXCLUDED.calibration_time_start_ms,
                calibration_time_end_ms = EXCLUDED.calibration_time_end_ms,
                calibration_warning = EXCLUDED.calibration_warning,
                calibration_status = EXCLUDED.calibration_status
            """,
            (
                device_state_id,
                device_timestamp_ms,
                to_float(device_payload.get("boardTemperature")),
                normalize_epoch_ms(calibration.get("setTimeCalibration")),
                to_float(calibration.get("calibration")),
                normalize_epoch_ms(calibration.get("calibrationTimeStart")),
                normalize_epoch_ms(calibration.get("calibrationTimeEnd")),
                to_int(calibration.get("warning")),
                calibration.get("status"),
            ),
        )

        sensors = device_payload.get("sensors")
        if not isinstance(sensors, list):
            return

        for sensor in sensors:
            if not isinstance(sensor, dict):
                continue
            sensor_id = to_int(sensor.get("id"))
            if sensor_id is None:
                continue
            cur.execute(
                """
                INSERT INTO gas_sensors (
                    device_state_id,
                    device_timestamp_ms,
                    sensor_id,
                    substance_code,
                    value,
                    scale_dimension,
                    signal,
                    sensor_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_state_id, device_timestamp_ms, sensor_id)
                DO UPDATE SET
                    substance_code = EXCLUDED.substance_code,
                    value = EXCLUDED.value,
                    scale_dimension = EXCLUDED.scale_dimension,
                    signal = EXCLUDED.signal,
                    sensor_status = EXCLUDED.sensor_status
                """,
                (
                    device_state_id,
                    device_timestamp_ms,
                    sensor_id,
                    sensor.get("substanceCode"),
                    to_float(sensor.get("value")),
                    sensor.get("scaleDimension"),
                    to_int(sensor.get("signal")),
                    sensor.get("status"),
                ),
            )

    def _upsert_dust(
        self, cur, device_state_id: int, device_timestamp_ms: int, device_payload: Dict[str, Any]
    ) -> None:
        cur.execute(
            """
            INSERT INTO dust_state (
                device_state_id,
                device_timestamp_ms,
                humidity,
                temp,
                pm1_concentration,
                pm2_concentration,
                pm10_concentration,
                tsp_concentration,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_state_id, device_timestamp_ms)
            DO UPDATE SET
                humidity = EXCLUDED.humidity,
                temp = EXCLUDED.temp,
                pm1_concentration = EXCLUDED.pm1_concentration,
                pm2_concentration = EXCLUDED.pm2_concentration,
                pm10_concentration = EXCLUDED.pm10_concentration,
                tsp_concentration = EXCLUDED.tsp_concentration,
                status = EXCLUDED.status
            """,
            (
                device_state_id,
                device_timestamp_ms,
                to_float(device_payload.get("humidity")),
                to_float(device_payload.get("temp")),
                to_float(device_payload.get("pm1Concentration")),
                to_float(device_payload.get("pm2Concentration")),
                to_float(device_payload.get("pm10Concentration")),
                to_float(device_payload.get("tspConcentration")),
                device_payload.get("status") if isinstance(device_payload.get("status"), bool) else None,
            ),
        )

    def _upsert_meteo(
        self, cur, device_state_id: int, device_timestamp_ms: int, device_payload: Dict[str, Any]
    ) -> None:
        cur.execute(
            """
            INSERT INTO meteo_state (
                device_state_id,
                device_timestamp_ms,
                atm_press,
                air_temp,
                air_hum,
                hor_win_dir,
                hor_win_spd
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_state_id, device_timestamp_ms)
            DO UPDATE SET
                atm_press = EXCLUDED.atm_press,
                air_temp = EXCLUDED.air_temp,
                air_hum = EXCLUDED.air_hum,
                hor_win_dir = EXCLUDED.hor_win_dir,
                hor_win_spd = EXCLUDED.hor_win_spd
            """,
            (
                device_state_id,
                device_timestamp_ms,
                to_float(device_payload.get("atmPress")),
                to_float(device_payload.get("airTemp")),
                to_float(device_payload.get("airHum")),
                to_float(device_payload.get("horWinDir")),
                to_float(device_payload.get("horWinSpd")),
            ),
        )

    def _upsert_ivtm(
        self, cur, device_state_id: int, device_timestamp_ms: int, device_payload: Dict[str, Any]
    ) -> None:
        error_list: Any = device_payload.get("sensorIVTMerror")
        if isinstance(error_list, list):
            errors = [str(item) for item in error_list]
        else:
            errors = []

        cur.execute(
            """
            INSERT INTO ivtm_state (
                device_state_id,
                device_timestamp_ms,
                sensor_ivtm_hum,
                sensor_ivtm_temp,
                sensor_ivtm_error
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (device_state_id, device_timestamp_ms)
            DO UPDATE SET
                sensor_ivtm_hum = EXCLUDED.sensor_ivtm_hum,
                sensor_ivtm_temp = EXCLUDED.sensor_ivtm_temp,
                sensor_ivtm_error = EXCLUDED.sensor_ivtm_error
            """,
            (
                device_state_id,
                device_timestamp_ms,
                to_float(device_payload.get("sensorIVTMhum")),
                to_float(device_payload.get("sensorIVTMtemp")),
                errors,
            ),
        )
