from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import psycopg


MOSCOW_TIMEZONE = ZoneInfo("Europe/Moscow")
MOSECOM_PROXY_PORT = 8765
MOSECOM_ORIGIN = "https://mosecom.mos.ru"
MOSECOM_REFERER = "https://mosecom.mos.ru/karta/"
MOSECOM_SUBSTANCES = ("NO2", "O3", "NO", "SO2", "CO", "H2S")
MOSECOM_ADVISORY_LOCK_ID = 1_935_623_841
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_ATTEMPTS = 3


class MosecomCollector:
    def __init__(self, db_dsn: str) -> None:
        self.db_dsn = db_dsn

    def collect_previous_hour(self, scheduled_at: datetime | None = None) -> int:
        bucket_ms = previous_hour_bucket_ms(scheduled_at)
        with psycopg.connect(self.db_dsn) as connection:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT pg_try_advisory_xact_lock(%s)",
                    (MOSECOM_ADVISORY_LOCK_ID,),
                )
                if not cur.fetchone()[0]:
                    logging.info("Mosecom collection skipped: another process holds the collector lock")
                    return 0

                payloads = _fetch_all_substances()
                inserted_count = _store_payloads(cur, bucket_ms, payloads)

        logging.info("Stored %s Mosecom hourly readings for bucket %s", inserted_count, bucket_ms)
        return inserted_count


def next_collection_time(now: datetime | None = None) -> datetime:
    local_now = (now or datetime.now(MOSCOW_TIMEZONE)).astimezone(MOSCOW_TIMEZONE)
    collection_time = local_now.replace(minute=30, second=0, microsecond=0)
    if collection_time <= local_now:
        collection_time += timedelta(hours=1)
    return collection_time


def previous_hour_bucket_ms(now: datetime | None = None) -> int:
    local_now = (now or datetime.now(MOSCOW_TIMEZONE)).astimezone(MOSCOW_TIMEZONE)
    bucket_start = local_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    return int(bucket_start.timestamp() * 1000)


def _fetch_all_substances() -> dict[str, dict[str, Any]]:
    return {
        substance_code: _fetch_substance(substance_code)
        for substance_code in MOSECOM_SUBSTANCES
    }


def _fetch_substance(substance_code: str) -> dict[str, Any]:
    proxy_host = os.getenv("MQTT_BROKER_1_HOST", "").strip()
    if not proxy_host:
        raise RuntimeError("MQTT_BROKER_1_HOST is required for the Mosecom proxy")
    request_url = f"http://{proxy_host}:{MOSECOM_PROXY_PORT}/mosecom"
    request = Request(
        request_url,
        data=urlencode(
            {"locale": "ru_RU", "mapType": "air", "element": substance_code}
        ).encode("ascii"),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": MOSECOM_ORIGIN,
            "Referer": MOSECOM_REFERER,
            "Accept": "application/json",
            "User-Agent": "eco-monitoring-mosecom-collector/1.0",
        },
        method="POST",
    )

    last_error: Exception | None = None
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload = json.load(response)
            if not isinstance(payload, dict) or not isinstance(payload.get("able"), list):
                raise ValueError(f"Unexpected Mosecom response for {substance_code}")
            return payload
        except Exception as exc:
            last_error = exc
            if attempt < REQUEST_ATTEMPTS:
                logging.warning(
                    "Mosecom request failed for %s (attempt %s/%s): %s",
                    substance_code,
                    attempt,
                    REQUEST_ATTEMPTS,
                    exc,
                )
                time.sleep(attempt)

    raise RuntimeError(f"Failed to fetch Mosecom data for {substance_code}") from last_error


def _store_payloads(cur, bucket_ms: int, payloads: dict[str, dict[str, Any]]) -> int:
    station_payloads: dict[str, dict[str, Any]] = {}
    readings: list[tuple[str, str, dict[str, Any]]] = []

    for substance_code, payload in payloads.items():
        for status_key in ("able", "disable"):
            for station in payload.get(status_key, []):
                serial = _station_serial(station)
                if serial:
                    station_payloads[serial] = station
        for station in payload.get("able", []):
            serial = _station_serial(station)
            if serial and _to_float(station.get("modifyav")) is not None:
                readings.append((serial, substance_code, station))

    post_ids = {
        serial: _upsert_station(cur, serial, station)
        for serial, station in station_payloads.items()
    }
    inserted_count = 0
    for serial, substance_code, station in readings:
        cur.execute(
            """
            INSERT INTO mosecom_hourly_readings (
                monitoring_post_id,
                bucket_ms,
                substance_code,
                value,
                unit,
                pdk_ratio
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (monitoring_post_id, bucket_ms, substance_code)
            DO NOTHING
            RETURNING 1
            """,
            (
                post_ids[serial],
                bucket_ms,
                substance_code,
                _to_float(station.get("modifyav")),
                str(station.get("unit") or "мг/м3"),
                _to_float(station.get("pdk")),
            ),
        )
        if cur.fetchone() is not None:
            inserted_count += 1
    return inserted_count


def _upsert_station(cur, serial: str, station: dict[str, Any]) -> int:
    cur.execute(
        """
        INSERT INTO monitoring_posts (
            serial,
            name,
            post_type,
            latitude,
            longitude,
            source,
            is_confirmed,
            active_from
        )
        VALUES (%s, %s, 'stationary', %s, %s, 'mosecom', TRUE, NOW())
        ON CONFLICT (serial) WHERE active_to IS NULL
        DO UPDATE SET
            name = EXCLUDED.name,
            post_type = EXCLUDED.post_type,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            source = EXCLUDED.source,
            is_confirmed = EXCLUDED.is_confirmed
        RETURNING id
        """,
        (
            serial,
            _station_name(station),
            _to_float(station.get("latitude")),
            _to_float(station.get("longitude")),
        ),
    )
    return cur.fetchone()[0]


def _station_serial(station: dict[str, Any]) -> str | None:
    page_link = str(station.get("pageLink") or "").strip().strip("/")
    station_id = str(station.get("stationId") or "").strip()
    key = page_link or station_id
    return f"mosecom:{key}" if key else None


def _station_name(station: dict[str, Any]) -> str:
    return str(
        station.get("stationname")
        or station.get("name")
        or station.get("stationId")
        or "Мосэкомониторинг"
    )


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
