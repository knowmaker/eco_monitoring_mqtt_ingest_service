from __future__ import annotations

import json
import logging
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import psycopg


MOSCOW_TIMEZONE = ZoneInfo("Europe/Moscow")
MOSECOM_PROXY_PORT = 8765
PROFILE_ADVISORY_LOCK_ID = 1_935_623_842
PROFILE_COLLECTION_MINUTE = 30
SAME_LOCATION_DISTANCE_METERS = 10.0
EARTH_RADIUS_METERS = 6_371_008.8
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_ATTEMPTS = 3
PROFILE_DATE_FORMAT = "%d.%m.%Y %H:%M"


class MosecomProfileCollector:
    def __init__(self, db_dsn: str) -> None:
        self.db_dsn = db_dsn

    def collect(self, scheduled_at: datetime | None = None) -> int:
        local_now = (scheduled_at or datetime.now(MOSCOW_TIMEZONE)).astimezone(MOSCOW_TIMEZONE)
        complete_before = local_now.replace(minute=0, second=0, microsecond=0)
        inserted_count = 0

        with psycopg.connect(self.db_dsn) as connection:
            with connection.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (PROFILE_ADVISORY_LOCK_ID,))
                if not cur.fetchone()[0]:
                    logging.info("Mosecom profiler collection skipped: another process holds the collector lock")
                    return 0

                stations = _fetch_profile_stations()
                for station in stations:
                    station_id = str(station.get("station_id") or "").strip()
                    latitude = _to_float(station.get("lat"))
                    longitude = _to_float(station.get("lon"))
                    if not station_id or latitude is None or longitude is None:
                        logging.warning("Skipping invalid Mosecom profiler station: %r", station)
                        continue

                    chart = _fetch_profile_chart(station_id)
                    hourly_profiles = _parse_hourly_profiles(chart, complete_before)
                    monitoring_post_id = _resolve_station(
                        cur,
                        station_id=station_id,
                        name=str(station.get("name") or f"Профилемер {station_id}").strip(),
                        latitude=latitude,
                        longitude=longitude,
                    )
                    inserted_count += _store_missing_hours(cur, monitoring_post_id, hourly_profiles)

        logging.info("Inserted %s Mosecom profiler hourly levels", inserted_count)
        return inserted_count


def next_profile_collection_time(now: datetime | None = None) -> datetime:
    local_now = (now or datetime.now(MOSCOW_TIMEZONE)).astimezone(MOSCOW_TIMEZONE)
    collection_time = local_now.replace(
        minute=PROFILE_COLLECTION_MINUTE,
        second=0,
        microsecond=0,
    )
    if collection_time < local_now:
        collection_time += timedelta(hours=1)
    return collection_time


def _fetch_profile_stations() -> list[dict[str, Any]]:
    payload = json.loads(_proxy_request("/mosecom/profile-stations"))
    stations = payload.get("stations") if isinstance(payload, dict) else None
    if not isinstance(stations, list):
        raise ValueError("Unexpected Mosecom profiler station response")
    return [station for station in stations if isinstance(station, dict)]


def _fetch_profile_chart(station_id: str) -> bytes:
    return _proxy_request(f"/mosecom/profile-chart?{urlencode({'station_id': station_id})}")


def _proxy_request(path: str) -> bytes:
    proxy_host = os.getenv("MQTT_BROKER_1_HOST", "").strip()
    if not proxy_host:
        raise RuntimeError("MQTT_BROKER_1_HOST is required for the Mosecom proxy")
    request = Request(
        f"http://{proxy_host}:{MOSECOM_PROXY_PORT}{path}",
        headers={"User-Agent": "eco-monitoring-mosecom-profiler-collector/1.0"},
    )

    last_error: Exception | None = None
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return response.read()
        except Exception as exc:
            last_error = exc
            if attempt < REQUEST_ATTEMPTS:
                logging.warning(
                    "Mosecom profiler request failed (attempt %s/%s): %s",
                    attempt,
                    REQUEST_ATTEMPTS,
                    exc,
                )
                time.sleep(attempt)
    raise RuntimeError("Failed to fetch Mosecom profiler data") from last_error


def _parse_hourly_profiles(
    chart: bytes,
    complete_before: datetime,
) -> dict[int, list[tuple[float, float]]]:
    root = ElementTree.fromstring(chart)
    values: dict[tuple[int, float], list[float]] = defaultdict(list)
    covered_minutes: dict[int, set[int]] = defaultdict(set)

    for element in root.iter():
        if element.attrib.get("class") != "data-element":
            continue
        measured_at = _parse_profile_datetime(element.attrib.get("data-date"))
        if measured_at is None:
            continue
        bucket_start = measured_at.replace(minute=0, second=0, microsecond=0)
        if bucket_start >= complete_before:
            continue
        bucket_ms = int(bucket_start.timestamp() * 1000)
        covered_minutes[bucket_ms].add(measured_at.minute)

        height = _to_float(element.attrib.get("data-height"))
        temperature = _to_float(element.attrib.get("data-val"))
        if height is not None and temperature is not None:
            values[(bucket_ms, height)].append(temperature)

    complete_buckets = {
        bucket_ms
        for bucket_ms, minutes in covered_minutes.items()
        if 0 in minutes and 55 in minutes
    }
    profiles: dict[int, list[tuple[float, float]]] = defaultdict(list)
    for (bucket_ms, height), temperatures in values.items():
        if bucket_ms in complete_buckets:
            profiles[bucket_ms].append((height, sum(temperatures) / len(temperatures)))
    for levels in profiles.values():
        levels.sort(key=lambda item: item[0])
    return dict(sorted(profiles.items()))


def _parse_profile_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, PROFILE_DATE_FORMAT).replace(tzinfo=MOSCOW_TIMEZONE)
    except ValueError:
        return None


def _resolve_station(
    cur,
    *,
    station_id: str,
    name: str,
    latitude: float,
    longitude: float,
) -> int:
    serial = f"mosecom-profile:{station_id}"
    cur.execute(
        """
        SELECT id
        FROM monitoring_posts
        WHERE serial = %s AND active_to IS NULL
        LIMIT 1
        """,
        (serial,),
    )
    row = cur.fetchone()
    if row is not None:
        return row[0]

    latitude_delta = math.degrees(SAME_LOCATION_DISTANCE_METERS / EARTH_RADIUS_METERS)
    longitude_delta = latitude_delta / max(math.cos(math.radians(latitude)), 1e-6)
    cur.execute(
        """
        SELECT id, latitude, longitude
        FROM monitoring_posts
        WHERE active_to IS NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND latitude BETWEEN %s AND %s
          AND longitude BETWEEN %s AND %s
        """,
        (
            latitude - latitude_delta,
            latitude + latitude_delta,
            longitude - longitude_delta,
            longitude + longitude_delta,
        ),
    )
    closest_id = None
    closest_distance = SAME_LOCATION_DISTANCE_METERS
    for post_id, post_latitude, post_longitude in cur.fetchall():
        distance = _distance_meters(latitude, longitude, post_latitude, post_longitude)
        if distance <= closest_distance:
            closest_id = post_id
            closest_distance = distance
    if closest_id is not None:
        return closest_id

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
        RETURNING id
        """,
        (serial, name, latitude, longitude),
    )
    return cur.fetchone()[0]


def _store_missing_hours(
    cur,
    monitoring_post_id: int,
    hourly_profiles: dict[int, list[tuple[float, float]]],
) -> int:
    if not hourly_profiles:
        return 0
    cur.execute(
        """
        SELECT DISTINCT bucket_ms
        FROM mosecom_profile_hourly
        WHERE monitoring_post_id = %s
          AND bucket_ms = ANY(%s)
        """,
        (monitoring_post_id, list(hourly_profiles)),
    )
    existing_buckets = {row[0] for row in cur.fetchall()}
    rows = [
        (monitoring_post_id, bucket_ms, height, temperature)
        for bucket_ms, levels in hourly_profiles.items()
        if bucket_ms not in existing_buckets
        for height, temperature in levels
    ]
    if not rows:
        return 0
    cur.executemany(
        """
        INSERT INTO mosecom_profile_hourly (
            monitoring_post_id,
            bucket_ms,
            height,
            temperature
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (monitoring_post_id, bucket_ms, height) DO NOTHING
        """,
        rows,
    )
    return len(rows)


def _distance_meters(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    delta_latitude = math.radians(latitude_b - latitude_a)
    delta_longitude = math.radians(longitude_b - longitude_a)
    lat_a = math.radians(latitude_a)
    lat_b = math.radians(latitude_b)
    value = (
        math.sin(delta_latitude / 2) ** 2
        + math.cos(lat_a) * math.cos(lat_b) * math.sin(delta_longitude / 2) ** 2
    )
    return 2 * EARTH_RADIUS_METERS * math.asin(math.sqrt(value))


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None
