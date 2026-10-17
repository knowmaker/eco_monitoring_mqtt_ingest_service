from __future__ import annotations

import json
import os
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen


UPSTREAM_URL = "https://mosecom.mos.ru/wp-content/themes/moseco/map/stations-new.php"
PROFILE_STATIONS_URL = "https://mosecom.mos.ru/wp-content/themes/moseco/map3d/map-stations.php"
PROFILE_CHART_URL = "https://mosecom.mos.ru/wp-content/themes/moseco/img/temperature-profiler-chart.php"
UPSTREAM_ORIGIN = "https://mosecom.mos.ru"
UPSTREAM_REFERER = "https://mosecom.mos.ru/karta/"
CA_FILE = os.getenv(
    "MOSECOM_CA_FILE",
    "/opt/mosecom-proxy/globalsign-gcc-r6-alphassl-ca-2025.pem",
)
CACHE_TTL_SECONDS = 60
MAX_REQUEST_BYTES = 1024
UPSTREAM_TIMEOUT_SECONDS = 30

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.load_verify_locations(CA_FILE)
CACHE: dict[str, tuple[float, bytes]] = {}
CACHE_LOCK = threading.Lock()


def _fetch_cached(cache_key: str, request: Request) -> bytes:
    now = time.monotonic()
    with CACHE_LOCK:
        cached = CACHE.get(cache_key)
        if cached is not None and cached[0] > now:
            return cached[1]

    with urlopen(
        request,
        timeout=UPSTREAM_TIMEOUT_SECONDS,
        context=SSL_CONTEXT,
    ) as response:
        body = response.read()

    with CACHE_LOCK:
        CACHE[cache_key] = (time.monotonic() + CACHE_TTL_SECONDS, body)
    return body


def fetch_substance(substance_code: str) -> bytes:
    request = Request(
        UPSTREAM_URL,
        data=urlencode(
            {"locale": "ru_RU", "mapType": "air", "element": substance_code}
        ).encode("ascii"),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": UPSTREAM_ORIGIN,
            "Referer": UPSTREAM_REFERER,
            "Accept": "application/json",
            "User-Agent": "eco-monitoring-mosecom-proxy/1.0",
        },
        method="POST",
    )
    body = _fetch_cached(f"substance:{substance_code}", request)

    payload = json.loads(body)
    if not isinstance(payload, dict) or not isinstance(payload.get("able"), list):
        raise ValueError("Unexpected Mosecom response")
    return body


def fetch_profile_stations() -> bytes:
    request = Request(
        PROFILE_STATIONS_URL,
        headers={
            "Accept": "application/json",
            "Referer": UPSTREAM_REFERER,
            "User-Agent": "eco-monitoring-mosecom-proxy/1.0",
        },
    )
    body = _fetch_cached("profile-stations", request)
    payload = json.loads(body)
    if not isinstance(payload, dict) or not isinstance(payload.get("stations"), list):
        raise ValueError("Unexpected Mosecom profiler station response")
    return body


def fetch_profile_chart(station_id: str) -> bytes:
    request = Request(
        f"{PROFILE_CHART_URL}?{urlencode({'station_id': station_id})}",
        headers={
            "Accept": "image/svg+xml",
            "Referer": UPSTREAM_REFERER,
            "User-Agent": "eco-monitoring-mosecom-proxy/1.0",
        },
    )
    body = _fetch_cached(f"profile-chart:{station_id}", request)
    if b"<svg" not in body[:1024]:
        raise ValueError("Unexpected Mosecom profiler chart response")
    return body


class MosecomProxyHandler(BaseHTTPRequestHandler):
    server_version = "MosecomProxy/1.0"

    def do_GET(self) -> None:
        request_url = urlsplit(self.path)
        if request_url.path == "/health":
            self._send_response(200, b'{"status":"ok"}')
            return
        if request_url.path == "/mosecom/profile-stations":
            try:
                body = fetch_profile_stations()
            except Exception as exc:
                self.log_error("Profiler station request failed: %s", exc)
                self._send_response(502, b'{"error":"upstream request failed"}')
                return
            self._send_response(200, body)
            return
        if request_url.path != "/mosecom/profile-chart":
            self.send_error(404)
            return

        station_id = parse_qs(request_url.query).get("station_id", [""])[0]
        if not station_id.isdigit() or int(station_id) <= 0:
            self.send_error(400, "Invalid station_id")
            return
        try:
            body = fetch_profile_chart(station_id)
        except Exception as exc:
            self.log_error("Profiler chart request failed for %s: %s", station_id, exc)
            self._send_response(502, b'{"error":"upstream request failed"}')
            return
        self._send_response(200, body, "image/svg+xml; charset=utf-8")

    def do_POST(self) -> None:
        if self.path != "/mosecom":
            self.send_error(404)
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.send_error(400)
            return
        if content_length <= 0 or content_length > MAX_REQUEST_BYTES:
            self.send_error(400)
            return

        params = parse_qs(self.rfile.read(content_length).decode("ascii"))
        substance_code = params.get("element", [""])[0]
        if not substance_code:
            self.send_error(400, "Missing element")
            return

        try:
            body = fetch_substance(substance_code)
        except Exception as exc:
            self.log_error("Upstream request failed for %s: %s", substance_code, exc)
            self._send_response(502, b'{"error":"upstream request failed"}')
            return

        self._send_response(200, body)

    def _send_response(
        self,
        status: int,
        body: bytes,
        content_type: str = "application/json; charset=utf-8",
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", 8765), MosecomProxyHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
