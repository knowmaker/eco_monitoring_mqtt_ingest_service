from __future__ import annotations

import json
import os
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode
from urllib.request import Request, urlopen


UPSTREAM_URL = "https://mosecom.mos.ru/wp-content/themes/moseco/map/stations-new.php"
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


def fetch_substance(substance_code: str) -> bytes:
    now = time.monotonic()
    with CACHE_LOCK:
        cached = CACHE.get(substance_code)
        if cached is not None and cached[0] > now:
            return cached[1]

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
        with urlopen(
            request,
            timeout=UPSTREAM_TIMEOUT_SECONDS,
            context=SSL_CONTEXT,
        ) as response:
            body = response.read()

        payload = json.loads(body)
        if not isinstance(payload, dict) or not isinstance(payload.get("able"), list):
            raise ValueError("Unexpected Mosecom response")

        CACHE[substance_code] = (now + CACHE_TTL_SECONDS, body)
        return body


class MosecomProxyHandler(BaseHTTPRequestHandler):
    server_version = "MosecomProxy/1.0"

    def do_GET(self) -> None:
        if self.path != "/health":
            self.send_error(404)
            return
        self._send_response(200, b'{"status":"ok"}')

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

    def _send_response(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", 8765), MosecomProxyHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
