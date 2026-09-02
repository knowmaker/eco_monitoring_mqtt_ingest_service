import json
import logging
import os
import time

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

from .db_writer import MqttIngestDbWriter


DEFAULT_CLIENT_ID = "eco-monitoring-mqtt-ingest-writer"


def load_broker_configs() -> list[dict[str, str | int | None]]:
    broker_configs: list[dict[str, str | int | None]] = []

    broker_count = int(os.getenv("MQTT_BROKER_COUNT", "3"))
    for index in range(1, broker_count + 1):
        prefix = f"MQTT_BROKER_{index}"
        host = os.getenv(f"{prefix}_HOST", "").strip()
        if not host:
            continue
        topic = os.getenv(f"{prefix}_TOPIC", "").strip()
        if not topic:
            raise SystemExit(f"{prefix}_TOPIC is required when {prefix}_HOST is set.")

        broker_configs.append(
            {
                "name": os.getenv(f"{prefix}_NAME", f"broker-{index}"),
                "host": host,
                "port": int(os.getenv(f"{prefix}_PORT", "1883")),
                "topic": topic,
                "client_id": os.getenv(f"{prefix}_CLIENT_ID", f"{DEFAULT_CLIENT_ID}-{index}"),
                "username": os.getenv(f"{prefix}_USERNAME", "").strip() or None,
                "password": os.getenv(f"{prefix}_PASSWORD"),
                "keepalive": int(os.getenv(f"{prefix}_KEEPALIVE", "60")),
            }
        )

    return broker_configs


def build_client(config: dict[str, str | int | None], ingest_writer: MqttIngestDbWriter) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=str(config["client_id"]))
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    if config["username"]:
        client.username_pw_set(str(config["username"]), str(config["password"] or ""))

    def on_connect(_client, _userdata, _flags, reason_code, _properties):
        if not reason_code.is_failure:
            logging.info(
                "Connected to MQTT %s (%s:%s); subscribing to %s",
                config["name"],
                config["host"],
                config["port"],
                config["topic"],
            )
            _client.subscribe(str(config["topic"]))
        else:
            logging.error("MQTT connect failed for %s: reason_code=%s", config["name"], reason_code)

    def on_message(_client, _userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("MQTT payload root must be JSON object.")
            ingest_writer.write_payload(payload)
        except Exception:
            logging.exception(
                "Failed to process MQTT message from broker=%s topic=%s",
                config["name"],
                msg.topic,
            )

    client.on_connect = on_connect
    client.on_message = on_message
    return client


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    db_dsn = os.getenv("DB_DSN")
    if not db_dsn:
        raise SystemExit("DB_DSN is required in environment.")

    ingest_writer = MqttIngestDbWriter(db_dsn=db_dsn)

    broker_configs = load_broker_configs()
    if not broker_configs:
        raise SystemExit("Set at least one MQTT broker in MQTT_BROKER_1_HOST.")

    clients = [build_client(config, ingest_writer) for config in broker_configs]
    try:
        for config, client in zip(broker_configs, clients):
            logging.info("Connecting to MQTT %s (%s:%s)", config["name"], config["host"], config["port"])
            client.connect_async(str(config["host"]), int(config["port"]), int(config["keepalive"]))
            client.loop_start()

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping MQTT ingest service")
    finally:
        for client in clients:
            client.loop_stop()
            client.disconnect()
        ingest_writer.close()
