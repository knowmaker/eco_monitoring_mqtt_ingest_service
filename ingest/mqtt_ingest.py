import json
import logging
import os
from typing import Dict

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

from .db_writer import MqttIngestDbWriter


def build_topic_aggregation_map(topic_avg5: str, topic_avg20: str) -> Dict[str, int]:
    topic_aggregation_map: Dict[str, int] = {}
    if topic_avg5:
        topic_aggregation_map[topic_avg5] = 5
    if topic_avg20:
        topic_aggregation_map[topic_avg20] = 20
    return topic_aggregation_map


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    db_dsn = os.getenv("DB_DSN")
    if not db_dsn:
        raise SystemExit("DB_DSN is required in environment.")

    ingest_writer = MqttIngestDbWriter(db_dsn=db_dsn)

    mqtt_host = os.getenv("MQTT_HOST", "broker.emqx.io")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_topic_avg5 = os.getenv("MQTT_TOPIC_AVG5MIN", "devices/data/avg5min")
    mqtt_topic_avg20 = os.getenv("MQTT_TOPIC_AVG20MIN", "devices/data/avg20min")
    mqtt_client_id = os.getenv("MQTT_CLIENT_ID", "eco-monitoring-mqtt-ingest-writer")
    mqtt_username = os.getenv("MQTT_USERNAME")
    mqtt_password = os.getenv("MQTT_PASSWORD")
    mqtt_keepalive = int(os.getenv("MQTT_KEEPALIVE", "60"))

    topic_aggregation_map = build_topic_aggregation_map(mqtt_topic_avg5, mqtt_topic_avg20)
    if not topic_aggregation_map:
        raise SystemExit("Set at least one MQTT topic in MQTT_TOPIC_AVG5MIN / MQTT_TOPIC_AVG20MIN.")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=mqtt_client_id)
    if mqtt_username:
        client.username_pw_set(mqtt_username, mqtt_password)

    def on_connect(_client, _userdata, _flags, reason_code, _properties):
        if reason_code == 0:
            subscribed_topics = list(topic_aggregation_map.keys())
            logging.info("Connected to MQTT %s:%s; subscribing to %s", mqtt_host, mqtt_port, subscribed_topics)
            for topic in subscribed_topics:
                _client.subscribe(topic)
        else:
            logging.error("MQTT connect failed: reason_code=%s", reason_code)

    def on_message(_client, _userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("MQTT payload root must be JSON object.")
            if msg.topic not in topic_aggregation_map:
                logging.warning("Ignoring message from unexpected topic=%s", msg.topic)
                return
            aggregation_period_min = topic_aggregation_map[msg.topic]
            ingest_writer.write_payload(payload, aggregation_period_min=aggregation_period_min)
        except Exception:
            logging.exception("Failed to process MQTT message from topic=%s", msg.topic)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(mqtt_host, mqtt_port, mqtt_keepalive)
        client.loop_forever()
    finally:
        ingest_writer.close()
