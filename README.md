# MQTT -> Data Ingest Service

Сервис подписывается на MQTT-топики и сохраняет JSON в таблицы из `schema_timescale.sql`.

## Что делает

- Читает JSON из `broker.emqx.io` (или другого брокера из `.env`).
- Раскладывает payload по таблицам:
  - `plc_state`
  - `device_state`
  - `gas_state`, `gas_sensors`
  - `dust_state`
  - `meteo_state`
  - `ivtm_state`

## Подготовка

```powershell
cd eco_monitoring_mqtt_ingest_service
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Отредактируйте `.env`:

- `DB_DSN` - строка подключения к PostgreSQL/Timescale
- `MQTT_HOST` - адрес MQTT-брокера
- `MQTT_PORT` - порт MQTT-брокера
- `MQTT_TOPIC_AVG5MIN` - топик `devices/data/avg5min`
- `MQTT_TOPIC_AVG20MIN` - топик `devices/data/avg20min`
- `MQTT_CLIENT_ID` - client id MQTT-клиента
- `MQTT_USERNAME` / `MQTT_PASSWORD` - при необходимости
- `MQTT_KEEPALIVE` - keepalive в секундах

Агрегация определяется автоматически по топику:

- `devices/data/avg5min` -> `aggregation_period_min = 5`
- `devices/data/avg20min` -> `aggregation_period_min = 20`

## Запуск

```powershell
python main.py
```
