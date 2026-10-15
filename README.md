# MQTT -> Data Ingest Service

Сервис подписывается на индивидуальный MQTT-топик каждого брокера и сохраняет JSON в таблицы из `schema_timescale.sql`.

## Что делает

- Читает JSON из MQTT-брокеров, заданных в локальном `.env`.
- В 30, 40 и 50 минут каждого часа получает концентрации CO, NO, NO2, SO2, O3, H2S, PM10 и PM2.5 от Мосэкомониторинга за предыдущий час.
- Для каждого брокера использует свой `MQTT_BROKER_N_TOPIC`.
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
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Конфигурация

Все значения подключения хранятся только в локальном `.env`.
Обязательные и поддерживаемые переменные:

- `DB_DSN`
- `MQTT_BROKER_COUNT`
- `MQTT_BROKER_N_NAME`
- `MQTT_BROKER_N_HOST`
- `MQTT_BROKER_N_PORT`
- `MQTT_BROKER_N_TOPIC`
- `MQTT_BROKER_N_CLIENT_ID`
- `MQTT_BROKER_N_USERNAME`
- `MQTT_BROKER_N_PASSWORD`
- `MQTT_BROKER_N_KEEPALIVE`

`N` - номер брокера от `1` до `MQTT_BROKER_COUNT`. Брокер с пустым `MQTT_BROKER_N_HOST` пропускается.
Relay Мосэкомониторинга использует хост из `MQTT_BROKER_1_HOST` и порт `8765`.

## Запуск

```powershell
python main.py
```
