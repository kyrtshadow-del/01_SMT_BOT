# Event и latest_metrics — контракт данных ingestion

Этот файл фиксирует, **какой формат событий и latest‑среза** мы считаем
«нормой» для всех источников (`wialon`, `wialon_ips`, `galileosky` и т.д.).

Цель: бот, Monitoring v2, анализ и тесты опираются на **единый контракт**,
а адаптеры разных протоколов только приводят входные данные к нему.

---

## 1. Event (pipeline.events.Event)

Python‑модель:

```python
Event(
    unit_id: int,
    device_ts: int,     # unixtime (UTC), когда событие произошло на устройстве
    received_ts: int,   # unixtime (UTC), когда мы его приняли/записали
    latitude: float | None,
    longitude: float | None,
    speed: float | None,   # км/ч
    course: float | None,  # градусы 0–360
    params: Mapping[str, Any],
    source: str,           # "wialon" | "wialon_ips" | "galileosky" | ...
    raw_payload: Mapping[str, Any],
)
```

В файле `events.jsonl` храним `Event.as_dict()`:

```json
{
  "unit_id": 16095,
  "device_ts": 1732213260,
  "received_ts": 1732213263,
  "latitude": 52.133928,
  "longitude": 42.089836,
  "speed": 12.0,
  "course": 349.0,
  "params": {
    "pwr_ext": 12.24,
    "inputs_status": 12,
    "outputs_status": 0,
    "rs485_fls12": 3970.0,
    "rs485_fls22": 3773.0,
    "altitude_m": 180,
    "satellites": 7,
    "hdop": 0.6,
    "gsm_status": 3,
    "gsm_level": 3,
    "gsm_roaming": false,
    "dev_status": 14849,
    "dev_status_flags": {
      "ignition": true,
      "gps_ok": true
    }
  },
  "source": "wialon_ips",
  "raw_payload": {
    "...": "сырой вид пакета / токены / hex"
  }
}
```

Ключевые договорённости:

- `device_ts`/`received_ts` — всегда секундный unixtime (UTC).
- `latitude`/`longitude` — градусы в WGS‑84 (десятичные), `None`, если координата явно некорректна или отсутствует.
- `speed` — км/ч; `course` — градусы 0–360; если нет данных — `None`.
- Всё, что относится к навигации, питанию, сенсорам и флагам, кладём в `params`.
- `raw_payload` — максимально близкий к источнику вид данных (строка пакета, список токенов, TLV‑словарь и т.п.).

---

## 2. latest_metrics.json

Файл `data/pipeline_storage/latest_metrics.json` — быстрый срез по unit_id:

```json
{
  "16095": {
    "device_ts": 1732213260,
    "received_ts": 1732213263,
    "lat": 52.133928,
    "lon": 42.089836,
    "speed": 12.0,
    "course": 349.0,
    "params": {
      "...": "те же ключи, что в Event.params"
    }
  }
}
```

Правила:

- Ключ верхнего уровня — строковый `unit_id`.
- Внутренний словарь 1:1 соответствует последнему `Event` по этому юниту:
  - `device_ts`/`received_ts` — скопированы из события;
  - `lat`/`lon`/`speed`/`course` — скопированы из полей Event;
  - `params` — копия `Event.params`.
- Источник (`source`) в latest‑срез не сохраняется; при необходимости его можно восстановить по событиям.

TTL:

- В `LatestTelemetryStore` есть `LATEST_METRICS_TTL_SEC` (по умолчанию 2 дня).
- При каждом сохранении старые записи, у которых `device_ts`/`received_ts` старше TTL, удаляются.

---

## 3. Обязательные имена и единицы в params

Чтобы карточка/бот/отчёты работали одинаково для разных протоколов, адаптеры
должны использовать единые ключи и единицы измерения:

- Навигация и качество:
  - `altitude_m` — высота, метры.
  - `satellites` — число спутников (int).
  - `hdop` — HDOP в исходных единицах (как правило, дробное число).
- Питание:
  - `pwr_ext` — внешнее питание, Вольты (float).
  - `pwr_int` — питание терминала/АКБ, Вольты (float).
- Дискреты:
  - `inputs_status` — битовая маска входов (int).
  - `outputs_status` — битовая маска выходов (int).
- Счётчики:
  - `mileage_m` / `odometer_m` — пробег, метры.
- RS‑485 топливо и температура:
  - `rs485_flsXX` — сырые/приведённые значения уровня топлива (float; л или «сырые единицы» — зависит от конкретного профиля).
  - `rs485_tX` — температура ДУТа/датчика, °C.
- Драйверы и статусы устройства:
  - `ibutton_code` — идентификатор ключа.
  - `dev_status` — битовая маска статуса терминала (int), при наличии:
    - `dev_status_flags` — предрасшифровка (`ignition`, `gps_ok`, `accel_alarm`…).
  - `gsm_status` — сырые флаги GSM; дополнительно:
    - `gsm_level` — уровень сигнала (0–…);
    - `gsm_roaming` — bool.

Если источник даёт нестандартные имена (например, `pwr_ext_v`, `fuel_lvl1`), адаптер обязан
перемаппить их в общие ключи.

---

## 4. Поведение адаптеров источников

Все адаптеры (`WialonStreamAdapter`, `WialonIPSStreamAdapter`,
`GalileoskyStreamAdapter` и будущие) должны:

1. Строить только `Event` в описанном выше виде.
2. Сбрасывать очевидно невалидные значения:
   - координаты с абс. величиной >90/180 или практически 0 → `None`;
   - скорость с явным выбросом (ограничения задаются в адаптере);
   - очень старые/будущие `device_ts` могут отфильтровываться или помечаться в `params`.
3. Не дублировать поля: всё, что логически относится к сенсорам/флагам/качеству, кладётся в `params`.
4. По возможности заполнять `raw_payload` для диагностики (строка пакета, TLV‑словарь, токены IPS и т.п.).

Этот документ считаем «истиной по умолчанию» для всех изменений вокруг ingestion.
