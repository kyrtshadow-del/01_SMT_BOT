# План по протоколам и статус работ (Wialon IPS / Galileosky)

## Цель
Принять поток телеметрии через прямые протоколы, писать в локальное хранилище и выводить в бот/WEB без ручной настройки.

## Текущее состояние (2025‑12‑02)
- ✅ Врезан RawPacket‑хаб в `run_stream.py`; Galileosky и Wialon IPS отдают `RawPacket` в общий конвейер.
- ✅ Разнесены порты: Galileosky 8088, Wialon IPS 18081 (по умолчанию).
- ✅ Авто‑сборка `unit_snapshot.v2` (ленивая + фоновая) убирает 503 на `/web/api/units`.
- ✅ Запись Wialon IPS в файлы исправлена: `data/pipeline_storage/…/events.jsonl` растёт на боевом и тестовом трафике.
- ✅ Поднята локальная инфраструктура Redis + Postgres (`docker-compose`), настроены `REDIS_URL` и `DEVICE_REGISTRY_DSN`.
- ✅ Включён `DeviceRegistry`: при старте stream логирует `device_registry: loaded 3 entries` (три тестовых устройства из Registry).
- ✅ Включён `ShadowService`: при старте есть `shadow_service: enabled url=redis://…`; неизвестные IMEI попадают в Redis (`shadow:device:wialon_ips:*`).
- ⚠️ Для неизвестных `RawPacket` по‑прежнему есть fallback на synthetic unit_id в `run_stream.py` — это временный костыль, планируем его полностью убрать.
- ⚠️ Registry сейчас содержит только тестовые устройства (3 шт.); основной парк (7k+) не импортирован.
- ⚠️ Нет UI‑инструмента в Monitoring v2 для просмотра/привязки устройств из Shadow (работаем через SQL/CLI).

## План «чистого потока» (Registry + Shadow, без synthetic unit_id)

Жёсткое правило: **в архив и latest попадают только устройства, зарегистрированные в Registry**. Все остальные идут в Shadow и не создают `Event`/unit в snapshot.

### Этап 1. Очистка потока в `run_stream.py`

1.1) Для сообщений `RawPacket`:
- при `registry.resolve(protocol, uid)` → найдено: создавать `Event` с `unit_id` из Registry и писать в `RawStorage`/`LatestTelemetryStore`/snapshot;
- при `registry.resolve` → не найдено: отправлять пакет в `ShadowService` и **не** создавать `Event` (никаких synthetic unit_id).

1.2) Удалить `_synthetic_unit_id` и всю логику `synthetic_unit` в ветке `RawPacket` (`run_stream.py`), оставить synthetic только там, где это явно нужно для legacy (если такие места останутся).

1.3) Уже сделано в рамках подготовки:
- Wialon IPS адаптер переведён на `RawPacket` (как Galileosky), synthetic unit не создаёт;
- Shadow/Registry включены, проверено на тестовых IMEI и одном «левом» IMEI из Shadow;
- сырой день `2025‑12‑01` очищен от тестовых synthetic‑записей, snapshot v2 пересобран (теперь в UI только реальные юниты).

**Текущий шаг:** находимся на этапе **1.1–1.2** — переход к чистому потоку без synthetic unit_id в `run_stream.py`.

### Этап 2. Массовый импорт парка в Registry

2.1) Реализовать CLI‑утилиту `pipeline/cli/import_fleet.py`, которая по CSV/TSV:
- создаёт/обновляет записи в `units` (name, sensors_config),
- создаёт/обновляет `devices` (protocol, uid),
- создаёт/обновляет `unit_device_links` (unit_id, device_id, priority),
- работает идемпотентно (можно запускать много раз без дублей).

2.2) Подготовить CSV с полным парком (≈7000 устройств) и прогнать импорт.

2.3) После импорта: Registry при старте должен логировать `device_registry: loaded <N> entries`, а synthetic‑ветка для `RawPacket` не должна срабатывать вообще (для известных IMEI в Shadow не появляются новые ключи).

### Этап 3. Админ‑UI для Registry/Shadow

3.1) В Monitoring v2 добавить экран «Неизвестные устройства» на основе Redis‑ключей `shadow:device:*`.

3.2) Действия оператора:
- привязать устройство к существующему юниту (создание записи в `unit_device_links` + очистка Shadow);
- создать новый юнит + привязка (insert в `units`/`devices`/`unit_device_links`);
- пометить устройство как игнорируемое (опционально, через дополнительный флаг/таблицу).

3.3) После появления UI большинство операций с Registry будут идти через веб, SQL/CLI останутся только для массовых миграций.

### Этап 4. Наблюдаемость и сервисные задачи

4.1) `latest_metrics.json`: убедиться, что файл создаётся/обновляется и используется web‑фронтом для Monitoring v2.

4.2) Health‑эндпоинт stream (`STREAM_HEALTH_HTTP`):
- экспорт health JSON и Prometheus‑метрик по IPS/Galileosky;
- добавить в чек‑листы мониторинга.

4.3) После стабилизации ingestion включить/подключить SensorCalculator/UnitActors поверх чистого потока (отдельный roadmap).

## Проверки (чек-лист)
- `ss -ltnp | grep 18081` → LISTEN на `0.0.0.0:18081`.
- В логах `wialon_ips.ingest` есть `peer=...` строки при реальном трафике.
- `events.jsonl` растёт, `latest_metrics.json` существует.
- Shadow: ключи `shadow:device:wialon_ips:*` в Redis при неизвестных IMEI.
- Registry: `unit_device_links` в PG содержит IMEI с протоколом `wialon_ips`.
