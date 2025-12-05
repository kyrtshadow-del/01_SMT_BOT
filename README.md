# SMT Local Monitoring

Актуальное состояние на 2025‑12.

## Что это
- Локальный телематический стек (ingest → хранение → WEB/бот) без завязки на внешний кэш Wialon.
- Источники: Wialon IPS, Galileosky (push), можно комбинировать через `PIPELINE_SOURCE_LIST`.
- Все UI слои работают с локальными данными: снапшот, UnitConfig, latest_metrics, события/trips в Postgres.

## Требования
- Python 3.12+, virtualenv `.venv`.
- Postgres (таблицы `events`, `trips`, registry) — `DEVICE_REGISTRY_DSN` или `DATABASE_URL`.
- Redis — курсоры trip_worker и Shadow/Inbox (`REDIS_URL`).

## Быстрый старт
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
WEB_PORT=8000 ./scripts/start_galileosky_stack.sh
```
Порты по умолчанию: web 8000, IPS 18081, Galileosky 8088.

## Поток данных
- `pipeline/cli/run_stream.py` пишет события в `data/pipeline_storage/<YYYY-MM-DD>/events*.jsonl` **и** в Postgres `events` (если задан DSN).
- `pipeline/cli/run_trip_worker.py` читает `events`, хранит состояние в Redis, пишет поездки в `trips`.
- WEB Monitoring v2 (кнопка 📅) использует `/web/api/history/trips|track`, данные берутся из `events/trips`.
- Shadow/Inbox: Redis + `pipeline/api/web_v2.py`; локальные Leaflet ассеты `web/static/vendor/leaflet/` (`L.Icon.Default.imagePath="/static/vendor/leaflet/images/"`).

## Если нет поездок в UI
1. `psql "$DEVICE_REGISTRY_DSN" -c "select count(*) from events;"`
2. `psql "$DEVICE_REGISTRY_DSN" -c "select count(*) from trips;"`
3. Если events только в файлах — импортируй:
   ```bash
   .venv/bin/python scripts/import_events_to_db.py
   ```
4. Убедись, что run_stream и trip_worker запущены и используют один DSN/Redis.

## Полезные команды
- Запуск всего стека: `WEB_PORT=8000 ./scripts/start_galileosky_stack.sh`
- Только web: `PYTHONPATH=. uvicorn pipeline.api.web_app:app --host 0.0.0.0 --port 8000`
- Только ingest: `PYTHONPATH=. .venv/bin/python pipeline/cli/run_stream.py --units cached`
- Trip worker: `PYTHONPATH=. .venv/bin/python pipeline/cli/run_trip_worker.py`
- Импорт событий в БД: `.venv/bin/python scripts/import_events_to_db.py`

## Структура
- `bot_new.py`, `bot/` — Telegram‑бот.
- `pipeline/` — ingest, API, конфиги, trip detector.
- `web/` — фронт Monitoring v2 (HTML/JS/CSS, Leaflet локально).
- `scripts/start_galileosky_stack.sh` — one‑button запуск стека.
- `data/`, `logs/`, `backups/`, `node_modules/` и прочие артефакты — в `.gitignore`.

## Траблшут
- Иконки Leaflet 404 → проверь `web/static/vendor/leaflet/images/` и `imagePath` в `app.js`.
- Порт 8000 занят → скрипт сам убьёт процессы; иначе `lsof -ti :8000 | xargs kill`.
- История пуста при наличии событий → см. раздел «Если нет поездок в UI».
