# Registry/Shadow enablement (Postgres + Redis)

## Quick start (local, docker compose)

1. Ensure Docker/Compose is available.
2. From repo root:

```bash
docker compose up -d redis postgres  # uses docker-compose.yml we added
```

3. Create `.env` (or export env vars) with:

```
REDIS_URL=redis://127.0.0.1:6379/0
DEVICE_REGISTRY_ENABLED=1
DEVICE_REGISTRY_DSN=postgresql://smt_user:smt_password@127.0.0.1:5432/smt_telematics
```

4. Перезапусти стек:

```bash
SKIP_TUNNEL=1 ./scripts/start_galileosky_stack.sh
```

5. Проверки:
   - `logs/run_all_galileosky.log` содержит `device_registry: loaded <N> entries`.
   - В Redis: `redis-cli keys shadow:device:wialon_ips:*` — видны неизвестные IMEI.
   - Файл `data/pipeline_storage/latest_metrics.json` появляется и обновляется.

## Массовый импорт парка в Registry

CLI: `python -m pipeline.cli.import_fleet path/to/fleet.csv [--dsn ...] [--delimiter ;] [--priority 0]`

Формат строк: `Unit Name;protocol;uid[;priority]` (поддерживаются `;`, `,`, таб). Скрипт идемпотентен: создаёт записи в `units`, `devices`, `unit_device_links` при отсутствии; повторный запуск дубликатов не добавит. DSN берётся из `DEVICE_REGISTRY_DSN`/`DATABASE_URL`/`PG*` env, если `--dsn` не задан.

## Postgres schema (минимум для DeviceRegistry)

Требуются таблицы: `devices (protocol, uid, hardware, firmware)`, `units (id, sensors_config jsonb)`, `unit_device_links (device_id, unit_id, priority, valid_to)`.
Пример SQL:

```sql
CREATE TABLE IF NOT EXISTS devices (
  id serial PRIMARY KEY,
  protocol text NOT NULL,
  uid text NOT NULL,
  hardware text,
  firmware text,
  UNIQUE (protocol, uid)
);

CREATE TABLE IF NOT EXISTS units (
  id serial PRIMARY KEY,
  name text,
  sensors_config jsonb
);

CREATE TABLE IF NOT EXISTS unit_device_links (
  id serial PRIMARY KEY,
  device_id int NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  unit_id int NOT NULL REFERENCES units(id) ON DELETE CASCADE,
  priority int NOT NULL DEFAULT 0,
  valid_to timestamptz,
  UNIQUE (device_id, unit_id, valid_to)
);
```

Заполни `devices`/`units`/`unit_device_links` по своим данным; DeviceRegistry читает эти поля один раз на старте.

## Запуск без туннеля

Для локальных тестов достаточно: `SKIP_TUNNEL=1 ./scripts/start_galileosky_stack.sh` — скрипт сам загрузит `.env` и убедится, что порты свободны.
