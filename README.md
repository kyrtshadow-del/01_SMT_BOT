# SMT Local Bot / SMT Local Monitoring

## Быстрый запуск Galileosky‑стека (туннель + stream + bot)
1. Подготовить SSH‑ключ с доступом к VPS (публичный IP) и убедиться, что в `sshd_config` на VPS включено `GatewayPorts clientspecified`.
2. На локальной машине в корне проекта выполнить:
   ```
   TUNNEL_HOST=77.232.134.54 TUNNEL_USER=root ./scripts/start_galileosky_stack.sh
   ```
   Скрипт сам:
   - убивает процессы/занятый 8088 локально и на VPS;
   - поднимает autossh `-R 0.0.0.0:8088:localhost:8088` (порт открыт наружу);
   - запускает `run_all` в режиме `PIPELINE_SOURCE_KIND=galileosky` (бот + stream + snapshot).
3. Проверка: `tail -f logs/galileosky_ingest.log` — должны идти новые строки; `ls data/pipeline_storage/<дата>/events.jsonl` — появляются события.
4. На стороне ретранслятора включить **полный бинарный Galileosky** (с навигационным тегом 0x30). Пока ретранслятор отдаёт урезанные кадры без координат, lat/lon не пишутся. При появлении 0x30 парсер автоматически начнёт сохранять координаты и RS485.

Единый документ по текущему состоянию проекта на 2025‑11‑22.  
После прочтения должно быть понятно:

- что это за система;
- как устроены бот, pipeline и WEB‑Monitoring;
- какие HTTP‑API есть сейчас;
- как всё запускать локально;
- на чём мы остановились и что делать дальше.

---

## 1. Назначение системы

SMT Local — локальная телематическая система, похожая на Wialon:

- забирает данные из Wialon (и в перспективе других источников);
- хранит **локальный снапшот объектов** и **UnitConfig** в файловой структуре;
- предоставляет:
  - Telegram‑бот (поиск, карточка, датчики, сервисные команды);
  - WEB‑Monitoring v2 (список ТС + карта + карточка);
  - pipeline/ingestion (стриминг и latest_metrics без прямой завязки на Wialon‑кэш).

Ключевой принцип: **все UI‑слои работают только на локальных данных**  
(`unit_snapshot.json.gz`, `latest_metrics.json`, `unit_configs/*.json`), Wialon нужен лишь как источник.

---

## 2. Структура репозитория

Основные каталоги и файлы:

- `bot_new.py` — entrypoint Telegram‑бота (ConversationHandler и glue к `bot/` + pipeline).
- `bot/` — инфраструктура бота:
  - `bot/constants.py`, `bot/logging_setup.py`, `bot/stores.py`, `bot/token_store.py` — базовые зависимости;
  - `bot/wialon_client.py` — клиент к Wialon API;
  - `bot/features/*.py` — карточка, поиск, датчики, ДУТы и т.д.
- `pipeline/` — ingestion + локальный API:
- `pipeline/services/unit_snapshot_service.py` — снапшот объектов (`data/unit_snapshot.json.gz`);
- `pipeline/services/storage_service.py`, `pipeline/storage/latest_metrics.py` — сырые события и `latest_metrics.json`;
- `pipeline/config/unit_config.py` — датаклассы UnitConfig;
  - `pipeline/config/unit_config_service.py` — сервис чтения/записи UnitConfig (`unit_configs/*.json`);
  - `pipeline/config/defaults.py` — загрузка настроек из env;
  - `pipeline/api/web_app.py` — FastAPI‑приложение с WEB‑Monitoring v2;
  - `pipeline/api/web_v2.py` — JSON‑API Monitoring (см. раздел API).
- `web/` — фронт Monitoring v2:
  - `web/templates/index.html` — единственный HTML для Monitoring;
  - `web/static/v2/app.js` — логика UI (Leaflet + виртуальный список + карточка);
  - `web/static/v2/app.css` — стили.

### Что нового в Monitoring v2 (UI карточки, 2025‑11‑23)
- Настройки карточки в одном списке: секции включаются/выключаются чекбоксом и переупорядочиваются перетаскиванием за ☰.
- Быстрые «магнитные» зоны «в начало/в конец» при drag, изменения сохраняются сразу (localStorage + `/web/api/settings/panel`), карточка обновляется без перезагрузки.
- Плавная FLIP‑анимация при перестановке секций; фиксированные блоки «Статус» и «Местоположение» всегда сверху и не двигаются.
- Кнопки «Сбросить по умолчанию» и «Закрыть» вынесены в единый футер модалки, список компактный, без лишних стрелок.
- 2025‑11‑24: оптимизации скорости и UI:
  - карточка открывается мгновенно из превью (данные из списка), детали догружаются фоном; кеш деталей 60 с + LRU 300 карточек;
  - `/api/units` отдает `card_preview` (адрес, params<=8), фронт использует его для первого кадра;
  - feed принимает `watch_ids` (видимые/выбранные/worklist) и отдает только нужные обновления;
  - быстрый поиск поддерживает токены через пробел и опциональный поиск по ID (переключатель внутри поля поиска);
  - статусы: 🛑 Остановка, 🅿️ Стоянка, 🚗 Движение; обновлены быстрые фильтры.
  - В параметрах/датчиках скрываются служебные `raw_*/unknown_*/crc_/packet_` поля для чистого UI.
- `data/` — локальные данные (игнорируются в git):
  - `data/unit_snapshot.json.gz` — снапшот объектов;
  - `data/unit_snapshot.meta.json` — метаданные снапшота;
  - `data/unit_configs/*.json` — UnitConfig по unit_id;
  - `data/pipeline_storage/...` — сырые события и `latest_metrics.json`.
- `logs/` — рабочие логи:
  - `logs/unit_snapshot_watch.log` — обновление снапшота;
  - `logs/stream_runner.log` и др.;
- `logs/web_monitoring.log` — JSON‑лог Monitoring (см. ниже).
  - `logs/galileosky_ingest.log` — отдельный лог приёма Galileosky (TCP 8088, туннель через autossh).
  - `logs/galileosky_ingest_health.json` — health‑снимки по Galileosky (events, eps, last_uid).
  - `logs/galileosky_ingest.prom` — текстовый экспорт метрик Galileosky для node_exporter.
  - `logs/wialon_ips_ingest.log` — лог прямого приёма Wialon IPS.
  - `logs/wialon_ips_ingest_health.json` — health‑снимки Wialon IPS (eps, last_uid, synthetic/anomaly).
  - `logs/wialon_ips_ingest.prom` — текстовый экспорт метрик Wialon IPS для node_exporter (textfile).
- `docs/events_schema.md` — контракт `Event` и `latest_metrics.json` для всех источников ingestion.
- `dev-notes.md-old`, `README.utf8.md-old` — история технических правок и прежний README (бот, pipeline, web).
- `HANDOFF_AND_ARCHITECTURE.md-old`, `RECENT_PATCHES.md-old` — старые архитектурные заметки и сводка последних патчей.
- `docs/*.md-old` — детальные схемы UnitConfig, структура Wialon и чек-листы по Monitoring.
- `AGENTS.md` — актуальные правила для подключаемых агентов/инструментов (контекст проекта, не архив).

---

## 3. Процессы рантайма

### 3.1. Pipeline + бот

Обычно запускается одним скриптом:

```bash
source .venv/bin/activate
python3 pipeline/cli/run_all.py
```

Для Wialon IPS (push, порт 8088 по умолчанию):

```bash
source .venv/bin/activate
PIPELINE_SOURCE_KIND=wialon_ips python3 pipeline/cli/run_all.py --units cached
```

Этот запуск:

- поднимает ingestion/streamинг (Wialon → pipeline_storage → latest_metrics);
- обновляет `unit_snapshot.json.gz` и UnitConfig;
- стартует Telegram‑бота (`bot_new.py`).

Мульти‑listener (пример: Wialon IPS + Galileosky одновременно):

```bash
source .venv/bin/activate
PIPELINE_SOURCE_LIST=wialon_ips,galileosky python3 pipeline/cli/run_stream.py --units cached
```

Metrics/alerts:
- Prometheus textfile для IPS и Galileosky: `logs/wialon_ips_ingest.prom`, `logs/galileosky_ingest.prom` (events_total, eps, synthetic_total, anomaly_total).
- Health JSON: `logs/wialon_ips_ingest_health.json`, `logs/galileosky_ingest_health.json`.

### 3.4. Движение / зажигание (Wialon‑подобно)
- Детекция **персонально для каждого юнита** (порог сохраняется в `data/ignition_cache.json`).
- Приоритет источников: `acc/ignition` → `dev_status` бит0 → `inputs_status` (бит `IGNITION_INPUT_BIT` или `advanced.ignition_input_bit` в UnitConfig) → ручные пороги `advanced.ignition_threshold_v_on/off` → авто‑порог по напряжению `pwr_ext`.
- Авто‑порог: берём до 200 событий за 2 дня, делим на «движение» (speed>3) и «стоянку» (speed≤0.5) только для обучения; если медианы отличаются больше чем `IGNITION_VOLTAGE_DELTA` (по умолчанию 1.0 В), ставим on/off пороги и гистерезис. Если разрыва нет — напряжение не используется.
- Статусы: `moving`, `park_ign_on`, `park_ign_off`, `stopped`, `offline`. Видны чипами в списке и в карточке (не нужно открывать карточку, чтобы понять состояние).
- Напряжение, если приходит как `pwr_ext/pwr_int`, выводится в «Датчики» с двумя знаками после запятой.
- Опциональный HTTP `/health` и `/metrics`: включить `STREAM_HEALTH_HTTP=1` (порт 9100, читает health/metrics из `logs/*.json` и `logs/*.prom`).

### 3.1.1. Настройка poller'а

С 2025‑11‑19 ingestion использует приоритетный планировщик: каждое ТС получает индивидуальный интервал опроса в зависимости от свежести телеметрии. Горячие юниты (последнее сообщение <60 с) запрашиваются каждые ~2 с, «тёплые» (<10 мин) — раз в 10 с, холодные и неактивные — с плавным бэк-оффом до 3 мин. Планировщик построен на куче, учитывает повторные попытки и больше не блокирует остальные токены при единичных 502/429.

Тонкая настройка доступна через env vars (значения по умолчанию указаны в скобках):

- `PIPELINE_POLL_ACTIVE_INTERVAL_SEC` (2.0) — интервал для объектов с «живой» телеметрией.
- `PIPELINE_POLL_WARM_INTERVAL_SEC` (10.0) — опрос «тёплых» объектов.
- `PIPELINE_POLL_COLD_INTERVAL_SEC` (45.0) — базовый интервал для холодных объектов.
- `PIPELINE_POLL_IDLE_INTERVAL_SEC` (180.0) — максимальный интервал простоя.
- `PIPELINE_POLL_ACTIVE_AGE_SEC` / `PIPELINE_POLL_WARM_AGE_SEC` (60 / 600) — границы свежести.
- `PIPELINE_POLL_EMPTY_BACKOFF_SEC` (15.0) — насколько удлинять интервал при последовательных пустых ответах.
- `PIPELINE_POLL_JITTER` (0.15) — распределение нагрузки по времени.
- `PIPELINE_POLL_BATCH_SIZE` (512) — максимум юнитов в одном проходе.

Переменные можно задать перед запуском `pipeline/cli/run_all.py` или standalone `run_stream.py` — поведение poller’а обновится без правок кода.

### 3.2. WEB‑Monitoring v2 (FastAPI + фронт)

Запуск:

```bash
source .venv/bin/activate
PYTHONPATH=. uvicorn pipeline.api.web_app:app --host 0.0.0.0 --port 8000
```

После запуска:

- `/` — заглушка с ссылкой на `/web/monitoring/`;
- `/web/monitoring/` — основная страница Monitoring v2;
- `/static/...` — статика фронта.

---

## 4. Модель данных

### 4.1. UnitSnapshot (`data/unit_snapshot.json.gz`)

Хранит:

- список юнитов по id (`items_by_id`);
- базовые поля: `nm` (name), `device.uid/hardware`, `contacts`, `sensors/sens`, `meta` и т.д.

Используется:

- для списка объектов в боте и в WEB‑Monitoring (`/web/api/units`);
- как источник region/area, базовых hw/uid, сенсоров если нет UnitConfig.

### 4.2. LatestTelemetry (`latest_metrics.json`)

Формат `unit_id → {device_ts, received_ts, lat, lon, speed, params...}`.

Нужен для:

- онлайн/оффлайн статуса;
- координат, скорости;
- сырых параметров (adc*, acc, flags и т.п.) для карточки.

### 4.3. UnitConfig (`data/unit_configs/*.json`)

Единый конфиг юнита:

- `general` — имя, UID/IMEI, телефоны, hardware;
- `icon` — будущие настройки иконок для WEB;
- `sensors[]` — датчики (тип, формулы, калибровки, validation);
- `counters` — пробег/моточасы/топливо;
- `profile[]`, `extra`, `advanced` — произвольные поля/настройки мониторинга.

Используется:

- ботовской карточкой для расчёта ДУТ и статусов;
- WEB‑карточкой Monitoring v2 для списка датчиков и фильтрации.

### 4.4. Status helpers

`pipeline/engine/status.py`:

- `compute_status(snapshot, latest, unit_config, health_ok=True)` →
  `{online, status_label, reason, age_sec, expected_interval_sec, speed, last_ts}`;
- динамические пороги считаются от `expected_interval_sec` (кеш либо UnitConfig, по умолчанию 300 сек),
  с жёсткими рамками `ONLINE_MIN/MAX_SEC`, `WARN_MIN_SEC`, `CRIT_MIN_SEC`;
- отличает «канал упал» (`reason=no_connection`), «источник мёртв» (`no_source`) и «канал жив, но терминал молчит» (`no_data`);
- если динамики нет, откатывается к фиксированному `MONITORING_ONLINE_SEC` (фолбэк 900 сек).

Движение/остановка/стоянка (операторский взгляд):

- `moving` → «🚗 Движение» (скорость >0, объект едет);
- `stop` + `park_ign_on/park_ign_off` → «🛑 Остановка» (короткий простой до порога `STOP_TO_PARK_SEC`, по ПДД — остановка);
- `stopped` → «🅿️ Стоянка» (длительный простой, длительность `stop_duration_s >= STOP_TO_PARK_SEC`).

#### 4.4.2. Отладка свежести данных (ingestion)

- Для прямых источников (`wialon_ips`, `galileosky`) pipeline принимает **каждое событие по мере поступления**: адаптеры пишут его в `events.jsonl` и сразу обновляют `latest_metrics.json` через `LatestTelemetryStore.update_from_events`.
- Статус в Monitoring считается по `device_ts` (времени в пакете терминала), а не по моменту записи; если ретранслятор отдаёт историю с запозданием, Monitoring честно покажет «Нет данных N мин/ч», даже если TCP‑поток плотный.
- Базовый чек:
  - посмотреть tail `logs/galileosky_ingest_health.json` / `logs/wialon_ips_ingest_health.json` (есть ли события и какой `last_event_ts`);
  - по конкретному `unit_id` найти события в `data/pipeline_storage/YYYY-MM-DD/events-*.jsonl` и сравнить `device_ts` с текущим временем;
  - в `data/pipeline_storage/latest_metrics.json` проверить возраст `device_ts`/`received_ts` для юнита — Monitoring использует именно эти значения.
- Если `device_ts` регулярно отстаёт на 20–40 минут для большинства ТС, корень проблемы на стороне источника (терминал/IPS‑ретранслятор), а не в локальном приёме.

#### 4.4.1. Статусы связи и актуальности (операторский взгляд)

В Monitoring v2 оператор видит **одну строку статуса** по объекту, в которую уже свернута информация о связи и давности данных. Базовые варианты:

- `Онлайн · обновлено N мин назад` — есть связь с источником, последнее сообщение достаточно свежее.
- `Нет связи N мин/ч` — считаем объект оффлайн (по времени с последнего сообщения и настройкам порогов).
- `Онлайн, нет данных N мин` — источник жив, но терминал давно не отправлял телеметрию (важно отличать от падения ingestion).
- `Нет связи N ч` — давно нет телеметрии; объект в глубоком оффлайне.

Точные правила расчёта порогов и статусов описаны в `docs/ARCHITECTURE_ROADMAP.md`, а мотивация и сравнение с Wialon — в `docs/ARCHITECTURE_DIALOG.md`.

Кеш ожидаемых интервалов сообщений хранится в `data/expected_interval.json` и пересчитывается утилитой:

```bash
python3 -m pipeline.cli.recalc_expected_intervals --window-days 2 --recalc-period-hours 12
# или обёртка
./scripts/recalc_expected_intervals.sh
```

Пример systemd‑таймера (лежит в `scripts/recalc_expected_intervals.service.sample` и `.timer.sample`):

```bash
sudo cp scripts/recalc_expected_intervals.service.sample /etc/systemd/system/recalc_expected_intervals.service
sudo cp scripts/recalc_expected_intervals.timer.sample /etc/systemd/system/recalc_expected_intervals.timer
sudo systemctl daemon-reload
sudo systemctl enable --now recalc_expected_intervals.timer
```

Таймер гоняет пересчёт каждые 30 минут, сам сервис берёт `MONITORING_ONLINE_SEC` как порог «живого» окна и ограничивает до 200 юнитов за прогон.
Быстрая установка одной командой: `sudo scripts/install_expected_timer.sh` (копирует sample unit и включает таймер).

Без sudo (user‑level systemd), если запущен `systemctl --user`:

```bash
scripts/install_expected_timer_user.sh
# проверка
systemctl --user status recalc_expected_intervals.timer
```

Кратко про пороги статуса:
- expected_interval_sec: дефолт 300 с (UnitConfig → кеш → дефолт), clamp [60, 900].
- online_dynamic_sec = clamp(k1 * expected, min=ONLINE_MIN_SEC=300, max=ONLINE_MAX_SEC=1200).
- stale_warn_sec = max(WARN_MIN_SEC=600, k2 * expected), stale_crit_sec = max(CRIT_MIN_SEC=10800, k3 * expected).
- reason: `ok` / `no_data` (источник жив, данных давно нет) / `no_connection` (оффлайн по порогу) / `no_source` (health ingest красный).

Фильтры Monitoring v2 опираются на те же статусы:
- Связь: `Онлайн`, `Онлайн, нет данных` (reason=`no_data`), `Оффлайн` (по `online`/`reason`);
- Движение: `Движение` (`moving`), `Остановка` (`stop`/`park_ign_*`), `Стоянка` (`stopped`);
- Зажигание: «Зажигание вкл» (`ignition=true`) / «Зажигание выкл» (`ignition=false`), без изменения того, как ключ показывается в строке объекта.

Фронт автоматически использует новый `status_label`, отдельные настройки для диспетчера не нужны.

---

## 5. WEB‑Monitoring v2: API

Все эндпоинты под префиксом `/web`.

### 5.1. Статика и HTML

- `GET /web/monitoring/` → HTML (шаблон `index.html`):
  - прокидывает `static_version` (mtime файлов `app.js/app.css`) →  
    браузер всегда подхватывает свежую статику с `?v=...`.
- `GET /static/...` → CSS/JS/картинки.

### 5.2. Авторизация и worklist

Авторизация в WEB‑Monitoring теперь всегда строится на login/password:

```http
POST /web/api/login
Content-Type: application/json
Body: {"login": "admin", "password": "secret123"}
→ {"session_id": "<uuid>"}
```

- Клиент сохраняет `session_id` в `localStorage` (`smt_session_v2`).
- Все дальнейшие запросы передают заголовок `X-Session-Id`.

Как создать первого пользователя (bootstrap):

```bash
source .venv/bin/activate
PYTHONPATH=. python3 -m pipeline.cli.bootstrap_hierarchy --ensure-root "Компания"
PYTHONPATH=. python3 -m pipeline.cli.bootstrap_hierarchy --create-user admin --password secret123 --display-name "Админ" --admin --node-id 1
```

- Первый запуск `--ensure-root` создаст корневой узел (обычно `id=1`).
- `--create-user` добавит пользователя `admin` с правами администратора и привязкой к этому узлу.

Worklist (рабочий список):

- `GET /web/api/worklist` → `[unit_id]`
- `POST /web/api/worklist` → добавить юниты (`{"unit_ids":[...]}`)
- `PUT /web/api/worklist` → заменить списком
- `DELETE /web/api/worklist/{id}` → удалить из worklist

### 5.3. Список юнитов

```http
GET /web/api/units?q=&online=&has_fuel=
→ List[UnitListItem]
```

`UnitListItem`:

- `id`, `name`, `reg_number`, `uid`, `hw`, `region`;
- `online` (bool), `status` (`moving/stopped/offline`), `status_label` (строка);
- `speed`, `last_ts`, `last_ts_age_sec`;
- координаты `lat/lon`;
- `has_fuel` (наличие ДУТ по UnitConfig/снапшоту);
- `offline_reason` (например, «Нет телеметрии (latest_metrics)», «Нет данных 1.5 ч»);
- `sensor_tags` (имена/типы сенсоров для фильтрации);
- `tooltip_data` — компактный блок для hover (статус, время, адрес, геозоны).

Фильтры:

- `q` — по name/uid (частичное совпадение);
- `online=true/false`;
- `has_fuel=true/false`.

### 5.4. Детальная информация (карточка)

```http
GET /web/api/units/{unit_id}
→ UnitDetail
```

`UnitDetail`:

- `item: UnitListItem` — те же поля, что в списке;
- `snapshot` — словарь из снапшота (если есть);
- `latest` — словарь из latest_metrics (если есть);
- `unit_config` — словарь UnitConfig (если найден);
- `status_details` — результат `compute_status`;
- `tooltip_data` — как в списке;
- `card_data` — уже собранные блоки для UI:
  - `status`, `location`, `counters`;
  - `sensors[]` (первые N сенсоров из UnitConfig или снапшота);
  - `connectivity` (UID, hardware, phones);
  - `params[]` — первые N параметров из `latest.params`;
  - `profile`, `custom_fields`, `drivers`, `trailers`, `passengers`.
- `filter_options`:
  - `sensors` — список имён/типов сенсоров (`["ДУТ1","ДУТ",...]`);
  - `params` — список названий параметров (`["adc1","pwr_ext",...]`);
  - `statuses` — `["online","offline"]`.

### 5.5. Live‑feed (онлайн‑обновление)

```http
GET /web/api/units/feed?since=<timestamp>
→ { ts, updates[], reset }
```

- `ts` — актуальный `mtime` `latest_metrics.json`.
- `updates[]` — облегчённые записи по юнитам:  
  `{id, online, status, status_label, last_ts, last_ts_age_sec, lat, lon, speed, has_fuel, offline_reason}`.
- `reset=true` — фронту стоит перезапросить `/web/api/units`.

Фронт:

- Раз в 10 секунд опрашивает feed;
- Обновляет `units[]` и маркеры;
- Перерисовывает список и, при необходимости, открытую карточку.

### 5.6. Настройки панели/карточки

```http
GET /web/api/settings/panel
PUT /web/api/settings/panel
```

`PanelSettingsDTO`:

- `tabs: ViewConfig[]` — список вкладок Monitoring:
  - **по умолчанию всегда есть**:  
    `{id:"work", name:"Рабочий", filters:{worklist:true}}`
- `active_tab_id` — id активной вкладки (по умолчанию `"work"`);
- `card_sections` — какие блоки карточки показывать (`status`, `location`, `sensors`, ...).

Сейчас:

- вкладка «Все» полностью убрана;
- базовый сценарий — вкладка «Рабочий» + пользовательские вкладки, сохраняемые на сервере.

### 5.7. Клиентские логи

```http
POST /web/api/logs/client
Body: {"event": "...", "message": "...", "detail": {...}}
```

Используется фронтом для:

- `card_select`, `card_detail_loaded`, `card_detail_failed`, `card_render_error`, `card_hidden`.
- Логи пишутся в `logs/web_monitoring.log` (текстом JSON‑строк).

### 5.8. Actions по юниту

```http
POST /web/api/units/{unit_id}/actions/latest-event
→ {"status":"ok","result":{day,device_ts,received_ts,lat,lon,speed}}
```

Используется кнопкой «📡» в карточке:

- показывает пользователю время и координаты последнего сообщения (по `latest_metrics` или raw событиям).

---

## 6. WEB‑Monitoring v2: поведение UI

(файл `web/static/v2/app.js`)

Основные элементы:

- Вверху: логотип, заголовок «Мониторинг v2 (beta)», шестерёнка настроек карточки, кнопка «⟳ Обновить».
- Слева:
  - поле поиска по имени/UID;
  - кнопка `+ Добавить` (модалка выбора объектов для Worklist);
  - вкладки над поиском: **Рабочий** + пользовательские;
  - виртуальный список объектов.
- Справа: карта (Leaflet + MarkerCluster).

### 6.1. Виртуальный список

- Высота строки фиксирована (`ROW_H=60`), используется DOM‑пул.
- При скролле обновляет только видимые строки.
- При клике:
  - выбирает юнит (`selectedId`);
  - загружает `UnitDetail`;
  - открывает карточку под соответствующей строкой;
  - подсвечивает строку (`.unit-row.is-selected`).

### 6.2. Карточка объекта (inline)

Текущее поведение:

- Карточка всегда встраивается в левый список, **под выбранной строкой**.
- Содержит:
  - шапку (имя, госномер, итоговый статус, чипы ДУТ, кнопки действий);
  - компактные секции «Статус и движение», «Местоположение», «Датчики», «Подключение», «Параметры» (и остальные включённые блоки).
- Шапка:
  - имя занимает всю ширину блока (перенос по словам);
  - госномер под именем (если есть);
  - под ними — строка с текстом офлайн‑причины;
  - справа — иконки:
    - `📍` — центрировать карту на объекте;
    - `📡` — запросить «последнее сообщение» (через actions API);
    - `⚙️` — открыть настройки карточки (чекбоксы блоков);
    - `✕` — закрыть карточку.

Карточка можно закрыть:

- кликом по `✕`;
- если выбранный юнит исчез из списка при фильтрации — карточка автоматически скрывается.

---

## 7. Telegram‑бот (high‑level)

Файл `bot_new.py` и пакет `bot/`:

- Команды:
  - старт, помощь;
  - поиск объектов;
  - карточка (полная информация + датчики + ДУТ);
  - настройки датчиков, добавление ДУТ, тарировка (через UnitConfig).
- Взаимодействие с WEB:
  - выдаёт ссылку `/web/monitoring/?token=<one-time>`  
    (в Monitoring токен сохраняется в сессии).

Логика бота сейчас максимально переведена на локальные данные (UnitSnapshot, UnitConfig, latest_metrics), Wialon нужен только для ingestion и некоторых сервисных действий.

---

## 8. Конфигурация и окружение

### 8.1. Основные env‑переменные

- `PIPELINE_STORAGE_ROOT` — корень pipeline‑хранилища (`data/pipeline_storage` по умолчанию).
- `PIPELINE_SOURCE_KIND` — источник данных (`wialon` по умолчанию).
- `PIPELINE_WIALON_TOKEN` / `WIALON_TOKEN` — основной токен Wialon.
- `PIPELINE_WIALON_EXTRA_TOKENS` / `WIALON_EXTRA_TOKENS` — дополнительные токены для параллельной выгрузки.
- `PIPELINE_WIALON_HOST` / `WIALON_LOGIN_HOST` — URL Wialon Hosting.
- `MONITORING_ONLINE_SEC` — порог для определения online/offline (по умолчанию 600 сек).
- `PYTHONPATH` — должен указывать на корень проекта при запуске CLI/uvicorn.

### 8.2. Логи

- `logs/web_monitoring.log`:
  - события `list_units`, `card_select`, `card_detail_loaded/failed`, `card_render_error`;
  - полезно для диагностики WEB‑Monitoring (особенно карточки).
- `logs/stream_runner.log` — состояние ingestion;
- `logs/unit_snapshot_watch.log` — обновление снапшота.

---

## 9. Текущее состояние (2025‑11‑22)

Закрыто:

- WEB‑Monitoring v2 работает от локальных данных (UnitSnapshot + latest_metrics + UnitConfig).
- Живой список с виртуализацией и вкладками, базовая вкладка только «Рабочий».
- Live‑обновление данных через `/web/api/units/feed`.
- Карточка единообразно собирается на backend (UnitDetail) и встраивается в список.
- Логи веб‑карточки и действий пишутся в `logs/web_monitoring.log` (через `/web/api/logs/client`).
- Ingestion‑ядро:
  - прямой приём Wialon IPS стабилизирован (анализ tail‑параметров, флаги `gsm_level/gsm_roaming`, `dev_status_flags`, аномалии скорости/времени);
  - Galileosky‑listener принимает бинарные пакеты, нормализует их в `Event` и пишет в `events.jsonl`/`latest_metrics`;
  - контракт `Event/latest_metrics` вынесен в `docs/events_schema.md` и реализован во всех адаптерах;
  - health/metrics для обоих источников доступны через `logs/*_ingest_health.json` и `logs/*_ingest.prom` (+ опциональный HTTP `/health` и `/metrics`).
- Авторизация и права:
  - Monitoring больше не использует токены Wialon/бота — вход только по логину/паролю (`/web/api/login`), сессии лежат в `data/web_v2_sessions`;
  - добавлена база иерархии/прав `data/web_admin.sqlite3` (`nodes`, `users`, `units_meta`, `groups`), есть CLI для создания корневого узла («Компания»/«Доминант») и пользователей (`bootstrap_hierarchy`);
  - backend учитывает `user_id/node_id/is_admin` при выдаче `/web/api/units` и карточек, фронт всегда создаёт сессию через login‑модал.

В процессе / TODO:

1. **Tooltip/карточка:**
   - добавить больше параметров (топливо, ignition, ключевые сырые значения);
   - доработать hover по образцу Wialon (включая геозоны и важные датчики).
2. **Вкладки и фильтры:**
   - привести модалку «Новая вкладка» и фильтры в строго формализованный вид (единый объект фильтра);
   - поддержать автодополнение по регионам (геокодер).
3. **Настройки пользователя:**
   - вынести настройки карточки/tooltip/списка в отдельный `view_settings` (per user);
   - дать пользователю управлять порядком и видом секций.
4. **Иконки ТС:**
  - реализовать группировку по типам (легковые/грузовые/сельхоз/спецтехника);
  - загрузка и применение иконок (flaticon или свои).
5. **Аналитика и «анализ сливов»:**
  - новая реализация поверх pipeline (detector), в будущем отдельная вкладка/секция.
6. **Онлайн‑юниты Galileosky/Wialon IPS:**
   - удостовериться, что при запуске `scripts/start_galileosky_stack.sh` и/или `run_stream.py` health‑логи регулярно обновляются и `latest_metrics.json` наполняется свежими событиями — после очистки старых снапшотов система ждёт реального потока.

---

## 10. Как использовать этот файл

- Это единый источник правды по архитектуре и API Monitoring v2 + общему устройству проекта на конец 2025‑11‑22.
- Остальные `.md-old` оставлены как архив истории и справочники (UnitConfig schema, структура Wialon `.wlp` и т.п.).  
  Для повседневной разработки достаточно этого файла.
