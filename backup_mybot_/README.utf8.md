# SMT Local Bot / analiz-slivov

## Цель проекта
Построить локальную систему мониторинга (бот + pipeline), которая:
- забирает данные напрямую из Wialon (и в перспективе других источников);
- формирует и использует локальный снапшот объектов (UnitSnapshot) без зависимостей от legacy-кэша;
- предоставляет быстрый поиск/фильтрацию, карточки объектов, аналитику (в т.ч. сливов) на локальной стороне.

## Основные файлы и назначение
- `bot_new.py` — Telegram-бот: команды, карточка объекта, настройки, локальный поиск, интеграция с pipeline/Wialon.
- `pipeline/services/unit_snapshot_service.py` — единый снапшот объектов (`data/unit_snapshot.json.gz`), API для чтения/обновления.
- `pipeline/cli/run_stream.py` (+ `.ps1`) — CLI/скрипты запуска стриминга/ingestion.
- `pipeline/config/unit_config.py` — модель UnitConfig (dataclasses) для унифицированных настроек юнита (общие поля, датчики, калибровки и т.д.).
- `docs/wialon_unit_structure.md` — разбор структуры Wialon-юнита, сенсоров и таблиц расчёта.
- `logs/unit_snapshot_watch.log` — лог перестроек снапшота и получения метаданных устройств.
- `data/unit_snapshot.json.gz` — актуальный локальный снапшот.
- `tests/` — тесты для снапшота/метрик/сервисов.
- `AGENTS.md` — краткое описание проекта и правил для новых агентов.

## Технологии и библиотеки
- Python 3.10+, `asyncio`
- `python-telegram-bot` — бот и хендлеры
- `requests`/`urllib3` — синхронный клиент к Wialon
- `dataclasses`, `typing` — модели/схемы
- `pytest`/`unittest` — тесты
- `gzip`, `json`, `pathlib` — хранение снапшотов и конфигов

## Правила кода и ключевые зависимости
- Стиль PEP8 + type hints; логирование через `logging`.
- Новый функционал работает от `UnitSnapshotService`; `data/units.v1.json.gz` — legacy (не использовать в новой логике).
- Перед крупными правками `bot_new.py` — обязательный бэкап `bot_new.py.bak.YYYYMMDDHHMMSS`.
- Env vars:
  - `PIPELINE_WIALON_TOKEN` — основной токен Wialon
  - `PIPELINE_WIALON_EXTRA_TOKENS` — дополнительные токены (через пробел/запятую) для параллельной выгрузки
  - `PIPELINE_SOURCE_KIND` — источник (`wialon` по умолчанию)
  - `UNIT_DEVICE_*` — параметры батчинга/ретраев для метаданных устройств (см. код `bot_new.py`)
  - `PYTHONPATH` — путь до корня проекта при запуске CLI/скриптов

## Структура проекта (пример)
```
c:\bots\mybot
├─ bot_new.py
├─ AGENTS.md
├─ data/
│  └─ unit_snapshot.json.gz
├─ docs/
│  ├─ wialon_unit_structure.md
│  └─ unit_config_schema.md
├─ logs/
│  └─ unit_snapshot_watch.log
├─ pipeline/
│  ├─ api/
│  ├─ adapters/
│  ├─ cli/
│  ├─ config/
│  └─ services/
├─ tests/
└─ scripts/, OLD VERSION/, ...
```

## Быстрый старт

### Windows PowerShell
- Поток ingestion:
```
cd C:\bots\mybot
$env:PIPELINE_WIALON_TOKEN = 'ТОКЕН'
powershell -ExecutionPolicy Bypass -File "pipeline\cli\run_stream.ps1" -Units cached -Interval 5 -SourceKind wialon
```
- Ребилд снапшота вручную (создаёт/обновляет `data/unit_snapshot.json.gz`):
```
python _refresh_snapshot.py
```

### Ubuntu bash
- Поток ingestion:
```
cd /path/to/mybot
export PIPELINE_WIALON_TOKEN='ТОКЕН'
export PYTHONPATH=$PWD
python -m pipeline.cli.run_stream --units cached --interval 5 --source-kind wialon
```
- Ребилд снапшота:
```
export PYTHONPATH=$PWD
python _refresh_snapshot.py
```

## Что сделано (ядро)
- Единый снапшот (`UnitSnapshotService`) — карточки/поиск читают данные локально из `data/unit_snapshot.json.gz`.
- Получение метаданных устройства (UID/HW):
  - Добавлен флаг `UNIT_FLAGS_DEVICE_META` и включён в `UNIT_FLAGS_STATS` для `core/search_items` — UID/HW приходят сразу.
  - Реализован fallback `_fetch_device_meta_with_batches` — прямые `core/search_item` с ретраями и токен‑воркерами, подробные логи.
  - Поля аккуратно маппятся в запись юнита (`uid`, `hw`, `hardware`, а также `device.uid/hardware`).
- Логи снапшота отражают прогресс/итоги: `rebuild start/done`, `inline device details N/N`, `refreshed`.
- Подготовлено ядро модели UnitConfig:
  - `pipeline/config/unit_config.py` — dataclasses: общие настройки, HW, сенсоры, валидация, калибровка (points XY + segments X a b).
  - `pipeline/adapters/wialon_wlp_import.py` + `pipeline/cli/import_wlp.py` — импорт `.wlp` напрямую в UnitConfig/UnitConfigService.
  - `docs/wialon_unit_structure.md` — анализ структуры Wialon (сенсоры, таблицы расчёта, параметры датчика).
- Локальный поиск ближайших: в хранилище добавлены координаты, базовый поиск с радиусом по умолчанию 100 м.
- Карточка/настройки подхватывают данные из UnitConfig (импорт `.wlp`), так что структура свойств объектов соответствует Wialon, но хранится локально.
- Добавлена команда «Добавить ДУТ»: бот запрашивает название датчика в Wialon, подтягивает его параметры/тарировку через API и записывает эквивалентные `SensorConfig` в наш UnitConfig (с экспортом тарировок в CSV).
- В карточке появилась отдельная секция датчиков: для каждого датчика из UnitConfig отображаем вычисленное значение (с учётом выражений/тарировок) и сырые данные.

## Текущее состояние
- Карточка объекта и поиск используют новый снапшот; `uid/hardware` присутствуют.
- Поток обновления и ручной ребилд работают; актуальное состояние видно в `logs/unit_snapshot_watch.log`.
- Кнопка «Загрузить в кэш» — legacy; для нового контура не нужна (данные подтягиваются автопотоком/ребилдом).

## План далее
- Завершить переход UI/карточки/поиска на новый снапшот:
  - Удалить оставшиеся вызовы legacy (`units.v1.json.gz`) и кнопку «Загрузить в кэш».
  - Доделать меню «Настройки» и сохранение параметров (радиус и др.), чтобы не сбрасывались.
- Завершить `docs/unit_config_schema.md` + тесты на сериализацию/валидацию `UnitConfig`.
- Адаптеры загрузки UnitConfig из сторонних СМТ (поверх общего формата), локальный редактор/импорт.
- Редактор/импорт: CSV/HTML-страница для статических параметров; расчёты сенсоров — локально.
- Сенсоры и таблицы расчёта: генерация `segments (X a b)` из `XY`‑пар; валидация; унифицированная модель для разных типов датчиков.
- Производительность: довести мульти‑токенный режим для любых «тяжёлых» выгрузок.
- Возможность локального переименования объектов/override свойств в UnitConfig и синхронизация этих данных с UI.

## Диагностика/логирование
- `logs/unit_snapshot_watch.log` — прогресс `rebuild start/done`, `inline device details`, ретраи/ошибки 502.
- Если UID/HW не видны в карточке — проверить:
  - есть ли они в `data/unit_snapshot.json.gz`;
  - не устарел ли снапшот (`dump_ts`);
  - корректность токенов и лимиты (см. сообщения в логе).

## Тесты
- Запуск:
```
python -m pytest -q
```
или
```
python -m unittest
```
- Покрытие: `UnitSnapshotService` (чтение/запись/фолбэк), поиск ближайших, базовые метрики. Тесты для UnitConfig/калибровок будут добавлены далее.
