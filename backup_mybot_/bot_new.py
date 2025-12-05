# -*- coding: utf-8 -*-
"""\nТелеграм-бот для внесения произвольных полей, показа статистики (ДУТ) и отправки команд (TCP) в Wialon.\n\nЛОГИКА:\n— Внизу только реплай-кнопка: «🔍 Найти объект».\n— Всегда сначала поиск → карточка статистики отдельным сообщением.\n— Под карточкой инлайн-кнопки упорядочены построчно: «📑 Отчёт» + «📍 Ближайшие объекты»,\n  ниже строка «📡 Команда» + «⚙️ Параметры», затем одиночные «📝 Произвольные поля»\n  и «📊 Анализ Сливов», и внизу «🔄 Обновить» + «↩️ К списку».\n— При переходе к «полям» или «команде» кнопки под карточкой скрываются (карточка остаётся).\n— «Назад» под карточкой удаляет карточку и возвращает к поиску.\n— «🔍 Найти объект» на любом шаге сбрасывает цепочку и начинает поиск заново.\n— Финальные сообщения «Готово…» и «Команда отправлена…» НЕ удаляются.\n— «Отмена» в отправке команды — завершает диалог.\n— После завершения диалога ЛЮБОЕ новое текстовое сообщение снова запускает поиск (через entry_point).\n— Жёсткая причина «Text must be non-empty» убрана.\n"""
import asyncio
import contextlib
import copy
import csv
import functools
import gzip
import html
import io
import json
import logging
import logging.handlers
import math
import inspect
import os
import traceback
import pathlib
from pathlib import Path
import random
import re
import sqlite3
import statistics
import sys
import threading
import time
import uuid
import weakref
import shutil
import zipfile
import queue
from collections import deque
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple, TypeVar, Union, Deque, Mapping
from datetime import date, datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlencode, quote_plus, urlsplit

from dataclasses import dataclass, field

import requests
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from requests.adapters import HTTPAdapter

import httpx

from dut_cache import (
    RE_ANY_DUT,
    RE_MAIN_FUEL,
    RE_MAIN_FUEL_PREFIX,
    RE_FUEL_ALTS,
    EXACT_FUEL_TYPE,
    FUEL_MIN_VALID_L,
    FUEL_MAX_VALID_L,
    format_liters,
    normalize_fuel_value,
    extract_params_from_item,
    extract_param_hint_from_sensor,
    extract_assigned_param_name,
    cached_filtered_custom_sensors,
)
from detector_dut_filter import (
    SensorMeta,
    explain_rejection as dut_filter_explain,
    list_dut_candidates,
    resolve_target_duts,
    select_primary_dut,
)
import detector as fuel_detector
import messages_loader
from drain_analysis import DrainEventRecord, build_drain_report
from drain_status import edit_or_post_status_message
from wln_exporter import convert_unit_to_wln, sanitize_filename
from pipeline.adapters.wialon_wlp_import import sensor_config_from_entry
from pipeline.config.defaults import load_from_env as load_pipeline_config
from pipeline.events import Event
from pipeline.config.unit_config import GeneralConfig, SensorConfig, UnitConfig, SensorCalibration, CalibrationPoint
from pipeline.config.unit_config_service import UnitConfigService, UnitConfigValidationError
from pipeline.engine import SensorCalculator, build_day_summary
from pipeline.services.storage_service import PipelineStorageService, get_pipeline_storage_service
from pipeline.services.unit_snapshot_service import (
    UnitSnapshotRecord,
    get_unit_snapshot_service,
    DEFAULT_SNAPSHOT_PATH,
)
from pipeline.services.unit_snapshot_sync import sync_unit_snapshot_from_payload
from pipeline.services.unit_index import get_unit_index
from telegram import (
    Update,
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
    InputFile,
    CallbackQuery,
)
from telegram.error import BadRequest, TimedOut
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

@functools.lru_cache(maxsize=1)
def _pipeline_card_enabled() -> bool:
    """Return True when stats cards should be rendered from pipeline storage."""
    try:
        return load_pipeline_config().source_kind.lower() != "wialon"
    except Exception:
        return False


@functools.lru_cache(maxsize=1)
def _pipeline_search_enabled() -> bool:
    """Return True when unit search should be served from pipeline storage."""

    return _pipeline_card_enabled()


def _ensure_settings_defaults(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: Optional[int] = None,
) -> Dict[str, Any]:
    settings = context.user_data.setdefault("settings", {})
    if chat_id is not None and not context.user_data.get("_settings_loaded"):
        loaded = _load_persisted_settings(chat_id)
        if loaded:
            settings.update(loaded)
        context.user_data["_settings_loaded"] = True
        context.user_data["_settings_chat_id"] = chat_id
    if "nearby_radius_m" not in settings:
        settings["nearby_radius_m"] = DEFAULT_NEARBY_RADIUS_M
    return settings


def _get_nearby_radius(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: Optional[int] = None,
) -> int:
    settings = _ensure_settings_defaults(context, chat_id=chat_id)
    try:
        value = int(settings.get("nearby_radius_m", DEFAULT_NEARBY_RADIUS_M))
    except (TypeError, ValueError):
        value = DEFAULT_NEARBY_RADIUS_M
    return max(NEARBY_RADIUS_MIN_M, min(NEARBY_RADIUS_MAX_M, value))


def _set_nearby_radius(
    context: ContextTypes.DEFAULT_TYPE,
    value: int,
    *,
    chat_id: Optional[int] = None,
) -> int:
    if chat_id is not None:
        _ensure_settings_defaults(context, chat_id=chat_id)
    value = max(NEARBY_RADIUS_MIN_M, min(NEARBY_RADIUS_MAX_M, int(value)))
    settings = _ensure_settings_defaults(context)
    settings["nearby_radius_m"] = value
    if chat_id is None:
        chat_id = context.user_data.get("_settings_chat_id")
    if isinstance(chat_id, int):
        _persist_settings_from_context(context)
    return value




# --- GEO UTILITIES -----------------------------------------------------------
import time as _time_module  # avoid confusion with time aliasing below


class _GeoTimer:
    def __init__(self, label: str):
        self.label = label
        self.started = _time_module.perf_counter()

    def done(self) -> Tuple[str, float]:
        return self.label, _time_module.perf_counter() - self.started


def _geo_scrub_params(params: Any) -> str:
    try:
        payload = json.dumps(params, ensure_ascii=False)
        payload = re.sub(r'"sid"\s*:\s*"[^"]+"', '"sid":"***"', payload)
        payload = re.sub(r'"token"\s*:\s*"[^"]+"', '"token":"***"', payload)
        return payload
    except Exception:
        return str(params)


# ---------------------------------------------------------------------------

# =================== ЛОГИ ===================
BASE_DIR = pathlib.Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
UNIT_CONFIG_DIR = DATA_DIR / "unit_configs"
UNIT_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("wialon_cf_bot")

# Dedicated cancel-trace logger
CANCEL_LOG_PATH = LOG_DIR / "cancel.log"
cancel_log = logging.getLogger("cancel_trace")
cancel_log.propagate = False
if not cancel_log.handlers:
    os.makedirs(LOG_DIR, exist_ok=True)
    cancel_handler = logging.FileHandler(CANCEL_LOG_PATH, encoding="utf-8")
    cancel_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    cancel_log.addHandler(cancel_handler)

NEARBY_LOG_PATH = LOG_DIR / "nearby.log"
nearby_log = logging.getLogger("nearby_diag")
nearby_log.propagate = False
if not nearby_log.handlers:
    handler = logging.FileHandler(NEARBY_LOG_PATH, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    nearby_log.addHandler(handler)

DUT_CAL_LOG_PATH = LOG_DIR / "dut_cal.log"
dut_cal_log = logging.getLogger("dut_cal_trace")
dut_cal_log.propagate = False
if not dut_cal_log.handlers:
    handler = logging.FileHandler(DUT_CAL_LOG_PATH, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    dut_cal_log.addHandler(handler)

_SETTINGS_STORE_PATH = DATA_DIR / "user_settings.json"
_SETTINGS_LOCK = threading.Lock()


def _read_all_persisted_settings() -> Dict[str, Dict[str, Any]]:
    if not _SETTINGS_STORE_PATH.exists():
        return {}
    try:
        data = json.loads(_SETTINGS_STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if isinstance(v, dict)}


def _load_persisted_settings(chat_id: int) -> Dict[str, Any]:
    payload = _read_all_persisted_settings()
    entry = payload.get(str(chat_id))
    if isinstance(entry, dict):
        return dict(entry)
    return {}


def _persist_user_settings(chat_id: int, settings: Dict[str, Any]) -> None:
    clean = {k: v for k, v in settings.items() if not k.startswith("_")}
    with _SETTINGS_LOCK:
        data = _read_all_persisted_settings()
        data[str(chat_id)] = clean
        _SETTINGS_STORE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _persist_settings_from_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = context.user_data.get("_settings_chat_id")
    if not isinstance(chat_id, int):
        return
    settings = context.user_data.get("settings")
    if not isinstance(settings, dict):
        return
    try:
        _persist_user_settings(chat_id, settings)
    except Exception as exc:
        log.debug("persist_user_settings failed: %s", exc)
cancel_log.setLevel(logging.INFO)

# Dedicated drain-cache logger
DRAIN_CACHE_LOG_PATH = LOG_DIR / "drain_cache.log"
drain_cache_log = logging.getLogger("drain_cache_watch")
drain_cache_log.propagate = False
if not drain_cache_log.handlers:
    os.makedirs(LOG_DIR, exist_ok=True)
    drain_cache_handler = logging.FileHandler(DRAIN_CACHE_LOG_PATH, encoding="utf-8")
    drain_cache_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    drain_cache_log.addHandler(drain_cache_handler)
drain_cache_log.setLevel(logging.INFO)

# Dedicated message-cache store logger
MESSAGE_CACHE_STORE_LOG_PATH = LOG_DIR / "message_cache_store.log"
message_cache_store_log = logging.getLogger("message_cache_store")
message_cache_store_log.propagate = False
if not message_cache_store_log.handlers:
    os.makedirs(LOG_DIR, exist_ok=True)
    message_cache_store_handler = logging.FileHandler(MESSAGE_CACHE_STORE_LOG_PATH, encoding="utf-8")
    message_cache_store_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    message_cache_store_log.addHandler(message_cache_store_handler)
message_cache_store_log.setLevel(logging.INFO)

# Dedicated concurrency-control logger
CONCURRENCY_CONTROL_LOG_PATH = LOG_DIR / "concurrency_control.log"
concurrency_log = logging.getLogger("concurrency_control")
concurrency_log.propagate = False
if not concurrency_log.handlers:
    os.makedirs(LOG_DIR, exist_ok=True)
    concurrency_handler = logging.FileHandler(CONCURRENCY_CONTROL_LOG_PATH, encoding="utf-8")
    concurrency_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    concurrency_log.addHandler(concurrency_handler)
concurrency_log.setLevel(logging.INFO)

# Dedicated message load logger
MESSAGE_LOAD_LOG_PATH = LOG_DIR / "message_load.log"
message_load_log = logging.getLogger("message_load")
message_load_log.propagate = False
if not message_load_log.handlers:
    os.makedirs(LOG_DIR, exist_ok=True)
    message_load_handler = logging.FileHandler(MESSAGE_LOAD_LOG_PATH, encoding="utf-8")
    message_load_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    message_load_log.addHandler(message_load_handler)
message_load_log.setLevel(logging.INFO)

# Dedicated sensor-calc diagnostics logger
MESSAGE_CALC_LOG_PATH = LOG_DIR / "message_calc.log"
message_calc_log = logging.getLogger("message_calc")
message_calc_log.propagate = False
if not message_calc_log.handlers:
    os.makedirs(LOG_DIR, exist_ok=True)
    message_calc_handler = logging.FileHandler(MESSAGE_CALC_LOG_PATH, encoding="utf-8")
    message_calc_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    message_calc_log.addHandler(message_calc_handler)
message_calc_log.setLevel(logging.INFO)

# Dedicated token-rotation logger
TOKEN_ROTATION_LOG_PATH = LOG_DIR / "token_rotation.log"
token_rotation_log = logging.getLogger("token_rotation")
token_rotation_log.propagate = False
if not token_rotation_log.handlers:
    os.makedirs(LOG_DIR, exist_ok=True)
    token_rotation_handler = logging.FileHandler(TOKEN_ROTATION_LOG_PATH, encoding="utf-8")
    token_rotation_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
token_rotation_log.addHandler(token_rotation_handler)
token_rotation_log.setLevel(logging.INFO)

ACCESS_DENIED_CACHE_PATH = (BASE_DIR / "data/drain_messages/access_denied_units.json").resolve()
_ACCESS_DENIED_CACHE: Set[int] = set()
_ACCESS_DENIED_CACHE_LOADED = False
_ACCESS_DENIED_CACHE_LOCK = threading.Lock()
ACCESS_DENIED_ASYNC_LOCK = asyncio.Lock()


async def _safe_answer_callback(query: Optional[CallbackQuery], *args: Any, **kwargs: Any) -> None:
    """Answer callback queries ignoring stale/duplicate errors."""
    if query is None:
        return
    try:
        await query.answer(*args, **kwargs)
    except BadRequest as exc:
        message = str(exc).lower()
        if "query is too old" in message or "query id is invalid" in message:
            log.debug("callback answer skipped: %s", exc)
            return
        raise
    except Exception as exc:
        log.debug("callback answer failed: %s", exc)


async def _safe_edit_callback_text(
    query: Optional[CallbackQuery],
    text: str,
    *,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = None,
) -> bool:
    """Edit callback message text ignoring 'message is not modified' errors."""
    if query is None:
        return False
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        return True
    except BadRequest as exc:
        message = str(exc).lower()
        if "message is not modified" in message:
            log.debug("callback edit skipped (no changes): %s", exc)
            return False
        raise
    except Exception as exc:
        log.debug("callback edit failed: %s", exc)
        return False

def _stack_brief(limit: int = 6) -> str:
    try:
        frames = traceback.format_stack(limit=limit)
    except Exception:
        return "unavailable"
    cleaned = [frame.strip() for frame in frames[:-1]]
    return " > ".join(cleaned)

# --- GEO LOG SETUP -----------------------------------------------------------
GEO_LOG_PATH = "logs/geozones.log"
GEO_LOG_LEVEL = "INFO"

geo_log = logging.getLogger("geo")
geo_log.propagate = False

if not any(isinstance(h, logging.handlers.BaseRotatingHandler) for h in geo_log.handlers):
    os.makedirs(os.path.dirname(GEO_LOG_PATH) or ".", exist_ok=True)
    h = logging.handlers.TimedRotatingFileHandler(
        GEO_LOG_PATH, when="midnight", backupCount=7, encoding="utf-8"
    )
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    geo_log.addHandler(h)

try:
    geo_log.setLevel(getattr(logging, GEO_LOG_LEVEL))
except Exception:
    geo_log.setLevel(logging.INFO)
# ---------------------------------------------------------------------------

# =================== DUT LOG SETUP ==========================================
DUT_LOG_PATH = "logs/dut_report.log"
DUT_LOG_LEVEL = "INFO"

dut_log = logging.getLogger("dut_report")
dut_log.propagate = False

if not dut_log.handlers:
    os.makedirs(os.path.dirname(DUT_LOG_PATH) or ".", exist_ok=True)
    handler = logging.handlers.TimedRotatingFileHandler(
        DUT_LOG_PATH, when="midnight", backupCount=7, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    dut_log.addHandler(handler)

try:
    dut_log.setLevel(getattr(logging, DUT_LOG_LEVEL))
except Exception:
    dut_log.setLevel(logging.INFO)
# ---------------------------------------------------------------------------

# =================== DETECTOR LOG BINDING ==================================
detector_log = logging.getLogger("detector")
detector_log.propagate = False

if not detector_log.handlers:
    for handler in dut_log.handlers:
        detector_log.addHandler(handler)

drain_log = logging.getLogger("drain_debug")
drain_log.propagate = False

if not drain_log.handlers:
    os.makedirs("logs", exist_ok=True)
    drain_handler = logging.handlers.TimedRotatingFileHandler(
        "logs/drain_debug.log", when="midnight", backupCount=14, encoding="utf-8"
    )
    drain_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    drain_log.addHandler(drain_handler)

drain_level = os.getenv("DRAIN_LOG_LEVEL", "DEBUG").upper()
try:
    drain_log.setLevel(getattr(logging, drain_level))
except Exception:
    drain_log.setLevel(logging.DEBUG)

for handler in drain_log.handlers:
    if handler not in detector_log.handlers:
        detector_log.addHandler(handler)

detector_level = os.getenv("DETECTOR_LOG_LEVEL", "DEBUG").upper()
try:
    detector_log.setLevel(getattr(logging, detector_level))
except Exception:
    detector_log.setLevel(logging.DEBUG)
# ---------------------------------------------------------------------------

# =================== CARD PERFORMANCE LOG ==================================
CARD_LOG_PATH = "logs/card_perf.log"
card_log = logging.getLogger("card_perf")
card_log.propagate = False

if not card_log.handlers:
    os.makedirs(os.path.dirname(CARD_LOG_PATH) or ".", exist_ok=True)
    card_handler = logging.handlers.TimedRotatingFileHandler(
        CARD_LOG_PATH, when="midnight", backupCount=14, encoding="utf-8"
    )
    card_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    card_log.addHandler(card_handler)

card_level = os.getenv("CARD_LOG_LEVEL", "INFO").upper()
try:
    card_log.setLevel(getattr(logging, card_level))
except Exception:
    card_log.setLevel(logging.INFO)

SNAPSHOT_LOG_PATH = LOG_DIR / "unit_snapshot_watch.log"
snapshot_log = logging.getLogger("unit_snapshot_watch")
snapshot_log.propagate = False
if not snapshot_log.handlers:
    handler = logging.handlers.TimedRotatingFileHandler(
        SNAPSHOT_LOG_PATH, when="midnight", backupCount=14, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    snapshot_log.addHandler(handler)
snapshot_level = os.getenv("SNAPSHOT_LOG_LEVEL", "INFO").upper()
try:
    snapshot_log.setLevel(getattr(logging, snapshot_level))
except Exception:
    snapshot_log.setLevel(logging.INFO)
# ---------------------------------------------------------------------------

# =================== ENV ===================
def _env_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value


TELEGRAM_BOT_TOKEN = _env_str("TELEGRAM_BOT_TOKEN", "").strip()
LOGIN_HOST = _env_str("LOGIN_HOST", "").strip()
if not LOGIN_HOST:
    raise RuntimeError(
        "LOGIN_HOST не задан в .env — укажите базовый хост платформы (например https://hosting.wialon-online.ru)"
    )
WIALON_LOGIN_HOST = LOGIN_HOST
_WIALON_HOST_RAW = _env_str("WIALON_HOST", "").strip()
WIALON_HOST = _WIALON_HOST_RAW or WIALON_LOGIN_HOST
PUBLIC_CALLBACK_URL = _env_str("PUBLIC_CALLBACK_URL", "").strip()
HMAC_SECRET = _env_str("HMAC_SECRET", "").strip()
DEBUG_REPORT_ERRORS_TO_CHAT = _env_str("DEBUG_REPORT_ERRORS_TO_CHAT", "0").strip() not in {"", "0", "false", "False"}

log.info("Telematics hosts configured: api=%s login=%s", WIALON_HOST, WIALON_LOGIN_HOST)

HTTP_POOL_MAX = int(_env_str("HTTP_POOL_MAX", "64") or 64)
HTTP_TIMEOUT_CONN = float(_env_str("HTTP_TIMEOUT_CONN", "5.0") or 5.0)
HTTP_TIMEOUT_READ = float(_env_str("HTTP_TIMEOUT_READ", "20.0") or 20.0)

EXECUTOR_WORKERS = int(_env_str("EXECUTOR_WORKERS", "8") or 8)

# Кэш геокодинга
GEOCODE_CACHE_MAX = 512
GEOCODE_TTL_SEC_BUCKET = 300  # 5 минут (скользящая корзина)

# Кэш шаблонов отчётов (сек)
REPORT_TEMPLATES_TTL = 900  # 15 мин

# Включение индексов/миграции для админки
ADMIN_RUN_MIGRATIONS = True

EXECUTOR = ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS)

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return float(default)
    try:
        return float(raw.strip())
    except ValueError:
        return float(default)


def _env_token_list(name: str) -> List[str]:
    raw = _env_str(name, "")
    if not raw:
        return []
    parts = re.split(r"[\s,;]+", raw.replace("\n", " ").replace("\r", " "))
    tokens: List[str] = []
    for part in parts:
        token = part.strip()
        if token:
            tokens.append(token)
    return tokens


def _env_status_set(name: str, default: str) -> Set[int]:
    raw = _env_str(name, default)
    values: Set[int] = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            value = int(part)
        except ValueError:
            continue
        if value > 0:
            values.add(value)
    return values


NEARBY_LIMIT = max(1, _env_int("NEARBY_LIMIT", 5))
NEARBY_MAX_KM = _env_float("NEARBY_MAX_KM", 0.0)
DEFAULT_NEARBY_RADIUS_M = max(1, _env_int("NEARBY_RADIUS_M", 100))
NEARBY_RADIUS_STEP_M = max(1, _env_int("NEARBY_RADIUS_STEP_M", 50))
NEARBY_RADIUS_MIN_M = max(1, _env_int("NEARBY_RADIUS_MIN_M", 50))
NEARBY_RADIUS_MAX_M = max(NEARBY_RADIUS_MIN_M, _env_int("NEARBY_RADIUS_MAX_M", 5000))
DRAIN_ANALYSIS_CONCURRENCY = max(1, min(32, _env_int("DRAIN_ANALYSIS_CONCURRENCY", 10)))
DRAIN_ANALYSIS_CONCURRENCY_MAX = max(DRAIN_ANALYSIS_CONCURRENCY, min(64, _env_int("DRAIN_ANALYSIS_CONCURRENCY_MAX", 32)))
DRAIN_ANALYSIS_CONCURRENCY_PER_TOKEN = max(1, _env_int("DRAIN_ANALYSIS_CONCURRENCY_PER_TOKEN", 1))
DRAIN_ANALYSIS_SESSION_POOL_BASE = max(1, min(16, _env_int("DRAIN_ANALYSIS_SESSION_POOL", 4)))
DRAIN_ANALYSIS_SESSION_POOL = DRAIN_ANALYSIS_SESSION_POOL_BASE
DRAIN_ANALYSIS_SESSION_POOL_PER_TOKEN = max(0, _env_int("DRAIN_ANALYSIS_SESSION_POOL_PER_TOKEN", 1))
DRAIN_ANALYSIS_CHUNK_SIZE = max(1, _env_int("DRAIN_ANALYSIS_CHUNK_SIZE", 500))
DRAIN_MESSAGE_SERIES_TARGET_STEP = max(1, _env_int("DRAIN_MESSAGE_SERIES_TARGET_STEP", 30))
DRAIN_ANALYSIS_PROGRESS_FORCE_STEP = max(1, _env_int("DRAIN_ANALYSIS_PROGRESS_FORCE_STEP", 40))
DRAIN_ANALYSIS_PROGRESS_REFRESH_SECONDS = max(
    0.5, _env_float("DRAIN_ANALYSIS_PROGRESS_REFRESH_SECONDS", 2.0)
)
DRAIN_ANALYSIS_MESSAGE_LOAD_TIMEOUT = max(
    30.0, float(_env_str("DRAIN_ANALYSIS_MESSAGE_LOAD_TIMEOUT", "120.0") or 120.0)
)
DRAIN_ANALYSIS_UNLOAD_TIMEOUT = max(
    5.0, float(_env_str("DRAIN_ANALYSIS_UNLOAD_TIMEOUT", "30.0") or 30.0)
)
MESSAGE_CACHE_MAX_PERIOD_DAYS = max(1, _env_int("MESSAGE_CACHE_MAX_PERIOD_DAYS", 31))
WIALON_EXTRA_TOKENS = _env_token_list("WIALON_EXTRA_TOKENS")
WIALON_TOKEN_ROTATE_AFTER_UNITS = max(1, _env_int("WIALON_TOKEN_ROTATE_AFTER_UNITS", 280))
WIALON_TOKEN_COOLDOWN_SECONDS = max(0.0, _env_float("WIALON_TOKEN_COOLDOWN_SECONDS", 180.0))
WIALON_TOKEN_ERROR_COOLDOWN_SECONDS = max(0.0, _env_float("WIALON_TOKEN_ERROR_COOLDOWN_SECONDS", 600.0))
WIALON_TOKEN_ROTATE_HTTP_STATUSES = _env_status_set("WIALON_TOKEN_ROTATE_HTTP_STATUSES", "429,502,503,504")
WIALON_BATCH_SLOW_THRESHOLD = max(0.0, _env_float("WIALON_BATCH_SLOW_THRESHOLD", 0.75))
WIALON_BATCH_FAST_THRESHOLD = max(0.0, _env_float("WIALON_BATCH_FAST_THRESHOLD", 0.40))
WIALON_BATCH_MIN_SIZE = max(10, _env_int("WIALON_BATCH_MIN_SIZE", 80))
WIALON_BATCH_INITIAL_LIMIT = max(WIALON_BATCH_MIN_SIZE, _env_int("WIALON_BATCH_INITIAL_LIMIT", 160))
WIALON_BATCH_WARMUP_LIMIT = max(WIALON_BATCH_MIN_SIZE, _env_int("WIALON_BATCH_WARMUP_LIMIT", 140))
WIALON_BATCH_WARMUP_BATCHES = max(1, _env_int("WIALON_BATCH_WARMUP_BATCHES", 1))
WIALON_PARALLEL_TOKEN_LIMIT = max(1, _env_int("WIALON_PARALLEL_TOKEN_LIMIT", 12))
WIALON_PARALLEL_TOKEN_WARMUP = max(1, min(WIALON_PARALLEL_TOKEN_LIMIT, _env_int("WIALON_PARALLEL_TOKEN_WARMUP", 8)))
WIALON_PARALLEL_TOKEN_RAMP_BATCHES = max(1, _env_int("WIALON_PARALLEL_TOKEN_RAMP_BATCHES", 2))
WIALON_TOKEN_SOFT_COOLDOWN_SECONDS = max(0.0, _env_float("WIALON_TOKEN_SOFT_COOLDOWN_SECONDS", 60.0))
WIALON_TOKEN_SOFT_COOLDOWN_AVG_THRESHOLD = max(
    0.0, _env_float("WIALON_TOKEN_SOFT_COOLDOWN_AVG_THRESHOLD", 0.6)
)
WIALON_TOKEN_SOFT_COOLDOWN_DURATION = max(
    0.0, _env_float("WIALON_TOKEN_SOFT_COOLDOWN_DURATION", 90.0)
)
WIALON_TOKEN_HARD_COOLDOWN_SECONDS = max(0.0, _env_float("WIALON_TOKEN_HARD_COOLDOWN_SECONDS", 120.0))
WIALON_TOKEN_HARD_COOLDOWN_AVG_THRESHOLD = max(0.0, _env_float("WIALON_TOKEN_HARD_COOLDOWN_AVG_THRESHOLD", 2.0))
WIALON_TOKEN_HARD_COOLDOWN_DURATION = max(0.0, _env_float("WIALON_TOKEN_HARD_COOLDOWN_DURATION", 240.0))
WIALON_TOKEN_HARD_COOLDOWN_STREAK = max(1, _env_int("WIALON_TOKEN_HARD_COOLDOWN_STREAK", 2))
WIALON_TOKEN_AVG_WINDOW = max(1, _env_int("WIALON_TOKEN_AVG_WINDOW", 4))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    return raw.lower() not in {"0", "false", "no", "off"}


def _parse_admin_ids(raw: str) -> set[int]:
    result: set[int] = set()
    for part in (raw or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            log.warning("ADMIN_CHAT_IDS содержит некорректное значение: %s", part)
    return result


ADMIN_WHITELIST = _parse_admin_ids(_env_str("ADMIN_CHAT_IDS", ""))


GEO_CACHE_PATH_RAW = "data/geozones.v1.json.gz"
GEO_CACHE_PATH = (BASE_DIR / GEO_CACHE_PATH_RAW).resolve()
GEO_CACHE_TTL = 43200
GEO_CACHE_REFRESH_INTERVAL = 3600
GEO_CACHE_QUIET_HOURS_RAW = ""
GEO_CACHE_JITTER = 0.0
GEO_CACHE_CHUNK_RES_DEFAULT = 20
GEO_CACHE_SLEEP_BETWEEN_CHUNKS_MS_DEFAULT = 200
GEO_INCLUDE_CENTER_DEFAULT = False
GEO_FILE_UPDATER_ENABLED = True
GEO_MAX_NAMES = 5

UNIT_SNAPSHOT_UPDATER_ENABLED = True
UNIT_SNAPSHOT_REFRESH_ENABLED = True
UNIT_SNAPSHOT_META_PATH = (BASE_DIR / "data/unit_snapshot.meta.json").resolve()
UNIT_SNAPSHOT_TTL = 24 * 60 * 60
UNIT_SNAPSHOT_REFRESH_INTERVAL = 24 * 60 * 60
UNIT_SNAPSHOT_JITTER = 0.0
UNIT_SNAPSHOT_PAGE_SIZE = 256
UNIT_SNAPSHOT_SLEEP_BETWEEN_PAGES_MS = 50
UNIT_DEVICE_DETAILS_TIMEOUT_SEC = float(os.getenv("UNIT_DEVICE_DETAILS_TIMEOUT_SEC", "20.0") or 20.0)
UNIT_DEVICE_DETAILS_MAX_RETRIES = int(os.getenv("UNIT_DEVICE_DETAILS_MAX_RETRIES", "3") or 3)
UNIT_DEVICE_BATCH_SIZE = int(os.getenv("UNIT_DEVICE_BATCH_SIZE", "16") or 16)
UNIT_DEVICE_BATCH_RETRIES = int(os.getenv("UNIT_DEVICE_BATCH_RETRIES", "5") or 5)
UNIT_DEVICE_BATCH_BACKOFF = float(os.getenv("UNIT_DEVICE_BATCH_BACKOFF", "2.0") or 2.0)
UNIT_DEVICE_BATCH_BACKOFF_MAX = float(os.getenv("UNIT_DEVICE_BATCH_BACKOFF_MAX", "30.0") or 30.0)
UNIT_DEVICE_WORKERS = int(os.getenv("UNIT_DEVICE_WORKERS", "4") or 4)
UNIT_DEVICE_TOKENS_LIMIT = int(os.getenv("UNIT_DEVICE_TOKENS_LIMIT", "6") or 6)
DUT_USE_BATCH = True
DUT_BATCH_SIZE = 50
DUT_PARALLELISM_ENABLED = False
DUT_PARALLELISM_LIMIT = 4
DUT_REPORT_CACHE_ONLY = True
DUT_BATCH_RETRY_INITIAL_DELAY = 0.5
DUT_BATCH_RETRY_MAX_DELAY = 8.0

_GEOCODE_CLIENTS = weakref.WeakValueDictionary()


class TokenStore:
    def __init__(self, db_path: pathlib.Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def connect(self) -> sqlite3.Connection:
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """\n                CREATE TABLE IF NOT EXISTS user_tokens (\n                    chat_id INTEGER PRIMARY KEY,\n                    user_id INTEGER,\n                    token TEXT NOT NULL,\n                    token_tail TEXT,\n                    created_at TEXT NOT NULL,\n                    updated_at TEXT NOT NULL\n                )\n                """
            )
            conn.execute(
                """\n                CREATE TABLE IF NOT EXISTS known_users (\n                    chat_id INTEGER PRIMARY KEY,\n                    user_id INTEGER,\n                    username TEXT,\n                    first_name TEXT,\n                    last_name TEXT,\n                    language_code TEXT,\n                    is_bot INTEGER DEFAULT 0,\n                    updated_at TEXT NOT NULL\n                )\n                """
            )
            conn.execute(
                """\n                CREATE TABLE IF NOT EXISTS admins (\n                    chat_id INTEGER PRIMARY KEY,\n                    role TEXT NOT NULL CHECK(role IN ('admin','superadmin')),\n                    created_at TEXT NOT NULL\n                )\n                """
            )
            conn.execute(
                """\n                CREATE TABLE IF NOT EXISTS blocked_users (\n                    chat_id INTEGER PRIMARY KEY,\n                    reason TEXT,\n                    updated_at TEXT NOT NULL,\n                    by_admin INTEGER\n                )\n                """
            )
            conn.execute(
                """\n                CREATE TABLE IF NOT EXISTS user_admin_notes (\n                    chat_id INTEGER PRIMARY KEY,\n                    note TEXT NOT NULL,\n                    updated_at TEXT NOT NULL,\n                    by_admin INTEGER\n                )\n                """
            )
            conn.execute(
                """\n                CREATE TABLE IF NOT EXISTS admin_audit (\n                    id INTEGER PRIMARY KEY AUTOINCREMENT,\n                    admin_chat_id INTEGER NOT NULL,\n                    target_chat_id INTEGER,\n                    action TEXT NOT NULL,\n                    details_json TEXT,\n                    ts TEXT NOT NULL\n                )\n                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_tokens_updated_at ON user_tokens(updated_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_blocked_users_updated_at ON blocked_users(updated_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_admin_audit_ts ON admin_audit(ts)"
            )
            conn.commit()

    def get_token(self, chat_id: int) -> Optional[str]:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT token FROM user_tokens WHERE chat_id = ?",
                (int(chat_id),),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def save_token(self, chat_id: int, user_id: Optional[int], token: str) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            tail = token[-4:] if token else None
            conn.execute(
                """\n                INSERT INTO user_tokens (chat_id, user_id, token, token_tail, created_at, updated_at)\n                VALUES (?, ?, ?, ?, ?, ?)\n                ON CONFLICT(chat_id) DO UPDATE SET\n                    user_id=excluded.user_id,\n                    token=excluded.token,\n                    token_tail=excluded.token_tail,\n                    updated_at=excluded.updated_at\n                """,
                (
                    int(chat_id),
                    int(user_id) if user_id is not None else None,
                    token,
                    tail,
                    ts,
                    ts,
                ),
            )
            conn.commit()

    def remove_token(self, chat_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM user_tokens WHERE chat_id = ?", (int(chat_id),))
            conn.commit()

    def has_token(self, chat_id: int) -> bool:
        return self.get_token(chat_id) is not None

    def list_recipients(self) -> List[int]:
        with self.connect() as conn:
            rows = conn.execute(
                """\n                SELECT ku.chat_id\n                FROM known_users ku\n                LEFT JOIN blocked_users bu ON bu.chat_id = ku.chat_id\n                WHERE bu.chat_id IS NULL\n                """
            ).fetchall()
            recipients: List[int] = []
            for row in rows:
                try:
                    if row[0] is not None:
                        recipients.append(int(row[0]))
                except (TypeError, ValueError):
                    continue
            return recipients

    def upsert_user_profile(
        self,
        chat_id: int,
        user_id: Optional[int],
        username: Optional[str],
        first_name: Optional[str],
        last_name: Optional[str],
        language_code: Optional[str],
        is_bot: bool,
    ) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """\n                INSERT INTO known_users (chat_id, user_id, username, first_name, last_name, language_code, is_bot, updated_at)\n                VALUES (?, ?, ?, ?, ?, ?, ?, ?)\n                ON CONFLICT(chat_id) DO UPDATE SET\n                    user_id = excluded.user_id,\n                    username = excluded.username,\n                    first_name = excluded.first_name,\n                    last_name = excluded.last_name,\n                    language_code = excluded.language_code,\n                    is_bot = excluded.is_bot,\n                    updated_at = excluded.updated_at\n                """,
                (
                    int(chat_id),
                    int(user_id) if user_id is not None else None,
                    username,
                    first_name,
                    last_name,
                    language_code,
                    1 if is_bot else 0,
                    ts,
                ),
            )
            conn.commit()

    def get_user_profile(self, chat_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            cur = conn.execute(
                "SELECT * FROM known_users WHERE chat_id = ?",
                (int(chat_id),),
            )
            row = cur.fetchone()
            return row

    def is_blocked(self, chat_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT 1 FROM blocked_users WHERE chat_id = ? LIMIT 1",
                (int(chat_id),),
            )
            return cur.fetchone() is not None

    def set_blocked(
        self,
        chat_id: int,
        by_admin: int,
        blocked: bool,
        reason: Optional[str] = None,
    ) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            if blocked:
                conn.execute(
                    """\n                    INSERT INTO blocked_users (chat_id, reason, updated_at, by_admin)\n                    VALUES (?, ?, ?, ?)\n                    ON CONFLICT(chat_id) DO UPDATE SET\n                        reason = excluded.reason,\n                        updated_at = excluded.updated_at,\n                        by_admin = excluded.by_admin\n                    """,
                    (int(chat_id), reason, ts, int(by_admin)),
                )
            else:
                conn.execute(
                    "DELETE FROM blocked_users WHERE chat_id = ?",
                    (int(chat_id),),
                )
            conn.commit()

    def save_admin_note(self, chat_id: int, note: str, by_admin: int) -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """\n                INSERT INTO user_admin_notes (chat_id, note, updated_at, by_admin)\n                VALUES (?, ?, ?, ?)\n                ON CONFLICT(chat_id) DO UPDATE SET\n                    note = excluded.note,\n                    updated_at = excluded.updated_at,\n                    by_admin = excluded.by_admin\n                """,
                (int(chat_id), note, ts, int(by_admin)),
            )
            conn.commit()

    def delete_admin_note(self, chat_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM user_admin_notes WHERE chat_id = ?",
                (int(chat_id),),
            )
            conn.commit()

    def get_admin_note(self, chat_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            cur = conn.execute(
                "SELECT * FROM user_admin_notes WHERE chat_id = ?",
                (int(chat_id),),
            )
            return cur.fetchone()

    def log_admin_action(
        self,
        admin_chat_id: int,
        target_chat_id: Optional[int],
        action: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        ts = datetime.utcnow().isoformat()
        payload = json.dumps(details or {}, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """\n                INSERT INTO admin_audit (admin_chat_id, target_chat_id, action, details_json, ts)\n                VALUES (?, ?, ?, ?, ?)\n                """,
                (
                    int(admin_chat_id),
                    int(target_chat_id) if target_chat_id is not None else None,
                    action,
                    payload,
                    ts,
                ),
            )
            conn.commit()

    def get_admin_role(self, chat_id: int) -> Optional[str]:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT role FROM admins WHERE chat_id = ?",
                (int(chat_id),),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def ensure_admin(self, chat_id: int, role: str = "admin") -> None:
        ts = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """\n                INSERT OR IGNORE INTO admins (chat_id, role, created_at) VALUES (?, ?, ?)\n                """,
                (int(chat_id), role, ts),
            )
            conn.commit()


class ZoneFileStore:
    def __init__(self, path: pathlib.Path):
        self.path = pathlib.Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._payload: Optional[Dict[str, Any]] = None
        self._payload_mtime: Optional[float] = None
        self.mtime: Optional[float] = None
        self.strategy: Optional[str] = None
        self.flags_used: Optional[int] = None

    def load_if_present(self) -> Optional[Dict[str, Any]]:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            with self._lock:
                self._payload = None
                self._payload_mtime = None
                self.mtime = None
                self.strategy = None
                self.flags_used = None
            return None
        with self._lock:
            if (
                isinstance(self._payload, dict)
                and self._payload_mtime == stat.st_mtime
            ):
                self.mtime = stat.st_mtime
                return copy.deepcopy(self._payload)
        try:
            with gzip.open(self.path, "rt", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            return None
        except Exception as exc:
            geo_log.error("file_cache: failed to read %s: %s", self.path, exc)
            return None
        if not isinstance(payload, dict):
            geo_log.error(
                "file_cache: invalid payload type %s in %s",
                type(payload).__name__,
                self.path,
            )
            return None
        with self._lock:
            self._payload = payload
            self._payload_mtime = stat.st_mtime
            self.mtime = stat.st_mtime
            self.strategy = payload.get("strategy") if isinstance(payload.get("strategy"), str) else None
            flags_val = payload.get("flags_used")
            self.flags_used = int(flags_val) if isinstance(flags_val, int) else None
        return copy.deepcopy(payload)

    def save_atomic(self, payload: Dict[str, Any]) -> None:
        tmp_name = f"{self.path.name}.tmp-{os.getpid()}-{threading.get_ident()}"
        tmp_path = self.path.with_name(tmp_name)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        serializable = copy.deepcopy(payload)
        try:
            with gzip.open(tmp_path, "wt", encoding="utf-8") as fh:
                json.dump(serializable, fh, ensure_ascii=False)
        except Exception:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            except Exception as exc:
                geo_log.debug("file_cache: failed to remove tmp %s: %s", tmp_path, exc)
            raise
        os.replace(tmp_path, self.path)
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            stat = None
        with self._lock:
            self._payload = serializable
            self._payload_mtime = stat.st_mtime if stat else None
            self.mtime = stat.st_mtime if stat else None
            self.strategy = serializable.get("strategy") if isinstance(serializable.get("strategy"), str) else None
            flags_val = serializable.get("flags_used")
            self.flags_used = int(flags_val) if isinstance(flags_val, int) else None


ZONE_STORE = ZoneFileStore(GEO_CACHE_PATH)


@dataclass
class RotationTokenConfig:
    token: str
    label: str
    is_primary: bool = False
    client: Optional["WialonClient"] = None  # type: ignore[name-defined]
    cooldown_until: float = 0.0
    consecutive_errors: int = 0


@dataclass
class MessageCacheDownloadResult:
    status: str
    day: str
    label: str
    start_ts: int
    end_ts: int
    completed: int
    total: int
    failed: int
    payload: Optional[Dict[str, Any]] = None


class DrainMessageCacheStore:
    VERSION = 1

    def __init__(self, base_dir: pathlib.Path):
        self.base_dir = pathlib.Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.partial_dir = self.base_dir / "partial"
        self.partial_dir.mkdir(parents=True, exist_ok=True)
        self.series_target_step = DRAIN_MESSAGE_SERIES_TARGET_STEP
        self._unit_store_lock = threading.Lock()
        self.index_path = self.base_dir / "index.meta.json"
        self._index_lock = threading.Lock()
        self._index_cache: Dict[str, Dict[str, Any]] = {}
        self._index_mtime: Optional[float] = None

    @staticmethod
    def _stack_info(limit: int = 6) -> str:
        try:
            frames = traceback.format_stack(limit=limit)
        except Exception:
            return "unavailable"
        # drop the current frame
        cropped = [frame.strip() for frame in frames[:-1]]
        return " > ".join(cropped)

    def _normalize_day(self, day_value: Union[str, date, datetime]) -> Optional[str]:
        if isinstance(day_value, datetime):
            return day_value.date().isoformat()
        if isinstance(day_value, date):
            return day_value.isoformat()
        if isinstance(day_value, str):
            text = day_value.strip()
            if not text:
                return None
            try:
                parsed = datetime.fromisoformat(text)
                return parsed.date().isoformat()
            except Exception:
                try:
                    parsed_date = datetime.strptime(text, "%Y-%m-%d")
                    return parsed_date.date().isoformat()
                except Exception:
                    return None
        return None

    def _path_for_day(self, normalized_day: str) -> pathlib.Path:
        safe_day = normalized_day.replace("/", "-")
        return self.base_dir / f"messages-{safe_day}.json.gz"

    def _partial_root_for_day(self, normalized_day: str) -> pathlib.Path:
        safe_day = normalized_day.replace("/", "-")
        return self.partial_dir / f"messages-{safe_day}"

    def _partial_units_dir(self, normalized_day: str) -> pathlib.Path:
        return self._partial_root_for_day(normalized_day) / "units"

    def _partial_meta_path(self, normalized_day: str) -> pathlib.Path:
        return self._partial_root_for_day(normalized_day) / "meta.json"

    def _partial_failures_path(self, normalized_day: str) -> pathlib.Path:
        return self._partial_root_for_day(normalized_day) / "failures.txt"

    def _write_json_atomic(self, target: pathlib.Path, payload: Dict[str, Any]) -> None:
        tmp_name = f"{target.name}.tmp-{os.getpid()}-{threading.get_ident()}"
        tmp_path = target.with_name(tmp_name)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        os.replace(tmp_path, target)

    def _load_ready_index_locked(self) -> Dict[str, Dict[str, Any]]:
        try:
            stat = self.index_path.stat()
        except FileNotFoundError:
            self._index_cache = {}
            self._index_mtime = None
            return {}
        if self._index_mtime == stat.st_mtime and self._index_cache is not None:
            return copy.deepcopy(self._index_cache)
        try:
            with open(self.index_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except Exception as exc:
            log.debug("message_cache: failed to read index %s: %s", self.index_path, exc)
            raw = {}
        index: Dict[str, Dict[str, Any]] = {}
        if isinstance(raw, dict):
            for day_key, entry in raw.items():
                if not isinstance(day_key, str) or not isinstance(entry, dict):
                    continue
                normalized_entry = dict(entry)
                normalized_entry["day"] = day_key
                index[day_key] = normalized_entry
        self._index_cache = index
        self._index_mtime = stat.st_mtime
        return copy.deepcopy(self._index_cache)

    def _write_ready_index_locked(self, index: Dict[str, Dict[str, Any]]) -> None:
        payload = {day: dict(entry) for day, entry in index.items()}
        self._write_json_atomic(self.index_path, payload)
        try:
            stat = self.index_path.stat()
            self._index_mtime = stat.st_mtime
        except FileNotFoundError:
            self._index_mtime = None
        self._index_cache = copy.deepcopy(index)

    def _load_ready_index(self) -> Dict[str, Dict[str, Any]]:
        with self._index_lock:
            return self._load_ready_index_locked()

    def _set_ready_index_entry(self, day_value: Union[str, date, datetime], entry: Dict[str, Any]) -> None:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return
        entry_copy = dict(entry)
        entry_copy["day"] = normalized
        with self._index_lock:
            index = self._load_ready_index_locked()
            index[normalized] = entry_copy
            self._write_ready_index_locked(index)

    def _remove_ready_index_entry(self, day_value: Union[str, date, datetime]) -> None:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return
        with self._index_lock:
            index = self._load_ready_index_locked()
            if normalized in index:
                index.pop(normalized, None)
                self._write_ready_index_locked(index)

    def _entry_from_ready_payload(self, normalized_day: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        units_payload = payload.get("units")
        units_count = len(units_payload) if isinstance(units_payload, dict) else 0
        failed_units = payload.get("failed_units")
        failed_count = len(set(failed_units)) if isinstance(failed_units, list) else 0
        total_units = payload.get("total_units")
        try:
            total_units_int = int(total_units) if total_units is not None else None
        except Exception:
            total_units_int = None
        completed_units = payload.get("completed_units")
        try:
            completed_units_int = int(completed_units) if completed_units is not None else None
        except Exception:
            completed_units_int = None
        entry = {
            "day": normalized_day,
            "dump_ts": payload.get("dump_ts"),
            "start_ts": payload.get("start_ts"),
            "end_ts": payload.get("end_ts"),
            "units": units_count,
            "failed_units": failed_count,
            "status": "ready",
            "label": payload.get("label"),
            "total_units": total_units_int or units_count,
            "completed_units": completed_units_int or units_count,
        }
        if payload.get("incomplete"):
            entry["status"] = "incomplete"
        return entry

    def _load_ready_payload_from_file(self, path: pathlib.Path) -> Optional[Dict[str, Any]]:
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            return None
        except Exception as exc:
            log.debug("message_cache: failed to inspect %s: %s", path, exc)
            return None
        return payload if isinstance(payload, dict) else None

    def _downsample_series_points(self, series: Any, step: int) -> List[List[Any]]:
        try:
            points = list(series or [])
        except TypeError:
            return []
        if not points:
            return []
        filtered: List[List[Any]] = []
        next_threshold: Optional[int] = None
        last_valid_point: Optional[List[Any]] = None
        for raw_point in points:
            if not isinstance(raw_point, (list, tuple)) or not raw_point:
                continue
            point_list = list(raw_point)
            ts_raw = point_list[0]
            try:
                ts_val = int(ts_raw)
            except Exception:
                continue
            last_valid_point = point_list
            if not filtered:
                filtered.append(point_list)
                next_threshold = ts_val + step
                continue
            if next_threshold is None:
                next_threshold = ts_val + step
            if ts_val < next_threshold:
                continue
            filtered.append(point_list)
            while ts_val >= next_threshold:
                next_threshold += step
        if last_valid_point:
            try:
                last_ts = int(last_valid_point[0])
            except Exception:
                last_ts = None
            if last_ts is not None:
                prev_ts: Optional[int] = None
                if filtered:
                    try:
                        prev_ts = int(filtered[-1][0])
                    except Exception:
                        prev_ts = None
                if prev_ts != last_ts:
                    filtered.append(last_valid_point)
        return filtered

    def _downsample_samples(self, samples: Any, step: int) -> List[Dict[str, Any]]:
        try:
            records = list(samples or [])
        except TypeError:
            return []
        if not records:
            return []
        filtered: List[Dict[str, Any]] = []
        next_threshold: Optional[int] = None
        last_valid: Optional[Dict[str, Any]] = None
        for record in records:
            if not isinstance(record, dict):
                continue
            ts_raw = record.get("ts")
            try:
                ts_val = int(ts_raw)
            except Exception:
                continue
            last_valid = record
            if not filtered:
                filtered.append(record)
                next_threshold = ts_val + step
                continue
            if next_threshold is None:
                next_threshold = ts_val + step
            if ts_val < next_threshold:
                continue
            filtered.append(record)
            while ts_val >= next_threshold:
                next_threshold += step
        if last_valid:
            try:
                last_ts = int(last_valid.get("ts"))
            except Exception:
                last_ts = None
            if last_ts is not None:
                prev_ts: Optional[int] = None
                if filtered:
                    try:
                        prev_ts = int(filtered[-1].get("ts"))
                    except Exception:
                        prev_ts = None
                if prev_ts != last_ts:
                    filtered.append(last_valid)
        return filtered

    def _filter_unit_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = copy.deepcopy(payload)
        step = max(1, int(self.series_target_step))
        if step <= 1:
            return data

        raw_msg_count = payload.get("raw_msg_count")
        if raw_msg_count is None:
            raw_msg_count = payload.get("msg_count")
            if raw_msg_count is None:
                samples_raw = payload.get("samples")
                if isinstance(samples_raw, list):
                    raw_msg_count = len(samples_raw)

        series_raw = data.get("series")
        filtered_series: Dict[Any, List[List[Any]]] = {}
        if isinstance(series_raw, dict):
            for sensor_id, series in series_raw.items():
                filtered_series[sensor_id] = self._downsample_series_points(series, step)
            data["series"] = filtered_series

        speed_series_raw = data.get("speed_series")
        filtered_speed_series: Optional[List[List[Any]]] = None
        if isinstance(speed_series_raw, list):
            filtered_speed_series = self._downsample_series_points(speed_series_raw, step)
            data["speed_series"] = filtered_speed_series

        aux_raw = data.get("aux_series")
        if isinstance(aux_raw, dict):
            filtered_aux: Dict[str, List[List[Any]]] = {}
            for key, series in aux_raw.items():
                filtered_aux[str(key)] = self._downsample_series_points(series, step)
            data["aux_series"] = filtered_aux

        samples_raw = data.get("samples")
        filtered_samples: Optional[List[Dict[str, Any]]] = None
        if isinstance(samples_raw, list):
            filtered_samples = self._downsample_samples(samples_raw, step)
            data["samples"] = filtered_samples

        sensor_ids = data.get("sensor_ids")
        if isinstance(sensor_ids, list) and filtered_series:
            data["no_data"] = all(len(filtered_series.get(sid, [])) == 0 for sid in sensor_ids)

        filtered_msg_count_candidates: List[int] = []
        if filtered_series:
            filtered_msg_count_candidates.append(
                max((len(points) for points in filtered_series.values()), default=0)
            )
        if filtered_samples:
            filtered_msg_count_candidates.append(len(filtered_samples))
        if filtered_speed_series:
            filtered_msg_count_candidates.append(len(filtered_speed_series))

        filtered_msg_count = max(filtered_msg_count_candidates) if filtered_msg_count_candidates else 0
        data["msg_count"] = filtered_msg_count
        if raw_msg_count is not None:
            data["raw_msg_count"] = raw_msg_count
        return data

    def list_days(self) -> List[Dict[str, Any]]:
        ready_index = self._load_ready_index()
        entries_by_day: Dict[str, Dict[str, Any]] = {day: dict(entry) for day, entry in ready_index.items()}
        missing_entries: List[Tuple[str, Dict[str, Any]]] = []

        for path in sorted(self.base_dir.glob("messages-*.json.gz")):
            stem = path.name.replace("messages-", "").replace(".json.gz", "")
            normalized = self._normalize_day(stem) or stem
            if normalized in entries_by_day:
                continue
            payload = self._load_ready_payload_from_file(path)
            if not isinstance(payload, dict):
                continue
            day_value = payload.get("day") if isinstance(payload.get("day"), str) else normalized
            normalized_day = self._normalize_day(day_value) or normalized
            if normalized_day in entries_by_day:
                continue
            entry = self._entry_from_ready_payload(normalized_day, payload)
            entries_by_day[normalized_day] = entry
            missing_entries.append((normalized_day, entry))

        if missing_entries:
            with self._index_lock:
                index = self._load_ready_index_locked()
                updated = False
                for day, entry in missing_entries:
                    if day not in index:
                        index[day] = dict(entry)
                        updated = True
                if updated:
                    self._write_ready_index_locked(index)

        for partial in self.list_partial_days():
            day_value = partial.get("day")
            if not day_value:
                continue
            normalized_day = self._normalize_day(day_value) or day_value
            entry = entries_by_day.get(normalized_day, {"day": normalized_day})
            meta = partial.get("meta") or {}
            completed_ids = partial.get("units") or []
            failure_ids = partial.get("failures") or []
            entry["partial"] = True
            entry["partial_units"] = len(set(completed_ids))
            entry["partial_failed"] = len(set(failure_ids))
            total_units_val = meta.get("total_units")
            try:
                entry["partial_total"] = int(total_units_val)
            except Exception:
                entry["partial_total"] = None
            entry.setdefault("label", meta.get("label"))
            entry.setdefault("dump_ts", meta.get("updated_ts") or meta.get("dump_ts"))
            entry.setdefault("start_ts", meta.get("start_ts"))
            entry.setdefault("end_ts", meta.get("end_ts"))
            entry["partial_status"] = meta.get("status") or "in_progress"
            if entry.get("status") not in {"ready", "incomplete"}:
                entry["status"] = "partial"
            entries_by_day[normalized_day] = entry

        return [entries_by_day[key] for key in sorted(entries_by_day.keys())]

    def list_partial_days(self) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for root in sorted(self.partial_dir.glob("messages-*")):
            if not root.is_dir():
                continue
            stem = root.name.replace("messages-", "")
            info = self.get_partial_info(stem)
            if info:
                entries.append(info)
        return entries

    def get_partial_info(self, day_value: Union[str, date, datetime]) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return None
        root = self._partial_root_for_day(normalized)
        if not root.exists():
            return None

        meta: Dict[str, Any] = {}
        meta_path = self._partial_meta_path(normalized)
        try:
            with open(meta_path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
            if isinstance(loaded, dict):
                meta = loaded
        except FileNotFoundError:
            meta = {}
        except Exception as exc:
            log.debug("message_cache: failed to read partial meta %s: %s", meta_path, exc)

        units_dir = self._partial_units_dir(normalized)
        unit_ids: List[int] = []
        if units_dir.exists():
            for file_path in units_dir.glob("*.json"):
                if file_path.name.endswith(".raw.json"):
                    continue
                if not file_path.is_file():
                    continue
                try:
                    unit_ids.append(int(file_path.stem))
                except Exception:
                    continue

        failure_ids: List[int] = []
        failures_path = self._partial_failures_path(normalized)
        if failures_path.exists():
            try:
                with open(failures_path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        text = line.strip()
                        if not text:
                            continue
                        try:
                            failure_ids.append(int(text))
                        except Exception:
                            continue
            except Exception as exc:
                log.debug("message_cache: failed to read partial failures %s: %s", failures_path, exc)

        return {
            "day": normalized,
            "meta": meta,
            "units": sorted(set(unit_ids)),
            "failures": sorted(set(failure_ids)),
        }

    def has_partial(self, day_value: Union[str, date, datetime]) -> bool:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return False
        return self._partial_root_for_day(normalized).exists()

    def ensure_partial(
        self,
        day_value: Union[str, date, datetime],
        *,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        label: Optional[str] = None,
        total_units: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized = self._normalize_day(day_value)
        if not normalized:
            raise ValueError(f"invalid day value: {day_value}")
        root = self._partial_root_for_day(normalized)
        units_dir = self._partial_units_dir(normalized)
        root.mkdir(parents=True, exist_ok=True)
        units_dir.mkdir(parents=True, exist_ok=True)
        self._remove_ready_index_entry(normalized)

        info = self.get_partial_info(normalized)
        meta_payload = info.get("meta") if isinstance(info, dict) else {}
        if not isinstance(meta_payload, dict):
            meta_payload = {}
        meta_payload["day"] = normalized
        meta_payload["version"] = self.VERSION
        if start_ts is not None:
            try:
                meta_payload["start_ts"] = int(start_ts)
            except Exception:
                meta_payload["start_ts"] = start_ts
        if end_ts is not None:
            try:
                meta_payload["end_ts"] = int(end_ts)
            except Exception:
                meta_payload["end_ts"] = end_ts
        if label is not None:
            meta_payload["label"] = label
        if total_units is not None:
            try:
                meta_payload["total_units"] = int(total_units)
            except Exception:
                meta_payload["total_units"] = total_units
        meta_payload["status"] = "in_progress"
        meta_payload["updated_ts"] = int(time.time())
        self._write_json_atomic(self._partial_meta_path(normalized), meta_payload)
        drain_cache_log.info(
            "ensure_partial day=%s total=%s status=%s caller=%s",
            normalized,
            meta_payload.get("total_units"),
            meta_payload.get("status"),
            self._stack_info(),
        )
        return meta_payload

    def update_partial_meta(
        self,
        day_value: Union[str, date, datetime],
        *,
        status: Optional[str] = None,
        completed: Optional[int] = None,
        total: Optional[int] = None,
        failed: Optional[int] = None,
    ) -> None:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return
        info = self.get_partial_info(normalized) or {}
        meta_payload = info.get("meta") if isinstance(info, dict) else {}
        if not isinstance(meta_payload, dict):
            meta_payload = {}
        if status is not None:
            meta_payload["status"] = status
        if completed is not None:
            try:
                meta_payload["completed_units"] = int(completed)
            except Exception:
                meta_payload["completed_units"] = completed
        if total is not None:
            try:
                meta_payload["total_units"] = int(total)
            except Exception:
                meta_payload["total_units"] = total
        if failed is not None:
            try:
                meta_payload["failed_units"] = int(failed)
            except Exception:
                meta_payload["failed_units"] = failed
        meta_payload["updated_ts"] = int(time.time())
        meta_payload["day"] = normalized
        meta_payload.setdefault("version", self.VERSION)
        self._write_json_atomic(self._partial_meta_path(normalized), meta_payload)
        drain_cache_log.info(
            "update_partial_meta day=%s status=%s completed=%s total=%s failed=%s caller=%s",
            normalized,
            meta_payload.get("status"),
            meta_payload.get("completed_units"),
            meta_payload.get("total_units"),
            meta_payload.get("failed_units"),
            self._stack_info(),
        )

    def store_partial_unit(
        self,
        day_value: Union[str, date, datetime],
        unit_id: Union[str, int],
        payload: Dict[str, Any],
    ) -> None:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return
        units_dir = self._partial_units_dir(normalized)
        units_dir.mkdir(parents=True, exist_ok=True)
        try:
            unit_key = str(int(unit_id))
        except Exception:
            unit_key = str(unit_id)
        final_path = units_dir / f"{unit_key}.json"
        raw_path = units_dir / f"{unit_key}.raw.json"
        raw_payload = copy.deepcopy(payload)
        filtered_payload = self._filter_unit_payload(payload)
        with self._unit_store_lock:
            self._write_json_atomic(raw_path, raw_payload)
            self._write_json_atomic(final_path, filtered_payload)
            with contextlib.suppress(FileNotFoundError, PermissionError):
                raw_path.unlink()
        drain_cache_log.info(
            "store_partial_unit day=%s unit=%s caller=%s",
            normalized,
            unit_key,
            self._stack_info(),
        )

    def record_partial_failure(
        self, day_value: Union[str, date, datetime], unit_id: Union[str, int]
    ) -> None:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return
        failures_path = self._partial_failures_path(normalized)
        failures_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(failures_path, "a", encoding="utf-8") as fh:
                fh.write(f"{int(unit_id)}\n")
            drain_cache_log.info(
                "record_partial_failure day=%s unit=%s caller=%s",
                normalized,
                unit_id,
                self._stack_info(),
            )
        except Exception as exc:
            log.debug(
                "message_cache: failed to record partial failure day=%s unit=%s: %s",
                normalized,
                unit_id,
                exc,
            )

    def clear_partial_failure(
        self, day_value: Union[str, date, datetime], unit_id: Union[str, int]
    ) -> None:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return
        failures_path = self._partial_failures_path(normalized)
        if not failures_path.exists():
            return
        try:
            with open(failures_path, "r", encoding="utf-8") as fh:
                lines = fh.readlines()
        except Exception as exc:
            log.debug(
                "message_cache: failed to read partial failures for cleanup %s: %s",
                failures_path,
                exc,
            )
            return
        unit_text = str(unit_id).strip()
        if not unit_text:
            target = ""
        else:
            try:
                target = str(int(unit_text))
            except Exception:
                target = unit_text
        filtered = [line for line in lines if line.strip() != target]
        if filtered == lines:
            return
        tmp_path = failures_path.with_name(f"{failures_path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                fh.writelines(filtered)
            os.replace(tmp_path, failures_path)
            drain_cache_log.info(
                "clear_partial_failure day=%s unit=%s caller=%s",
                normalized,
                unit_id,
                self._stack_info(),
            )
        except Exception as exc:
            log.debug(
                "message_cache: failed to rewrite partial failures day=%s unit=%s: %s",
                normalized,
                unit_id,
                exc,
            )
            with contextlib.suppress(Exception):
                tmp_path.unlink()

    def partial_unit_ids(self, day_value: Union[str, date, datetime]) -> Set[int]:
        info = self.get_partial_info(day_value)
        if not info:
            return set()
        return set(info.get("units") or [])

    def load_day(self, day_value: Union[str, date, datetime]) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return None
        path = self._path_for_day(normalized)
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            return None
        except Exception as exc:
            log.debug("message_cache: failed to load %s: %s", path, exc)
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def save_day(self, day_value: Union[str, date, datetime], payload: Dict[str, Any]) -> None:
        normalized = self._normalize_day(day_value)
        if not normalized:
            raise ValueError(f"invalid day value: {day_value}")
        data = copy.deepcopy(payload)
        data["version"] = self.VERSION
        data["day"] = normalized
        units_raw = data.get("units")
        if isinstance(units_raw, dict):
            normalized_units: Dict[str, Any] = {}
            for unit_id, unit_payload in units_raw.items():
                unit_copy = copy.deepcopy(unit_payload)
                series_raw = unit_copy.get("series")
                if isinstance(series_raw, dict):
                    unit_copy["series"] = {
                        str(sensor_id): list(series or []) for sensor_id, series in series_raw.items()
                    }
                stats_raw = unit_copy.get("stats")
                if isinstance(stats_raw, dict):
                    unit_copy["stats"] = {
                        str(sensor_id): dict(stats) if isinstance(stats, dict) else {}
                        for sensor_id, stats in stats_raw.items()
                    }
                normalized_units[str(unit_id)] = unit_copy
            data["units"] = normalized_units
        tmp_path = self._path_for_day(f"{normalized}.tmp-{os.getpid()}-{threading.get_ident()}")
        final_path = self._path_for_day(normalized)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        with gzip.open(tmp_path, "wt", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        os.replace(tmp_path, final_path)
        try:
            entry = self._entry_from_ready_payload(normalized, data)
            self._set_ready_index_entry(normalized, entry)
        except Exception as exc:
            log.debug("message_cache: failed to refresh index for day=%s: %s", normalized, exc)

    def finalize_partial(
        self,
        day_value: Union[str, date, datetime],
        *,
        expected_total: Optional[int] = None,
        mark_incomplete: bool = False,
    ) -> Optional[Dict[str, Any]]:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return None
        info = self.get_partial_info(normalized)
        if not info:
            return None

        units_dir = self._partial_units_dir(normalized)
        units_payload: Dict[str, Any] = {}
        if units_dir.exists():
            for path in sorted(units_dir.glob("*.json")):
                if path.name.endswith(".raw.json"):
                    continue
                if not path.is_file():
                    continue
                try:
                    with open(path, "r", encoding="utf-8") as fh:
                        unit_payload = json.load(fh)
                except Exception as exc:
                    log.debug("message_cache: failed to read partial unit %s: %s", path, exc)
                    continue
                units_payload[path.stem] = unit_payload

        failures = info.get("failures") or []
        meta = info.get("meta") or {}
        dump_ts = int(time.time())
        total_units_val = expected_total or meta.get("total_units")
        try:
            total_units_int = int(total_units_val) if total_units_val is not None else None
        except Exception:
            total_units_int = None
        completed_units = len(units_payload)
        payload = {
            "start_ts": meta.get("start_ts"),
            "end_ts": meta.get("end_ts"),
            "dump_ts": dump_ts,
            "label": meta.get("label"),
            "units": units_payload,
            "failed_units": sorted(set(failures)),
            "completed_units": completed_units,
        }
        if total_units_int is not None:
            payload["total_units"] = total_units_int
        else:
            payload["total_units"] = completed_units

        incomplete = mark_incomplete
        if total_units_int is not None and completed_units < total_units_int:
            incomplete = True
        if payload["failed_units"]:
            incomplete = True
        if incomplete:
            payload["incomplete"] = True

        drain_cache_log.info(
            "finalize_partial start day=%s mark_incomplete=%s completed=%s total_hint=%s caller=%s",
            normalized,
            mark_incomplete,
            completed_units,
            total_units_int,
            self._stack_info(),
        )

        self.save_day(normalized, payload)
        drain_cache_log.info("finalize_partial saved day=%s", normalized)
        try:
            shutil.rmtree(self._partial_root_for_day(normalized))
            drain_cache_log.info("finalize_partial removed_partial_dir day=%s", normalized)
        except FileNotFoundError:
            drain_cache_log.info("finalize_partial partial_dir_missing day=%s", normalized)
        except Exception as exc:
            log.debug("message_cache: failed to remove partial %s: %s", normalized, exc)
            drain_cache_log.warning(
                "finalize_partial remove_partial_failed day=%s err=%s caller=%s",
                normalized,
                exc,
                self._stack_info(),
            )
        return payload

    def discard_partial(self, day_value: Union[str, date, datetime]) -> bool:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return False
        root = self._partial_root_for_day(normalized)
        if not root.exists():
            drain_cache_log.info("discard_partial skipped_missing day=%s caller=%s", normalized, self._stack_info())
            return False
        try:
            shutil.rmtree(root)
            drain_cache_log.info("discard_partial removed day=%s caller=%s", normalized, self._stack_info())
            return True
        except FileNotFoundError:
            drain_cache_log.info("discard_partial missing_on_remove day=%s caller=%s", normalized, self._stack_info())
            return False
        except Exception as exc:
            log.debug("message_cache: failed to discard partial %s: %s", root, exc)
            drain_cache_log.warning(
                "discard_partial failed day=%s err=%s caller=%s",
                normalized,
                exc,
                self._stack_info(),
            )
            return False

    def delete_day(self, day_value: Union[str, date, datetime]) -> bool:
        normalized = self._normalize_day(day_value)
        if not normalized:
            return False
        path = self._path_for_day(normalized)
        try:
            path.unlink()
            removed = True
            drain_cache_log.info("delete_day removed_file day=%s caller=%s", normalized, self._stack_info())
        except FileNotFoundError:
            removed = False
            drain_cache_log.info("delete_day file_missing day=%s caller=%s", normalized, self._stack_info())
        except Exception as exc:
            log.debug("message_cache: failed to delete %s: %s", path, exc)
            removed = False
            drain_cache_log.warning(
                "delete_day failed day=%s err=%s caller=%s",
                normalized,
                exc,
                self._stack_info(),
            )
        self.discard_partial(normalized)
        self._remove_ready_index_entry(normalized)
        return removed

    def clear_all(self) -> int:
        drain_cache_log.info("clear_all invoked caller=%s", self._stack_info())
        removed = 0
        for path in list(self.base_dir.glob("messages-*.json.gz")):
            try:
                path.unlink()
                removed += 1
            except FileNotFoundError:
                continue
            except Exception as exc:
                log.debug("message_cache: failed to remove %s: %s", path, exc)
        for root in list(self.partial_dir.glob("messages-*")):
            try:
                shutil.rmtree(root)
            except FileNotFoundError:
                continue
            except Exception as exc:
                log.debug("message_cache: failed to remove partial dir %s: %s", root, exc)
        with self._index_lock:
            self._index_cache = {}
            self._index_mtime = None
            try:
                self.index_path.unlink()
            except FileNotFoundError:
                pass
            except Exception as exc:
                log.debug("message_cache: failed to remove index %s: %s", self.index_path, exc)
        return removed


MESSAGE_CACHE_DIR = BASE_DIR / "data" / "drain_messages"
MESSAGE_CACHE = DrainMessageCacheStore(MESSAGE_CACHE_DIR)


def _parse_quiet_hours(raw: str) -> Optional[Tuple[int, int]]:
    if not raw:
        return None
    try:
        start_raw, end_raw = raw.split("-")
        h1, m1 = map(int, start_raw.split(":"))
        h2, m2 = map(int, end_raw.split(":"))
    except Exception:
        return None
    start = max(0, min(23, h1)) * 60 + max(0, min(59, m1))
    end = max(0, min(23, h2)) * 60 + max(0, min(59, m2))
    return start, end


def _in_quiet_hours(qh: Optional[Tuple[int, int]]) -> bool:
    if not qh:
        return True
    now_local = datetime.now()
    current = now_local.hour * 60 + now_local.minute
    start, end = qh
    if start <= end:
        return start <= current < end
    return current >= start or current < end


def refresh_file_cache_forever(
    client_factory: Callable[[], "WialonClient"], interval: int, ttl: int
) -> None:
    interval = max(60, int(interval) if interval else 60)
    ttl = max(0, int(ttl) if ttl else 0)
    qh = _parse_quiet_hours(GEO_CACHE_QUIET_HOURS_RAW)
    jitter = max(0.0, min(0.5, float(GEO_CACHE_JITTER)))
    chunk_res = max(1, GEO_CACHE_CHUNK_RES_DEFAULT)
    sleep_ms = max(0, GEO_CACHE_SLEEP_BETWEEN_CHUNKS_MS_DEFAULT)
    include_center = bool(GEO_INCLUDE_CENTER_DEFAULT)

    while True:
        if jitter > 0:
            time.sleep(random.uniform(0, jitter) * max(5.0, float(interval)))

        try:
            fresh_payload = ZONE_STORE.load_if_present()
            age = time.time() - (ZONE_STORE.mtime or 0)
            if fresh_payload and age < ttl:
                geo_log.debug(
                    "file_cache: fresh (age=%.0fs < ttl=%ss), skip rebuild",
                    age,
                    ttl,
                )
                time.sleep(interval)
                continue

            if not _in_quiet_hours(qh):
                geo_log.info("file_cache: outside quiet hours, skip this cycle")
                time.sleep(interval)
                continue

            geo_log.info("file_cache: rebuild start include_center=%s", include_center)
            cli = client_factory()
            resource_ids = cli.list_resource_ids()
            flags = 16 | (4 if include_center else 0)

            if not getattr(cli, "_zone_all_strategy", None) and resource_ids:
                try:
                    cli._zone_all_strategy = cli._detect_zone_all_strategy(resource_ids[0], flags)
                except Exception as exc:
                    geo_log.warning("file_cache: detect strategy failed err=%r", exc)
                    cli._zone_all_strategy = "omit"

            strategy_label = cli._zone_all_strategy or cli.GEO_ZONE_ALL_STRATEGY or "omit"
            geo_log.info("file_cache: using strategy=%s", strategy_label)

            zone_ids_by_resource: Dict[int, List[int]] = {}
            zone_meta: Dict[str, Dict[str, Any]] = {}
            for idx in range(0, len(resource_ids), chunk_res):
                part = resource_ids[idx : idx + chunk_res]
                started = time.perf_counter()
                for rid in part:
                    params = cli._zone_all_params(rid, None, flags)
                    try:
                        resp = cli.request("resource/get_zone_data", params)
                    except Exception as exc:
                        geo_log.debug(
                            "file_cache: resource %s load failed: %s",
                            rid,
                            exc,
                        )
                        continue
                    parsed = cli._parse_zone_data_response(resp)
                    zone_ids_by_resource[rid] = sorted(parsed.keys())
                    for zid, payload in parsed.items():
                        meta_key = f"{rid}:{zid}"
                        name_val = None
                        if isinstance(payload, dict):
                            name_val = payload.get("n") or payload.get("name") or payload.get("nm")
                        if not isinstance(name_val, str):
                            name_val = f"ID {zid}"
                        if include_center:
                            center = None
                            if isinstance(payload, dict):
                                center = payload.get("ct") or payload.get("c") or payload.get("center")
                            zone_meta[meta_key] = {"n": name_val, "ct": center}
                        else:
                            zone_meta[meta_key] = {"n": name_val, "ct": None}
                geo_log.info(
                    "file_cache: chunk %s..%s done in %.2fs (res=%s)",
                    idx,
                    idx + len(part) - 1,
                    time.perf_counter() - started,
                    len(part),
                )
                if sleep_ms:
                    time.sleep(sleep_ms / 1000.0)

            payload = {
                "version": 1,
                "dump_ts": int(time.time()),
                "strategy": strategy_label,
                "flags_used": flags,
                "zone_ids_by_resource": {rid: ids for rid, ids in zone_ids_by_resource.items()},
                "zone_meta": zone_meta,
            }
            ZONE_STORE.save_atomic(payload)
            total_zones = sum(len(v) for v in zone_ids_by_resource.values())
            geo_log.info(
                "file_cache: rebuild done zones=%s resources=%s",
                total_zones,
                len(zone_ids_by_resource),
            )
        except Exception as exc:
            geo_log.error("file_cache: rebuild failed: %r", exc)

        time.sleep(interval)


def _normalize_unit_sensors(sens_raw: Any) -> Dict[str, Dict[str, Any]]:
    sensors: Dict[str, Dict[str, Any]] = {}
    if isinstance(sens_raw, dict):
        iterable = sens_raw.values()
    elif isinstance(sens_raw, list):
        iterable = sens_raw
    else:
        return sensors

    for idx, sensor in enumerate(iterable, start=1):
        if not isinstance(sensor, dict):
            continue
        sensor_copy = copy.deepcopy(sensor)
        sid = sensor_copy.get("id")
        key: Optional[str] = None
        if isinstance(sid, bool):
            sid = int(sid)
        if isinstance(sid, (int, float)):
            try:
                numeric_id = int(sid)
            except Exception:
                numeric_id = None
            if numeric_id is not None:
                sensor_copy["id"] = numeric_id
                key = str(numeric_id)
        elif isinstance(sid, str) and sid.strip():
            key = sid.strip()
            try:
                sensor_copy["id"] = int(key)
            except Exception:
                pass
        if not key:
            key = f"idx:{idx}"
        sensors[key] = sensor_copy
    return sensors


def _ensure_unit_item_normalized(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}

    normalized = copy.deepcopy(raw)
    name = normalized.get("nm")
    if isinstance(name, str):
        normalized["nm"] = name.strip()
    try:
        normalized["id"] = int(normalized.get("id"))
    except Exception:
        pass

    sensors = _normalize_unit_sensors(normalized.get("sens"))
    if sensors:
        normalized["sens"] = sensors
    else:
        normalized.pop("sens", None)

    for key in ("pos", "lmsg"):
        value = normalized.get(key)
        if not isinstance(value, dict):
            normalized.pop(key, None)

    return normalized


def _apply_device_meta_fields(entry: Dict[str, Any], meta: Mapping[str, Any]) -> None:
    if not isinstance(entry, dict) or not isinstance(meta, Mapping):
        return
    device_block = entry.get("device")
    if isinstance(device_block, dict):
        target_device = device_block
    else:
        target_device = {}
        entry["device"] = target_device

    device_uid = meta.get("device_uid")
    if device_uid:
        entry["uid"] = device_uid
        target_device["uid"] = device_uid

    device_type_id = meta.get("device_type_id")
    if device_type_id is not None:
        entry["hw"] = device_type_id
        target_device["hw"] = device_type_id

    device_type_name = meta.get("device_type_name")
    if device_type_name:
        entry["hardware"] = device_type_name
        target_device["hardware"] = device_type_name


def _collect_device_clients(base_client: "WialonClient") -> Tuple[List[Tuple[str, "WialonClient"]], List["WialonClient"]]:
    cfg = load_pipeline_config()
    raw_tokens: List[Tuple[str, str]] = []
    primary_token = getattr(cfg, "wialon_token", None)
    if primary_token:
        raw_tokens.append(("primary", primary_token))
    for idx, token in enumerate(getattr(cfg, "wialon_extra_tokens", ()), 1):
        if token:
            raw_tokens.append((f"extra-{idx}", token))
    deduped: List[Tuple[str, str]] = []
    seen: Set[str] = set()
    for label, token in raw_tokens:
        token = token.strip()
        if not token or token in seen:
            continue
        seen.add(token)
        deduped.append((label, token))
    if not deduped:
        return [], []
    clients: List[Tuple[str, WialonClient]] = []
    extra_clients: List[WialonClient] = []
    base_token = getattr(base_client, "token", None)
    for label, token in deduped[: UNIT_DEVICE_TOKENS_LIMIT or len(deduped)]:
        if base_token and token == base_token and all(c is not base_client for _, c in clients):
            clients.append((label, base_client))
        else:
            new_client = WialonClient(base_client.host, token)
            clients.append((label, new_client))
            extra_clients.append(new_client)
    if not clients:
        clients.append(("primary", base_client))
    return clients, extra_clients


def _fetch_device_meta_with_batches(
    base_client: "WialonClient",
    unit_ids: Sequence[int],
) -> Dict[int, Dict[str, Any]]:
    if not unit_ids:
        return {}
    clients, extra_clients = _collect_device_clients(base_client)
    if not clients:
        snapshot_log.warning("unit_snapshot: no tokens available for device details")
        return {}
    chunk_size = max(1, UNIT_DEVICE_BATCH_SIZE)
    retries = max(1, UNIT_DEVICE_BATCH_RETRIES)
    backoff = UNIT_DEVICE_BATCH_BACKOFF
    max_backoff = UNIT_DEVICE_BATCH_BACKOFF_MAX
    workers = max(1, min(len(clients), UNIT_DEVICE_WORKERS))
    task_queue: queue.Queue[Tuple[int, List[int]]] = queue.Queue()
    chunks = list(_chunked(unit_ids, chunk_size))
    total_chunks = len(chunks)
    for idx, chunk in enumerate(chunks, 1):
        task_queue.put((idx, list(chunk)))
    results: Dict[int, Dict[str, Any]] = {}
    results_lock = threading.Lock()

    device_flags = 1 | UNIT_FLAGS_DEVICE_META

    def _execute_batch(label: str, client: "WialonClient", chunk_index: int, chunk: List[int]) -> None:
        processed = 0
        for uid in chunk:
            delay = backoff
            attempt = 0
            while True:
                attempt += 1
                try:
                    data = client.request("core/search_item", {"id": uid, "flags": device_flags})
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    snapshot_log.warning(
                        "unit_snapshot: device chunk=%s/%s token=%s unit=%s attempt=%s err=%s",
                        chunk_index,
                        total_chunks,
                        label,
                        uid,
                        attempt,
                        exc,
                    )
                    if attempt >= retries:
                        break
                    client.abort_pending_requests()
                    base_client._sleep_with_cancel(min(delay, max_backoff))
                    delay = min(delay * 2, max_backoff)
                    continue

                if isinstance(data, dict) and data.get("error"):
                    snapshot_log.debug(
                        "unit_snapshot: device response error unit=%s err=%s",
                        uid,
                        data.get("error"),
                    )
                    break

                item = data.get("item") if isinstance(data, dict) else None
                if not isinstance(item, dict):
                    break
                details = base_client._extract_device_details(item)
                if details:
                    with results_lock:
                        results[uid] = details
                    processed += 1
                break
        if processed:
            snapshot_log.debug(
                "unit_snapshot: device chunk=%s/%s token=%s processed=%s",
                chunk_index,
                total_chunks,
                label,
                processed,
            )

    def _worker(label: str, client: "WialonClient") -> None:
        while True:
            try:
                chunk_index, chunk = task_queue.get_nowait()
            except queue.Empty:
                break
            try:
                _execute_batch(label, client, chunk_index, chunk)
            finally:
                task_queue.task_done()

    snapshot_log.info(
        "unit_snapshot: device details via batch tokens=%s workers=%s chunk_size=%s total_chunks=%s",
        len(clients),
        workers,
        chunk_size,
        total_chunks,
    )
    threads: List[threading.Thread] = []
    try:
        for label, client in clients[:workers]:
            thread = threading.Thread(
                target=_worker,
                name=f"device-meta-{label}",
                args=(label, client),
                daemon=True,
            )
            thread.start()
            threads.append(thread)
        task_queue.join()
    finally:
        for thread in threads:
            thread.join(timeout=0.1)
        for extra in extra_clients:
            with contextlib.suppress(Exception):
                extra.close()

    snapshot_log.info(
        "unit_snapshot: device details fetched units=%s/%s",
        len(results),
        len(unit_ids),
    )
    return results


def _build_unit_snapshot_payload(
    client: "WialonClient", *, page_size: int, sleep_between_pages_ms: int
) -> Tuple[Dict[int, Dict[str, Any]], int]:
    spec = {
        "itemsType": "avl_unit",
        "propName": "sys_name",
        "propValueMask": "*",
        "sortType": "sys_name",
    }
    items_by_id: Dict[int, Dict[str, Any]] = {}
    offset = 0
    total_loaded = 0
    sleep_between = max(0, int(sleep_between_pages_ms))

    while True:
        params = {
            "spec": spec,
            "force": 1,
            "flags": UNIT_FLAGS_SNAPSHOT,
            "from": offset,
            "to": offset + page_size - 1,
        }
        started = time.perf_counter()
        try:
            snapshot_log.debug(
                "unit_snapshot: core/search_items request offset=%s size=%s", offset, page_size
            )
            data = client.request("core/search_items", params)
        except Exception as exc:
            snapshot_log.error(
                "unit_snapshot: core/search_items failed offset=%s size=%s err=%s",
                offset,
                page_size,
                exc,
            )
            raise
        duration = time.perf_counter() - started
        raw_items = data.get("items", []) if isinstance(data, dict) else []
        snapshot_log.debug(
            "unit_snapshot: page from=%s count=%s duration=%.3fs",
            offset,
            len(raw_items),
            duration,
        )
        if not raw_items:
            break
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            try:
                uid = int(raw.get("id"))
            except Exception:
                continue
            name = str(raw.get("nm") or f"id {uid}")
            minimal_entry: Dict[str, Any] = {"id": uid, "nm": name}
            inline_meta = client._extract_device_details(raw)
            if inline_meta:
                _apply_device_meta_fields(minimal_entry, inline_meta)
            device_block = minimal_entry.get("device")
            payload: Dict[str, Any] = {"nm": name}
            if isinstance(device_block, dict) and device_block:
                compact_device = {}
                if device_block.get("uid"):
                    compact_device["uid"] = device_block["uid"]
                if device_block.get("hardware"):
                    compact_device["hardware"] = device_block["hardware"]
                if compact_device:
                    payload["device"] = compact_device
            if minimal_entry.get("uid"):
                payload["uid"] = minimal_entry["uid"]
            if minimal_entry.get("hardware"):
                payload["hardware"] = minimal_entry["hardware"]
            items_by_id[uid] = payload
        total_loaded += len(raw_items)
        if len(raw_items) < page_size:
            break
        offset += page_size
        if sleep_between:
            time.sleep(sleep_between / 1000.0)

    return items_by_id, total_loaded


def _write_unit_snapshot_meta(meta: Dict[str, Any]) -> None:
    try:
        UNIT_SNAPSHOT_META_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(UNIT_SNAPSHOT_META_PATH, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, ensure_ascii=False)
    except Exception as exc:
        log.debug("unit_snapshot: failed to write meta: %s", exc)


def _load_unit_snapshot_meta() -> Dict[str, Any]:
    try:
        with open(UNIT_SNAPSHOT_META_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception as exc:
        log.debug("unit_snapshot: failed to read meta: %s", exc)
        return {}
    return dict(data) if isinstance(data, dict) else {}


def _persist_unit_snapshot(items_by_id: Dict[int, Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    snapshot = {uid: copy.deepcopy(item) for uid, item in items_by_id.items()}
    payload = {
        "version": 1,
        "dump_ts": int(time.time()),
        "flags_used": UNIT_FLAGS_SNAPSHOT,
        "items_by_id": {str(uid): item for uid, item in snapshot.items()},
    }
    try:
        service = get_unit_snapshot_service()
        snapshot_path = getattr(service, "snapshot_path", DEFAULT_SNAPSHOT_PATH)
        updated = sync_unit_snapshot_from_payload(
            payload,
            service,
            source_kind="unit_snapshot",
            force=True,
        )
        if updated:
            snapshot_log.info(
                "unit_snapshot refreshed path=%s units=%s dump_ts=%s",
                snapshot_path,
                len(snapshot),
                payload["dump_ts"],
            )
        else:
            snapshot_log.debug(
                "unit_snapshot skipped path=%s dump_ts=%s",
                snapshot_path,
                payload["dump_ts"],
            )
    except Exception as exc:
        snapshot_log.warning("unit_snapshot: persist failed: %s", exc)
    try:
        _write_unit_snapshot_meta(
            {
                "count": len(snapshot),
                "dump_ts": payload["dump_ts"],
                "updated_ts": int(time.time()),
            }
        )
    except Exception:
        pass
    return snapshot


def _rebuild_unit_snapshot(
    client: "WialonClient",
) -> Tuple[Dict[int, Dict[str, Any]], int, float]:
    page_size = max(1, int(UNIT_SNAPSHOT_PAGE_SIZE))
    sleep_between = UNIT_SNAPSHOT_SLEEP_BETWEEN_PAGES_MS
    started = time.perf_counter()
    snapshot_log.info(
        "unit_snapshot: rebuild start host=%s page_size=%s sleep_ms=%s",
        getattr(client, "host", "?"),
        page_size,
        sleep_between,
    )
    items_by_id, total_loaded = _build_unit_snapshot_payload(
        client,
        page_size=page_size,
        sleep_between_pages_ms=sleep_between,
    )
    snapshot = _persist_unit_snapshot(items_by_id)
    duration = time.perf_counter() - started
    snapshot_log.info(
        "unit_snapshot: rebuild done units=%s total_loaded=%s duration=%.2fs",
        len(snapshot),
        total_loaded,
        duration,
    )
    return snapshot, total_loaded, duration


def rebuild_unit_snapshot_sync(client: "WialonClient") -> Tuple[int, int, float]:
    log.info("unit_snapshot: manual rebuild requested")
    snapshot, total_loaded, duration = _rebuild_unit_snapshot(client)
    count = len(snapshot)
    log.info(
        "unit_snapshot: manual rebuild finished units=%s total_loaded=%s duration=%.3fs",
        count,
        total_loaded,
        duration,
    )
    return count, total_loaded, duration


def refresh_unit_snapshot_forever(
    client_factory: Callable[[], "WialonClient"], interval: int, ttl: int
) -> None:
    interval = max(10, int(interval) if interval else 10)
    ttl = max(0, int(ttl) if ttl else 0)
    jitter = max(0.0, float(UNIT_SNAPSHOT_JITTER))

    while True:
        started = time.time()
        meta = _load_unit_snapshot_meta()
        last_dump_ts = int(meta.get("dump_ts") or 0) if isinstance(meta.get("dump_ts"), (int, float)) else 0
        age = started - int(meta.get("updated_ts") or 0)
        should_refresh = not meta or (ttl and age >= ttl)
        if not should_refresh:
            snapshot_log.debug(
                "unit_snapshot: skip refresh age=%.0fs ttl=%ss dump_ts=%s",
                age,
                ttl,
                last_dump_ts,
            )
        else:
            try:
                log.info(
                    "unit_snapshot: scheduled refresh start interval=%ss ttl=%ss page_size=%s",
                    interval,
                    ttl,
                    UNIT_SNAPSHOT_PAGE_SIZE,
                )
                cli = client_factory()
                try:
                    _rebuild_unit_snapshot(cli)
                finally:
                    with contextlib.suppress(Exception):
                        cli.close()
            except Exception as exc:
                log.error("unit_snapshot: refresh failed: %r", exc, exc_info=True)

        sleep_time = float(interval)
        if jitter > 0.0:
            sleep_time += random.uniform(0.0, jitter) * max(5.0, float(interval))
        elapsed = time.time() - started
        remaining = max(15.0, sleep_time - max(0.0, elapsed))
        time.sleep(remaining)


token_store = TokenStore(DATA_DIR / "auth_tokens.sqlite3")

# Кэш клиентов Wialon (по chat_id) и глобальный кэш зон
_WIALON_CLIENT_CACHE_LOCK = threading.Lock()
_WIALON_CLIENT_CACHE: Dict[int, "WialonClient"] = {}

_ZONE_CACHE_LOCK = threading.Lock()
_ZONE_CACHE_TS: float = 0.0
_ZONE_CACHE_DATA: Dict[int, Dict[int, Dict[str, Any]]] = {}

_GEO_FILE_UPDATER_LOCK = threading.Lock()
_GEO_FILE_UPDATER_STARTED = False

_UNIT_SNAPSHOT_UPDATER_LOCK = threading.Lock()
_UNIT_SNAPSHOT_UPDATER_STARTED = False
_UNIT_SNAPSHOT_DAEMON_LOCK = threading.Lock()
_UNIT_SNAPSHOT_DAEMON_STARTED = False


@functools.lru_cache(maxsize=1)
def get_unit_config_service() -> UnitConfigService:
    return UnitConfigService(UNIT_CONFIG_DIR, schema_path=None)


@functools.lru_cache(maxsize=512)
def _load_unit_config(unit_id: int) -> Optional[UnitConfig]:
    service = get_unit_config_service()
    try:
        return service.load(unit_id)
    except FileNotFoundError:
        return None
    except UnitConfigValidationError as exc:
        log.warning("unit_config: validation failed unit=%s err=%s", unit_id, exc)
        return None
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("unit_config: failed to load unit=%s err=%s", unit_id, exc)
        return None


def ensure_geo_file_updater(client: "WialonClient") -> None:
    if not GEO_FILE_UPDATER_ENABLED:
        return
    global _GEO_FILE_UPDATER_STARTED
    with _GEO_FILE_UPDATER_LOCK:
        if _GEO_FILE_UPDATER_STARTED:
            return
        if not client.token:
            geo_log.info("file_cache: cannot start updater without token")
            return

        host = client.host
        token = client.token

        def factory() -> "WialonClient":
            return WialonClient(host, token)

        thread = threading.Thread(
            target=refresh_file_cache_forever,
            args=(factory, GEO_CACHE_REFRESH_INTERVAL, GEO_CACHE_TTL),
            name="geo-file-cache",
            daemon=True,
        )
        thread.start()
        _GEO_FILE_UPDATER_STARTED = True
        geo_log.info(
            "file_cache: updater started interval=%ss ttl=%ss quiet=%s jitter=%.2f path=%s",
            GEO_CACHE_REFRESH_INTERVAL,
            GEO_CACHE_TTL,
            GEO_CACHE_QUIET_HOURS_RAW or "any",
            GEO_CACHE_JITTER,
            str(GEO_CACHE_PATH),
        )


def ensure_unit_snapshot_updater(client: "WialonClient") -> None:
    if not UNIT_SNAPSHOT_UPDATER_ENABLED:
        return
    global _UNIT_SNAPSHOT_UPDATER_STARTED
    with _UNIT_SNAPSHOT_UPDATER_LOCK:
        if _UNIT_SNAPSHOT_UPDATER_STARTED:
            return
        if not client.token:
            log.info("unit_file_cache: cannot start updater without token")
            return

        host = client.host
        token = client.token

        def factory() -> "WialonClient":
            return WialonClient(host, token)

        thread = threading.Thread(
            target=refresh_unit_snapshot_forever,
            args=(factory, UNIT_SNAPSHOT_REFRESH_INTERVAL, UNIT_SNAPSHOT_TTL),
            name="unit-snapshot-cache",
            daemon=True,
        )
        thread.start()
        _UNIT_SNAPSHOT_UPDATER_STARTED = True
        log.info(
            "unit_snapshot: updater started interval=%ss ttl=%ss page_size=%s",
            UNIT_SNAPSHOT_REFRESH_INTERVAL,
            UNIT_SNAPSHOT_TTL,
            UNIT_SNAPSHOT_PAGE_SIZE,
        )


def _set_active_ui(context: ContextTypes.DEFAULT_TYPE, ui: Optional[str]) -> None:
    if not ui:
        context.chat_data.pop(ACTIVE_UI_KEY, None)
        return
    context.chat_data[ACTIVE_UI_KEY] = ui


def _active_ui(context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    return context.chat_data.get(ACTIVE_UI_KEY)


async def _mute_admin_ui(context: ContextTypes.DEFAULT_TYPE) -> None:
    panel = context.chat_data.get("admin_panel") or {}
    anchor = panel.get("anchor")
    if not anchor:
        return
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=anchor["chat_id"],
            message_id=anchor["message_id"],
            reply_markup=None,
        )
    except Exception as exc:
        log.debug("suppress_admin_keyboard failed: %s", exc)


async def _activate_objects_ui(context: ContextTypes.DEFAULT_TYPE) -> None:
    await _mute_admin_ui(context)
    _set_active_ui(context, UI_OBJECTS)



async def _suppress_objects_keyboards(context: ContextTypes.DEFAULT_TYPE) -> None:
    await clear_stats_buttons(context)
    _clear_stats_generation(context)
    anchor = context.user_data.get("anchor")
    if not anchor:
        return
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=anchor["chat_id"],
            message_id=anchor["message_id"],
            reply_markup=None,
        )
    except Exception as exc:
        log.debug("suppress_objects_keyboards failed: %s", exc)

T = TypeVar("T")

# =================== AUTH HELPERS ===================


class NotAuthorized(Exception):
    """Raised when a chat doesn't have a stored Wialon token."""


class UserBlocked(Exception):
    """Raised when a chat is blocked from using the bot."""


def wialon_for_chat(chat_id: Optional[int]) -> "WialonClient":
    if chat_id is None:
        log.warning("Запрос к Wialon без известного chat_id")
        raise NotAuthorized("chat_id is missing")
    if token_store.is_blocked(chat_id):
        log.warning("Попытка обращения к Wialon от заблокированного chat_id=%s", chat_id)
        raise UserBlocked(f"blocked chat {chat_id}")
    token = token_store.get_token(chat_id)
    if not token:
        log.warning("Попытка обращения к Wialon без авторизации (chat_id=%s)", chat_id)
        raise NotAuthorized(f"no token for chat {chat_id}")
    with _WIALON_CLIENT_CACHE_LOCK:
        cached = _WIALON_CLIENT_CACHE.get(chat_id)
        if cached and cached.token == token and cached.host == WIALON_HOST:
            return cached
        log.debug("Создание клиента Wialon для chat_id=%s", chat_id)
        client = WialonClient(WIALON_HOST, token)
        _WIALON_CLIENT_CACHE[chat_id] = client
        return client


async def run_blocking(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    loop = asyncio.get_running_loop()
    bound = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(EXECUTOR, bound)


def _load_access_denied_cache_sync() -> None:
    global _ACCESS_DENIED_CACHE_LOADED, _ACCESS_DENIED_CACHE
    with _ACCESS_DENIED_CACHE_LOCK:
        if _ACCESS_DENIED_CACHE_LOADED:
            return
        try:
            if ACCESS_DENIED_CACHE_PATH.exists():
                data = json.loads(ACCESS_DENIED_CACHE_PATH.read_text(encoding="utf-8"))
                _ACCESS_DENIED_CACHE = {
                    int(item)
                    for item in data
                    if isinstance(item, int) or (isinstance(item, str) and item.strip().lstrip("-").isdigit())
                }
        except Exception as exc:
            logging.getLogger("wialon_cf_bot").debug("access_denied cache load failed: %s", exc)
            _ACCESS_DENIED_CACHE = set()
        _ACCESS_DENIED_CACHE_LOADED = True


def _get_access_denied_cache_snapshot() -> Set[int]:
    with _ACCESS_DENIED_CACHE_LOCK:
        return set(_ACCESS_DENIED_CACHE)


def _record_access_denied_cache_sync(unit_id: int) -> bool:
    global _ACCESS_DENIED_CACHE
    with _ACCESS_DENIED_CACHE_LOCK:
        if unit_id in _ACCESS_DENIED_CACHE:
            return False
        _ACCESS_DENIED_CACHE.add(unit_id)
        ACCESS_DENIED_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        ACCESS_DENIED_CACHE_PATH.write_text(
            json.dumps(sorted(_ACCESS_DENIED_CACHE)),
            encoding="utf-8",
        )
        return True


# Provide the blocking helper to the messages loader so it reuses the shared executor.
async def require_wialon_client(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    reason: Optional[str] = None,
) -> Optional["WialonClient"]:
    chat = update.effective_chat
    chat_id = chat.id if chat else None
    try:
        return wialon_for_chat(chat_id)
    except NotAuthorized:
        await prompt_authorization(update, context, reason or "Для работы требуется авторизация в Wialon.")
        return None
    except UserBlocked:
        await notify_blocked_user(update, context)
        return None

# =================== СОСТОЯНИЯ ===================
STATE_AUTH_WAIT_TOKEN = 5
STATE_MENU = 10
STATE_FIND_QUERY = 20
STATE_WAIT_UNIT = 30
STATE_CF_NAME = 40
STATE_CF_VALUE = 41
STATE_CF_MORE = 50
STATE_CMD_VALUE = 60  # ввод текста команды
STATE_PARAM_QUERY = 70
STATE_REPORT_NAME = 80
STATE_REPORT_PERIOD = 81
STATE_REPORT_FORMAT = 82
STATE_REPORT_WAIT = 83
STATE_EXPORT_MENU = 90
STATE_GRAPH_MENU = 96
STATE_GRAPH_PERIOD = 97
STATE_GRAPH_CUSTOM_DATE = 98
STATE_DRAIN_PROMPT_REFRESH = 99
STATE_DRAIN_WAIT_DATE = 100
STATE_DRAIN_WAIT_MSG_DATE = 101
STATE_DRAIN_WAIT_MSG_DELETE = 102
STATE_DRAIN_WAIT_MSG_PARTIAL = 103
STATE_DRAIN_WAIT_MSG_PERIOD = 104
STATE_WLN_EXPORT_WAIT_DATE = 105
STATE_ADD_DUT_WAIT_NAME = 106

ACTIVE_UI_KEY = "active_ui"
UI_OBJECTS = "objects"
UI_ADMIN = "admin"

OBJECTS_MODE_LIST = "LIST"
OBJECTS_MODE_CARD_IDLE = "CARD_IDLE"
OBJECTS_MODE_CARD_FLOW_FIELDS = "CARD_FLOW_FIELDS"
OBJECTS_MODE_CARD_FLOW_REPORTS = "CARD_FLOW_REPORTS"
OBJECTS_MODE_CARD_FLOW_SENSORS = "CARD_FLOW_SENSORS"
OBJECTS_MODE_CARD_FLOW_NEARBY = "CARD_FLOW_NEARBY"
OBJECTS_MODE_CARD_FLOW_COMMAND = "CARD_FLOW_COMMAND"
OBJECTS_MODE_CARD_FLOW_GRAPH = "CARD_FLOW_GRAPH"

SEARCH_GENERATION_KEY = "search_generation_token"
STATS_GENERATION_KEY = "stats_generation_token"
STATS_GENERATION_UNIT_KEY = "stats_generation_unit"

# Режимы
MODE_NONE = "none"
MODE_CF = "cf"
MODE_CMD = "cmd"
MODE_PARAMS = "params"
MODE_REPORT = "report"
MODE_EXPORT = "export"
MODE_GRAPH = "graph"

CF_STAGE_NAME = "name"
CF_STAGE_VALUE = "value"
CF_STAGE_MORE = "more"
CF_DUMP_CALLBACK = "cf:dump"
CF_DUMP_BUTTON_TEXT = "📄 Все поля"

# Кнопка внизу
MENU_BUTTON_FIND = "🔍 Найти объект"
MENU_BUTTON_DRAIN_ANALYSIS = "📊 Анализ сливов"
MENU_BUTTON_ADMIN = "🛠 Админ панель"
MENU_BUTTON_EXPORT = "📄 Выгрузить отчёт"
MENU_BUTTON_WLN_EXPORT = "📦 Экспорт WLN"
MENU_BUTTON_SETTINGS = "⚙️ Настройки"
EXPORT_JOB_KEY = "export_job"
WLN_EXPORT_KEY = "wln_export"
DRAIN_ANALYSIS_KEY = "drain_analysis"
DRAIN_ANALYSIS_STATUS_KEY = "status_message"
DRAIN_ANALYSIS_MSG_STATUS_KEY = "message_cache_status"
DRAIN_ANALYSIS_MSG_CANCEL_KEY = "message_cache_cancel_event"
DRAIN_ANALYSIS_MSG_ACTIVE_SESSIONS_KEY = "message_cache_active_sessions"
DRAIN_ANALYSIS_ACTIVE_SESSIONS_KEY = "drain_analysis_active_sessions"
DRAIN_ANALYSIS_MSG_REQUEST_KEY = "message_cache_request"
DRAIN_ANALYSIS_MSG_PARTIAL_KEY = "message_cache_partial"
DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY = "message_cache_range_queue"
DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY = "message_cache_range_label"
DRAIN_ANALYSIS_DATE_KEY = "selected_date"
DRAIN_ANALYSIS_RANGE_KEY = "selected_range"
DRAIN_ANALYSIS_SNAPSHOT_TS_KEY = "snapshot_ts"
DRAIN_ANALYSIS_UNITS_META_KEY = "units_meta"
DRAIN_ANALYSIS_PROMPT_TEXT_KEY = "drain_prompt_text"
DRAIN_ANALYSIS_PROMPT_MARKUP_KEY = "drain_prompt_markup"
DRAIN_ANALYSIS_PREFETCH_TASK_KEY = "drain_prefetch_task"

SEARCH_UNITS_PAGE_SIZE = 20
SEARCH_UNITS_LIMIT = 50
ID_QUERY_RE = re.compile(r"^\s*id\s+(\d{1,10})\s*$", re.IGNORECASE)
REPORT_PAGE_SIZE = 10
REPORT_START_LIST_SIZE = 5

REPORT_START_PROMPT = (
    "Введите название отчёта (мин. 5 символов) или выберите из списка ниже."
)
REPORT_START_EMPTY_PROMPT = (
    "По текущей маске шаблоны не найдены. Введите название отчёта (мин. 5 символов)."
)

AUTH_PROMPT_TEXT = (
    "Для работы бота требуется авторизация.\n"
    "Получите токен Wialon (read-only) и отправьте его в ответ одним сообщением."
)


def is_admin_chat(chat_id: Optional[int]) -> bool:
    if chat_id is None:
        return False
    if chat_id in ADMIN_WHITELIST:
        return True
    role = token_store.get_admin_role(chat_id)
    return role in {"admin", "superadmin"}


def _hmac_hex(secret: str, data: str) -> str:
    import hashlib
    import hmac

    return hmac.new(secret.encode(), data.encode(), hashlib.sha256).hexdigest()


def build_login_url(tg_chat_id: int, nonce: str) -> str:
    redirect_uri = f"{PUBLIC_CALLBACK_URL.rstrip('/')}/wialon/callback?tg_chat_id={tg_chat_id}&nonce={nonce}"
    if HMAC_SECRET:
        redirect_uri += f"&sig={_hmac_hex(HMAC_SECRET, f'{tg_chat_id}|{nonce}') }"
    params = {
        "access_type": -1,
        "duration": 0,
        "redirect_uri": redirect_uri,
    }
    return f"{WIALON_LOGIN_HOST.rstrip('/')}/login.html?{urlencode(params, doseq=False, safe='/:?=&')}"


def kb_auth_actions() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if PUBLIC_CALLBACK_URL:
        rows.append([InlineKeyboardButton("🎟 Получить токен", callback_data="auth:get_link")])
    rows.append([InlineKeyboardButton("🔑 У меня есть токен", callback_data="auth:have_token")])
    return InlineKeyboardMarkup(rows)


def looks_like_token(text: str) -> bool:
    text = (text or "").strip()
    if len(text) < 32:
        return False
    if any(ch.isspace() for ch in text):
        return False
    return True


async def prompt_authorization(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    reason: Optional[str] = None,
) -> int:
    _clear_export_job(context, cancel=True)
    context.user_data.clear()
    context.user_data["auth_expected"] = True
    message_text = AUTH_PROMPT_TEXT
    if reason:
        message_text = f"{reason}\n\n{message_text}"

    target_message = update.effective_message
    if update.callback_query:
        target_message = update.callback_query.message or target_message
    if target_message:
        await target_message.reply_text(message_text, reply_markup=ReplyKeyboardRemove())
        await target_message.reply_text("Выберите действие:", reply_markup=kb_auth_actions())
        return STATE_AUTH_WAIT_TOKEN

    chat = update.effective_chat
    if chat:
        await chat.send_message(message_text, reply_markup=ReplyKeyboardRemove())
        await chat.send_message("Выберите действие:", reply_markup=kb_auth_actions())
    else:
        log.warning("prompt_authorization вызван без message и callback_query")
    return STATE_AUTH_WAIT_TOKEN


def remember_user(update: Update) -> None:
    chat = update.effective_chat
    if not chat:
        return
    user = update.effective_user
    username = None
    first_name = None
    last_name = None
    language_code = None
    user_id = None
    is_bot = False
    if user:
        username = user.username
        first_name = user.first_name
        last_name = user.last_name
        language_code = user.language_code
        user_id = user.id
        is_bot = bool(user.is_bot)
    else:
        username = chat.username
        first_name = getattr(chat, "first_name", None)
        last_name = getattr(chat, "last_name", None)
    token_store.upsert_user_profile(
        chat.id,
        user_id,
        username,
        first_name,
        last_name,
        language_code,
        is_bot,
    )


async def notify_blocked_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat:
        return
    if update.callback_query:
        try:
            await _safe_answer_callback(update.callback_query, "Вы заблокированы", show_alert=True)
        except Exception:
            pass
        message = "\u0414\u0435\u0439\u0441\u0442\u0432\u0438\u0435 \u043e\u0442\u043c\u0435\u043d\u0435\043d\u043e."
    else:
        message = "\u0414\u0435\u0439\u0441\u0442\u0432\u0438\u0435 \u043e\u0442\u043c\u0435\u043d\u0435\043d\u043e."
    if message:
        try:
            await message.reply_text("🚫 Вы заблокированы. Свяжитесь с поддержкой.")
        except Exception:
            log.debug("Не удалось отправить уведомление о блокировке для chat_id=%s", chat.id)


async def ensure_authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    remember_user(update)
    if chat and token_store.has_token(chat.id):
        if token_store.is_blocked(chat.id):
            await notify_blocked_user(update, context)
            return False
        return True
    if chat and token_store.is_blocked(chat.id):
        await notify_blocked_user(update, context)
        return False
    await prompt_authorization(update, context)
    return False


def verify_wialon_token_sync(token: str) -> Tuple[bool, Optional[str]]:
    client = WialonClient(WIALON_HOST, token)
    try:
        client.login()
        return True, None
    except Exception as e:
        return False, str(e)


async def auth_token_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.message
    if not message:
        return STATE_AUTH_WAIT_TOKEN
    remember_user(update)
    chat = update.effective_chat
    if chat and token_store.is_blocked(chat.id):
        await notify_blocked_user(update, context)
        return STATE_AUTH_WAIT_TOKEN
    token = (message.text or "").strip()
    if not token:
        await message.reply_text("Пришлите токен одной строкой.")
        return STATE_AUTH_WAIT_TOKEN
    await message.reply_text("⏳ Проверяю токен…")
    loop = asyncio.get_running_loop()
    token_tail = token[-6:] if token else ""
    log.info(
        "auth: verifying token tail=%s chat=%s host=%s",
        token_tail,
        chat.id if chat else None,
        WIALON_HOST,
    )
    ok, err = await loop.run_in_executor(EXECUTOR, verify_wialon_token_sync, token)
    if not ok:
        log.warning(
            "Token verification failed: %s (host=%s tail=%s chat=%s)",
            err,
            WIALON_HOST,
            token_tail,
            chat.id if chat else None,
        )
        await message.reply_text(
            "❌ Не удалось проверить токен. Убедитесь, что вставили его полностью и попробуйте снова.",
        )
        return STATE_AUTH_WAIT_TOKEN

    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None
    token_store.save_token(chat_id, user_id, token)
    log.info(
        "auth: token saved chat=%s tail=%s host=%s",
        chat_id,
        token_tail,
        WIALON_HOST,
    )
    _clear_export_job(context, cancel=True)
    context.user_data.clear()

    greeting_text = (
        "✅ Токен принят! Привет! Я помогу найти объект, показать его карточку и работать с параметрами."
    )
    await message.reply_text(
        greeting_text,
        reply_markup=reply_menu(chat_id=chat_id),
    )

    return await _start_search_prompt_from_trigger(update, context)


async def auth_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)
    chat = q.message.chat if q.message else None
    remember_user(update)
    if chat and token_store.is_blocked(chat.id):
        await notify_blocked_user(update, context)
        return STATE_AUTH_WAIT_TOKEN
    if q.data == "auth:get_link":
        if not PUBLIC_CALLBACK_URL:
            await q.message.reply_text(
                "Параметр PUBLIC_CALLBACK_URL не настроен. Свяжитесь с администратором.",
            )
        else:
            nonce = uuid.uuid4().hex
            url = build_login_url(chat.id if chat else 0, nonce)
            await q.message.reply_html(
                "<b>Шаг 1.</b> Перейдите по ссылке и авторизуйтесь в Wialon.\n"
                "После авторизации токен придёт в этот чат от облачного сервиса.\n\n"
                "Скопируйте токен и отправьте его сюда одной строкой.\n\n"
                f"<a href=\"{url}\">Открыть страницу логина Wialon</a>",
            )
    elif q.data == "auth:have_token":
        await q.message.reply_text("Отправьте токен сообщением — я его проверю и сохраню.")
    return STATE_AUTH_WAIT_TOKEN

# =================== Wialon ===================
UNIT_FLAGS_BASE = 1
UNIT_FLAGS_BASE_PLUS_FIELDS = UNIT_FLAGS_BASE + 8  # для CF
UNIT_FLAGS_ADMIN_FIELDS = 128
UNIT_FLAGS_DEVICE_META = 0x100
UNIT_FLAGS_SENSORS = UNIT_FLAGS_BASE | 4096
UNIT_FLAGS_STATS = (
    UNIT_FLAGS_BASE + 4096 + 1024 + 2097152 + 8 + UNIT_FLAGS_DEVICE_META
)  # base+sensors+lastmsg/pos+connection+profile+device
UNIT_FLAGS_SNAPSHOT = UNIT_FLAGS_BASE | UNIT_FLAGS_DEVICE_META
HW_TYPES_CACHE_TTL = 3600  # 1 час
ONLINE_THRESHOLD_SEC = 10 * 60  # 10 минут для «На связи»
YANDEX_STATIC_MAP_ZOOM = 15
YANDEX_STATIC_MAP_SIZE = (600, 400)
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

MONTH_NAME_TO_NUM: Dict[str, int] = {
    "январь": 1,
    "января": 1,
    "янв": 1,
    "февраль": 2,
    "февраля": 2,
    "фев": 2,
    "март": 3,
    "марта": 3,
    "мар": 3,
    "апрель": 4,
    "апреля": 4,
    "апр": 4,
    "май": 5,
    "мая": 5,
    "июнь": 6,
    "июня": 6,
    "июн": 6,
    "июл": 7,
    "июль": 7,
    "июля": 7,
    "август": 8,
    "августа": 8,
    "авг": 8,
    "сентябрь": 9,
    "сентября": 9,
    "сен": 9,
    "октябрь": 10,
    "октября": 10,
    "окт": 10,
    "ноябрь": 11,
    "ноября": 11,
    "ноя": 11,
    "декабрь": 12,
    "декабря": 12,
    "дек": 12,
}
# Дополнительные эвристики распознавания датчиков уровня топлива
RE_DETECTOR_DUT_NAME = re.compile(
    r"(?:\bДУТ\b|\bDUT\b|\bLLS\b|fuel\s*level)", re.IGNORECASE
)
DUT_TYPE_ALIASES = {
    "lls",
    "fuel",
    "fuel_level",
    "fuel level",
    "flevel",
    "dut",
}
ZONE_DATA_CACHE_TTL = 300
class ReportCancelledError(Exception):
    """Исключение, используемое для прерывания генерации отчёта."""

def _geocode_bucket_key(lat: float, lon: float) -> Tuple[float, float, int]:
    t_bucket = int(time.time() // GEOCODE_TTL_SEC_BUCKET)
    return (round(float(lat), 4), round(float(lon), 4), t_bucket)


@lru_cache(maxsize=GEOCODE_CACHE_MAX)
def _geocode_cached(key: Tuple[Any, ...]) -> str:
    client_id, lat, lon, _ = key
    client = _GEOCODE_CLIENTS.get(client_id)
    if client is None:
        raise RuntimeError("geocode client unavailable")
    return client._reverse_geocode_uncached(float(lat), float(lon))


class WialonSessionPool:
    """Manage a limited set of independent Wialon sessions for parallel loading."""

    def __init__(
        self,
        clients: List["WialonClient"],
        owns_clients: bool,
        requested_size: int,
    ):
        self._queue: asyncio.Queue["WialonClient"] = asyncio.Queue()
        for client in clients:
            self._queue.put_nowait(client)
        self._clients = clients
        self._owns_clients = owns_clients
        self._requested_size = max(1, requested_size)
        self._closed = False

    @classmethod
    async def create(
        cls,
        base_client: "WialonClient",
        desired_size: int,
    ) -> "WialonSessionPool":
        desired = max(1, desired_size)
        clones: List["WialonClient"] = []
        for idx in range(desired):
            try:
                clone = base_client.spawn_subsession()
            except Exception as exc:
                log.warning(
                    "drain_analysis: failed to spawn subsession %s/%s: %s",
                    idx + 1,
                    desired,
                    exc,
                )
                break
            clones.append(clone)
        if not clones:
            log.warning(
                "drain_analysis: falling back to single-session mode for message loading"
            )
            pool = cls([base_client], owns_clients=False, requested_size=desired)
        else:
            pool = cls(clones, owns_clients=True, requested_size=desired)
        return pool

    @property
    def size(self) -> int:
        return len(self._clients)

    @property
    def requested_size(self) -> int:
        return self._requested_size

    @property
    def owns_clients(self) -> bool:
        return self._owns_clients

    async def acquire(self) -> "WialonClient":
        if self._closed:
            raise RuntimeError("WialonSessionPool is closed")
        client = await self._queue.get()
        return client

    def release(self, client: "WialonClient") -> None:
        if self._closed:
            if self._owns_clients:
                client.close()
            return
        self._queue.put_nowait(client)

    def _replace_client(self, old: "WialonClient", new: "WialonClient") -> None:
        replaced = False
        for idx, existing in enumerate(self._clients):
            if existing is old:
                self._clients[idx] = new
                replaced = True
                break
        if not replaced:
            self._clients.append(new)
        with contextlib.suppress(Exception):
            old.close()
        self._queue.put_nowait(new)

    def recycle(self, client: "WialonClient", *, broken: bool = False) -> None:
        if self._closed:
            if self._owns_clients:
                with contextlib.suppress(Exception):
                    client.close()
            return
        if broken and self._owns_clients:
            replacement: Optional["WialonClient"] = None
            try:
                replacement = client.spawn_subsession()
            except Exception as exc:
                log.warning(
                    "drain_analysis: failed to respawn session after error: %s",
                    exc,
                )
            if replacement is not None:
                self._replace_client(client, replacement)
                return
        self.release(client)

    @contextlib.asynccontextmanager
    async def session(self) -> Iterator["WialonClient"]:
        client = await self.acquire()
        try:
            yield client
        finally:
            self.release(client)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_clients:
            for client in self._clients:
                with contextlib.suppress(Exception):
                    client.close()
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()


def _normalize_wialon_endpoints(host_value: str) -> Tuple[str, str]:
    raw = (host_value or "").strip()
    if not raw:
        raise ValueError("Wialon host must not be empty")
    if not raw.startswith(("http://", "https://")):
        normalized = f"https://{raw}"
    else:
        normalized = raw
    normalized = normalized.strip()
    lower = normalized.lower().rstrip("/")
    ajax_suffix = "/ajax.html"
    full_suffix = "/wialon/ajax.html"
    wialon_suffix = "/wialon"
    if lower.endswith(full_suffix):
        host_prefix = normalized[: -len(ajax_suffix)].rstrip("/")
        base_url = normalized.rstrip("/")
    elif lower.endswith(ajax_suffix):
        host_prefix = normalized[: -len(ajax_suffix)].rstrip("/")
        base_url = normalized.rstrip("/")
    elif lower.endswith(wialon_suffix):
        host_prefix = normalized.rstrip("/")
        base_url = f"{host_prefix}/ajax.html"
    else:
        host_prefix = normalized.rstrip("/")
        base_url = f"{host_prefix}/wialon/ajax.html"
    return host_prefix.rstrip("/"), base_url.rstrip("/")


class AsyncWialonClient:
    """Asynchronous subset of the Wialon API used for message loading routines."""

    def __init__(
        self,
        host: str,
        token: str,
        *,
        zone_strategy: Optional[str] = None,
        allowed_rids_cache: Optional[Tuple[float, List[int]]] = None,
    ) -> None:
        self.host, self.base_url = _normalize_wialon_endpoints(host)
        self.token = token
        self.sid: Optional[str] = None
        self._zone_all_strategy = zone_strategy
        self._allowed_rids_cache = allowed_rids_cache or (0.0, [])
        self._http_timeout = httpx.Timeout(
            connect=HTTP_TIMEOUT_CONN,
            read=HTTP_TIMEOUT_READ,
            write=HTTP_TIMEOUT_READ,
            pool=None,
        )
        self._http_limits = httpx.Limits(
            max_connections=HTTP_POOL_MAX,
            max_keepalive_connections=HTTP_POOL_MAX,
        )
        self._client = self._create_client()
        self._auth_lock = asyncio.Lock()
        self._cancel_predicate: Optional[Callable[[], bool]] = None
        self._last_calc_series_raw: Any = None

    def _create_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=self._http_timeout, limits=self._http_limits)

    async def aclose(self) -> None:
        with contextlib.suppress(Exception):
            await self._client.aclose()

    async def spawn_subsession(self) -> "AsyncWialonClient":
        clone = AsyncWialonClient(
            self.host,
            self.token,
            zone_strategy=self._zone_all_strategy,
            allowed_rids_cache=self._allowed_rids_cache,
        )
        clone.sid = self.sid
        clone._cancel_predicate = self._cancel_predicate
        return clone

    def set_cancel_predicate(self, predicate: Callable[[], bool]) -> None:
        self._cancel_predicate = predicate

    def clear_cancel_predicate(self) -> None:
        self._cancel_predicate = None

    def _check_cancelled(self) -> None:
        if not self._cancel_predicate:
            return
        cancelled = False
        try:
            cancelled = bool(self._cancel_predicate())
        except Exception as exc:  # pragma: no cover - defensive logging
            log.debug("AsyncWialonClient cancel predicate raised: %s", exc)
        if cancelled:
            raise asyncio.CancelledError()

    async def _sleep_with_cancel(self, seconds: float) -> None:
        if seconds <= 0:
            return
        deadline = time.monotonic() + seconds
        interval = min(0.25, seconds)
        while True:
            self._check_cancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(interval, max(0.0, remaining)))

    async def abort_pending_requests(self) -> None:
        cancelled = False
        try:
            self._check_cancelled()
        except asyncio.CancelledError:
            cancelled = True
        log.info("AsyncWialonClient[%s]: abort_pending_requests triggered cancel_flag=%s", id(self), cancelled)
        await self._client.aclose()
        self._client = self._create_client()
        if cancelled:
            cancel_log.info("async_client abort_pending_requests closed after cancel id=%s", id(self))

    async def login(self) -> None:
        async with self._auth_lock:
            if self.sid:
                return
            data = await self._call_rpc("token/login", {"token": self.token}, include_sid=False)
            self.sid = data.get("eid") or data.get("sid")
            if not self.sid:
                raise RuntimeError(f"Не удалось получить SID при аутентификации: {data}")

    async def request(
        self,
        svc: str,
        params: Dict[str, Any],
        *,
        include_sid: bool = True,
        retry_on_session_error: bool = True,
    ) -> Dict[str, Any]:
        if include_sid and not self.sid:
            await self.login()
        data = await self._call_rpc(svc, params, include_sid=include_sid)
        if (
            include_sid
            and retry_on_session_error
            and isinstance(data, dict)
            and data.get("error") in (1, 4, 6)
        ):
            self.sid = None
            await self.login()
            data = await self._call_rpc(svc, params, include_sid=True)
        return data

    async def _call_rpc(self, svc: str, params: Dict[str, Any], *, include_sid: bool) -> Dict[str, Any]:
        timer = _GeoTimer(f"rpc:{svc}")
        try:
            data = await self._request_raw(svc, params, include_sid=include_sid)
        except Exception as exc:
            label, elapsed = timer.done()
            geo_log.error(
                "%s FAIL in %.3fs err=%r params=%s",
                label,
                elapsed,
                exc,
                _geo_scrub_params(params)[:400],
            )
            raise
        else:
            label, elapsed = timer.done()
            err = data.get("error") if isinstance(data, dict) else None
            geo_log.debug(
                "%s ok in %.3fs err=%s params=%s",
                label,
                elapsed,
                err,
                _geo_scrub_params(params)[:400],
            )
            return data

    async def _request_raw(self, svc: str, params: Dict[str, Any], *, include_sid: bool) -> Dict[str, Any]:
        try:
            params_str = params if isinstance(params, str) else json.dumps(params, ensure_ascii=False)
        except Exception as exc:
            raise RuntimeError(f"Не удалось сериализовать params для {svc}: {exc}") from exc

        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        max_attempts = 5
        attempts = 0
        backoff = 0.5
        last_error: Optional[Exception] = None

        while attempts < max_attempts:
            self._check_cancelled()
            attempts += 1
            form = {"svc": svc, "params": params_str}
            target_url = self.base_url.rstrip("/") or self.base_url
            if include_sid and self.sid:
                form["sid"] = self.sid

            try:
                response = await self._client.post(target_url, data=form, headers=headers)
                response.raise_for_status()
                try:
                    return response.json()
                except ValueError as exc:
                    content_type = response.headers.get("Content-Type", "")
                    snippet = response.text[:500] if response.text else ""
                    last_error = RuntimeError(
                        "Не удалось декодировать JSON от Wialon "
                        f"(HTTP {response.status_code}, {content_type}): {snippet}"
                    )
                    if attempts >= max_attempts:
                        raise last_error
                    sleep_time = backoff
                    backoff = min(backoff * 2, 8.0)
                    log.warning(
                        "rpc %s JSON decode failed attempt=%s/%s sleep=%.1fs: %s",
                        svc,
                        attempts,
                        max_attempts,
                        sleep_time,
                        exc,
                    )
                    await self._sleep_with_cancel(sleep_time)
            except asyncio.CancelledError:
                raise
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code if exc.response else None
                last_error = exc
                if status_code in (401, 403):
                    log.warning("rpc %s received %s, re-login", svc, status_code)
                    self.sid = None
                    try:
                        await self.login()
                    except Exception as auth_exc:
                        last_error = auth_exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after HTTP error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                await self._sleep_with_cancel(sleep_time)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after network error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                await self._sleep_with_cancel(sleep_time)
            except Exception as exc:
                last_error = exc
                if isinstance(exc, RuntimeError) and "client has been closed" in str(exc):
                    log.warning("rpc %s detected closed HTTP client, recreating", svc)
                    with contextlib.suppress(Exception):
                        await self._client.aclose()
                    self._client = self._create_client()
                    if include_sid:
                        self.sid = None
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after unexpected error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                await self._sleep_with_cancel(sleep_time)

        if last_error:
            raise last_error
        raise RuntimeError(f"Не удалось выполнить запрос Wialon для {svc}")

    async def load_messages_interval(
        self,
        unit_id: int,
        time_from: int,
        time_to: int,
        *,
        flags: int = 0x0000,
        flags_mask: int = 0xFF00,
        load_count: int = 0xFFFFFFFF,
    ) -> Dict[str, Any]:
        params = {
            "itemId": int(unit_id),
            "timeFrom": int(time_from),
            "timeTo": int(time_to),
            "flags": int(flags),
            "flagsMask": int(flags_mask),
            "loadCount": int(load_count),
        }
        return await self.request("messages/load_interval", params)

    async def get_loaded_messages(self, unit_id: int, index_from: int, index_to: int) -> Any:
        params = {
            "itemId": int(unit_id),
            "indexFrom": int(max(0, index_from)),
            "indexTo": int(max(0, index_to)),
        }
        return await self.request("messages/get_messages", params)

    async def unload_messages(self, unit_id: Optional[int] = None) -> None:
        params: Dict[str, Any] = {}
        if unit_id is not None:
            try:
                params["itemId"] = int(unit_id)
            except Exception:
                params["itemId"] = unit_id
        try:
            await self.request("messages/unload", params)
        except Exception as exc:
            log.debug("messages/unload failed: %s", exc)

    async def calc_sensor_series(
        self,
        unit_id: int,
        sensor_id: Optional[int],
        *,
        width: Optional[int] = None,
        index_from: int = 0,
        index_to: Optional[int] = None,
    ) -> Union[List[Tuple[int, float]], Any]:
        try:
            idx_from = max(0, int(index_from))
        except Exception:
            idx_from = 0
        try:
            idx_to = int(index_to) if index_to is not None else 0
        except Exception:
            idx_to = 0

        params: Dict[str, Any] = {
            "source": "",
            "unitId": int(unit_id),
            "indexFrom": idx_from,
            "indexTo": idx_to,
        }
        if isinstance(sensor_id, (list, tuple, set)):
            raise ValueError("calc_sensor_series does not accept multiple sensor IDs; use sensorId=0 instead")
        if sensor_id is None:
            params["sensorId"] = 0
        else:
            params["sensorId"] = int(sensor_id)
        if width is not None:
            try:
                params["width"] = max(1, int(width))
            except Exception:
                params["width"] = width
        self._last_calc_series_params = dict(params)
        data = await self.request("unit/calc_sensors", params)
        self._last_calc_series_raw = data
        sensor_id_value = params.get("sensorId")
        if sensor_id_value == 0 or isinstance(sensor_id_value, list):
            return data
        if isinstance(data, list):
            cleaned: List[Tuple[int, float]] = []
            for entry in data:
                if isinstance(entry, list) and entry:
                    try:
                        ts = int(entry[0])
                        val = float(entry[1])
                    except Exception:
                        continue
                    cleaned.append((ts, val))
            return cleaned
        return []


class AsyncWialonSessionPool:
    """Pool wrapper over AsyncWialonClient instances."""

    def __init__(
        self,
        clients: List[AsyncWialonClient],
        owns_clients: bool,
        requested_size: int,
    ) -> None:
        self._queue: asyncio.Queue[AsyncWialonClient] = asyncio.Queue()
        for client in clients:
            self._queue.put_nowait(client)
        self._clients = clients
        self._owns_clients = owns_clients
        self._requested_size = max(1, requested_size)
        self._closed = False

    @classmethod
    async def create_from_sync(cls, base_client: "WialonClient", desired_size: int) -> "AsyncWialonSessionPool":
        desired = max(1, desired_size)
        base_async = await AsyncWialonClientBuilder.from_sync(base_client)
        clients: List[AsyncWialonClient] = [base_async]
        for idx in range(desired - 1):
            try:
                clone = await base_async.spawn_subsession()
            except Exception as exc:
                log.warning(
                    "message_cache: failed to spawn async subsession %s/%s: %s",
                    idx + 1,
                    desired - 1,
                    exc,
                )
                break
            clients.append(clone)
        return cls(clients, owns_clients=True, requested_size=desired)

    @property
    def size(self) -> int:
        return len(self._clients)

    @property
    def requested_size(self) -> int:
        return self._requested_size

    @property
    def owns_clients(self) -> bool:
        return self._owns_clients

    async def acquire(self) -> AsyncWialonClient:
        if self._closed:
            raise RuntimeError("AsyncWialonSessionPool is closed")
        return await self._queue.get()

    def release(self, client: AsyncWialonClient) -> None:
        if self._closed:
            if self._owns_clients:
                asyncio.create_task(client.aclose())
            return
        self._queue.put_nowait(client)

    async def recycle(self, client: AsyncWialonClient, *, broken: bool = False) -> None:
        if self._closed:
            if self._owns_clients:
                await client.aclose()
            return
        if broken and self._owns_clients:
            replacement: Optional[AsyncWialonClient] = None
            try:
                replacement = await client.spawn_subsession()
            except Exception as exc:
                log.warning("message_cache: failed to respawn async session after error: %s", exc)
            if replacement is not None:
                await client.aclose()
                self._clients.append(replacement)
                self._queue.put_nowait(replacement)
                return
        self.release(client)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_clients:
            await asyncio.gather(*(client.aclose() for client in self._clients), return_exceptions=True)
        while not self._queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()


class AsyncWialonClientBuilder:
    """Factory helpers for AsyncWialonClient."""

    @staticmethod
    async def from_sync(base_client: "WialonClient") -> AsyncWialonClient:
        async_client = AsyncWialonClient(
            base_client.host,
            base_client.token,
            zone_strategy=getattr(base_client, "_zone_all_strategy", None),
            allowed_rids_cache=getattr(base_client, "_allowed_rids_cache", (0.0, [])),
        )
        async_client.sid = base_client.sid
        async_client._cancel_predicate = getattr(base_client, "_cancel_predicate", None)
        if not async_client.sid:
            await async_client.login()
        return async_client

class WialonClient:
    # env-переключатель (опционально): omit|null|empty
    GEO_ZONE_ALL_STRATEGY = ""

    def __init__(self, host: str, token: str, session: Optional[requests.Session] = None):
        self.host, self.base_url = _normalize_wialon_endpoints(host)
        self.token = token
        self.sid: Optional[str] = None
        self._http_adapter_kwargs = {
            "pool_connections": HTTP_POOL_MAX,
            "pool_maxsize": HTTP_POOL_MAX,
            "max_retries": 0,
        }
        self.session = session or requests.Session()
        adapter = HTTPAdapter(**self._http_adapter_kwargs)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self._auth_lock = threading.Lock()
        self._session_external = session is not None
        self._session_lock: Optional[threading.Lock] = threading.Lock() if self._session_external else None
        self._session_local = threading.local()
        self._session_local.session = self.session
        self._report_templates_cache: Tuple[float, List[Dict[str, Any]]] = (0.0, [])
        self._zone_all_strategy: Optional[str] = None  # 'omit'|'null'|'empty'
        self._allowed_rids_cache: Tuple[float, List[int]] = (0.0, [])
        self._last_zone_details: Dict[Tuple[int, int], Dict[str, Any]] = {}
        self._geocode_cache_id = id(self)
        self._hw_types_cache: Tuple[float, Dict[int, str]] = (0.0, {})
        _GEOCODE_CLIENTS[self._geocode_cache_id] = self
        self._last_calc_series_raw: Any = None
        self._last_rpc_response: Any = None
        self._cancel_predicate: Optional[Callable[[], bool]] = None

    def spawn_subsession(self) -> "WialonClient":
        """Create a new client instance sharing the same host and token."""

        clone = WialonClient(self.host, self.token)
        clone._zone_all_strategy = self._zone_all_strategy  # type: ignore[attr-defined]
        clone._cancel_predicate = self._cancel_predicate
        return clone

    def set_cancel_predicate(self, predicate: Callable[[], bool]) -> None:
        self._cancel_predicate = predicate

    def clear_cancel_predicate(self) -> None:
        self._cancel_predicate = None

    def _check_cancelled(self) -> None:
        predicate = self._cancel_predicate
        if not predicate:
            return
        cancelled = False
        try:
            cancelled = bool(predicate())
        except Exception as exc:
            log.debug("WialonClient cancel predicate raised: %s", exc)
        if cancelled:
            log.info("WialonClient[%s]: cancellation detected", id(self))
            raise asyncio.CancelledError()

    def _sleep_with_cancel(self, seconds: float) -> None:
        if seconds <= 0:
            return
        deadline = time.monotonic() + seconds
        interval = min(0.25, seconds)
        while True:
            self._check_cancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(interval, max(0.0, remaining)))

    def close(self) -> None:
        """Close the underlying HTTP session if it was created internally."""

        if self._session_external:
            return
        session = getattr(self._session_local, "session", None)
        if session is None:
            session = self.session
        try:
            session.close()
        except Exception:
            pass
        with contextlib.suppress(Exception):
            _GEOCODE_CLIENTS.pop(self._geocode_cache_id, None)


    def abort_pending_requests(self) -> None:
        cancelled = False
        try:
            self._check_cancelled()
        except asyncio.CancelledError:
            cancelled = True
        log.info("WialonClient[%s]: abort_pending_requests triggered cancel_flag=%s", id(self), cancelled)
        session = getattr(self._session_local, "session", None)
        if session is None:
            session = self.session
        with contextlib.suppress(Exception):
            session.close()
        self._session_local.session = self._create_session()
        self.session = self._session_local.session
        if cancelled:
            cancel_log.info("sync_client abort_pending_requests closed after cancel id=%s", id(self))

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        adapter = HTTPAdapter(**self._http_adapter_kwargs)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    @contextlib.contextmanager
    def _session_scope(self) -> Iterator[requests.Session]:
        if self._session_external:
            lock = self._session_lock
            if lock:
                lock.acquire()
            try:
                yield self.session
            finally:
                if lock:
                    lock.release()
            return

        session = getattr(self._session_local, "session", None)
        if session is None:
            session = self._create_session()
            self._session_local.session = session
        yield session

    def _request_raw(self, svc: str, params: Dict[str, Any], include_sid: bool = True) -> Dict[str, Any]:
        # Wialon Remote API ожидает form-urlencoded:
        # svc=<svc>&params=<json-string>&sid=<sid>
        try:
            params_str = params if isinstance(params, str) else json.dumps(params, ensure_ascii=False)
        except Exception as exc:
            raise RuntimeError(f"Не удалось сериализовать params для {svc}: {exc}") from exc

        # гарантируем корректный Content-Type
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

        max_attempts = 5
        attempts = 0
        backoff = 0.5
        last_error: Optional[Exception] = None

        while attempts < max_attempts:
            self._check_cancelled()
            attempts += 1
            form = {"svc": svc, "params": params_str}
            target_url = self.base_url.rstrip("/") or self.base_url
            if include_sid and self.sid:
                form["sid"] = self.sid

            try:
                with self._session_scope() as session:
                    resp = session.post(
                        target_url,
                        data=form,  # ВАЖНО: не json=..., а data=...
                        headers=headers,
                        timeout=(HTTP_TIMEOUT_CONN, HTTP_TIMEOUT_READ),
                    )
                resp.raise_for_status()
                try:
                    return resp.json()
                except ValueError as exc:
                    content_type = resp.headers.get("Content-Type", "")
                    snippet = resp.text[:500] if resp.text else ""
                    last_error = RuntimeError(
                        "Не удалось получить JSON от Wialon "
                        f"(HTTP {resp.status_code}, {content_type}): {snippet}"
                    )
                    if attempts >= max_attempts:
                        raise last_error
                    sleep_time = backoff
                    backoff = min(backoff * 2, 8.0)
                    log.warning(
                        "rpc %s JSON decode failed attempt=%s/%s sleep=%.1fs: %s",
                        svc,
                        attempts,
                        max_attempts,
                        sleep_time,
                        exc,
                    )
                    self._sleep_with_cancel(sleep_time)
                else:
                    break
            except asyncio.CancelledError:
                raise
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response else None
                last_error = exc
                if status_code in (401, 403):
                    log.warning("rpc %s received %s, re-login", svc, status_code)
                    self.sid = None
                    try:
                        self.login()
                    except Exception as auth_exc:
                        last_error = auth_exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after HTTP error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                self._sleep_with_cancel(sleep_time)
                continue
            except requests.RequestException as exc:
                last_error = exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after network error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                self._sleep_with_cancel(sleep_time)
                continue
            except Exception as exc:
                last_error = exc
                if attempts >= max_attempts:
                    break
                sleep_time = backoff
                backoff = min(backoff * 2, 8.0)
                log.info(
                    "rpc %s retry after unexpected error attempt=%s/%s sleep=%.1fs err=%s",
                    svc,
                    attempts,
                    max_attempts,
                    sleep_time,
                    exc,
                )
                self._sleep_with_cancel(sleep_time)
                continue

        if last_error:
            raise last_error
        raise RuntimeError(f"Не удалось выполнить запрос Wialon для {svc}")

    def _call_rpc(self, svc: str, params: Dict[str, Any], include_sid: bool = True) -> Dict[str, Any]:
        timer = _GeoTimer(f"rpc:{svc}")
        try:
            data = self._request_raw(svc, params, include_sid=include_sid)
        except Exception as exc:
            label, elapsed = timer.done()
            geo_log.error(
                "%s FAIL in %.3fs err=%r params=%s",
                label,
                elapsed,
                exc,
                _geo_scrub_params(params)[:400],
            )
            raise
        else:
            label, elapsed = timer.done()
            err = data.get("error") if isinstance(data, dict) else None
            geo_log.debug(
                "%s ok in %.3fs err=%s params=%s",
                label,
                elapsed,
                err,
                _geo_scrub_params(params)[:400],
            )
            return data

    def login(self) -> None:
        with self._auth_lock:
            data = self._call_rpc("token/login", {"token": self.token}, include_sid=False)
            self.sid = data.get("eid") or data.get("sid")
            if not self.sid:
                raise RuntimeError(f"Не удалось получить SID при авторизации: {data}")

    def request(self, svc: str, params: Dict[str, Any], retry_on_session_error: bool = True) -> Dict[str, Any]:
        if not self.sid:
            self.login()
        data = self._call_rpc(svc, params, include_sid=True)
        self._last_rpc_response = data
        if isinstance(data, dict) and data.get("error") in (1, 4, 6) and retry_on_session_error:
            self.login()
            data = self._call_rpc(svc, params, include_sid=True)
            self._last_rpc_response = data
        return data

    def _get_hw_types_map(self, use_cache: bool = True) -> Dict[int, str]:
        now = time.time()
        cache_ts, cached = self._hw_types_cache
        if use_cache and cached and (now - cache_ts) < HW_TYPES_CACHE_TTL:
            return dict(cached)

        try:
            data = self.request("core/get_hw_types", {})
        except Exception:
            if cached:
                return dict(cached)
            raise

        raw_items: List[Dict[str, Any]] = []
        if isinstance(data, list):
            raw_items = [dict(item) for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            candidate_lists = []
            for key in ("items", "hwtypes", "hwTypes", "types", "list", "values"):
                value = data.get(key)
                if isinstance(value, list):
                    candidate_lists.append(value)
            if candidate_lists:
                for lst in candidate_lists:
                    raw_items.extend(dict(item) for item in lst if isinstance(item, dict))
            elif all(isinstance(val, dict) for val in data.values()):
                for key, val in data.items():
                    entry = dict(val)
                    if "id" not in entry:
                        entry["id"] = key
                    raw_items.append(entry)

        mapping: Dict[int, str] = {}
        for entry in raw_items:
            hw_id_raw = entry.get("id") or entry.get("i") or entry.get("hw") or entry.get("hwid")
            try:
                hw_id = int(hw_id_raw)
            except Exception:
                continue
            name = entry.get("name") or entry.get("nm") or entry.get("n") or entry.get("title")
            if isinstance(name, str):
                mapping[hw_id] = name.strip()
            elif hw_id not in mapping:
                mapping[hw_id] = ""

        if mapping:
            self._hw_types_cache = (now, {k: v for k, v in mapping.items() if v})
            return {k: v for k, v in mapping.items() if v}

        if cached:
            return dict(cached)
        return {}

    def _extract_device_details(self, item: Mapping[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        unit_raw = item.get("id")
        try:
            result["unit_id"] = int(unit_raw)
        except Exception:
            if unit_raw is not None:
                result["unit_id"] = unit_raw

        uid_value = item.get("uid") or item.get("unique_id") or item.get("uniqueId")
        device_uid: Optional[str] = None
        if isinstance(uid_value, (int, float)):
            device_uid = str(int(uid_value))
        elif isinstance(uid_value, str) and uid_value.strip():
            device_uid = uid_value.strip()
        if device_uid:
            result["device_uid"] = device_uid

        hw_entry = item.get("hw")
        hw_id: Optional[int] = None
        hw_name: Optional[str] = None
        if isinstance(hw_entry, dict):
            hw_raw = hw_entry.get("id") or hw_entry.get("i") or hw_entry.get("hw")
            try:
                hw_id = int(hw_raw)
            except Exception:
                hw_id = None
            name_val = hw_entry.get("name") or hw_entry.get("nm") or hw_entry.get("n")
            if isinstance(name_val, str) and name_val.strip():
                hw_name = name_val.strip()
        else:
            try:
                hw_id = int(hw_entry)
            except Exception:
                hw_id = None

        if hw_id is not None:
            result["device_type_id"] = hw_id
            if not hw_name:
                try:
                    hw_name = self._get_hw_types_map().get(hw_id)
                except Exception:
                    hw_name = None
        if hw_name:
            result["device_type_name"] = hw_name

        return result

    def get_unit_device_details(
        self,
        unit_id: int,
        *,
        timeout: float = 20.0,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        params = {"id": int(unit_id), "flags": 1 | 0x100}
        attempt = 0
        while True:
            attempt += 1
            try:
                data = self.request("core/search_item", params)
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt >= max_retries:
                    raise
                snapshot_log.debug(
                    "unit_snapshot: device details retry unit=%s attempt=%s err=%s",
                    unit_id,
                    attempt,
                    exc,
                )
                self._sleep_with_cancel(min(timeout, 2.0))
        item = data.get("item") if isinstance(data, dict) else {}
        if not isinstance(item, dict):
            return {}

        return self._extract_device_details(item)

    def get_unit_assigned_drivers(self, unit_id: int) -> List[Dict[str, Any]]:
        params = {"unitId": int(unit_id)}
        data = self.request("resource/get_unit_drivers", params)

        raw_entries: List[Dict[str, Any]] = []
        if isinstance(data, list):
            raw_entries = [dict(item) for item in data if isinstance(item, dict)]
        elif isinstance(data, dict):
            for key in ("drivers", "items", "list", "result", "value"):
                value = data.get(key)
                if isinstance(value, list):
                    raw_entries.extend(dict(item) for item in value if isinstance(item, dict))
            for key, val in data.items():
                if isinstance(val, list) and key not in {"drivers", "items", "list", "result", "value"}:
                    raw_entries.extend(
                        dict(item) for item in val if isinstance(item, dict)
                    )
            if not raw_entries and all(isinstance(val, dict) for val in data.values()):
                for key, val in data.items():
                    entry = dict(val)
                    if "id" not in entry:
                        entry["id"] = key
                    raw_entries.append(entry)

        drivers: List[Dict[str, Any]] = []
        for entry in raw_entries:
            driver_id_raw = entry.get("id") or entry.get("i") or entry.get("driver_id")
            try:
                driver_id = int(driver_id_raw)
            except Exception:
                driver_id = None
            name = entry.get("name") or entry.get("nm") or entry.get("n") or entry.get("title")
            code = entry.get("code") or entry.get("c") or entry.get("driver_code")
            drivers.append(
                {
                    "id": driver_id,
                    "name": name.strip() if isinstance(name, str) else None,
                    "code": code.strip() if isinstance(code, str) else None,
                }
            )

        drivers.sort(
            key=lambda d: (
                (d.get("name") or "").casefold(),
                (d.get("code") or "").casefold(),
                d.get("id") or 0,
            )
        )
        return drivers

    def list_units_with_pos(self) -> List[Dict[str, Any]]:
        spec = {
            "itemsType": "avl_unit",
            "propName": "sys_name",
            "propValueMask": "*",
            "sortType": "sys_name",
        }
        params = {"spec": spec, "force": 1, "flags": 1 + 1024, "from": 0, "to": 0}
        data = self.request("core/search_items", params)
        items = (data or {}).get("items") or []
        out = []
        for it in items:
            if not isinstance(it, dict):
                continue
            pos = it.get("pos") or {}
            lat = pos.get("y") if isinstance(pos, dict) else None
            if lat is None and isinstance(pos, dict):
                lat = pos.get("lat")
            lon = pos.get("x") if isinstance(pos, dict) else None
            if lon is None and isinstance(pos, dict):
                lon = pos.get("lon")
            out.append(
                {
                    "id": it.get("id"),
                    "nm": it.get("nm"),
                    "pos": {
                        "lat": lat,
                        "lon": lon,
                        "t": (pos.get("t") if isinstance(pos, dict) else None),
                    },
                }
            )
        return out

    def list_accessible_resource_ids(self) -> List[int]:
        params = {
            "spec": {
                "itemsType": "resource",
                "propName": "sys_name",
                "propValueMask": "*",
                "sortType": "sys_name",
            },
            "force": 1,
            "flags": 1,
            "from": 0,
            "to": 0,
        }
        data = self.request("core/search_items", params)
        items = (data or {}).get("items") or []
        result: List[int] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            rid = item.get("id") or item.get("i")
            if isinstance(rid, int):
                result.append(rid)
                continue
            try:
                result.append(int(rid))
            except Exception:
                continue
        if result:
            return result
        try:
            legacy = self.list_resource_ids()
        except Exception:
            legacy = []
        return list(legacy)

    def get_allowed_rids(self, force_refresh: bool = False) -> List[int]:
        env_raw = ""
        if env_raw:
            allowed: List[int] = []
            for part in env_raw.split(","):
                token = part.strip()
                if token.isdigit():
                    try:
                        allowed.append(int(token))
                    except Exception:
                        continue
            return allowed

        cache_ts, cached = self._allowed_rids_cache
        if cached and not force_refresh:
            if time.time() - cache_ts < 60:
                return list(cached)

        try:
            accessible = self.list_accessible_resource_ids()
        except Exception as exc:
            geo_log.error("[zones] failed to list accessible resources: %s", exc)
            accessible = []
        self._allowed_rids_cache = (time.time(), list(accessible))
        return list(accessible)

    def find_zones_for_unit(
        self, unit_id: int, lat: float, lon: float
    ) -> Dict[int, List[int]]:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except Exception:
            return {}
        if not math.isfinite(lat_f) or not math.isfinite(lon_f):
            return {}

        allowed_rids = self.get_allowed_rids()
        used_rids = [rid for rid in allowed_rids if isinstance(rid, int)]
        sid_short = (self.sid or "-")[:8]
        geo_log.info(
            "[zones] SID=%s allowed_rids=%s used_rids=%s lat,lon=(%.6f,%.6f)",
            sid_short,
            len(allowed_rids),
            len(used_rids),
            lat_f,
            lon_f,
        )
        if not used_rids:
            geo_log.info("[zones] no allowed rids for this SID -> none")
            return {}

        zone_map = {str(rid): [] for rid in used_rids}
        params = {"spec": {"lat": lat_f, "lon": lon_f, "zoneId": zone_map}}
        timer = _GeoTimer("zones:get_zones_by_point")
        try:
            data = self.request("resource/get_zones_by_point", params)
        except Exception:
            _, elapsed = timer.done()
            geo_log.error(
                "[zones] get_zones_by_point failed in %.3fs for uid=%s", elapsed, unit_id
            )
            raise

        _, elapsed = timer.done()
        hits: Dict[int, List[int]] = {}
        if isinstance(data, dict):
            for rid_key, zids in data.items():
                if rid_key in {"error", "total"}:
                    continue
                try:
                    rid = int(rid_key)
                except Exception:
                    continue
                if not isinstance(zids, list):
                    continue
                cleaned: List[int] = []
                for zid in zids:
                    try:
                        cleaned.append(int(zid))
                    except Exception:
                        continue
                if cleaned:
                    hits[rid] = cleaned
        hits_total = sum(len(zones) for zones in hits.values())
        hits_summary = {rid: len(zones) for rid, zones in hits.items()}
        geo_log.info(
            "[zones] get_zones_by_point -> hits_total=%s by_res=%s in %.3fs",
            hits_total,
            hits_summary,
            elapsed,
        )
        return hits

    def resolve_zone_names(self, hits: Dict[int, List[int]]) -> List[str]:
        names: List[str] = []
        seen: Set[str] = set()
        self._last_zone_details = {}
        if not hits:
            geo_log.info("[zones] resolve -> no hits")
            return names

        timer = _GeoTimer("zones:get_zone_data")
        for rid, zone_ids in hits.items():
            if not zone_ids:
                continue
            params = {
                "itemId": int(rid),
                "col": [int(zid) for zid in zone_ids],
                "flags": 16,
            }
            try:
                data = self.request("resource/get_zone_data", params)
            except Exception as exc:
                geo_log.debug(
                    "[zones] resolve: get_zone_data failed rid=%s ids=%s err=%s",
                    rid,
                    len(zone_ids),
                    exc,
                )
                continue
            parsed = self._parse_zone_data_response(data)
            if parsed:
                self._update_zone_cache(rid, parsed)
            for zid, payload in parsed.items():
                if not isinstance(payload, dict):
                    payload = {"n": payload}
                raw_name = (payload.get("n") or payload.get("name") or "").strip()
                name = raw_name or f"ID {zid}"
                name_cf = name.casefold()
                center_lat, center_lon = self._extract_zone_center(payload)
                entry = {
                    "resource_id": rid,
                    "zone_id": zid,
                    "name": name,
                    "lat": center_lat,
                    "lon": center_lon,
                    "payload": dict(payload),
                }
                self._last_zone_details[(rid, zid)] = entry
                if name_cf not in seen:
                    names.append(name)
                    seen.add(name_cf)
        _, elapsed = timer.done()
        geo_log.info(
            "[zones] get_zone_data resolve -> names=%s in %.3fs",
            len(names),
            elapsed,
        )
        geo_log.debug(
            "[zones] names=%s",
            ", ".join(names[:20]) if names else "—",
        )
        return names

    def _ensure_geo_file_updater(self) -> None:
        try:
            ensure_geo_file_updater(self)
        except Exception as exc:
            geo_log.debug("file_cache: ensure updater failed: %s", exc)

    def _ensure_unit_snapshot_updater(self) -> None:
        try:
            ensure_unit_snapshot_updater(self)
        except Exception as exc:
            log.debug("unit_snapshot: ensure updater failed: %s", exc)

    def list_resource_ids(self) -> List[int]:
        params = {
            "spec": {
                "itemsType": "avl_resource",
                "propName": "sys_name",
                "propValueMask": "*",
                "sortType": "sys_name",
            },
            "force": 1,
            "flags": 1,
            "from": 0,
            "to": 0,
        }
        started = time.perf_counter()
        data = self.request("core/search_items", params)
        duration = time.perf_counter() - started
        items = data.get("items", []) if isinstance(data, dict) else []
        resource_ids: List[int] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            rid = item.get("id")
            try:
                resource_ids.append(int(rid))
            except Exception:
                continue
        geo_log.debug(
            "file_cache: search_items took %.3fs resources=%s",
            duration,
            len(resource_ids),
        )
        return resource_ids

    def list_report_templates(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Возвращает список шаблонов отчётов из всех ресурсов."""
        if use_cache:
            cache_ts, cached = self._report_templates_cache
            if cached and (time.time() - cache_ts) < REPORT_TEMPLATES_TTL:
                return [dict(tpl) for tpl in cached]

        spec = {
            "itemsType": "avl_resource",
            "propName": "sys_name",
            "propValueMask": "*",
            "sortType": "sys_name",
        }
        params = {"spec": spec, "force": 1, "flags": 8193, "from": 0, "to": 0}
        data = self.request("core/search_items", params)
        items = data.get("items", []) if isinstance(data, dict) else []
        templates: List[Dict[str, Any]] = []
        for item in items:
            rep = item.get("rep") or {}
            resource_id = item.get("id")
            resource_name = item.get("nm") or ""
            if not isinstance(rep, dict):
                continue
            for tpl in rep.values():
                if not isinstance(tpl, dict):
                    continue
                tpl_copy: Dict[str, Any] = dict(tpl)
                tpl_copy["resource_id"] = resource_id
                tpl_copy["resource_name"] = resource_name
                templates.append(tpl_copy)

        for tpl in templates:
            tpl["name"] = (tpl.get("name") or tpl.get("n") or "").strip()
            tpl["resource_name"] = (
                tpl.get("resource_name")
                or tpl.get("res_name")
                or tpl.get("rname")
                or ""
            ).strip()
            try:
                tpl["template_id"] = int(
                    tpl.get("template_id") or tpl.get("id") or tpl.get("i") or 0
                )
            except Exception:
                tpl["template_id"] = 0
            try:
                tpl["resource_id"] = int(tpl.get("resource_id") or tpl.get("rid") or 0)
            except Exception:
                tpl["resource_id"] = 0

        templates.sort(key=_report_template_sort_key)

        self._report_templates_cache = (time.time(), [dict(tpl) for tpl in templates])
        return templates

    @staticmethod
    def _resolve_format(fmt: str) -> Tuple[str, int, str]:
        fmt_cf = (fmt or "").strip().lower()
        if fmt_cf == "pdf":
            return "pdf", 2, "pdf"
        if fmt_cf in ("excel", "xlsx"):
            return "xlsx", 8, "xlsx"
        if fmt_cf == "xls":
            return "xls", 4, "xls"
        raise ValueError("Неподдерживаемый формат отчёта")

    def _build_export_params(self, fmt: str, output_name: str) -> Dict[str, Any]:
        _, format_code, _ = self._resolve_format(fmt)
        params = {
            "format": format_code,
            "pageWidth": 0,
            "headings": 1,
            "compress": 0,
            "attachMap": 0,
            "hideMapBasis": 0,
            "coding": "utf8",
            "outputFileName": output_name,
        }
        return params

    @staticmethod
    def _parse_disposition_filename(disposition: str) -> Optional[str]:
        if not disposition:
            return None
        match_utf = re.search(r"filename\*=(?:UTF-8''|utf-8'')([^;]+)", disposition)
        if match_utf:
            try:
                return requests.utils.unquote(match_utf.group(1))
            except Exception:
                return match_utf.group(1)
        match = re.search(r'filename="?([^";]+)"?', disposition)
        if match:
            return match.group(1)
        return None

    def generate_report_file(
        self,
        resource_id: int,
        template_id: int,
        interval_from: int,
        interval_to: int,
        fmt: str,
        cancel_event: Optional[threading.Event] = None,
        object_id: Optional[int] = None,
    ) -> Tuple[str, bytes]:
        fmt_cf = (fmt or "").strip().lower()
        canonical_fmt, _, extension = self._resolve_format(fmt_cf)

        if cancel_event and cancel_event.is_set():
            raise ReportCancelledError()

        report_context = {
            "resource_id": int(resource_id),
            "template_id": int(template_id),
            "object_id": int(object_id) if object_id is not None else 0,
            "from": int(interval_from),
            "to": int(interval_to),
            "sid": (self.sid or "")[-8:],
        }

        try:
            cleanup_params = {
                "reportResourceId": report_context["resource_id"],
                "reportTemplateId": report_context["template_id"],
            }
            cleanup_resp = self.request("report/cleanup_result", cleanup_params)
            if isinstance(cleanup_resp, dict) and cleanup_resp.get("error") not in (None, 0):
                log.warning(
                    "report/cleanup_result returned error %s for context %s",
                    cleanup_resp.get("error"),
                    report_context,
                )
        except Exception as exc:
            log.warning("report/cleanup_result failed for %s: %s", report_context, exc)

        params_exec = {
            "reportResourceId": report_context["resource_id"],
            "reportTemplateId": report_context["template_id"],
            "reportObjectId": report_context["object_id"],
            "reportObjectSecId": 0,
            "interval": {
                "from": report_context["from"],
                "to": report_context["to"],
                "flags": 0,
            },
        }
        exec_result = self.request("report/exec_report", params_exec)
        if isinstance(exec_result, dict) and exec_result.get("error"):
            raise RuntimeError(
                f"Wialon error {exec_result['error']} in report/exec_report: {exec_result}"
            )

        if cancel_event and cancel_event.is_set():
            raise ReportCancelledError()

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_name = f"report_{template_id}_{timestamp}"
        export_params = self._build_export_params(canonical_fmt, output_name)
        export_params.update(
            {
                "reportResourceId": report_context["resource_id"],
                "reportTemplateId": report_context["template_id"],
                "reportObjectId": report_context["object_id"],
            }
        )

        wait_messages = {"report not ready", "report has not been saved"}
        deadline = time.monotonic() + 60
        delay = 0.5
        attempt = 0

        while time.monotonic() < deadline:
            if cancel_event and cancel_event.is_set():
                raise ReportCancelledError()

            attempt += 1
            params = {
                "svc": "report/export_result",
                "params": json.dumps(export_params, ensure_ascii=False),
                "sid": self.sid or "",
            }

            with self._session_scope() as session:
                response = session.get(
                    self.base_url,
                    params=params,
                    timeout=(HTTP_TIMEOUT_CONN, max(HTTP_TIMEOUT_READ, 60.0)),
                )
            content_type = (response.headers.get("Content-Type") or "").lower()
            disposition = response.headers.get("Content-Disposition") or ""
            log.debug(
                "report/export_result attempt %s, sid=%s, content_type=%s, disposition=%s",
                attempt,
                (self.sid or "")[-8:],
                content_type,
                disposition,
            )

            if "attachment" in disposition.lower() or (
                "application" in content_type and "json" not in content_type and "html" not in content_type
            ) or any(ext in content_type for ext in ("pdf", "ms-excel", "spreadsheet")):
                filename = self._parse_disposition_filename(disposition)
                if not filename:
                    filename = f"{output_name}.{extension}"
                if cancel_event and cancel_event.is_set():
                    raise ReportCancelledError()
                return filename, response.content

            text_payload = (response.text or "").strip()
            lower_text = text_payload.lower()
            if any(msg in lower_text for msg in wait_messages):
                log.info(
                    "report/export_result not ready yet (attempt %s) for %s: %s",
                    attempt,
                    report_context,
                    text_payload[:200],
                )
            elif "json" in content_type or (text_payload.startswith("{") and text_payload.endswith("}")):
                try:
                    payload = response.json()
                except ValueError:
                    payload = {}
                error_code = payload.get("error") if isinstance(payload, dict) else None
                if error_code in (1001, 1002, 1003):
                    log.info(
                        "report/export_result waiting (error %s) attempt %s for %s",
                        error_code,
                        attempt,
                        report_context,
                    )
                elif error_code:
                    raise RuntimeError(
                        f"Wialon error {error_code} in report/export_result: {payload}"
                    )
                else:
                    log.info(
                        "report/export_result returned JSON without file on attempt %s: %s",
                        attempt,
                        payload,
                    )
            else:
                log.info(
                    "Unexpected report/export_result response (attempt %s): ct=%s body=%s",
                    attempt,
                    content_type,
                    text_payload[:200],
                )

            now = time.monotonic()
            if now >= deadline:
                break
            remaining = max(0.0, deadline - now)
            sleep_time = min(delay, remaining)
            delay = min(delay * 2, 5.0)
            if cancel_event:
                if cancel_event.wait(sleep_time):
                    raise ReportCancelledError()
            else:
                time.sleep(sleep_time)

        raise RuntimeError(
            "Не удалось подготовить файл отчёта за отведённое время."
        )

    # ---- Поиск юнитов ----
    def search_units(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        spec = {
            "itemsType": "avl_unit",
            "propName": "sys_name,profilefield",
            "propValueMask": f"*{query}*,*{query}*",
            "sortType": "sys_name",
            "propType": "property,profilefield",
            "or_logic": 1,
        }
        # 5121 = base (1) + last message (1024) + sensors (4096)
        params = {"spec": spec, "force": 1, "flags": 5121, "from": 0, "to": max(1, limit)}
        data = self.request("core/search_items", params)
        items = data.get("items", []) if isinstance(data, dict) else []
        results: List[Dict[str, Any]] = []
        for it in items:
            entry: Dict[str, Any] = {"id": it.get("id"), "nm": it.get("nm")}
            pos = it.get("pos") if isinstance(it, dict) else None
            if isinstance(pos, dict):
                entry["pos"] = dict(pos)
            lmsg = it.get("lmsg") if isinstance(it, dict) else None
            if isinstance(lmsg, dict):
                entry["lmsg"] = dict(lmsg)
            results.append(entry)
        return results

    # ---- Для CF ----
    def get_unit_with_fields(self, unit_id: int) -> Dict[str, Any]:
        data = self.request("core/search_item", {"id": unit_id, "flags": UNIT_FLAGS_BASE_PLUS_FIELDS})
        item = data.get("item", {}) if isinstance(data, dict) else {}
        return {"id": item.get("id"), "nm": item.get("nm"), "flds": item.get("flds", {})}

    @staticmethod
    def _normalize_custom_fields_block(raw_block: Any) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        if isinstance(raw_block, dict):
            for seq_key, payload in raw_block.items():
                if not isinstance(payload, dict):
                    continue
                entry: Dict[str, Any] = {"seq": str(seq_key)}
                raw_id = payload.get("id")
                try:
                    entry["id"] = int(raw_id)
                except (TypeError, ValueError):
                    entry["id"] = raw_id
                raw_name = payload.get("n")
                if isinstance(raw_name, str):
                    name = raw_name.strip()
                elif raw_name is None:
                    name = ""
                else:
                    name = str(raw_name).strip()
                entry["name"] = name
                raw_value = payload.get("v")
                if isinstance(raw_value, str):
                    value = raw_value.strip()
                elif raw_value is None:
                    value = ""
                else:
                    value = str(raw_value)
                entry["value"] = value
                entries.append(entry)

        def _sort_key(item: Dict[str, Any]) -> Tuple[int, Any]:
            seq_text = item.get("seq")
            try:
                return (0, int(seq_text))
            except (TypeError, ValueError):
                return (1, seq_text or "")

        entries.sort(key=_sort_key)
        return entries

    def get_unit_custom_fields_snapshot(
        self, unit_id: int, *, include_admin: bool = True
    ) -> Dict[str, Any]:
        flags = UNIT_FLAGS_BASE_PLUS_FIELDS | (UNIT_FLAGS_ADMIN_FIELDS if include_admin else 0)
        data = self.request("core/search_item", {"id": unit_id, "flags": flags})
        item = data.get("item", {}) if isinstance(data, dict) else {}
        return {
            "unit_id": item.get("id"),
            "unit_name": item.get("nm"),
            "custom_fields": self._normalize_custom_fields_block(item.get("flds")),
            "admin_fields": self._normalize_custom_fields_block(item.get("aflds")) if include_admin else [],
        }

    # ---- Для статистики ----
    def get_unit_full_for_stats(self, unit_id: int) -> Dict[str, Any]:
        data = self.request("core/search_item", {"id": unit_id, "flags": UNIT_FLAGS_STATS})
        return data.get("item", {}) if isinstance(data, dict) else {}

    def get_unit_sensors_detailed(self, unit_id: int) -> List[Dict[str, Any]]:
        payload = self.request("core/search_item", {"id": unit_id, "flags": UNIT_FLAGS_SENSORS})
        item = payload.get("item", {}) if isinstance(payload, dict) else {}
        sens_block = item.get("sens") or []
        if isinstance(sens_block, dict):
            iterable = sens_block.values()
        elif isinstance(sens_block, list):
            iterable = sens_block
        else:
            iterable = []
        sensors: List[Dict[str, Any]] = []
        for entry in iterable:
            if isinstance(entry, dict):
                sensors.append(dict(entry))
        return sensors

    def _geocode_host_component(self) -> str:
        parsed = urlsplit(self.host)
        netloc = parsed.netloc
        path = (parsed.path or "").strip("/")
        if netloc:
            if path:
                return f"{netloc}/{path}"
            return netloc
        host = (parsed.path or parsed.geturl() or self.host or "").strip("/")
        return host

    @staticmethod
    def _extract_address_from_geocoder(data: Any) -> Optional[str]:
        candidates: List[str] = []

        def _collect(obj: Any) -> None:
            if isinstance(obj, dict):
                for key in ("address", "addr", "name", "result", "value", "label"):
                    val = obj.get(key)
                    if isinstance(val, str) and val.strip():
                        candidates.append(val.strip())
                for key in ("objects", "list", "items", "value", "results"):
                    nested = obj.get(key)
                    if isinstance(nested, (list, tuple)):
                        for item in nested:
                            _collect(item)
            elif isinstance(obj, (list, tuple)):
                for item in obj:
                    _collect(item)
            elif isinstance(obj, str) and obj.strip():
                candidates.append(obj.strip())

        _collect(data)
        return candidates[0] if candidates else None

    def _reverse_geocode_uncached(self, lat: float, lon: float) -> str:
        if not self.sid:
            self.login()
        host_component = self._geocode_host_component()
        if not host_component:
            return ""
        coords_payload = json.dumps([{"lon": lon, "lat": lat}], ensure_ascii=False)
        url = f"https://geocode-maps.wialon.com/{host_component}/gis_geocode"
        params = {"coords": coords_payload, "uid": self.sid}
        geocode_started = time.perf_counter()
        try:
            with self._session_scope() as session:
                resp = session.get(
                    url,
                    params=params,
                    timeout=(HTTP_TIMEOUT_CONN, HTTP_TIMEOUT_READ),
                )
        except Exception as exc:
            log.debug(
                "gis_geocode request failed after %.3fs: %s",
                time.perf_counter() - geocode_started,
                exc,
            )
            return ""
        try:
            resp.raise_for_status()
        except Exception as exc:
            log.debug(
                "gis_geocode http error after %.3fs: %s",
                time.perf_counter() - geocode_started,
                exc,
            )
            return ""
        try:
            data = resp.json()
        except ValueError as exc:
            log.debug(
                "gis_geocode decode failed after %.3fs: %s",
                time.perf_counter() - geocode_started,
                exc,
            )
            return ""
        duration = time.perf_counter() - geocode_started
        result = self._extract_address_from_geocoder(data) or ""
        log.debug("gis_geocode completed in %.3fs (result=%s)", duration, bool(result))
        return result

    def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            return None
        if not (math.isfinite(lat_f) and math.isfinite(lon_f)):
            return None
        bucket_lat, bucket_lon, bucket = _geocode_bucket_key(lat_f, lon_f)
        cache_key = (self._geocode_cache_id, bucket_lat, bucket_lon, bucket)
        _GEOCODE_CLIENTS[self._geocode_cache_id] = self
        try:
            cached = _geocode_cached(cache_key)
        except Exception:
            cached = self._reverse_geocode_uncached(bucket_lat, bucket_lon)
        return cached or None

    @staticmethod
    def _parse_zone_data_response(data: Any) -> Dict[int, Dict[str, Any]]:
        result: Dict[int, Dict[str, Any]] = {}
        if not data:
            return result
        containers: List[Dict[Any, Any]] = []
        if isinstance(data, dict):
            for key in ("zones", "zl", "items"):
                value = data.get(key)
                if isinstance(value, dict):
                    containers.append(value)
            if not containers:
                containers.append(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    zid = item.get("id")
                    try:
                        zid_int = int(zid)
                    except Exception:
                        continue
                    result[zid_int] = item
            return result
        for container in containers:
            for key, value in container.items():
                try:
                    zid_int = int(key)
                except Exception:
                    continue
                if isinstance(value, dict):
                    result[zid_int] = value
                else:
                    result[zid_int] = {"n": value}
        return result

    @staticmethod
    def _extract_zone_center(zone_data: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        if not isinstance(zone_data, dict):
            return None, None
        candidates = []
        for key in ("ct", "c", "center"):
            val = zone_data.get(key)
            if isinstance(val, dict):
                candidates.append(val)
        for center in candidates:
            lat = center.get("y") if isinstance(center, dict) else None
            lon = center.get("x") if isinstance(center, dict) else None
            if lat is None and isinstance(center, dict):
                lat = center.get("lat")
            if lon is None and isinstance(center, dict):
                lon = center.get("lon")
            try:
                if lat is not None and lon is not None:
                    return float(lat), float(lon)
            except Exception:
                continue
        points = zone_data.get("p") or zone_data.get("points")
        if isinstance(points, list) and points:
            sum_lat = 0.0
            sum_lon = 0.0
            count = 0
            for pt in points:
                if not isinstance(pt, dict):
                    continue
                lat = pt.get("y") if isinstance(pt, dict) else None
                lon = pt.get("x") if isinstance(pt, dict) else None
                if lat is None and isinstance(pt, dict):
                    lat = pt.get("lat")
                if lon is None and isinstance(pt, dict):
                    lon = pt.get("lon")
                try:
                    if lat is not None and lon is not None:
                        sum_lat += float(lat)
                        sum_lon += float(lon)
                        count += 1
                except Exception:
                    continue
            if count:
                return sum_lat / count, sum_lon / count
        return None, None

    def _zone_all_params(
        self, resource_id: int, zone_ids: Optional[List[int]], flags: int
    ) -> Dict[str, Any]:
        if zone_ids:
            return {
                "itemId": int(resource_id),
                "col": [int(z) for z in zone_ids],
                "flags": int(flags),
            }

        strategy = self._zone_all_strategy or self.GEO_ZONE_ALL_STRATEGY or "omit"
        if strategy == "omit":
            return {"itemId": int(resource_id), "flags": int(flags)}
        if strategy == "null":
            return {"itemId": int(resource_id), "col": None, "flags": int(flags)}
        return {"itemId": int(resource_id), "col": [], "flags": int(flags)}

    def _detect_zone_all_strategy(self, sample_resource_id: int, flags: int) -> str:
        trials = [
            ("omit", {"itemId": int(sample_resource_id), "flags": int(flags)}),
            (
                "null",
                {"itemId": int(sample_resource_id), "col": None, "flags": int(flags)},
            ),
            (
                "empty",
                {"itemId": int(sample_resource_id), "col": [], "flags": int(flags)},
            ),
        ]
        for name, params in trials:
            try:
                data = self.request("resource/get_zone_data", params)
                parsed = self._parse_zone_data_response(data)
                geo_log.info(
                    "zone_detect: strategy=%s zones=%s", name, len(parsed)
                )
                if parsed:
                    return name
            except Exception as exc:
                geo_log.warning("zone_detect: strategy=%s err=%r", name, exc)
        return "omit"

    def _request_zone_data(
        self, resource_id: int, zone_ids: Optional[List[int]], flags: int
    ) -> Dict[int, Dict[str, Any]]:
        params = self._zone_all_params(resource_id, zone_ids, flags)
        strategy_label = "explicit"
        if not zone_ids:
            if "col" not in params:
                strategy_label = "omit"
            else:
                col_value = params.get("col")
                if col_value is None:
                    strategy_label = "null"
                elif col_value == []:
                    strategy_label = "empty"
        started = time.perf_counter()
        resp = self.request("resource/get_zone_data", params)
        duration = time.perf_counter() - started
        parsed = self._parse_zone_data_response(resp)
        geo_log.debug(
            "zone_cache: get_zone_data item=%s strategy=%s flags=%s zones=%s in %.3fs",
            resource_id,
            strategy_label if not zone_ids else "explicit-list",
            flags,
            len(parsed),
            duration,
        )
        return parsed

    def _filter_zone_cache_by_allowed(
        self, cache: Dict[int, Dict[int, Dict[str, Any]]]
    ) -> Dict[int, Dict[int, Dict[str, Any]]]:
        if not cache:
            return {}
        allowed_raw = [rid for rid in self.get_allowed_rids() if isinstance(rid, int)]
        allowed: Set[int] = {int(rid) for rid in allowed_raw}
        if not allowed:
            geo_log.info("[zones] zone_cache filter -> no allowed rids")
            return {}
        filtered = {rid: dict(zones) for rid, zones in cache.items() if rid in allowed}
        if len(filtered) != len(cache):
            geo_log.debug(
                "[zones] zone_cache filter: total=%s allowed=%s kept=%s",
                len(cache),
                len(allowed),
                len(filtered),
            )
        return filtered

    def _load_zone_cache(self) -> Optional[Dict[int, Dict[int, Dict[str, Any]]]]:
        global _ZONE_CACHE_TS, _ZONE_CACHE_DATA
        self._ensure_geo_file_updater()
        now = time.time()
        with _ZONE_CACHE_LOCK:
            cache_ts = _ZONE_CACHE_TS
            existing_cache = {
                rid: dict(zones)
                for rid, zones in _ZONE_CACHE_DATA.items()
                if isinstance(zones, dict)
            }
        cache_age = now - cache_ts if cache_ts else None
        if existing_cache and cache_age is not None and cache_age < ZONE_DATA_CACHE_TTL:
            return self._filter_zone_cache_by_allowed(existing_cache)

        payload = ZONE_STORE.load_if_present()
        if isinstance(payload, dict):
            zone_cache: Dict[int, Dict[int, Dict[str, Any]]] = {}
            zone_ids_by_resource = payload.get("zone_ids_by_resource")
            zone_meta_raw = payload.get("zone_meta")
            zone_meta = zone_meta_raw if isinstance(zone_meta_raw, dict) else {}
            if isinstance(zone_ids_by_resource, dict):
                for rid_key, zone_ids in zone_ids_by_resource.items():
                    try:
                        rid_int = int(rid_key)
                    except Exception:
                        continue
                    zones_map: Dict[int, Dict[str, Any]] = {}
                    if isinstance(zone_ids, list):
                        for zid in zone_ids:
                            try:
                                zid_int = int(zid)
                            except Exception:
                                continue
                            meta_key = f"{rid_int}:{zid_int}"
                            meta = zone_meta.get(meta_key) if isinstance(zone_meta, dict) else {}
                            name_val = None
                            ct_val = None
                            if isinstance(meta, dict):
                                name_val = meta.get("n") or meta.get("name")
                                ct_val = meta.get("ct")
                            if not isinstance(name_val, str) or not name_val.strip():
                                name_val = f"ID {zid_int}"
                            zones_map[zid_int] = {"n": name_val.strip(), "ct": ct_val}
                    if zones_map:
                        zone_cache[rid_int] = zones_map
            strategy_val = payload.get("strategy")
            if isinstance(strategy_val, str) and strategy_val:
                self._zone_all_strategy = strategy_val

            with _ZONE_CACHE_LOCK:
                _ZONE_CACHE_DATA = {
                    rid: dict(zones) for rid, zones in zone_cache.items()
                }
                _ZONE_CACHE_TS = time.time()
                refreshed = {
                    rid: dict(zones)
                    for rid, zones in _ZONE_CACHE_DATA.items()
                    if isinstance(zones, dict)
                }
            if refreshed:
                return self._filter_zone_cache_by_allowed(refreshed)
            return {}

        if existing_cache:
            return self._filter_zone_cache_by_allowed(existing_cache)
        return None

    def _update_zone_cache(self, resource_id: int, updates: Dict[int, Dict[str, Any]]) -> None:
        if not updates:
            return
        global _ZONE_CACHE_TS, _ZONE_CACHE_DATA
        with _ZONE_CACHE_LOCK:
            resource_cache = _ZONE_CACHE_DATA.setdefault(resource_id, {})
            for zid, payload in updates.items():
                if isinstance(payload, dict):
                    resource_cache[zid] = payload
            _ZONE_CACHE_TS = max(_ZONE_CACHE_TS, time.time())

    def get_zone_center(self, resource_id: int, zone_id: int) -> Optional[Tuple[float, float]]:
        try:
            rid_int = int(resource_id)
            zid_int = int(zone_id)
        except Exception:
            return None
        cached_zone: Optional[Dict[str, Any]] = None
        with _ZONE_CACHE_LOCK:
            resource_cache = _ZONE_CACHE_DATA.get(rid_int, {})
            zone_info = resource_cache.get(zid_int)
            if isinstance(zone_info, dict):
                cached_zone = dict(zone_info)
        if isinstance(cached_zone, dict):
            lat, lon = self._extract_zone_center(cached_zone)
            if lat is not None and lon is not None:
                return lat, lon
        try:
            fetched = self._request_zone_data(rid_int, [zid_int], 20)
        except Exception as exc:
            geo_log.debug(
                "Failed to fetch zone center for resource %s zone %s: %s",
                rid_int,
                zid_int,
                exc,
            )
            return None
        if not fetched:
            return None
        self._update_zone_cache(rid_int, fetched)
        zone_data = fetched.get(zid_int)
        if not isinstance(zone_data, dict):
            return None
        lat, lon = self._extract_zone_center(zone_data)
        if lat is None or lon is None:
            return None
        return lat, lon

    def get_unit_geozone_details(
        self, unit_id: int, lat: Optional[float], lon: Optional[float]
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {"names": [], "hits": {}, "entries": []}
        if lat is None or lon is None:
            return result

        hits = self.find_zones_for_unit(unit_id, lat, lon)
        result["hits"] = hits
        if not hits:
            return result

        names = self.resolve_zone_names(hits)
        result["names"] = names

        entries_by_name: Dict[str, Dict[str, Any]] = {}
        for (rid, zid), entry in self._last_zone_details.items():
            if not isinstance(entry, dict):
                continue
            name = (entry.get("name") or "").strip()
            if not name:
                continue
            name_cf = name.casefold()
            if name_cf not in entries_by_name:
                entries_by_name[name_cf] = {
                    "resource_id": rid,
                    "zone_id": zid,
                    "name": name,
                    "lat": entry.get("lat"),
                    "lon": entry.get("lon"),
                }

        ordered_entries: List[Dict[str, Any]] = []
        for name in names:
            name_cf = name.casefold()
            entry = entries_by_name.get(name_cf)
            if entry:
                ordered_entries.append(dict(entry))

        result["entries"] = ordered_entries
        return result

    # ---- Custom fields helpers ----
    def _find_custom_field_id(self, flds: Dict[str, Any], name_lower: str) -> Optional[int]:
        if not flds:
            return None
        for _, fld in flds.items():
            n = (fld.get("n") or "").strip().lower()
            if n == name_lower:
                return int(fld.get("id"))
        return None

    def update_custom_field(self, unit_id: int, name: str, value: str) -> Tuple[str, int]:
        unit = self.get_unit_with_fields(unit_id)
        existing_id = self._find_custom_field_id(unit.get("flds", {}), name.strip().lower())

        if existing_id is None:
            params = {"itemId": unit_id, "id": 0, "callMode": "create", "n": name, "v": value}
            data = self.request("item/update_custom_field", params)
            if isinstance(data, list) and data:
                return "create", int(data[0])
            if isinstance(data, dict) and data.get("error") == 7:
                raise PermissionError("Недостаточно прав ADF_ACL_ITEM_EDIT_CFIELDS.")
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(f"Wialon error {data['error']} in item/update_custom_field create")
            raise RuntimeError(f"Неожиданный ответ при создании поля: {data}")
        else:
            params = {"itemId": unit_id, "id": existing_id, "callMode": "update", "n": name, "v": value}
            data = self.request("item/update_custom_field", params)
            if isinstance(data, list) and data:
                return "update", existing_id
            if isinstance(data, dict) and data.get("error") == 7:
                raise PermissionError("Недостаточно прав ADF_ACL_ITEM_EDIT_CFIELDS.")
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(f"Wialon error {data['error']} in item/update_custom_field update")
            raise RuntimeError(f"Неожиданный ответ при обновлении поля: {data}")

    def delete_custom_field(self, unit_id: int, field_id: int) -> None:
        params = {"itemId": unit_id, "id": field_id, "callMode": "delete"}
        data = self.request("item/update_custom_field", params)
        if isinstance(data, dict) and data.get("error"):
            if data.get("error") == 7:
                raise PermissionError("Недостаточно прав ADF_ACL_ITEM_EDIT_CFIELDS.")
            raise RuntimeError(f"Wialon error {data['error']} in item/update_custom_field delete")

    # ---- Статистика: ДУТ ----
    @staticmethod
    def _pick_fuel_sensor(unit_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        candidates = list_dut_candidates(unit_item or {})
        primary = select_primary_dut(candidates)
        chosen: Optional[SensorMeta] = None
        if primary:
            chosen = primary
        elif candidates:
            chosen = next((meta for meta in candidates if not meta.is_zero_value), candidates[0])
        return chosen.raw_sensor if isinstance(chosen, SensorMeta) else None

    def load_last_message(self, unit_id: int) -> None:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        params = {"itemId": unit_id, "lastTime": now_ts, "lastCount": 1, "flags": 0, "flagsMask": 0, "loadCount": 1}
        data = self.request("messages/load_last", params)
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"Wialon error {data['error']} in messages/load_last")

    def calc_sensor_value(self, unit_id: int, sensor_id: int) -> Optional[float]:
        params = {"source": "", "indexFrom": 0, "indexTo": 1, "unitId": unit_id, "sensorId": int(sensor_id)}
        data = self.request("unit/calc_sensors", params)
        if not isinstance(data, list) or not data:
            return None
        values_map = data[0] if isinstance(data[0], dict) else {}
        key = str(sensor_id)
        val = values_map.get(key)
        try:
            return float(val) if val is not None else None
        except Exception:
            return None

    def load_messages_interval(
        self,
        unit_id: int,
        time_from: int,
        time_to: int,
        *,
        flags: int = 0x0000,
        flags_mask: int = 0xFF00,
        load_count: int = 4294967295,
    ) -> Dict[str, Any]:
        params = {
            "itemId": int(unit_id),
            "timeFrom": int(time_from),
            "timeTo": int(time_to),
            "flags": int(flags),
            "flagsMask": int(flags_mask),
            "loadCount": int(load_count),
        }
        return self.request("messages/load_interval", params)

    def get_loaded_messages(
        self,
        unit_id: int,
        index_from: int,
        index_to: int,
    ) -> Any:
        params = {
            "itemId": int(unit_id),
            "indexFrom": int(max(0, index_from)),
            "indexTo": int(max(0, index_to)),
        }
        return self.request("messages/get_messages", params)

    def unload_messages(self, unit_id: Optional[int] = None) -> None:
        params: Dict[str, Any] = {}
        if unit_id is not None:
            try:
                params["itemId"] = int(unit_id)
            except Exception:
                params["itemId"] = unit_id
        try:
            self.request("messages/unload", params)
        except Exception as exc:
            log.debug("messages/unload failed: %s", exc)

    def calc_sensor_series(
        self,
        unit_id: int,
        sensor_id: Optional[int],
        *,
        width: Optional[int] = None,
        index_from: int = 0,
        index_to: Optional[int] = None,
    ) -> Union[List[Tuple[int, float]], Any]:
        try:
            idx_from = max(0, int(index_from))
        except Exception:
            idx_from = 0
        try:
            idx_to = int(index_to) if index_to is not None else 0
        except Exception:
            idx_to = 0
        params: Dict[str, Any] = {
            "source": "",
            "indexFrom": idx_from,
            "indexTo": idx_to,
            "unitId": int(unit_id),
        }
        if isinstance(sensor_id, (list, tuple, set)):
            raise ValueError("calc_sensor_series does not accept multiple sensor IDs; use sensorId=0 instead")
        if sensor_id is None:
            params["sensorId"] = 0
        else:
            params["sensorId"] = int(sensor_id)
        if width is not None:
            try:
                params["width"] = max(1, int(width))
            except Exception:
                params["width"] = width
        self._last_calc_series_params = dict(params)
        data = self.request("unit/calc_sensors", params)
        self._last_calc_series_raw = data
        sensor_id_value = params.get("sensorId")
        if sensor_id_value == 0:
            return data
        return self._extract_sensor_series(data, int(sensor_id_value))

    @staticmethod
    def _extract_sensor_series(data: Any, sensor_id: int) -> List[Tuple[int, float]]:
        points: Dict[int, float] = {}

        def _normalize_timestamp(value: Any, depth: int = 0) -> Optional[int]:
            if depth > 3:
                return None
            if isinstance(value, (int, float)):
                try:
                    return int(float(value))
                except Exception:
                    return None
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                try:
                    return int(float(text))
                except Exception:
                    return None
            if isinstance(value, dict):
                for key in ("x", "t", "time", "ts", "timestamp", "left", "right"):
                    if key in value:
                        ts_candidate = _normalize_timestamp(value[key], depth + 1)
                        if ts_candidate is not None:
                            return ts_candidate
                for inner in value.values():
                    ts_candidate = _normalize_timestamp(inner, depth + 1)
                    if ts_candidate is not None:
                        return ts_candidate
            if isinstance(value, (list, tuple)):
                for item in value:
                    ts_candidate = _normalize_timestamp(item, depth + 1)
                    if ts_candidate is not None:
                        return ts_candidate
            return None

        def _normalize_value(value: Any, depth: int = 0) -> Optional[float]:
            if depth > 3:
                return None
            if isinstance(value, (int, float)):
                try:
                    return float(value)
                except Exception:
                    return None
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                if "," in text and "." not in text:
                    text = text.replace(",", ".")
                try:
                    return float(text)
                except Exception:
                    return None
            if isinstance(value, dict):
                for key in (
                    "v",
                    "value",
                    "val",
                    "avg",
                    "y",
                    "top",
                    "bottom",
                    "min",
                    "max",
                    "1",
                    str(sensor_id),
                ):
                    if key in value:
                        candidate = _normalize_value(value[key], depth + 1)
                        if candidate is not None:
                            return candidate
                for inner in value.values():
                    candidate = _normalize_value(inner, depth + 1)
                    if candidate is not None:
                        return candidate
            if isinstance(value, (list, tuple)):
                for item in value:
                    candidate = _normalize_value(item, depth + 1)
                    if candidate is not None:
                        return candidate
            return None

        def _coerce_point(candidate: Any) -> None:
            if isinstance(candidate, (list, tuple)):
                if len(candidate) < 2:
                    return
                ts_raw, val_raw = candidate[0], candidate[1]
            elif isinstance(candidate, dict):
                ts_raw = (
                    candidate.get("x")
                    or candidate.get("t")
                    or candidate.get("time")
                    or candidate.get("ts")
                )
                if ts_raw is None:
                    ts_raw = candidate
                val_raw = (
                    candidate.get("y")
                    or candidate.get("v")
                    or candidate.get("value")
                    or candidate.get("val")
                )
                if val_raw is None:
                    val_raw = candidate
            else:
                return

            ts_val = _normalize_timestamp(ts_raw)
            if ts_val is None:
                return
            val_val = _normalize_value(val_raw)
            if val_val is None:
                return
            points[ts_val] = val_val

        def _walk(obj: Any) -> None:
            if obj is None:
                return
            if isinstance(obj, (list, tuple, set)):
                _coerce_point(obj)
                for item in obj:
                    _walk(item)
                return
            if isinstance(obj, dict):
                sid_key = str(sensor_id)
                if sid_key in obj and isinstance(obj[sid_key], (list, tuple, dict)):
                    _walk(obj[sid_key])
                _coerce_point(obj)
                for key in (
                    "values",
                    "value",
                    "result",
                    "results",
                    "data",
                    "items",
                    "list",
                    "series",
                    "segments",
                ):
                    if key in obj:
                        _walk(obj[key])
                for key in ("left", "right", "bottom", "top", "avg", "min", "max", "first", "last"):
                    if key in obj:
                        _walk(obj[key])
                for key, value in obj.items():
                    if key in {"error", "sensorId", "unitId", "type", "name", "sensor", "u"}:
                        continue
                    if isinstance(key, str) and key.isdigit() and int(key) != sensor_id:
                        continue
                    if isinstance(value, (list, tuple, dict, set)):
                        _walk(value)
                return
            _coerce_point(obj)

        _walk(data)
        ordered = sorted(points.items(), key=lambda item: item[0])
        return [(ts, val) for ts, val in ordered]

    def batch_calc_sensor_values(
        self,
        pairs: List[Tuple[int, int]],
        *,
        chunk_size: int = 50,
    ) -> Dict[Tuple[int, int], Optional[float]]:
        setattr(self, "_last_batch_calc_call_count", 0)
        setattr(self, "_last_batch_calc_total_pairs", len(pairs))
        if not pairs:
            return {}
        try:
            chunk_size = max(1, int(chunk_size))
        except Exception:
            chunk_size = 50

        results: Dict[Tuple[int, int], Optional[float]] = {}
        total_pairs = len(pairs)
        chunk_index = 0
        batch_requests = 0
        for start in range(0, total_pairs, chunk_size):
            self._check_cancelled()
            chunk_index += 1
            chunk = pairs[start : start + chunk_size]
            calls: List[Dict[str, Any]] = []
            for unit_id, sensor_id in chunk:
                try:
                    uid = int(unit_id)
                    sid = int(sensor_id)
                except Exception:
                    results[(unit_id, sensor_id)] = None
                    continue
                calls.append(
                    {
                        "svc": "unit/calc_sensors",
                        "params": {
                            "source": "",
                            "indexFrom": 0,
                            "indexTo": 1,
                            "unitId": uid,
                            "sensorId": sid,
                        },
                    }
                )

            if not calls:
                continue

            attempt = 0
            backoff = DUT_BATCH_RETRY_INITIAL_DELAY
            data: Any = None
            while True:
                self._check_cancelled()
                attempt += 1
                timer = _GeoTimer(f"dut:batch_calc[{chunk_index}]#{attempt}")
                try:
                    batch_requests += 1
                    data = self.request("core/batch", {"calls": calls})
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    label, elapsed = timer.done()
                    sleep_time = min(backoff, DUT_BATCH_RETRY_MAX_DELAY)
                    log.warning(
                        "dut batch chunk %s retry attempt=%s sleep=%.1fs err=%s",
                        chunk_index,
                        attempt,
                        sleep_time,
                        exc,
                    )
                    self.sid = None
                    try:
                        self.login()
                    except Exception as auth_exc:
                        log.debug(
                            "dut batch chunk %s login retry failed: %s",
                            chunk_index,
                            auth_exc,
                        )
                    self._sleep_with_cancel(sleep_time)
                    backoff = min(backoff * 2, DUT_BATCH_RETRY_MAX_DELAY)
                    self._check_cancelled()
                    continue
                else:
                    label, elapsed = timer.done()
                    log.info(
                        "dut batch chunk %s: size=%s elapsed=%.3fs attempt=%s",
                        chunk_index,
                        len(calls),
                        elapsed,
                        attempt,
                    )
                    break

            if data is None:
                continue

            if not isinstance(data, list):
                log.debug(
                    "dut batch chunk %s unexpected response type: %s",
                    chunk_index,
                    type(data).__name__,
                )
                for entry in chunk:
                    results.setdefault((entry[0], entry[1]), None)
                continue

            for idx, pair in enumerate(chunk):
                unit_id, sensor_id = pair
                value: Optional[float] = None
                entry = data[idx] if idx < len(data) else None
                if isinstance(entry, dict) and entry.get("error") not in (None, 0):
                    log.debug(
                        "dut batch element error unit=%s sensor=%s err=%s",
                        unit_id,
                        sensor_id,
                        entry.get("error"),
                    )
                elif isinstance(entry, list) and entry:
                    values_map = entry[0] if isinstance(entry[0], dict) else {}
                    raw = values_map.get(str(sensor_id)) if isinstance(values_map, dict) else None
                    try:
                        value = float(raw) if raw is not None else None
                    except Exception:
                        value = None
                elif isinstance(entry, dict) and "result" in entry:
                    payload = entry.get("result")
                    if isinstance(payload, list) and payload:
                        values_map = payload[0] if isinstance(payload[0], dict) else {}
                        raw = values_map.get(str(sensor_id)) if isinstance(values_map, dict) else None
                        try:
                            value = float(raw) if raw is not None else None
                        except Exception:
                            value = None
                else:
                    log.debug(
                        "dut batch element unexpected entry unit=%s sensor=%s type=%s",
                        unit_id,
                        sensor_id,
                        type(entry).__name__ if entry is not None else None,
                    )
                results[(unit_id, sensor_id)] = value

        log.info(
            "dut batch finished: total_pairs=%s chunks=%s",
            total_pairs,
            chunk_index,
        )
        setattr(self, "_last_batch_calc_call_count", batch_requests)
        return results

    # ---- Команды ----
    def send_custom_tcp(self, unit_id: int, text: str, timeout: int = 10) -> Dict[str, Any]:
        params = {
            "itemId": int(unit_id),
            "commandType": "custom_msg",
            "commandName": "custom_msg",
            "linkType": "tcp",
            "param": str(text),  # без жёсткой проверки на пустоту
            "timeout": int(timeout),
            "flags": 0,
        }
        data = self.request("unit/send_cmd", params)
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError("send_cmd_failed")
        return data


# =================== REPLY/INLINE UI ===================
def reply_menu(chat_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    row: List[KeyboardButton] = [
        KeyboardButton(MENU_BUTTON_FIND),
        KeyboardButton(MENU_BUTTON_DRAIN_ANALYSIS),
        KeyboardButton(MENU_BUTTON_SETTINGS),
        KeyboardButton(MENU_BUTTON_WLN_EXPORT),
    ]
    if is_admin_chat(chat_id):
        row.append(KeyboardButton(MENU_BUTTON_ADMIN))
    buttons: List[List[KeyboardButton]] = [row]
    return ReplyKeyboardMarkup(
        buttons,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Нажмите «🔍 Найти объект»…",
    )


def _settings_summary(context: ContextTypes.DEFAULT_TYPE) -> str:
    radius = _get_nearby_radius(context)
    lines = ["Настройки:", f"• Ближайшие объекты: {radius} м"]
    unit_info = _build_settings_unit_info(context)
    if unit_info:
        lines.append("")
        lines.extend(unit_info)
    return "\n".join(lines)


def _settings_nearby_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    radius = _get_nearby_radius(context)
    return (
        "Ближайшие объекты.\n"
        f"Текущий радиус: {radius} м.\n"
        "Используйте кнопки ниже, чтобы изменить значение."
    )


def _build_settings_unit_info(context: ContextTypes.DEFAULT_TYPE) -> List[str]:
    unit_id = _current_unit_id(context)
    if unit_id is None:
        return []
    config = _load_unit_config(unit_id)
    if not config:
        return []
    lines = [f"Текущий объект: {config.general.name if config.general and config.general.name else f'id {unit_id}'}"]
    general = config.general
    if general:
        if general.uid:
            lines.append(f" UID: {general.uid}")
        if general.hardware:
            lines.append(f" Устройство: {general.hardware}")
    if config.hw_config and config.hw_config.hardware and (not general or config.hw_config.hardware != general.hardware):
        lines.append(f" HW конфиг: {config.hw_config.hardware}")
    sensor_count = len(config.sensors)
    if sensor_count:
        lines.append(f" Сенсоров в конфиге: {sensor_count}")
    return lines


def _format_unit_config_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    unit_id = _current_unit_id(context)
    if unit_id is None:
        return "Нет выбранного объекта. Вернитесь к списку и выберите объект."
    config = _load_unit_config(unit_id)
    if not config:
        return "Для выбранного объекта нет локального UnitConfig. Импортируйте .wlp и повторите попытку."
    lines = [f"<b>UnitConfig — объект {unit_id}</b>"]
    general = config.general
    if general:
        lines.append(f"Имя: {general.name or '—'}")
        if general.uid:
            lines.append(f"UID/IMEI: {general.uid}")
        if general.phone:
            lines.append(f"SIM: {general.phone}")
    hw = config.hw_config
    if hw:
        lines.append("")
        lines.append("<b>Аппаратные параметры</b>")
        if hw.hardware:
            lines.append(f"• Устройство: {hw.hardware}")
        for param in hw.params[:5]:
            title = param.label or param.name
            if not title:
                continue
            value = param.value
            display = value if value not in (None, "") else param.default
            lines.append(f"• {title}: {display}")
        if len(hw.params) > 5:
            lines.append(f"… и ещё {len(hw.params) - 5}")
    if config.sensors:
        lines.append("")
        lines.append("<b>Сенсоры</b>")
        for sensor in config.sensors[:5]:
            desc_parts = []
            if sensor.type:
                desc_parts.append(sensor.type)
            if sensor.units:
                desc_parts.append(sensor.units)
            desc = f" ({', '.join(desc_parts)})" if desc_parts else ""
            lines.append(f"• {sensor.name}{desc}")
        if len(config.sensors) > 5:
            lines.append(f"… и ещё {len(config.sensors) - 5}")
    return "\n".join(lines)


def _current_unit_id(context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    chosen = context.user_data.get("chosen_unit")
    if isinstance(chosen, dict):
        try:
            return int(chosen.get("id"))
        except Exception:
            return None
    return None


def _current_unit_has_config(context: ContextTypes.DEFAULT_TYPE) -> bool:
    unit_id = _current_unit_id(context)
    if unit_id is None:
        return False
    return _load_unit_config(unit_id) is not None


def kb_settings_main(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    radius = _get_nearby_radius(context)
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton(f"Ближайшие объекты ({radius} м)", callback_data="settings:nearby")]
    ]
    if _current_unit_has_config(context):
        rows.append([InlineKeyboardButton("Свойства UnitConfig", callback_data="settings:unitcfg")])
    rows.append([InlineKeyboardButton("Закрыть", callback_data="settings:close")])
    return InlineKeyboardMarkup(rows)


def kb_settings_nearby(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("−50 м", callback_data="settings:radius:-"),
                InlineKeyboardButton("Сброс 100 м", callback_data="settings:radius:reset"),
                InlineKeyboardButton("+50 м", callback_data="settings:radius:+"),
            ],
            [InlineKeyboardButton("Назад", callback_data="settings:back")],
        ]
    )


def kb_settings_unit_config() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="settings:back")]])


async def settings_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remember_user(update)
    chat = update.effective_chat
    chat_id = chat.id if chat else None
    _ensure_settings_defaults(context, chat_id=chat_id)
    text = _settings_summary(context)
    markup = kb_settings_main(context)
    message = await update.message.reply_text(text, reply_markup=markup, parse_mode=None)
    context.user_data["settings_msg"] = {"chat_id": message.chat_id, "message_id": message.message_id}
    return _objects_mode_to_state(context)


async def settings_buttons_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    parts = (q.data or "").split(":", 1)
    action = parts[1] if len(parts) > 1 else ""
    await _safe_answer_callback(q)
    chat = update.effective_chat
    chat_id = chat.id if chat else None
    _ensure_settings_defaults(context, chat_id=chat_id)

    if action == "close":
        try:
            await q.message.delete()
        except Exception:
            pass
        context.user_data.pop("settings_msg", None)
        return _objects_mode_to_state(context)

    if action == "add_dut":
        return await _start_add_dut_flow(q, context)

    if action == "add_dut":
        return await _start_add_dut_flow(q, context)

    if action == "nearby":
        await q.edit_message_text(_settings_nearby_text(context), reply_markup=kb_settings_nearby(context))
        return _objects_mode_to_state(context)
    if action == "unitcfg":
        text = _format_unit_config_text(context)
        await q.edit_message_text(text, reply_markup=kb_settings_unit_config(), parse_mode="HTML")
        return _objects_mode_to_state(context)

    if action == "back":
        await q.edit_message_text(_settings_summary(context), reply_markup=kb_settings_main(context))
        return _objects_mode_to_state(context)

    if action == "radius:+":
        _set_nearby_radius(context, _get_nearby_radius(context) + NEARBY_RADIUS_STEP_M, chat_id=chat_id)
        await q.edit_message_text(_settings_nearby_text(context), reply_markup=kb_settings_nearby(context))
        return _objects_mode_to_state(context)

    if action == "radius:-":
        _set_nearby_radius(context, _get_nearby_radius(context) - NEARBY_RADIUS_STEP_M, chat_id=chat_id)
        await q.edit_message_text(_settings_nearby_text(context), reply_markup=kb_settings_nearby(context))
        return _objects_mode_to_state(context)

    if action == "radius:reset":
        _set_nearby_radius(context, DEFAULT_NEARBY_RADIUS_M, chat_id=chat_id)
        await q.edit_message_text(_settings_nearby_text(context), reply_markup=kb_settings_nearby(context))
        return _objects_mode_to_state(context)

    return _objects_mode_to_state(context)






def kb_start_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(MENU_BUTTON_FIND, callback_data="start:find")]]
    )

def kb_cancel(back_code: Optional[str] = None) -> InlineKeyboardMarkup:
    row = []
    if back_code:
        row.append(InlineKeyboardButton("⬅️ Назад", callback_data=back_code))
    row.append(InlineKeyboardButton("❌ Отмена", callback_data="action:cancel"))
    return InlineKeyboardMarkup([row])


def kb_cf_controls(back_code: Optional[str] = None) -> InlineKeyboardMarkup:
    row: List[InlineKeyboardButton] = []
    if back_code:
        row.append(InlineKeyboardButton("⬅️ Назад", callback_data=back_code))
    row.append(InlineKeyboardButton(CF_DUMP_BUTTON_TEXT, callback_data=CF_DUMP_CALLBACK))
    row.append(InlineKeyboardButton("❌ Отмена", callback_data="action:cancel"))
    return InlineKeyboardMarkup([row])


def kb_cmd_entry_controls(back_code: str = "back:cmd_unit") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("↩️ К карточке", callback_data=back_code),
                InlineKeyboardButton("🔍 Новый поиск", callback_data="back:find"),
            ]
        ]
    )


def kb_export_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Закрыть", callback_data="export:cancel")]])


def _normalize_units_offset(total: int, offset: int) -> int:
    if total <= 0:
        return 0
    if offset < 0:
        return 0
    if offset >= total:
        last_page = ((total - 1) // SEARCH_UNITS_PAGE_SIZE) * SEARCH_UNITS_PAGE_SIZE
        return max(last_page, 0)
    return offset




def _get_search_units(context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    units = context.user_data.get("search_units")
    if not isinstance(units, list):
        legacy = context.user_data.get("search_units_all")
        if isinstance(legacy, list):
            return list(legacy)
        return []
    return list(units)


def _set_search_units(context: ContextTypes.DEFAULT_TYPE, units: List[Dict[str, Any]]) -> None:
    stored = [dict(u) for u in units]
    context.user_data["search_units"] = stored
    context.user_data["search_units_all"] = list(stored)
    context.user_data["search_results"] = {str(u.get('id')): u for u in stored}
    context.user_data["search_units_offset"] = 0
    context.user_data["search_quick_mask"] = ""


def _set_quick_mask(context: ContextTypes.DEFAULT_TYPE, mask: str) -> None:
    context.user_data["search_quick_mask"] = mask


def _quick_mask(context: ContextTypes.DEFAULT_TYPE) -> str:
    return str(context.user_data.get("search_quick_mask") or "")


def _apply_quick_filter(units: List[Dict[str, Any]], mask: str) -> List[Dict[str, Any]]:
    if not mask:
        return list(units)
    trimmed = mask.strip()
    if not trimmed:
        return list(units)
    if len(trimmed) >= 3:
        return list(units)
    needle = trimmed.casefold()
    filtered: List[Dict[str, Any]] = []
    for unit in units:
        name = str(unit.get('nm') or '')
        if needle in name.casefold():
            filtered.append(unit)
    return filtered




def _unit_status_markers(unit: Dict[str, Any]) -> Tuple[str, str]:
    last_ts: Optional[int] = None
    if isinstance(unit, dict):
        lmsg = unit.get("lmsg")
        if isinstance(lmsg, dict):
            ts = lmsg.get("t")
            try:
                last_ts = int(ts) if ts is not None else None
            except (TypeError, ValueError):
                last_ts = None
        if last_ts is None:
            pos = unit.get("pos")
            if isinstance(pos, dict):
                ts = pos.get("t")
                try:
                    last_ts = int(ts) if ts is not None else None
                except (TypeError, ValueError):
                    last_ts = None

    online = _is_online(last_ts)
    online_dot = "🟢" if online else "🔴"

    pos_ts: Optional[int] = None
    if isinstance(unit, dict):
        pos = unit.get("pos")
        if isinstance(pos, dict):
            ts = pos.get("t")
            try:
                pos_ts = int(ts) if ts is not None else None
            except (TypeError, ValueError):
                pos_ts = None
    if pos_ts is None:
        pos_ts = last_ts
    sat_dot, _ = _human_ago(pos_ts)
    return online_dot, sat_dot


def _reset_search_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("search_units", None)
    context.user_data.pop("search_units_all", None)
    context.user_data.pop("search_units_offset", None)
    context.user_data.pop("search_quick_mask", None)
    context.user_data.pop("search_results", None)
    context.user_data.pop("stats_unit_item", None)
    context.user_data.pop("graph_context", None)
    context.user_data.pop(SEARCH_GENERATION_KEY, None)
    _clear_stats_generation(context)

def _filtered_units(context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    units = _get_search_units(context)
    mask = _quick_mask(context)
    return _apply_quick_filter(units, mask)





def _list_header(total: int, empty_hint: Optional[str] = None) -> str:
    lines = [f"Найдено: {total}"]
    if total:
        lines.append("Выберите объект:")
    else:
        lines.append(empty_hint or "Список пуст. Введите запрос для поиска.")
    return "\n\n".join(lines)

def _objects_mode(context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    return context.user_data.get("objects_mode")


def _set_objects_mode(context: ContextTypes.DEFAULT_TYPE, mode: Optional[str]) -> None:
    if mode is None:
        context.user_data.pop("objects_mode", None)
    else:
        context.user_data["objects_mode"] = mode


def _anchor_message_id(context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    anchor = context.user_data.get("anchor")
    if not anchor:
        return None
    message_id = anchor.get("message_id")
    try:
        return int(message_id)
    except (TypeError, ValueError):
        return message_id


def _bump_search_generation(context: ContextTypes.DEFAULT_TYPE) -> str:
    token = uuid.uuid4().hex
    context.user_data[SEARCH_GENERATION_KEY] = token
    return token


def _current_search_generation(context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    token = context.user_data.get(SEARCH_GENERATION_KEY)
    return str(token) if token is not None else None


def _set_stats_generation(context: ContextTypes.DEFAULT_TYPE, unit_id: int) -> str:
    token = uuid.uuid4().hex
    context.user_data[STATS_GENERATION_KEY] = token
    context.user_data[STATS_GENERATION_UNIT_KEY] = unit_id
    return token


def _current_stats_generation(context: ContextTypes.DEFAULT_TYPE) -> Tuple[Optional[str], Optional[int]]:
    token = context.user_data.get(STATS_GENERATION_KEY)
    unit_id = context.user_data.get(STATS_GENERATION_UNIT_KEY)
    try:
        unit_id_int = int(unit_id) if unit_id is not None else None
    except (TypeError, ValueError):
        unit_id_int = None
    return (str(token) if token is not None else None, unit_id_int)


def _clear_stats_generation(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(STATS_GENERATION_KEY, None)
    context.user_data.pop(STATS_GENERATION_UNIT_KEY, None)


def _objects_mode_to_state(context: ContextTypes.DEFAULT_TYPE) -> int:
    mode = _objects_mode(context)
    if mode == OBJECTS_MODE_LIST:
        return STATE_WAIT_UNIT
    return STATE_FIND_QUERY



async def _render_units_page(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: Optional[int] = None,
    offset: Optional[int] = None,
    update: Optional[Update] = None,
    message: Optional[Message] = None,
    reanchor: bool = False,
) -> None:
    if _active_ui(context) != UI_OBJECTS:
        return
    units = _filtered_units(context)
    total = len(units)
    if offset is None:
        offset = context.user_data.get("search_units_offset") or 0
    normalized_offset = _normalize_units_offset(total, int(offset))
    context.user_data["search_units_offset"] = normalized_offset
    if total:
        markup: Optional[InlineKeyboardMarkup] = kb_units_page(units, normalized_offset)
    else:
        markup = kb_search_prompt(context)
    text = _list_header(total)
    anchor = context.user_data.get("anchor")
    target_message = message
    if target_message is None and update is not None:
        target_message = update.effective_message
    if reanchor:
        if target_message is not None:
            await delete_anchor(context)
            try:
                msg = await target_message.reply_text(
                    text,
                    reply_markup=markup,
                    parse_mode=None,
                )
            except Exception as exc:
                log.debug("reanchor units via reply failed: %s", exc)
            else:
                await set_anchor_on(msg, context)
                return
        if chat_id is not None:
            await delete_anchor(context)
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=markup,
                parse_mode=None,
            )
            await set_anchor_on(msg, context)
            return
    if anchor:
        await edit_anchor(context, text, markup, parse_mode=None)
        return
    if target_message is not None:
        msg = await target_message.reply_text(text, reply_markup=markup, parse_mode=None)
        await set_anchor_on(msg, context)
        return
    if chat_id is None:
        return
    msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=markup, parse_mode=None)
    await set_anchor_on(msg, context)

def kb_units_page(units: List[Dict[str, Any]], offset: int = 0) -> InlineKeyboardMarkup:
    total = len(units)
    normalized_offset = _normalize_units_offset(total, offset)
    slice_units = units[normalized_offset: normalized_offset + SEARCH_UNITS_PAGE_SIZE]
    rows: List[List[InlineKeyboardButton]] = []
    for unit in slice_units:
        online_dot, sat_dot = _unit_status_markers(unit)
        name = str(unit.get("nm") or "")
        unit_id = unit.get("id")
        unit_id_str = str(unit_id) if unit_id is not None else ""
        label_id = unit_id_str or "—"
        button_text = f"{online_dot}{sat_dot} {name} — id {label_id}".strip()
        rows.append(
            [
                InlineKeyboardButton(
                    button_text,
                    callback_data=f"unit:{unit_id_str}",
                )
            ]
        )
    total_pages = max((total + SEARCH_UNITS_PAGE_SIZE - 1) // SEARCH_UNITS_PAGE_SIZE, 1)
    current_page = min(total_pages, (normalized_offset // SEARCH_UNITS_PAGE_SIZE) + 1)
    nav_row: List[InlineKeyboardButton] = []
    if total_pages > 1 and normalized_offset > 0:
        prev_offset = max(normalized_offset - SEARCH_UNITS_PAGE_SIZE, 0)
        nav_row.append(
            InlineKeyboardButton("◀️ Назад", callback_data=f"units:more:{prev_offset}")
        )
    nav_row.append(
        InlineKeyboardButton(f"стр. {current_page}/{total_pages}", callback_data="units:page")
    )
    if total_pages > 1 and normalized_offset + SEARCH_UNITS_PAGE_SIZE < total:
        nav_row.append(
            InlineKeyboardButton(
                "▶️ Далее",
                callback_data=f"units:more:{normalized_offset + SEARCH_UNITS_PAGE_SIZE}",
            )
        )
    rows.append(nav_row)
    rows.append(
        [
            InlineKeyboardButton("↩️ К поиску", callback_data="back:find"),
            InlineKeyboardButton("❌ Отмена", callback_data="action:cancel"),
        ]
    )
    return InlineKeyboardMarkup(rows)



async def _handle_objects_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if _active_ui(context) == UI_ADMIN:
        chat = update.effective_chat
        log.debug(
            "admin: ignore object text user=%s",
            chat.id if chat else None,
        )
        return _objects_mode_to_state(context)

    if update.message and (update.message.text or "").strip() == MENU_BUTTON_SETTINGS:
        return await settings_entry(update, context)

    await _activate_objects_ui(context)
    text = (update.message.text or "").strip()
    mode = _objects_mode(context) or OBJECTS_MODE_LIST
    if mode == OBJECTS_MODE_CARD_IDLE:
        await clear_stats_buttons(context)
        _clear_stats_generation(context)
        _set_objects_mode(context, OBJECTS_MODE_LIST)
    elif mode not in {OBJECTS_MODE_LIST, OBJECTS_MODE_CARD_IDLE}:
        return STATE_WAIT_UNIT

    search_token = _bump_search_generation(context)

    if not text:
        await send_new_anchor_below(
            update,
            "Введите минимум 3 символа для поиска",
            kb_search_prompt(context),
            context,
            parse_mode=None,
        )
        return STATE_FIND_QUERY

    id_match = ID_QUERY_RE.match(text)
    if id_match:
        unit_id = int(id_match.group(1))
        cached = _lookup_unit_snapshot(unit_id) or {}
        unit_name = str(cached.get("nm") or f"id {unit_id}")
        context.user_data["chosen_unit"] = {"id": unit_id, "nm": unit_name}
        _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
        await delete_anchor(context)
        client = None
        if not _pipeline_card_enabled():
            client = await require_wialon_client(
                update,
                context,
                "Need Wialon access to show card.",
            )
            if not client:
                return STATE_AUTH_WAIT_TOKEN
        return await show_stats_then_actions(
            update,
            context,
            unit_id,
            unit_name,
            client,
            preserve_previous=True,
        )


    if len(text) < 3:
        units = _get_search_units(context)
        _set_quick_mask(context, text)
        if units:
            context.user_data["search_units_offset"] = 0
            _set_objects_mode(context, OBJECTS_MODE_LIST)
            await _render_units_page(
                context,
                chat_id=update.effective_chat.id if update.effective_chat else None,
                update=update,
                reanchor=True,
                offset=0,
            )
            return STATE_WAIT_UNIT
        await send_new_anchor_below(
            update,
            "Нужно минимум 3 символа для поиска.",
            kb_search_prompt(context),
            context,
            parse_mode=None,
        )
        return STATE_FIND_QUERY

    await edit_anchor(context, "Ищем по базе…", kb_search_prompt(context), parse_mode=None)
    anchor_message_id = _anchor_message_id(context)

    if _pipeline_search_enabled():
        try:
            units = await run_blocking(_search_units_locally, text, SEARCH_UNITS_LIMIT)
        except Exception as exc:
            log.exception("pipeline search failed: %s", exc)
            await send_new_anchor_below(
                update,
                "Локальный поиск временно недоступен. Повторите попытку позже.",
                kb_search_prompt(context),
                context,
                parse_mode=None,
            )
            return STATE_FIND_QUERY
        return await _present_search_results(
            update,
            context,
            units,
            search_token=search_token,
            client=None,
            anchor_message_id=anchor_message_id,
        )

    client = await require_wialon_client(update, context, "��'�?�+�< ��?����'�? �?�+�?���'�<, ���?�'�?�?����?���'��?�?.")
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    try:
        units = await run_blocking(client.search_units, text, limit=SEARCH_UNITS_LIMIT)
    except Exception as exc:
        if search_token != _current_search_generation(context):
            log.debug("Stale search error ignored for %s: %s", text, exc)
            return _objects_mode_to_state(context)
        log.exception("�?�?��+��� ���?��?��� units (router)")
        await send_new_anchor_below(
            update,
            f"�?�?��+��� ���?��?��� �? Wialon: {exc}",
            kb_search_prompt(context),
            context,
        )
        return STATE_FIND_QUERY

    return await _present_search_results(
        update,
        context,
        units,
        search_token=search_token,
        client=client,
        anchor_message_id=anchor_message_id,
    )

    client = await require_wialon_client(update, context, "Чтобы искать объекты, авторизуйтесь.")
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    await edit_anchor(context, "🔎 Ищу объекты…", kb_search_prompt(context), parse_mode=None)
    anchor_message_id = _anchor_message_id(context)
    try:
        units = await run_blocking(client.search_units, text, limit=SEARCH_UNITS_LIMIT)
    except Exception as exc:
        if search_token != _current_search_generation(context):
            log.debug("Stale search error ignored for %s: %s", text, exc)
            return _objects_mode_to_state(context)
        log.exception("Ошибка поиска units (router)")
        await send_new_anchor_below(
            update,
            f"Ошибка поиска в Wialon: `{exc}`",
            kb_search_prompt(context),
            context,
        )
        return STATE_FIND_QUERY

    if search_token != _current_search_generation(context):
        return _objects_mode_to_state(context)

    if _active_ui(context) != UI_OBJECTS:
        return _objects_mode_to_state(context)

    if anchor_message_id is not None:
        current_anchor_id = _anchor_message_id(context)
        if current_anchor_id is None or current_anchor_id != anchor_message_id:
            return _objects_mode_to_state(context)

    if not units:
        _reset_search_context(context)
        await send_new_anchor_below(
            update,
            "Объект не найден. Попробуйте ввести другое имя",
            kb_search_prompt(context),
            context,
        )
        return STATE_FIND_QUERY

    if len(units) == 1:
        unit_id = int(units[0]["id"])
        selected = {"id": unit_id, "nm": units[0]["nm"]}
        context.user_data["chosen_unit"] = selected
        _set_search_units(context, units)
        _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
        await delete_anchor(context)
        return await show_stats_then_actions(
            update,
            context,
            unit_id,
            selected["nm"],
            client,
            preserve_previous=True,
        )

    _set_search_units(context, units[:SEARCH_UNITS_LIMIT])
    _set_objects_mode(context, OBJECTS_MODE_LIST)
    await _render_units_page(
        context,
        chat_id=update.effective_chat.id if update.effective_chat else None,
        update=update,
        offset=0,
        reanchor=True,
    )
    return STATE_WAIT_UNIT

STATS_ACTIONS_PATTERN = r"^stats:(cf|cmd|refresh|params|report|nearby|to_list|graph|add_dut|sensors|dutcal|dutcal_refresh|clear_sensors)$"


def kb_stats_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📑 Отчёт", callback_data="stats:report"),
                InlineKeyboardButton("📍 Ближайшие объекты", callback_data="stats:nearby"),
            ],
            [
                InlineKeyboardButton("📡 Команда", callback_data="stats:cmd"),
                InlineKeyboardButton("⚙️ Параметры", callback_data="stats:params"),
            ],
            [InlineKeyboardButton("📝 Произвольные поля", callback_data="stats:cf")],
            [InlineKeyboardButton("📊 Анализ Сливов", callback_data="stats:graph")],
            [InlineKeyboardButton("➕ Добавить ДУТ", callback_data="stats:add_dut")],
            [InlineKeyboardButton("🔧 Настройки датчиков", callback_data="stats:sensors")],
            [InlineKeyboardButton("🗑 Очистить датчики", callback_data="stats:clear_sensors")],
            [InlineKeyboardButton("📈 Тарировка ДУТ", callback_data="stats:dutcal")],
        ]
    )


def kb_graph_periods() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🗓️ Вчера", callback_data="graph:period:yesterday")],
            [InlineKeyboardButton("📅 Произвольный день", callback_data="graph:period:custom")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="graph:back_card")],
        ]
    )


def _graph_context(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    ctx = context.user_data.get("graph_context")
    if not isinstance(ctx, dict):
        ctx = {}
        context.user_data["graph_context"] = ctx
    return ctx


async def _start_graph_flow(q, context: ContextTypes.DEFAULT_TYPE, unit: Dict[str, Any]) -> int:
    await clear_stats_buttons(context)
    _set_objects_mode(context, OBJECTS_MODE_CARD_FLOW_GRAPH)
    context.user_data["mode"] = MODE_GRAPH
    graph_ctx = _graph_context(context)
    graph_ctx.clear()
    try:
        unit_id = int(unit.get("id"))
    except Exception:
        unit_id = unit.get("id")
    unit_name = (unit.get("nm") or "").strip() or f"id {unit_id}" if unit_id is not None else "Без имени"
    graph_ctx.update({"unit_id": unit_id, "unit_name": unit_name})
    await delete_anchor(context)
    msg = await q.message.reply_text(
        "Выберите период графика топлива",
        reply_markup=kb_graph_periods(),
        parse_mode=None,
    )
    await set_anchor_on(msg, context)
    return STATE_GRAPH_PERIOD


async def _show_graph_periods(context: ContextTypes.DEFAULT_TYPE) -> None:
    await edit_anchor(context, "Выберите период графика топлива", kb_graph_periods(), parse_mode=None)



async def _send_drain_message(
    update_or_q,
    context: ContextTypes.DEFAULT_TYPE,
    message: str,
) -> None:
    target = getattr(update_or_q, "message", None)
    chat_id: Optional[int] = None
    if target is None and isinstance(update_or_q, Update):
        target = update_or_q.effective_message
    if target is not None:
        try:
            await target.reply_text(message, parse_mode=None)
            return
        except Exception as exc:
            log.debug("graph notify fallback: %s", exc)
            try:
                chat_id = target.chat_id
            except Exception:
                chat_id = None
    if chat_id is None:
        stats_meta = context.user_data.get("stats_msg")
        if isinstance(stats_meta, dict):
            chat_id = stats_meta.get("chat_id")
    if chat_id is not None:
        try:
            await context.bot.send_message(chat_id=chat_id, text=message, parse_mode=None)
        except Exception as exc:
            log.debug("graph notify send failed: %s", exc)


async def _handle_drain_summary(
    update_or_q,
    context: ContextTypes.DEFAULT_TYPE,
    client: "WialonClient",
    unit_id: int,
    unit_name: str,
    period: Union[str, Dict[str, Any]],
) -> bool:
    if isinstance(period, dict):
        try:
            start_ts = int(period["from"])
            end_ts = int(period["to"])
        except Exception as exc:
            raise ValueError("Некорректные границы периода для графика") from exc
        label = str(period.get("label") or "Произвольный день")
        period_key = str(period.get("key") or "custom")
    else:
        interval = build_report_interval(period)
        start_ts = int(interval["from"])
        end_ts = int(interval["to"])
        label = str(interval.get("label") or period)
        period_key = period

    if end_ts <= start_ts:
        end_ts = start_ts + 1

    trace_id = str(uuid.uuid4())[:8]
    t0 = time.perf_counter()

    log.info(
        "graph: start unit=%s name=%s period=%s from=%s to=%s (%s)",
        unit_id,
        unit_name,
        period_key,
        start_ts,
        end_ts,
        label,
    )
    drain_log.info(
        "trace=%s graph:start unit=%s period=%s %s..%s",
        trace_id,
        unit_id,
        period_key,
        start_ts,
        end_ts,
    )

    unit_item = context.user_data.get("stats_unit_item")
    if not isinstance(unit_item, dict) or int(unit_item.get("id") or -1) != int(unit_id):
        try:
            unit_item = await run_blocking(client.get_unit_full_for_stats, unit_id)
        except Exception as exc:
            log.debug("graph: reload unit failed: %s", exc)
            unit_item = {}
        if isinstance(unit_item, dict):
            context.user_data["stats_unit_item"] = copy.deepcopy(unit_item)

    dut_sensors, dut_summary, all_candidates = _prepare_dut_for_analysis(unit_item or {})
    if not dut_sensors:
        dut_log.info("detector: unit=%s dut_ids=() reason=no_sensors", unit_id)
        message = "\u041d\u0435 \u0443\u0434\u0430\043b\u043e\u0441\u044c \u043f\u043e\u0434\043e\0431\u0440\0430\0442\044c \u0414\u0423\u0422 \u0434\u043b\u044f \u0430\u043d\0430\043b\0438\u0437\u0430."
        await _send_drain_message(
            update_or_q,
            context,
            message,
        )
        drain_log.info("trace=%s message:%s", trace_id, message)
        drain_log.info(
            "trace=%s graph:done total=%.3fs reason=no_sensors",
            trace_id,
            time.perf_counter() - t0,
        )
        return False
    sensor_ids: List[int] = []
    sensor_meta_by_id: Dict[Any, Any] = {}
    for meta in dut_sensors:
        if meta.id is None:
            continue
        sensor_ids.append(meta.id)
        sensor_meta_by_id[meta.id] = meta
    sensor_meta_by_id["__summary__"] = dut_summary

    if not sensor_ids:
        dut_log.info("detector: unit=%s dut_ids=() reason=no_ids", unit_id)
        message = "\u041d\u0435 \u0443\u0434\u0430\043b\u043e\u0441\u044c \u043f\u043e\u0434\043e\0431\u0440\0430\0442\044c \u0414\u0423\u0422 \u0434\u043b\u044f \u0430\u043d\0430\043b\0438\u0437\u0430."
        await _send_drain_message(
            update_or_q,
            context,
            message,
        )
        drain_log.info("trace=%s message:%s", trace_id, message)
        drain_log.info(
            "trace=%s graph:done total=%.3fs reason=no_sensor_ids",
            trace_id,
            time.perf_counter() - t0,
        )
        return False

    log.info(
        "graph: dut sensors unit=%s count=%s ids=%s",
        unit_id,
        len(sensor_ids),
        sensor_ids,
    )
    dut_log.info("detector: unit=%s dut_ids=%s", unit_id, sensor_ids)
    dut_log.info("detector:dut_filter summary=%s", dut_summary)
    def _meta_to_dict(candidate: Any) -> Dict[str, Any]:
        if hasattr(candidate, "as_dict"):
            try:
                return candidate.as_dict()  # type: ignore[return-value]
            except Exception as exc:  # pragma: no cover - defensive logging helper
                log.debug("graph: meta.as_dict failed: %s", exc)
        if isinstance(candidate, dict):
            return candidate
        return {"repr": repr(candidate)}

    drain_log.debug(
        "dut: sensors detail=%s",
        [_meta_to_dict(meta) for meta in dut_sensors],
    )
    drain_log.debug(
        "dut: sensors raw=%s",
        [_meta_to_dict(meta) for meta in all_candidates],
    )
    log.info(
        "graph: dut filter summary unit=%s checked=%s kept=%s primary=%s zeros=%s",
        unit_id,
        dut_summary.get("checked"),
        dut_summary.get("kept"),
        (dut_summary.get("primary") or {}).get("id") if dut_summary.get("primary") else None,
        [z.get("id") for z in dut_summary.get("zeros") or []],
    )
    for meta in dut_sensors:
        log.info(
            "graph: dut candidate unit=%s id=%s name=%s primary=%s zero=%s match=%s param=%s value=%s",
            unit_id,
            meta.id,
            meta.name,
            meta.is_primary,
            meta.is_zero_value,
            meta.match_reason,
            meta.assigned_param or meta.param_hint,
            meta.value,
        )

    target_map = dut_summary.get("target_map") if isinstance(dut_summary, dict) else None
    if target_map is not None and not isinstance(target_map, dict):
        target_map = None

    async_client = await AsyncWialonClientBuilder.from_sync(client)
    t_load0 = time.perf_counter()
    try:
        (
            series_by_sensor,
            _stats_by_sensor,
            _index_from,
            _index_to,
            _msg_count,
            width_for_detector,
            speed_series,
            aux_series,
            samples,
        ) = await messages_loader.load_messages_and_series(
            async_client,
            unit_id,
            sensor_ids,
            start_ts,
            end_ts,
            dut_targets=target_map,
            request_timeout=DRAIN_ANALYSIS_MESSAGE_LOAD_TIMEOUT,
        )
        t_load1 = time.perf_counter()
        drain_log.info("trace=%s load:done dt=%.3fs", trace_id, t_load1 - t_load0)
    except Exception:
        t_load1 = time.perf_counter()
        drain_log.info("trace=%s load:fail dt=%.3fs", trace_id, t_load1 - t_load0)
        message = "\u041d\u0435 \u0443\u0434\u0430\043b\u043e\u0441\u044c \u043f\u043e\u043b\u0443\u0447\044c \u0434\u0430\043d\043d\044b\0435 \u043f\043e \u0414\u0423\u0422."
        await _send_drain_message(
            update_or_q,
            context,
            message,
        )
        drain_log.info("trace=%s message:%s", trace_id, message)
        drain_log.info(
            "trace=%s graph:done total=%.3fs reason=load_failed",
            trace_id,
            time.perf_counter() - t0,
        )
        log.exception("graph: sensor series failed")
        return False
    finally:
        try:
            await asyncio.wait_for(
                async_client.unload_messages(unit_id),
                timeout=DRAIN_ANALYSIS_UNLOAD_TIMEOUT,
            )
        except Exception as exc:
            log.debug("graph: unload messages failed: %s", exc)
        finally:
            await async_client.aclose()

    non_empty_series = [series_by_sensor[sid] for sid in sensor_ids if series_by_sensor.get(sid)]
    log.info(
        "graph: dut series non-empty=%s/%s",
        len(non_empty_series),
        len(sensor_ids),
    )
    dut_log.info(
        "detector: unit=%s non_empty_series=%s total_sensors=%s",
        unit_id,
        len(non_empty_series),
        len(sensor_ids),
    )

    if not non_empty_series:
        dut_log.info("detector: unit=%s reason=no_series", unit_id)
        message = "\u041d\u0435 \u0443\u0434\u0430\043b\u043e\u0441\u044c \u043f\u043e\043b\0443\u0447\0438\0442\044c \u0434\0430\043d\043d\044b\0435 \u043f\043e \u0414\u0423\u0422."
        await _send_drain_message(
            update_or_q,
            context,
            message,
        )
        drain_log.info("trace=%s message:%s", trace_id, message)
        drain_log.info(
            "trace=%s graph:done total=%.3fs reason=no_series",
            trace_id,
            time.perf_counter() - t0,
        )
        return False

    series_for_detector = {
        sid: series_by_sensor[sid]
        for sid in sensor_ids
        if series_by_sensor.get(sid)
    }
    if not series_for_detector:
        dut_log.info("detector: unit=%s reason=series_for_detector_empty", unit_id)
        message = "\u041d\u0435 \u0443\u0434\u0430\043b\u043e\u0441\u044c \u043f\u043e\043b\0443\u0447\0438\0442\044c \u0434\0430\043d\043d\044b\0435 \u043f\043e \u0414\u0423\u0422."
        await _send_drain_message(
            update_or_q,
            context,
            message,
        )
        drain_log.info("trace=%s message:%s", trace_id, message)
        drain_log.info(
            "trace=%s graph:done total=%.3fs reason=series_for_detector_empty",
            trace_id,
            time.perf_counter() - t0,
        )
        return False

    total_points = sum(len(series_for_detector[sid]) for sid in series_for_detector)
    log.info(
        "graph: series summary unit=%s sensors=%s points=%s",
        unit_id,
        len(series_for_detector),
        total_points,
    )

    drain_log.info(
        "trace=%s detector:series unit=%s sensors=%s width=%s",
        trace_id,
        unit_id,
        sorted(series_for_detector.keys()),
        width_for_detector if width_for_detector is not None else "raw",
    )

    if isinstance(dut_summary, dict):
        dut_summary.setdefault("detected_drains", [])

    detection_input = series_for_detector
    t_det0 = time.perf_counter()
    drain_log.info(
        "trace=%s detect:start sensors=%s speed_points=%s",
        trace_id,
        sorted(detection_input.keys()),
        len(speed_series or []),
    )
    drains = fuel_detector.detect_short_drains(
        detection_input,
        speed_series=speed_series,
        sensor_meta_by_id=sensor_meta_by_id,
        unit_summary=dut_summary,
        samples=samples,
    )
    t_det1 = time.perf_counter()
    drain_log.info("trace=%s detect:done dt=%.3fs result=%s", trace_id, t_det1 - t_det0, drains)


    if drains:
        log.info("graph: drains detected unit=%s count=%s", unit_id, len(drains))
        dut_log.info("detector: unit=%s drains_count=%s", unit_id, len(drains))
        for event in drains:
            drain_log.info(
                "detector event unit=%s start=%s end=%s drop=%.1f rate=%.2f",
                unit_id,
                event.get("start_ts"),
                event.get("end_ts"),
                event.get("total_drop_l") or 0.0,
                event.get("rate_lpm") or 0.0,
            )
        dut_summary["detected_drains"] = drains
        lines = ["\u041e\u0431\u043d\u0430\u0440\u0443\u0436\u0435\u043d\u044b \u0441\u043b\u0438\u0432\u044b \u0442\u043e\u043f\u043b\u0438\u0432\u0430:"]
        for idx, event in enumerate(drains, start=1):
            start_ts = event.get("start_ts")
            end_ts = event.get("end_ts")
            total_drop = event.get("total_drop_l") or 0.0
            duration_text = _fmt_duration(event.get("duration_s"))
            rate_lpm = event.get("rate_lpm") or 0.0
            lines.append(
                f"{idx}. {_fmt_ts_full(start_ts)} - {_fmt_ts_full(end_ts)}: -{total_drop:.1f} \u043b \u0437\u0430 {duration_text} ({rate_lpm:.1f} \u043b/\u043c\u0438\u043d)"
            )
            sensor_details: List[str] = []
            for sensor in event.get("per_sensor") or []:
                drop_l = sensor.get("drop_l")
                if drop_l is None or drop_l < fuel_detector.MIN_DROP_PER_SENSOR_L * 0.5:
                    continue
                label = (
                    sensor.get("name")
                    or sensor.get("param")
                    or (f"ID {sensor.get('sensor_id')}" if sensor.get("sensor_id") is not None else str(sensor.get("key")))
                )
                start_text = _fmt_liters(sensor.get("start_l"))
                end_text = _fmt_liters(sensor.get("end_l"))
                sensor_details.append(
                    f"   - {label}: -{drop_l:.1f} \u043b ({start_text} -> {end_text})"
                )
            if sensor_details:
                lines.extend(sensor_details)
            meta_parts: List[str] = []
            distance = event.get("event_distance_m")
            if distance is not None:
                meta_parts.append(f"\u0434\u0438\u0441\u0442\u0430\u043d\u0446\u0438\u044f {distance:.0f} \u043c")
            speed = event.get("event_max_speed_kmh")
            if speed is not None:
                meta_parts.append(f"\u043c\u0430\u043a\u0441. \u0441\u043a\u043e\u0440\u043e\u0441\u0442\u044c {speed:.1f} \u043a\u043c/\u0447")
            ignition_ratio = event.get("ignition_ratio")
            if ignition_ratio is not None:
                meta_parts.append(f"\u0437\u0430\u0436\u0438\u0433\u0430\u043d\u0438\u0435 {ignition_ratio * 100:.0f}% \u0432\u0440\u0435\u043c\u0435\u043d\u0438")
            valid_ratio = event.get("valid_ratio")
            if valid_ratio is not None:
                meta_parts.append(f"\u0432\u0430\u043b\u0438\u0434\u043d\u044b\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f {valid_ratio * 100:.0f}%")
            sats_avg = event.get("sats_avg")
            if sats_avg is not None:
                meta_parts.append(f"\u0441\u0440\u0435\u0434\u043d\u0435\u0435 \u0447\u0438\u0441\u043b\u043e \u0441\u043f\u0443\u0442\u043d\u0438\u043a\u043e\u0432 {sats_avg:.1f}")
            sats_min = event.get("sats_min")
            if sats_min is not None:
                meta_parts.append(f"\u043c\u0438\u043d. \u0441\u043f\u0443\u0442\u043d\u0438\u043a\u043e\u0432 {int(sats_min)}")
            signal_gap = event.get("signal_gap_s")
            if signal_gap:
                meta_parts.append(f"\u043f\u0440\u043e\u0432\u0430\u043b \u0441\u0432\u044f\u0437\u0438 {int(signal_gap)} \u0441")
            bad_samples = event.get("bad_samples")
            if bad_samples:
                meta_parts.append(f"\u043d\u0435\u0432\u0430\u043b\u0438\u0434\u043d\u044b\u0445 \u0442\u043e\u0447\u0435\u043a {int(bad_samples)}")
            if meta_parts:
                lines.append("   - " + "; ".join(meta_parts))
        drains_text = "\n".join(lines)
    else:
        log.info("graph: drains detected unit=%s count=0", unit_id)
        dut_log.info("detector: unit=%s drains_count=0", unit_id)
        drain_log.info("detector result unit=%s drains=0", unit_id)
        dut_summary["detected_drains"] = []
        drains_text = "\u0421\u043b\u0438\u0432\u043e\u0432 \u0442\u043e\u043f\u043b\u0438\u0432\u0430 \u043d\u0435 \u0432\u044b\u044f\u0432\u043b\u0435\u043d\u043e."

    target_lookup: Dict[str, Dict[str, Any]] = {}
    for name, info in (dut_summary.get("target_map") or {}).items():
        param_key = (info.get("param") or "").strip().lower()
        if not param_key or param_key in target_lookup:
            continue
        entry_info = dict(info)
        entry_info.setdefault("name", name)
        if info.get("display"):
            entry_info.setdefault("display", info.get("display"))
        target_lookup[param_key] = entry_info

    sensor_lines: List[str] = []
    mapping_pairs: List[str] = []
    used_mapping: Set[str] = set()
    for meta in dut_sensors:
        marker = "[*]" if meta.is_primary else "[-]"
        param_label_raw = (meta.assigned_param or meta.param_hint or "").strip()
        param_label = param_label_raw or "n/a"
        param_key = param_label_raw.lower() if param_label_raw else ""
        entry_info = target_lookup.get(param_key, {})
        display_name = (
            entry_info.get("name")
            or entry_info.get("display")
            or meta.name
            or (f"ID {meta.id}" if meta.id is not None else "sensor")
        )
        line = f"{marker} {display_name} ({param_label})"
        if meta.is_zero_value:
            line += " [\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043f\u0440\u0438\u0431\u043b\u0438\u0436\u0435\u043d\u043e \u043a 0]"
        if meta.match_reason:
            line += f" [{meta.match_reason}]"
        sensor_lines.append(line)
        pair = f"{display_name} -> {param_label}"
        if pair not in used_mapping and param_key in target_lookup:
            mapping_pairs.append(pair)
            used_mapping.add(pair)

    mapping_text = ", ".join(mapping_pairs) if mapping_pairs else "\u043d\u0435\u0442 \u0441\u043e\u043e\u0442\u0432\u0435\u0442\u0441\u0442\u0432\u0438\u0439"
    mapping_block = f"\u041f\u043e\u0434\u0431\u043e\u0440 \u0434\u0430\u0442\u0447\u0438\u043a\u043e\u0432: {mapping_text}"
    if sensor_lines:
        sensors_block = "\u0414\u0430\u0442\u0447\u0438\u043a\u0438 \u0434\u043b\u044f \u0430\u043d\u0430\u043b\u0438\u0437\u0430:\n" + "\n".join(sensor_lines)
    else:
        sensors_block = "\u0414\u0430\u0442\u0447\u0438\u043a\u0438 \u0434\u043b\u044f \u0430\u043d\u0430\u043b\u0438\u0437\u0430: \u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445"

    parts: List[str] = [drains_text, mapping_block, sensors_block]
    final_message = "\n\n".join(part for part in parts if part)

    await _send_drain_message(update_or_q, context, final_message)

    jlog_fn = getattr(fuel_detector, "_jlog", None)
    if callable(jlog_fn):
        try:
            jlog_fn(
                stage="bot_output",
                unit_id=unit_id,
                period=period_key,
                message=final_message,
                drains=drains,
                dut_mapping=mapping_pairs,
            )
        except Exception as exc:
            log.debug("graph: bot_output jlog failed: %s", exc)

    drain_log.info("trace=%s message:%s", trace_id, final_message.replace("\n", " | "))

    graph_ctx = _graph_context(context)
    graph_ctx.update(
        {
            "last_period": period_key,
            "last_points": total_points,
        }
    )
    t1 = time.perf_counter()
    drain_log.info("trace=%s graph:done total=%.3fs", trace_id, t1 - t0)
    return True

def kb_more() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Внести ещё", callback_data="more:yes"),
             InlineKeyboardButton("✅ Завершено", callback_data="more:no")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back:value"),
             InlineKeyboardButton(CF_DUMP_BUTTON_TEXT, callback_data=CF_DUMP_CALLBACK),
             InlineKeyboardButton("❌ Отмена", callback_data="action:cancel")],
        ]
    )

def kb_search_prompt(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    return kb_cancel()

def _format_report_label(tpl: Dict[str, Any]) -> str:
    name = (tpl.get("name") or "").strip() or f"ID {tpl.get('template_id')}"
    resource = (tpl.get("resource_name") or "").strip()
    if resource:
        return f"{name} ({resource})"
    return name

def _normalize_report_offset(total: int, offset: int) -> int:
    if total <= 0:
        return 0
    if offset < 0:
        return 0
    if offset >= total:
        last_page = ((total - 1) // REPORT_PAGE_SIZE) * REPORT_PAGE_SIZE
        return max(last_page, 0)
    return offset


def _report_candidates_visible(
    candidates: List[Dict[str, Any]], offset: int
) -> Tuple[List[Tuple[int, Dict[str, Any]]], bool, Optional[int], int]:
    normalized_offset = _normalize_report_offset(len(candidates), offset)
    slice_items = candidates[normalized_offset: normalized_offset + REPORT_PAGE_SIZE]
    visible = list(enumerate(slice_items, start=normalized_offset))
    has_more = normalized_offset + REPORT_PAGE_SIZE < len(candidates)
    next_offset = normalized_offset + REPORT_PAGE_SIZE if has_more else None
    return visible, has_more, next_offset, normalized_offset


def kb_report_candidates(
    candidates: List[Tuple[int, Dict[str, Any]]],
    has_more: bool,
    next_offset: Optional[int],
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for idx, tpl in candidates:
        label = _format_report_label(tpl)
        rows.append([InlineKeyboardButton(label, callback_data=f"report:choose:{idx}")])
    if has_more and next_offset is not None:
        rows.append([InlineKeyboardButton("▶ Далее 10", callback_data=f"report:more:{next_offset}")])
    rows.append([InlineKeyboardButton("↩️ К карточке", callback_data="back:unit")])
    return InlineKeyboardMarkup(rows)


def _store_report_candidates(
    context: ContextTypes.DEFAULT_TYPE,
    candidates: List[Dict[str, Any]],
    text: str,
    offset: int = 0,
) -> InlineKeyboardMarkup:
    stored = [dict(tpl) for tpl in candidates]
    visible, has_more, next_offset, normalized_offset = _report_candidates_visible(stored, offset)
    context.user_data["report_matches"] = stored
    context.user_data["report_offset"] = normalized_offset
    context.user_data["report_candidates_text"] = text
    return kb_report_candidates(visible, has_more, next_offset)


def _report_template_sort_key(tpl: Dict[str, Any]) -> Tuple[str, str, int]:
    name = (tpl.get("name") or "").casefold()
    resource_name = (tpl.get("resource_name") or "").casefold()
    try:
        template_id = int(tpl.get("template_id") or 0)
    except Exception:
        template_id = 0
    return name, resource_name, template_id


def _filter_report_templates(
    templates: List[Dict[str, Any]], mask: str
) -> List[Dict[str, Any]]:
    q = (mask or "").strip()
    normalized = sorted(templates, key=_report_template_sort_key)
    if not q:
        return normalized

    q_cf = q.casefold()
    exact = [t for t in normalized if (t.get("name") or "").casefold().strip() == q_cf]
    if exact:
        return exact

    filtered = [t for t in normalized if q_cf in (t.get("name") or "").casefold()]
    return filtered


def _build_report_start_prompt(
    context: ContextTypes.DEFAULT_TYPE,
    templates: List[Dict[str, Any]],
    mask: str,
) -> Tuple[str, InlineKeyboardMarkup]:
    mask_val = (mask or "").strip()
    context.user_data["report_search_mask"] = mask_val
    filtered = _filter_report_templates(templates, mask_val)
    limited = filtered[:REPORT_START_LIST_SIZE]
    if limited:
        text = REPORT_START_PROMPT
    else:
        text = REPORT_START_EMPTY_PROMPT if mask_val else REPORT_START_PROMPT
    markup = _store_report_candidates(context, limited, text, offset=0)
    return text, markup


def _build_report_candidates_markup(
    context: ContextTypes.DEFAULT_TYPE, offset: Optional[int] = None
) -> InlineKeyboardMarkup:
    candidates = context.user_data.get("report_matches") or []
    try:
        current_offset = int(offset if offset is not None else context.user_data.get("report_offset", 0))
    except (TypeError, ValueError):
        current_offset = 0
    visible, has_more, next_offset, normalized_offset = _report_candidates_visible(candidates, current_offset)
    context.user_data["report_offset"] = normalized_offset
    return kb_report_candidates(visible, has_more, next_offset)

def kb_report_periods() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🟢 Сегодня", callback_data="report:period:today"),
                InlineKeyboardButton("🟡 Вчера", callback_data="report:period:yesterday"),
            ],
            [
                InlineKeyboardButton("🔵 Последние 7 дней", callback_data="report:period:7days"),
            ],
            [
                InlineKeyboardButton("🟣 Последний месяц", callback_data="report:period:month"),
            ],
            [
                InlineKeyboardButton("🔁 Другой шаблон", callback_data="report:change"),
                InlineKeyboardButton("↩️ К карточке", callback_data="back:unit"),
            ],
        ]
    )

def kb_report_formats() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📄 PDF", callback_data="report:format:pdf"),
                InlineKeyboardButton("📊 Excel (XLSX)", callback_data="report:format:excel"),
            ],
            [
                InlineKeyboardButton("⬅️ Назад", callback_data="report:back:period"),
                InlineKeyboardButton("↩️ К карточке", callback_data="back:unit"),
            ],
        ]
    )


def describe_report_format(fmt: str) -> str:
    fmt_cf = (fmt or "").strip().lower()
    if fmt_cf == "pdf":
        return "PDF"
    if fmt_cf in ("excel", "xlsx"):
        return "Excel (XLSX)"
    if fmt_cf == "xls":
        return "Excel (XLS)"
    return fmt.upper() if fmt else "Неизвестный формат"

def kb_report_wait() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="report:cancel")]])


def kb_report_finish(has_unit: bool) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if has_unit:
        rows.append([InlineKeyboardButton("↩️ К карточке", callback_data="back:unit")])
    rows.append([InlineKeyboardButton("🔍 Найти новый объект", callback_data="back:find")])
    return InlineKeyboardMarkup(rows)

PARAM_PROMPT_TEXT = (
    "🔎 Выберите параметр.\n"
    "Введите минимум 2 символа или воспользуйтесь списком ниже."
)

def build_param_prompt_keyboard(matches: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if matches:
        chunk_size = 10
        columns: List[Tuple[int, List[str]]] = []
        for start in range(0, len(matches), chunk_size):
            columns.append((start, matches[start:start + chunk_size]))

        max_rows = max((len(col) for _, col in columns), default=0)
        for row_idx in range(max_rows):
            row_buttons: List[InlineKeyboardButton] = []
            for start_idx, col in columns:
                if row_idx >= len(col):
                    continue
                name = col[row_idx]
                global_idx = start_idx + row_idx
                row_buttons.append(
                    InlineKeyboardButton(name, callback_data=f"param:show:{global_idx}")
                )
            if row_buttons:
                rows.append(row_buttons)

        rows.append([
            InlineKeyboardButton("📥 Выгрузить найденные параметры", callback_data="param:download")
        ])
    rows.append([InlineKeyboardButton("📋 Показать все параметры", callback_data="param:show_all")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="param:back_stats")])
    return InlineKeyboardMarkup(rows)

def kb_param_results() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔍 Объект", callback_data="param:find_other")],
            [InlineKeyboardButton("📡 Команда", callback_data="param:cmd")],
            [
                InlineKeyboardButton("⬅️ Назад", callback_data="param:back_query"),
                InlineKeyboardButton("✅ Готово", callback_data="param:finish"),
            ],
        ]
    )

async def delete_param_result_message(context: ContextTypes.DEFAULT_TYPE) -> None:
    meta = context.user_data.pop("param_result_msg", None)
    if not meta:
        return
    try:
        await context.bot.delete_message(chat_id=meta["chat_id"], message_id=meta["message_id"])
    except Exception as e:
        log.debug(f"delete_param_result_message: {e}")

def store_param_result_payload(context: ContextTypes.DEFAULT_TYPE, text: str,
                               parse_mode: Optional[str]) -> None:
    context.user_data["param_result_payload"] = {"text": text, "parse_mode": parse_mode}

async def remove_reply_markup_safe(message) -> None:
    if not message:
        return
    try:
        await message.edit_reply_markup(reply_markup=None)
    except Exception as e:
        log.debug(f"remove_reply_markup_safe: {e}")


async def remove_reply_markup_by_meta(context: ContextTypes.DEFAULT_TYPE, meta: Optional[Dict[str, Any]]) -> None:
    if not isinstance(meta, dict):
        return
    chat_id = meta.get("chat_id")
    message_id = meta.get("message_id")
    if not isinstance(chat_id, int) or not isinstance(message_id, int):
        return
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=None,
        )
    except Exception as e:
        log.debug(f"remove_reply_markup_by_meta: {e}")

async def restore_params_after_cmd_return(update_or_q, context: ContextTypes.DEFAULT_TYPE) -> int:
    snapshot = context.user_data.pop("cmd_return_runtime", None) or {}

    matches_snapshot = snapshot.get("matches")
    if matches_snapshot is not None:
        context.user_data["param_current_matches"] = list(matches_snapshot)
    else:
        context.user_data.pop("param_current_matches", None)

    last_query = snapshot.get("last_query")
    if last_query is not None:
        context.user_data["param_last_query"] = last_query
    else:
        context.user_data.pop("param_last_query", None)

    last_note = snapshot.get("last_note")
    if last_note is not None:
        context.user_data["param_last_note"] = last_note
    else:
        context.user_data.pop("param_last_note", None)

    payload = context.user_data.pop("cmd_return_payload", None)
    context.user_data.pop("cmd_return", None)

    if isinstance(payload, dict) and payload.get("text") is not None:
        text = payload.get("text", "")
        parse_mode = payload.get("parse_mode")
        if update_or_q and getattr(update_or_q, "message", None):
            msg = await update_or_q.message.reply_text(
                text,
                reply_markup=kb_param_results(),
                parse_mode=parse_mode,
            )
            context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
            context.user_data["param_result_payload"] = dict(payload)
        context.user_data["mode"] = MODE_PARAMS
        return STATE_PARAM_QUERY

    matches = context.user_data.get("param_current_matches") or _ordered_param_names(context)
    note = context.user_data.get("param_last_note") or None
    await send_param_prompt(update_or_q, context, matches, note)
    return STATE_PARAM_QUERY

def _ordered_param_names(context: ContextTypes.DEFAULT_TYPE) -> List[str]:
    params_map: Dict[str, str] = context.user_data.get("params_map") or {}
    order: List[str] = context.user_data.get("params_order") or []
    seen = set()
    result: List[str] = []
    for name in order:
        if name in params_map and name not in seen:
            result.append(name)
            seen.add(name)
    for name in params_map.keys():
        if name not in seen:
            result.append(name)
            seen.add(name)
    return result

def filter_params(context: ContextTypes.DEFAULT_TYPE, query: str) -> List[str]:
    params_map: Dict[str, str] = context.user_data.get("params_map") or {}
    if not params_map:
        return []
    query_cf = query.casefold()
    names = _ordered_param_names(context)
    return [name for name in names if query_cf in name.casefold()]

def build_params_text(context: ContextTypes.DEFAULT_TYPE, names: List[str]) -> Tuple[str, Optional[str]]:
    params_map: Dict[str, str] = context.user_data.get("params_map") or {}
    if not names:
        return "❌ Параметр не найден", None

    limited_names = names[:40]
    entries_raw = [f"{name} = {params_map.get(name, '')}" for name in limited_names]
    truncated = len(names) > len(limited_names)

    lines = [html.escape(entry) for entry in entries_raw]
    pre_block = "<pre>" + "\n".join(lines) + "</pre>"
    if truncated:
        pre_block = f"{pre_block}\n\n… Показаны первые 40 параметров из {len(names)}."

    return pre_block, "HTML"

def clear_param_runtime(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in ("param_current_matches", "param_last_query", "param_last_note"):
        context.user_data.pop(key, None)

def clear_report_context(context: ContextTypes.DEFAULT_TYPE, cancel_job: bool = False) -> None:
    if cancel_job:
        job = context.user_data.pop("report_job", None)
        if job and isinstance(job, dict):
            event = job.get("cancel_event")
            if isinstance(event, threading.Event):
                event.set()
            job["final_message_sent"] = True
    keys = [
        "report_selected_template",
        "report_period",
        "report_format",
        "report_matches",
        "report_offset",
        "report_candidates_text",
        "report_templates",
        "report_last_query",
        "report_search_mask",
        "report_unit_id",
        "report_unit_meta",
    ]
    for key in keys:
        context.user_data.pop(key, None)
    if context.user_data.get("mode") == MODE_REPORT:
        context.user_data["mode"] = MODE_NONE

async def send_param_prompt(update_or_q, context: ContextTypes.DEFAULT_TYPE,
                            matches: List[str], note: Optional[str] = None) -> None:
    text = PARAM_PROMPT_TEXT
    if note:
        text = f"{text}\n\n{note}"
    markup = build_param_prompt_keyboard(matches)

    await delete_anchor(context)
    chat_id = None
    if isinstance(update_or_q, Update) and update_or_q.message:
        chat_id = update_or_q.message.chat_id
    elif hasattr(update_or_q, "message") and update_or_q.message:
        chat_id = update_or_q.message.chat_id
    if chat_id is None:
        return
    msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=markup, parse_mode=None)
    await set_anchor_on(msg, context)
    context.user_data["param_current_matches"] = list(matches)
    context.user_data["param_last_note"] = note or ""
    context.user_data["mode"] = MODE_PARAMS

async def update_param_prompt(context: ContextTypes.DEFAULT_TYPE, matches: List[str],
                              note: Optional[str] = None, update_obj=None) -> None:
    text = PARAM_PROMPT_TEXT
    if note:
        text = f"{text}\n\n{note}"
    markup = build_param_prompt_keyboard(matches)
    if "anchor" not in context.user_data:
        if update_obj is not None:
            await send_param_prompt(update_obj, context, matches, note)
        return
    await edit_anchor(context, text, markup, parse_mode=None)
    context.user_data["param_current_matches"] = list(matches)
    context.user_data["param_last_note"] = note or ""
    context.user_data["mode"] = MODE_PARAMS


# =================== HELPERS ===================
async def set_anchor_on(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["anchor"] = {"chat_id": message.chat_id, "message_id": message.message_id}

async def delete_anchor(context: ContextTypes.DEFAULT_TYPE) -> None:
    anchor = context.user_data.get("anchor")
    if not anchor:
        return
    try:
        await context.bot.delete_message(chat_id=anchor["chat_id"], message_id=anchor["message_id"])
    except Exception as e:
        log.debug(f"delete_anchor: {e}")
    finally:
        context.user_data.pop("anchor", None)

async def edit_anchor(context: ContextTypes.DEFAULT_TYPE, text: str,
                      markup: Optional[InlineKeyboardMarkup] = None,
                      parse_mode: Optional[str] = "Markdown") -> None:
    anchor = context.user_data.get("anchor")
    if not anchor:
        return
    try:
        await context.bot.edit_message_text(
            chat_id=anchor["chat_id"],
            message_id=anchor["message_id"],
            text=text,
            parse_mode=parse_mode,
            reply_markup=markup,
        )
    except Exception as e:
        log.debug(f"edit_anchor failed: {e}")

async def send_new_anchor_below(update: Update, text: str,
                                markup: Optional[InlineKeyboardMarkup],
                                context: ContextTypes.DEFAULT_TYPE,
                                parse_mode: Optional[str] = "Markdown") -> None:
    await delete_anchor(context)
    msg = await update.message.reply_text(text, parse_mode=parse_mode, reply_markup=markup)
    await set_anchor_on(msg, context)


def _get_export_anchor(context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, int]]:
    anchor = context.user_data.get("export_anchor")
    if not isinstance(anchor, dict):
        return None
    chat_id = anchor.get("chat_id")
    message_id = anchor.get("message_id")
    if not isinstance(chat_id, int) or not isinstance(message_id, int):
        return None
    return {"chat_id": chat_id, "message_id": message_id}


async def _set_export_anchor_on(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["export_anchor"] = {
        "chat_id": getattr(message, "chat_id", None),
        "message_id": getattr(message, "message_id", None),
    }


async def delete_export_anchor(context: ContextTypes.DEFAULT_TYPE) -> None:
    anchor = _get_export_anchor(context)
    if not anchor:
        return
    try:
        await context.bot.delete_message(
            chat_id=anchor["chat_id"],
            message_id=anchor["message_id"],
        )
    except Exception as exc:
        log.debug("delete_export_anchor failed: %s", exc)
    finally:
        context.user_data.pop("export_anchor", None)


async def _edit_export_anchor(
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = None,
) -> bool:
    anchor = _get_export_anchor(context)
    if not anchor:
        return False
    try:
        await context.bot.edit_message_text(
            chat_id=anchor["chat_id"],
            message_id=anchor["message_id"],
            text=text,
            parse_mode=parse_mode,
            reply_markup=markup,
        )
        return True
    except BadRequest as exc:
        message = str(exc)
        if "message is not modified" in message.lower():
            return True
        log.debug("edit_export_anchor bad request: %s", exc)
        context.user_data.pop("export_anchor", None)
        return False
    except Exception as exc:
        log.debug("edit_export_anchor failed: %s", exc)
        context.user_data.pop("export_anchor", None)
        return False


async def _render_export_panel(
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    markup: Optional[InlineKeyboardMarkup],
    *,
    parse_mode: Optional[str] = None,
    update_or_q: Optional[Any] = None,
    chat_id: Optional[int] = None,
) -> None:
    if await _edit_export_anchor(context, text, markup=markup, parse_mode=parse_mode):
        return

    target_message = None

    if update_or_q is not None:
        if isinstance(update_or_q, Update):
            message = update_or_q.effective_message
            if message is not None:
                target_message = message
                chat_id = message.chat_id
            elif update_or_q.effective_chat is not None:
                chat_id = update_or_q.effective_chat.id
        else:
            message = getattr(update_or_q, "message", None)
            if message is not None:
                target_message = message
                chat_id = message.chat_id
            else:
                chat = getattr(update_or_q, "effective_chat", None)
                if chat is not None:
                    chat_id = chat.id

    if target_message is not None:
        msg = await target_message.reply_text(
            text,
            reply_markup=markup,
            parse_mode=parse_mode,
        )
    else:
        if chat_id is None:
            anchor = _get_export_anchor(context)
            if anchor:
                chat_id = anchor.get("chat_id")
        if chat_id is None:
            return
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=markup,
            parse_mode=parse_mode,
        )

    await _set_export_anchor_on(msg, context)

def _fmt_ts_utc(ts: Optional[int]) -> str:
    if not ts:
        return "—"
    dt_local = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone()
    return dt_local.strftime("%Y-%m-%d %H:%M")


def _fmt_ts_full(ts: Optional[int]) -> str:
    if not ts:
        return "\u043d\u0435\u0442 \u0434\u0430\043d\u043d\u044b\u0445"
    dt_local = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone()
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")



def _fmt_duration(seconds: Optional[int]) -> str:
    if seconds is None:
        return "\u043d\u0435\u0442 \u0434\u0430\043d\043d\u044b\0445"
    seconds = max(0, int(seconds))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    parts: List[str] = []
    if hours:
        parts.append(f"{hours}\u0447")
    if minutes:
        parts.append(f"{minutes}\u043c\u0438\043d")
    if sec or not parts:
        parts.append(f"{sec}\u0441")
    return " ".join(parts)



def _fmt_liters(value: Optional[float]) -> str:
    if value is None:
        return "\u043d\u0435\u0442 \u0434\u0430\043d\043d\u044b\0445"
    return f"{value:.1f} \u043b"



def _extract_address_from_pos(pos: Dict[str, Any]) -> Optional[str]:
    if not isinstance(pos, dict):
        return None
    for key in ("addr", "address", "ad", "n", "nm", "txt", "dsc"):
        value = pos.get(key)
        if isinstance(value, str):
            value = value.strip()
            if value:
                return value
    return None


def _format_coords(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    if lat is None or lon is None:
        return None
    try:
        return f"{float(lat):.5f}, {float(lon):.5f}"
    except Exception:
        return None


def _build_map_url(address: Optional[str] = None, lat: Optional[float] = None, lon: Optional[float] = None) -> Optional[str]:
    if lat is not None and lon is not None:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            point_query = quote_plus(f"{lat_f:.6f}, {lon_f:.6f}")
            return f"https://yandex.ru/maps/?mode=search&text={point_query}"
        except Exception:
            pass
    if address:
        query = quote_plus(address.strip())
        if query:
            return f"https://yandex.ru/maps/?mode=search&text={query}"
    return None


def _build_yandex_static_map_url(
    lat: Optional[float],
    lon: Optional[float],
    *,
    zoom: int = YANDEX_STATIC_MAP_ZOOM,
    size: Tuple[int, int] = YANDEX_STATIC_MAP_SIZE,
) -> Optional[str]:
    if lat is None or lon is None:
        return None
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except Exception:
        return None
    if not (math.isfinite(lat_f) and math.isfinite(lon_f)):
        return None
    try:
        zoom_int = int(zoom)
    except Exception:
        zoom_int = YANDEX_STATIC_MAP_ZOOM
    width, height = size
    try:
        width_int = max(1, min(int(width), 650))
        height_int = max(1, min(int(height), 450))
    except Exception:
        width_int, height_int = YANDEX_STATIC_MAP_SIZE
    ll = f"{lon_f:.6f},{lat_f:.6f}"
    return (
        "https://static-maps.yandex.ru/1.x/?"
        f"ll={ll}&z={zoom_int}&l=map&size={width_int},{height_int}&pt={ll},pm2rdm"
    )


def _download_yandex_static_map(lat: Optional[float], lon: Optional[float]) -> Optional[bytes]:
    url = _build_yandex_static_map_url(lat, lon)
    if not url:
        return None
    try:
        response = requests.get(url, timeout=(HTTP_TIMEOUT_CONN, HTTP_TIMEOUT_READ))
        response.raise_for_status()
    except Exception as exc:
        log.debug("yandex static map fetch failed: %s", exc)
        return None
    content = response.content
    if not content:
        return None
    return content


def _format_line_with_link(line: str, link_text: str, url: str) -> str:
    safe_url = html.escape(url, quote=True)
    safe_text = html.escape(link_text)
    prefix, sep, _ = line.partition(":")
    if sep:
        prefix_html = html.escape(prefix + sep)
        return f"{prefix_html} <a href=\"{safe_url}\">{safe_text}</a>"
    return f"{html.escape(line)} <a href=\"{safe_url}\">{safe_text}</a>"


def _format_lines_with_links(lines: List[str], links: Dict[int, Tuple[str, str]]) -> str:
    formatted: List[str] = []

    def _format_value(value_text: str, link: Optional[Tuple[str, str]]) -> str:
        clean_value = value_text.lstrip() or "—"
        safe_value = html.escape(clean_value)
        value_core = f"<i>{safe_value}</i>"
        if link and all(link):
            safe_url = html.escape(link[1], quote=True)
            return f"<a href=\"{safe_url}\">{value_core}</a>"
        return value_core

    for idx, line in enumerate(lines):
        link = links.get(idx)
        prefix, sep, suffix = line.partition(":")
        if sep:
            label = prefix.strip()
            label_html = html.escape(label)
            value_html = _format_value(suffix, link)
            formatted.append(f"<b>{label_html}{sep}</b> {value_html}")
        else:
            if link and all(link):
                formatted.append(_format_line_with_link(line, link[0], link[1]))
            else:
                formatted.append(html.escape(line))

    return "\n".join(formatted)


def _haversine_km(lat1, lon1, lat2, lon2) -> Optional[float]:
    try:
        import math

        if None in (lat1, lon1, lat2, lon2):
            return None
        φ1, λ1, φ2, λ2 = map(math.radians, (float(lat1), float(lon1), float(lat2), float(lon2)))
        dφ = φ2 - φ1
        dλ = λ2 - λ1
        a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return 6371.0 * c
    except Exception:
        return None


def _fmt_km(d: Optional[float]) -> str:
    if d is None:
        return "—"
    from decimal import Decimal, ROUND_HALF_UP

    q = Decimal(str(d)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP).normalize()
    s = format(q, "f").rstrip("0").rstrip(".")
    return (s or "0") + " км"





def _fmt_distance_m(distance_m: Optional[float]) -> str:
    if distance_m is None:
        return "-"
    try:
        value = float(distance_m)
    except (TypeError, ValueError):
        return "-"
    if value < 1000:
        return f"{int(round(value))} м"
    return _fmt_km(value / 1000.0)


def build_report_interval(period_key: str) -> Dict[str, Any]:
    now = datetime.now()
    key = (period_key or "").strip().lower()
    if key == "today":
        start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = now
        label = "Сегодня"
    elif key == "yesterday":
        base = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = base
        end_dt = base + timedelta(days=1)
        label = "Вчера"
    elif key in ("7days", "last7", "week"):
        start_dt = now - timedelta(days=7)
        end_dt = now
        label = "Последние 7 дней"
    elif key in ("month", "30days", "last30"):
        start_dt = now - timedelta(days=30)
        end_dt = now
        label = "Последний месяц"
    else:
        raise ValueError("Неизвестный период отчёта")

    from_ts = int(start_dt.timestamp())
    to_ts = int(end_dt.timestamp())
    if to_ts <= from_ts:
        to_ts = from_ts + 1
    return {"from": from_ts, "to": to_ts, "label": label}


def _parse_custom_graph_day(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return None

    compact = raw.replace(" ", "")
    match = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", compact)

    year: Optional[int] = None
    month: Optional[int] = None
    day: Optional[int] = None

    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year_raw = match.group(3)
        if year_raw is not None:
            year = int(year_raw)
            if year < 100:
                year += 2000
    else:
        normalized = re.sub(r"\s+", " ", raw).strip().casefold()
        match_words = re.fullmatch(r"(\d{1,2})\s+([а-яё\.]+)(?:\s+(\d{2,4}))?", normalized)
        if match_words:
            day = int(match_words.group(1))
            month_token = match_words.group(2).strip(". ")
            month = MONTH_NAME_TO_NUM.get(month_token)
            if month is None and month_token.endswith("е"):
                month = MONTH_NAME_TO_NUM.get(month_token[:-1])
            year_raw = match_words.group(3)
            if year_raw is not None:
                year = int(year_raw)
                if year < 100:
                    year += 2000

    if day is None or month is None:
        return None

    if year is None:
        year = datetime.now(MOSCOW_TZ).year

    try:
        start_dt = datetime(year, month, day, 0, 0, 0, tzinfo=MOSCOW_TZ)
    except ValueError:
        return None

    end_dt = start_dt + timedelta(days=1)
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    if end_ts <= start_ts:
        end_ts = start_ts + 86400

    label = start_dt.strftime("%d.%m.%Y")
    return {"from": start_ts, "to": end_ts, "label": label, "key": "custom"}

def _human_ago(from_ts: Optional[int]) -> Tuple[str, str]:
    if not from_ts:
        return "⚪", "неизвестно"
    now = datetime.now(timezone.utc)
    dt = datetime.fromtimestamp(int(from_ts), tz=timezone.utc)
    delta = now - dt
    mins = int(delta.total_seconds() // 60)
    if mins < 30:
        dot = "🟢"
    elif mins < 24 * 60:
        dot = "🟡"
    else:
        dot = "🔴"
    if mins < 60:
        txt = f"{mins} мин"
    elif mins < 24 * 60:
        hours = mins // 60
        txt = f"{hours} ч"
    else:
        days = mins // (24 * 60)
        txt = f"{days} дн"
    return dot, txt

def _is_online(last_ts: Optional[int]) -> bool:
    if not last_ts:
        return False
    now = datetime.now(timezone.utc)
    dt = datetime.fromtimestamp(int(last_ts), tz=timezone.utc)
    return (now - dt) <= timedelta(seconds=ONLINE_THRESHOLD_SEC)

async def clear_stats_buttons(context: ContextTypes.DEFAULT_TYPE) -> None:
    smeta = context.user_data.get("stats_msg")
    if not smeta:
        return
    try:
        await context.bot.edit_message_reply_markup(chat_id=smeta["chat_id"], message_id=smeta["message_id"], reply_markup=None)
    except Exception as e:
        log.debug(f"clear_stats_buttons: {e}")


async def restore_stats_buttons(context: ContextTypes.DEFAULT_TYPE) -> None:
    smeta = context.user_data.get("stats_msg")
    if not smeta:
        return
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=smeta["chat_id"],
            message_id=smeta["message_id"],
            reply_markup=kb_stats_actions_with_refresh(),
        )
    except Exception as e:
        log.debug(f"restore_stats_buttons: {e}")


def _collect_dut_sensors(
    unit_item: Dict[str, Any]
) -> Tuple[List[SensorMeta], Dict[str, Any]]:
    """Возвращает отфильтрованные ДУТы и сводку для логирования."""

    sens_map = (unit_item or {}).get("sens")
    total_checked = 0
    if isinstance(sens_map, dict):
        total_checked = sum(1 for value in sens_map.values() if isinstance(value, dict))

    candidates = list_dut_candidates(unit_item or {})
    primary = select_primary_dut(candidates)
    primary_id = primary.id if primary else None
    if primary:
        for meta in candidates:
            meta.is_primary = meta.id == primary_id

    zero_info = [meta.as_dict() for meta in candidates if meta.is_zero_value]
    rejections = [r.as_dict() for r in dut_filter_explain(unit_item or {})]

    summary = {
        "stage": "dut_filter",
        "checked": total_checked,
        "kept": len(candidates),
        "candidates": [meta.as_dict() for meta in candidates],
        "primary": primary.as_dict() if primary else None,
        "zeros": zero_info,
        "rejected": rejections,
    }

    return candidates, summary

def _prepare_dut_for_analysis(
    unit_item: Dict[str, Any]
) -> Tuple[List[SensorMeta], Dict[str, Any], List[SensorMeta]]:
    """Готовит список ДУТ для анализа сливов с учётом параметров и дублей."""

    sens_map = (unit_item or {}).get("sens") or {}
    total_checked = 0
    if isinstance(sens_map, dict):
        total_checked = sum(1 for value in sens_map.values() if isinstance(value, dict))

    filtered_text, filtered_entries = cached_filtered_custom_sensors(unit_item or {})
    target_map = resolve_target_duts(unit_item or {}, (unit_item or {}).get("sens"))

    target_params: Dict[str, Dict[str, Any]] = {}
    filtered_entries_dicts: List[Dict[str, Any]] = []
    for entry in filtered_entries:
        entry_dict = {
            "name": entry.name,
            "param": entry.param,
            "display": entry.display,
            "legacy": entry.legacy,
            "strict_custom": entry.strict_custom,
            "value_text": entry.value_text,
            "value_numeric": entry.value_numeric,
            "param_present": entry.param_present,
        }
        filtered_entries_dicts.append(entry_dict)
    for dut_name, info in target_map.items():
        param = (info.get("param") or "").strip()
        if not param:
            continue
        key = param.lower()
        if key in target_params:
            continue
        entry_dict = {
            "name": dut_name,
            "param": param,
            "display": info.get("display") or dut_name,
            "legacy": info.get("legacy", False),
            "strict_custom": info.get("strict_custom", False),
            "value_text": str(info.get("value_numeric")) if info.get("value_numeric") is not None else "_",
            "value_numeric": info.get("value_numeric"),
            "param_present": True,
        }
        target_params[key] = entry_dict

    all_candidates = list_dut_candidates(unit_item or {})

    param_to_meta: Dict[str, List[SensorMeta]] = {}
    for meta in all_candidates:
        key = (meta.assigned_param or meta.param_hint or "").strip().lower()
        if not key:
            continue
        param_to_meta.setdefault(key, []).append(meta)

    chosen: List[SensorMeta] = []
    used_params: set[str] = set()

    for param_key, entry_info in target_params.items():
        candidates_for_param = param_to_meta.get(param_key, [])
        if not candidates_for_param:
            continue
        selected = next((meta for meta in candidates_for_param if not meta.is_zero_value), None)
        if selected is None:
            selected = candidates_for_param[0]
        if selected in chosen:
            continue
        used_params.add(param_key)
        chosen.append(selected)

    if not chosen:
        fallback_pool = [
            meta
            for meta in all_candidates
            if (meta.assigned_param or meta.param_hint) and not meta.is_zero_value
        ]
        fallback_pool.sort(
            key=lambda m: (m.name or m.assigned_param or str(m.id or "")).casefold()
        )
        for meta in fallback_pool:
            key = (meta.assigned_param or meta.param_hint or "").strip().lower()
            if not key or key in used_params:
                continue
            used_params.add(key)
            chosen.append(meta)
        if not chosen and all_candidates:
            chosen.append(all_candidates[0])

    chosen.sort(key=lambda m: (m.name or m.assigned_param or str(m.id or "")).casefold())

    primary = select_primary_dut(chosen) if chosen else None
    if primary:
        for meta in chosen:
            meta.is_primary = meta is primary

    zero_info = [meta.as_dict() for meta in all_candidates if meta.is_zero_value]
    rejections = [r.as_dict() for r in dut_filter_explain(unit_item or {})]

    summary = {
        "stage": "dut_filter",
        "checked": total_checked,
        "kept": len(chosen),
        "candidates": [meta.as_dict() for meta in chosen],
        "primary": primary.as_dict() if primary else None,
        "zeros": zero_info,
        "rejected": rejections,
        "all_candidates": [meta.as_dict() for meta in all_candidates],
        "filtered_text": filtered_text,
        "filtered_entries": filtered_entries_dicts,
        "target_params": sorted(target_params.keys()),
        "target_map": target_map,
        "selected_params": [
            (meta.assigned_param or meta.param_hint or "").strip().lower()
            for meta in chosen
            if (meta.assigned_param or meta.param_hint)
        ],
    }

    try:
        log.info("dut_select %s", json.dumps(summary, ensure_ascii=False))
    except Exception:
        log.info("dut_select %s", summary)

    jlog_fn = getattr(fuel_detector, "_jlog", None)
    if callable(jlog_fn):
        try:
            jlog_fn(stage="dut_select", summary=summary, chosen=[meta.as_dict() for meta in chosen])
        except Exception as exc:
            log.debug("dut_select jlog failed: %s", exc)

    return chosen, summary, all_candidates












async def delete_stats_map_message(context: ContextTypes.DEFAULT_TYPE) -> None:
    meta = context.user_data.pop("stats_map_msg", None)
    if not meta:
        return
    try:
        await context.bot.delete_message(chat_id=meta["chat_id"], message_id=meta["message_id"])
    except Exception as e:
        log.debug(f"delete_stats_map_message: {e}")

async def delete_stats_message(context: ContextTypes.DEFAULT_TYPE) -> None:
    await delete_stats_map_message(context)
    smeta = context.user_data.pop("stats_msg", None)
    if not smeta:
        return
    try:
        await context.bot.delete_message(chat_id=smeta["chat_id"], message_id=smeta["message_id"])
    except Exception as e:
        log.debug(f"delete_stats_message: {e}")

def _build_dut_lines2(
    client: "WialonClient",
    unit_item: Dict[str, Any],
    unit_id: int,
    *,
    precalc_main_value: Optional[float] = None,
    allow_calc: bool = True,
    rpc_stats: Optional[Dict[str, int]] = None,
) -> List[str]:
    """Строит строки по топливу: главное значение → диагностика ДУТn."""
    sens_map = (unit_item or {}).get("sens") or {}
    sensors = [v for v in sens_map.values() if isinstance(v, dict)]

    dut_candidates, _ = _collect_dut_sensors(unit_item or {})
    main_meta = next((meta for meta in dut_candidates if meta.is_primary), None)
    if main_meta is None and dut_candidates:
        main_meta = next((meta for meta in dut_candidates if not meta.is_zero_value), dut_candidates[0])

    main_sensor = main_meta.raw_sensor if isinstance(main_meta, SensorMeta) else None
    main_value: Optional[float] = None
    main_raw_value: Optional[float] = None
    value_source = "none"
    has_any_dut = bool(dut_candidates)

    if isinstance(main_sensor, dict):
        main_name = (main_sensor.get("n") or "").strip()
        if main_name and RE_ANY_DUT.match(main_name):
            has_any_dut = True

    def _safe_float(value: Any) -> Optional[float]:
        return normalize_fuel_value(value)

    def _is_plausible_fuel_value(val: Any) -> bool:
        fv = _safe_float(val)
        return fv is not None and (FUEL_MIN_VALID_L <= fv <= FUEL_MAX_VALID_L)

    def _consider_main_value(source: str, value: Any) -> bool:
        nonlocal main_value, main_raw_value, value_source
        fv = _safe_float(value)
        if fv is None:
            return False
        main_raw_value = fv
        value_source = source
        if _is_plausible_fuel_value(fv):
            main_value = fv
            return True
        return False

    precalc_used = False
    if precalc_main_value is not None:
        precalc_used = True
        _consider_main_value("precalc", precalc_main_value)

    if isinstance(main_meta, SensorMeta) and main_meta.value is not None:
        _consider_main_value("meta", main_meta.value)

    if isinstance(main_sensor, dict) and main_value is None:
        for key in ("last_value", "v", "value"):
            if _consider_main_value("cache", main_sensor.get(key)):
                break
        sid_raw = main_sensor.get("id")
        sid: Optional[int]
        try:
            sid = int(sid_raw)
        except Exception:
            sid = None
        if (
            main_value is None
            and sid is not None
            and not precalc_used
            and allow_calc
        ):
            try:
                if rpc_stats is not None:
                    rpc_stats["calc_single"] = rpc_stats.get("calc_single", 0) + 1
                calc_value = client.calc_sensor_value(unit_id, sid)
            except Exception as exc:
                log.debug("fuel: calc main sensor failed id=%s: %s", sid, exc)
            else:
                _consider_main_value("calc", calc_value)

    def _log_and_return(lines: List[str], failures: List[Tuple[Any, str, Optional[float], Optional[str]]]) -> List[str]:
        ms_id = main_sensor.get("id") if isinstance(main_sensor, dict) else None
        ms_name = (main_sensor.get("n") or "").strip() if isinstance(main_sensor, dict) else None
        ms_type = main_sensor.get("t") if isinstance(main_sensor, dict) else None
        log.debug(
            "fuel: main_sensor id=%s name=%r type=%r; value_source=%s; value=%s; failures=%s",
            ms_id,
            ms_name or None,
            ms_type,
            value_source,
            main_raw_value,
            failures,
        )
        return lines

    main_lines: List[str] = []
    if main_value is not None:
        formatted = format_liters(main_value)
        if formatted is not None:
            suffix = "" if formatted.endswith("л") else " л"
            main_lines.append(f"• ДУТ (уровень топлива): {formatted}{suffix}")

    failures: List[Tuple[Any, str, Optional[float], Optional[str]]] = []
    failure_lines: List[str] = []
    for sensor in sensors:
        name = (sensor.get("n") or "").strip()
        if not name:
            continue
        if not RE_ANY_DUT.match(name):
            continue
        has_any_dut = True
        try:
            sid = int(sensor.get("id"))
        except Exception:
            sid = None
        fv: Optional[float] = None
        for key in ("last_value", "v", "value"):
            if key in sensor:
                candidate = _safe_float(sensor.get(key))
                if candidate is not None:
                    fv = candidate
                    break
        if fv is None:
            continue
        if fv < 1.0:
            hint: Optional[str] = None
            try:
                hint = extract_param_hint_from_sensor(sensor)
            except Exception:
                hint = None
            if not hint:
                try:
                    hint = extract_assigned_param_name(sensor)
                except Exception:
                    hint = None
            suffix = f" по параметру {hint}" if hint else ""
            failure_lines.append(f"• {name} — не работает{suffix}")
            failures.append((sensor.get("id"), name, fv, hint))

    if has_any_dut and main_value is None:
        missing_line = "• нет последнего значения (кэш пуст)"
        if missing_line not in failure_lines:
            failure_lines.append(missing_line)
            failures.append((None, "cache-miss", None, None))

    if failure_lines:
        combined: List[str] = []
        if main_lines:
            combined.extend(main_lines)
        combined.extend(failure_lines)
        return _log_and_return(combined if combined else failure_lines, failures)

    if main_lines:
        return _log_and_return(main_lines, failures)

    if not has_any_dut:
        return _log_and_return(["• ДУТ не установлен"], failures)

    return _log_and_return([], failures)


def _dut_problem_descriptions(lines: List[str]) -> List[str]:
    issues: List[str] = []
    seen: Set[str] = set()
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        if text.startswith("•"):
            text = text[1:].strip()
        lowered = text.casefold()
        if "не установлен" in lowered:
            normalized = "ДУТ не установлен"
            if normalized not in seen:
                seen.add(normalized)
                issues.append(normalized)
            continue
        if "не работает" in lowered:
            if text not in seen:
                seen.add(text)
                issues.append(text)
            continue
        if "нет последнего значения" in lowered:
            normalized = "нет последнего значения (кэш пуст)"
            if normalized not in seen:
                seen.add(normalized)
                issues.append(normalized)
    return issues


def _clear_export_job(context: ContextTypes.DEFAULT_TYPE, *, cancel: bool = False) -> None:
    job = context.user_data.pop(EXPORT_JOB_KEY, None)
    if not cancel:
        return
    if isinstance(job, dict):
        cancel_event = job.get("cancel_event")
        if hasattr(cancel_event, "set"):
            try:
                cancel_event.set()
            except Exception:
                pass


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await delete_anchor(context)
    await delete_stats_message(context)
    _clear_export_job(context, cancel=True)
    await delete_export_anchor(context)
    context.user_data.clear()
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN

    await _activate_objects_ui(context)
    _set_objects_mode(context, OBJECTS_MODE_LIST)
    _reset_search_context(context)

    chat = update.effective_chat
    chat_id = chat.id if chat else None
    greeting_text = (
        "Привет! Я помогу найти объект, показать его карточку и работать с параметрами."
    )
    await update.message.reply_text(
        greeting_text,
        reply_markup=reply_menu(chat_id=chat_id),
    )
    await send_new_anchor_below(
        update,
        "Введите имя/госномер для поиска…",
        kb_search_prompt(context),
        context,
        parse_mode=None,
    )
    return STATE_FIND_QUERY

# ---- Глобальный рестарт по нижней кнопке ----
async def _start_search_prompt_from_trigger(
    update_or_q,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    await _activate_objects_ui(context)
    await clear_stats_buttons(context)
    await delete_anchor(context)
    await delete_param_result_message(context)
    clear_param_runtime(context)
    clear_report_context(context, cancel_job=True)
    context.user_data.pop("param_result_payload", None)
    context.user_data.pop("cmd_return", None)
    context.user_data.pop("cmd_return_payload", None)
    context.user_data.pop("cmd_return_runtime", None)
    context.user_data.pop("chosen_unit", None)
    context.user_data.pop("last_cmd_unit", None)
    _clear_export_job(context, cancel=True)
    await delete_export_anchor(context)
    _reset_search_context(context)
    context.user_data.pop("last_success_message", None)
    context.user_data.pop("params_map", None)
    context.user_data.pop("params_order", None)
    context.user_data["mode"] = MODE_NONE
    _set_objects_mode(context, OBJECTS_MODE_LIST)
    await send_new_anchor_below(
        update_or_q,
        "Введите имя/госномер для поиска…",
        kb_search_prompt(context),
        context,
        parse_mode=None,
    )
    return STATE_FIND_QUERY


async def global_restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if _active_ui(context) == UI_ADMIN:
        text = ""
        if update.message and update.message.text:
            text = update.message.text.strip()
        if text == MENU_BUTTON_FIND:
            chat = update.effective_chat
            chat_id = chat.id if chat else None
            log.info("admin: to_search user=%s -> set UI=OBJECTS", chat_id)
            panel_state = context.chat_data.get("admin_panel")
            if isinstance(panel_state, dict):
                panel_state.pop("awaiting", None)
            await _activate_objects_ui(context)
        else:
            chat = update.effective_chat
            log.debug("admin: ignore search trigger user=%s", chat.id if chat else None)
            return ConversationHandler.END

    if context.user_data.get("mode") == MODE_EXPORT:
        chat = update.effective_chat
        log.info(
            "export: auto-cancel on search trigger user=%s",
            chat.id if chat else None,
        )
        _clear_export_job(context, cancel=True)
        await delete_export_anchor(context)
        context.user_data["mode"] = MODE_NONE
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    return await _start_search_prompt_from_trigger(update, context)


def _load_cached_unit_snapshot_only() -> Tuple[Dict[int, Dict[str, Any]], Optional[int]]:
    return _load_snapshot_from_service()


def _lookup_unit_snapshot(unit_id: int) -> Optional[Dict[str, Any]]:
    """Fetch a unit entry from the UnitSnapshotService cache."""

    snapshot, _ = _load_snapshot_from_service()
    if not snapshot:
        return None
    entry = snapshot.get(unit_id)
    if not entry:
        return None
    return copy.deepcopy(entry)


def _bootstrap_unit_snapshot_from_cache() -> None:
    try:
        bundle = get_unit_snapshot_service().load_bundle()
    except Exception as exc:
        log.debug("unit_snapshot: bootstrap load failed: %s", exc, exc_info=True)
        return
    if bundle.units:
        log.info(
            "unit_snapshot: bootstrap units=%s dump_ts=%s",
            len(bundle.units),
            bundle.dump_ts,
        )


def _load_snapshot_from_service() -> Tuple[Dict[int, Dict[str, Any]], Optional[int]]:
    try:
        service = get_unit_snapshot_service()
        bundle = service.load_bundle()
    except Exception as exc:
        log.debug("unit_snapshot: service load failed: %s", exc, exc_info=True)
        return {}, None
    if not bundle.units:
        return {}, bundle.dump_ts
    snapshot: Dict[int, Dict[str, Any]] = {}
    for uid, record in bundle.units.items():
        try:
            payload = record.to_dict()
        except Exception:
            payload = {"nm": record.name, "id": uid}
        payload.setdefault("id", uid)
        snapshot[uid] = payload
    return snapshot, bundle.dump_ts


def _start_unit_snapshot_daemon() -> None:
    if not UNIT_SNAPSHOT_REFRESH_ENABLED:
        return
    global _UNIT_SNAPSHOT_DAEMON_STARTED
    with _UNIT_SNAPSHOT_DAEMON_LOCK:
        if _UNIT_SNAPSHOT_DAEMON_STARTED:
            return
        try:
            pipeline_cfg = load_pipeline_config()
        except Exception as exc:
            snapshot_log.debug("unit_snapshot: failed to load pipeline config: %s", exc)
            return
        token = pipeline_cfg.wialon_token
        if not token:
            for extra in pipeline_cfg.wialon_extra_tokens:
                if extra:
                    token = extra
                    break
        if not token:
            snapshot_log.info("unit_snapshot: skipped daemon start (no Wialon token)")
            return
        host = pipeline_cfg.wialon_host
        client: Optional[WialonClient] = None
        try:
            client = WialonClient(host, token)
            ensure_unit_snapshot_updater(client)
            snapshot_log.info("unit_snapshot: background updater armed host=%s", host)
        except Exception as exc:
            snapshot_log.warning("unit_snapshot: failed to start updater: %s", exc)
            return
        finally:
            if client is not None:
                with contextlib.suppress(Exception):
                    client.close()
        _UNIT_SNAPSHOT_DAEMON_STARTED = True


def _search_units_locally(query: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Search units via local cache and enrich them with latest telemetry timestamps."""

    index = get_unit_index()
    matches = index.search(query, limit=limit)
    if not matches:
        return []
    storage_service = get_pipeline_storage_service()
    enriched: List[Dict[str, Any]] = []
    for match in matches:
        entry = dict(match)
        unit_id = entry.get("id")
        if unit_id is not None:
            try:
                uid = int(unit_id)
            except (TypeError, ValueError):
                uid = None
            if uid is not None:
                _enrich_entry_with_unit_config(entry, uid)
                latest = storage_service.get_latest_metrics(uid)
                ts_val = None
                if isinstance(latest, dict):
                    ts_val = latest.get("device_ts") or latest.get("received_ts")
                if ts_val:
                    try:
                        entry["lmsg"] = {"t": int(ts_val)}
                    except (TypeError, ValueError):
                        pass
        enriched.append(entry)
    return enriched


def _enrich_entry_with_unit_config(entry: Dict[str, Any], unit_id: int) -> None:
    config = _load_unit_config(unit_id)
    if not config:
        return
    general = config.general
    if general:
        if general.name:
            entry["nm"] = general.name
        if general.hardware and "hardware" not in entry:
            entry["hardware"] = general.hardware
        if general.uid:
            entry["uid"] = general.uid
    if config.aliases:
        alias = config.aliases[0]
        if isinstance(alias, dict):
            label = alias.get("name") or alias.get("title")
            if label:
                entry["alias"] = label
    if config.profile:
        for field in config.profile:
            if field.name and field.value:
                entry.setdefault("profile", {})[field.name] = field.value
    if config.driving:
        entry.setdefault("config_meta", {})["driving"] = config.driving

async def _present_search_results(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    units: List[Dict[str, Any]],
    *,
    search_token: Optional[str],
    client: Optional["WialonClient"],
    anchor_message_id: Optional[int],
) -> int:
    if search_token != _current_search_generation(context):
        return _objects_mode_to_state(context)
    if _active_ui(context) != UI_OBJECTS:
        return _objects_mode_to_state(context)
    if anchor_message_id is not None:
        current_anchor_id = _anchor_message_id(context)
        if current_anchor_id is None or current_anchor_id != anchor_message_id:
            return _objects_mode_to_state(context)
    if not units:
        _reset_search_context(context)
        await send_new_anchor_below(
            update,
            "Нет результатов. Попробуйте другой запрос.",
            kb_search_prompt(context),
            context,
            parse_mode=None,
        )
        return STATE_FIND_QUERY
    if len(units) == 1:
        unit_id = int(units[0].get("id"))
        selected = {"id": unit_id, "nm": units[0].get("nm") or f"id {unit_id}"}
        context.user_data["chosen_unit"] = selected
        _set_search_units(context, units)
        _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
        await delete_anchor(context)
        return await show_stats_then_actions(
            update,
            context,
            unit_id,
            selected["nm"],
            client,
            preserve_previous=True,
        )
    _set_search_units(context, units[:SEARCH_UNITS_LIMIT])
    _set_objects_mode(context, OBJECTS_MODE_LIST)
    await _render_units_page(
        context,
        chat_id=update.effective_chat.id if update.effective_chat else None,
        update=update,
        offset=0,
        reanchor=True,
    )
    return STATE_WAIT_UNIT





def _format_cache_timestamp(ts: Optional[int]) -> str:
    if not ts:
        return "никогда"
    try:
        dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        return "неизвестно"
    dt_local = dt_utc.astimezone(MOSCOW_TZ)
    return dt_local.strftime("%d.%m.%Y %H:%M")


def _serialize_markup(markup: Optional[InlineKeyboardMarkup]) -> str:
    if markup is None:
        return ""
    try:
        return json.dumps(markup.to_dict(), ensure_ascii=False, sort_keys=True)
    except Exception:
        return ""


def _compose_drain_cache_prompt(
    runtime: Dict[str, Any]
) -> Tuple[str, InlineKeyboardMarkup]:
    snapshot: Dict[int, Dict[str, Any]] = runtime.get("units_snapshot") or {}
    dump_ts = runtime.get(DRAIN_ANALYSIS_SNAPSHOT_TS_KEY)
    units_count = len(snapshot)
    units_meta = runtime.get(DRAIN_ANALYSIS_UNITS_META_KEY)
    if isinstance(units_meta, dict):
        meta_count = units_meta.get("count")
        if meta_count is not None:
            try:
                units_count = int(meta_count)
            except Exception:
                pass
        meta_dump_ts = units_meta.get("dump_ts")
        if dump_ts is None and meta_dump_ts is not None:
            dump_ts = meta_dump_ts
    formatted_ts = _format_cache_timestamp(dump_ts)
    units_count_display: Union[int, str] = units_count
    if (
        units_count == 0
        and not snapshot
        and not (isinstance(units_meta, dict) and units_meta.get("count"))
    ):
        units_count_display = "—"

    message_cache_entries = runtime.get("message_cache_days") or []
    ready_days: List[str] = []
    partial_descriptions: List[str] = []
    for entry in message_cache_entries:
        if not isinstance(entry, dict):
            continue
        day_value = entry.get("day")
        if not isinstance(day_value, str):
            continue
        formatted_day = _format_message_cache_day(day_value)
        status = entry.get("status")
        if status == "ready" and not entry.get("partial"):
            ready_days.append(formatted_day)
            continue
        if status == "incomplete":
            total = entry.get("total_units") or entry.get("units") or 0
            completed = entry.get("completed_units") or entry.get("units") or 0
            failed = entry.get("failed_units") or 0
            desc = f"{formatted_day}: сохранено {completed}/{total}"
            if failed:
                desc += f", ошибки {failed}"
            partial_descriptions.append(desc)
            continue
        if entry.get("partial"):
            completed = entry.get("partial_units") or 0
            total = entry.get("partial_total")
            failed = entry.get("partial_failed") or 0
            if total:
                desc = f"{formatted_day}: частично {completed}/{total}"
            else:
                desc = f"{formatted_day}: частично {completed}"
            if failed:
                desc += f", ошибки {failed}"
            partial_descriptions.append(desc)
            continue
        ready_days.append(formatted_day)

    ready_days = sorted(dict.fromkeys(ready_days))

    prompt_lines = [
        "📦 Кэш данных объектов",
        f"Обновлялся: {formatted_ts} мск",
        f"Объектов в кэше: {units_count_display}",
        "",
        "🗃️ Кэш сообщений объектов",
    ]

    if ready_days:
        preview = ", ".join(ready_days[:5])
        prompt_lines.append(f"Доступные дни: {preview}")
        if len(ready_days) > 5:
            prompt_lines.append(f"… и ещё {len(ready_days) - 5}")
    if partial_descriptions:
        prompt_lines.append("Незавершённые загрузки:")
        for line in partial_descriptions[:5]:
            prompt_lines.append(f"• {line}")
        if len(partial_descriptions) > 5:
            prompt_lines.append(f"… и ещё {len(partial_descriptions) - 5}")
    if not ready_days and not partial_descriptions:
        prompt_lines.append("Кэш сообщений не найден.")

    prompt_lines.append("\nОбновить данные перед анализом?")

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 Обновить", callback_data="drain:refresh:yes"),
                InlineKeyboardButton("➡️ Продолжить", callback_data="drain:refresh:no"),
            ],
            [
                InlineKeyboardButton("💾 Загрузить сообщения", callback_data="drain:msg:load"),
                InlineKeyboardButton("📆 Выбрать период", callback_data="drain:msg:period"),
            ],
            [InlineKeyboardButton("🧹 Почистить сообщения", callback_data="drain:msg:clear")],
        ]
    )

    return "\n".join(prompt_lines), keyboard


def _format_message_cache_day(day: str) -> str:
    try:
        parsed = datetime.fromisoformat(day)
    except Exception:
        try:
            parsed = datetime.strptime(day, "%Y-%m-%d")
        except Exception:
            return day
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(MOSCOW_TZ).strftime("%d.%m.%Y")


def _build_partial_message_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("▶️ Докачать", callback_data="drain:msg:resume"),
                InlineKeyboardButton("📦 Использовать как есть", callback_data="drain:msg:use"),
            ],
            [
                InlineKeyboardButton("🗑️ Удалить", callback_data="drain:msg:discard"),
                InlineKeyboardButton("↩️ Назад", callback_data="drain:msg:back"),
            ],
        ]
    )


def _compose_partial_prompt(
    formatted_day: str,
    status: str,
    completed: int,
    total: Optional[int],
    failed: int,
) -> List[str]:
    lines: List[str] = []
    if status == "aborted":
        lines.append(f"⏹️ Загрузка за {formatted_day} остановлена.")
    else:
        lines.append(f"⚠️ Кэш за {formatted_day} неполный.")
    if total and total > 0:
        lines.append(f"Готово объектов: {completed}/{total}")
    else:
        lines.append(f"Готово объектов: {completed}")
    if failed:
        lines.append(f"Ошибки при загрузке: {failed}")
    lines.append("Выберите действие:")
    return lines


def _normalize_series_pairs(raw: Any) -> List[Tuple[int, float]]:
    pairs: List[Tuple[int, float]] = []
    if not isinstance(raw, list):
        return pairs
    for item in raw:
        ts_val: Optional[int] = None
        value_val: Optional[float] = None
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            try:
                ts_val = int(item[0])
            except Exception:
                ts_val = None
            try:
                value_val = float(item[1])
            except Exception:
                value_val = None
        elif isinstance(item, dict):
            t_candidate = item.get("ts") or item.get("t") or item.get("time") or item.get("timestamp")
            v_candidate = item.get("v") or item.get("value") or item.get("val")
            try:
                ts_val = int(t_candidate)
            except Exception:
                ts_val = None
            try:
                value_val = float(v_candidate)
            except Exception:
                value_val = None
        if ts_val is None or value_val is None:
            continue
        pairs.append((ts_val, value_val))
    pairs.sort(key=lambda item: item[0])
    return pairs


def _prepare_message_cache_units(units_raw: Any) -> Dict[int, Dict[str, Any]]:
    prepared: Dict[int, Dict[str, Any]] = {}
    if not isinstance(units_raw, dict):
        return prepared
    for key, payload in units_raw.items():
        try:
            unit_id = int(key)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        entry: Dict[str, Any] = {}
        sensor_ids_raw = payload.get("sensor_ids")
        sensor_ids: List[int] = []
        if isinstance(sensor_ids_raw, list):
            for sid in sensor_ids_raw:
                try:
                    sensor_ids.append(int(sid))
                except Exception:
                    continue
        entry["sensor_ids"] = sensor_ids

        series_map: Dict[int, List[Tuple[int, float]]] = {}
        raw_series = payload.get("series")
        if isinstance(raw_series, dict):
            for sid_key, series_payload in raw_series.items():
                try:
                    sid_int = int(sid_key)
                except Exception:
                    continue
                series_map[sid_int] = _normalize_series_pairs(series_payload)
        entry["series"] = series_map

        stats_map: Dict[int, Dict[str, Any]] = {}
        raw_stats = payload.get("stats")
        if isinstance(raw_stats, dict):
            for sid_key, stats_payload in raw_stats.items():
                try:
                    sid_int = int(sid_key)
                except Exception:
                    continue
                if isinstance(stats_payload, dict):
                    stats_map[sid_int] = dict(stats_payload)
        entry["stats"] = stats_map

        entry["speed_series"] = _normalize_series_pairs(payload.get("speed_series"))

        aux_map: Dict[str, List[Tuple[int, float]]] = {}
        aux_raw = payload.get("aux_series")
        if isinstance(aux_raw, dict):
            for aux_key, aux_payload in aux_raw.items():
                if isinstance(aux_payload, list):
                    aux_map[str(aux_key)] = _normalize_series_pairs(aux_payload)
        entry["aux_series"] = aux_map

        samples_raw = payload.get("samples")
        entry["samples"] = copy.deepcopy(samples_raw) if isinstance(samples_raw, list) else []

        try:
            entry["msg_count"] = int(payload.get("msg_count", 0) or 0)
        except Exception:
            entry["msg_count"] = 0
        try:
            entry["index_from"] = int(payload.get("index_from", 0) or 0)
        except Exception:
            entry["index_from"] = 0
        try:
            entry["index_to"] = int(payload.get("index_to", 0) or 0)
        except Exception:
            entry["index_to"] = 0
        entry["width_hint"] = payload.get("width_hint")
        entry["had_sensors"] = bool(payload.get("had_sensors"))
        entry["no_sensors"] = bool(payload.get("no_sensors"))
        entry["no_data"] = bool(payload.get("no_data"))
        entry["failed"] = bool(payload.get("failed"))

        prepared[unit_id] = entry
    return prepared


def _parse_drain_analysis_day(
    text: str, now_local: datetime
) -> Optional[Tuple[datetime, datetime, str]]:
    raw = (text or "").strip()
    if not raw:
        return None

    compact = raw.replace(" ", "")
    match = re.fullmatch(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?", compact)

    year: Optional[int] = None
    month: Optional[int] = None
    day: Optional[int] = None

    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year_raw = match.group(3)
        if year_raw is not None:
            year = int(year_raw)
            if year < 100:
                year += 2000
    else:
        normalized = re.sub(r"\s+", " ", raw).strip().casefold()
        match_words = re.fullmatch(r"(\d{1,2})\s+([а-яё\.]+)(?:\s+(\d{2,4}))?", normalized)
        if match_words:
            day = int(match_words.group(1))
            month_token = match_words.group(2).strip(". ")
            month = MONTH_NAME_TO_NUM.get(month_token)
            if month is None and month_token.endswith("е"):
                month = MONTH_NAME_TO_NUM.get(month_token[:-1])
            year_raw = match_words.group(3)
            if year_raw is not None:
                year = int(year_raw)
                if year < 100:
                    year += 2000

    if day is None or month is None:
        return None

    if year is None:
        year = now_local.year

    try:
        start_dt = datetime(year, month, day, 0, 0, 0, tzinfo=MOSCOW_TZ)
    except ValueError:
        return None

    if start_dt > now_local:
        try:
            start_dt = start_dt.replace(year=start_dt.year - 1)
        except ValueError:
            # handle Feb 29 on non-leap year by moving to Feb 28
            adjusted_year = start_dt.year - 1
            while adjusted_year >= start_dt.year - 2:
                try:
                    start_dt = start_dt.replace(year=adjusted_year)
                    break
                except ValueError:
                    adjusted_year -= 1
            else:
                return None

    end_dt = start_dt + timedelta(days=1)
    label = start_dt.strftime("%d.%m.%Y")
    return start_dt, end_dt, label


def _parse_drain_analysis_period(
    text: str, now_local: datetime
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    raw = (text or "").strip()
    if not raw:
        return None, "�� ������� ������. ����������, ������� ����� � �����."

    normalized = (
        raw.replace("—", "-")
        .replace("–", "-")
        .replace("\u2212", "-")
    )
    match = re.fullmatch(r"\s*(.+?)\s*-\s*(.+?)\s*", normalized)
    if not match:
        return None, "�� ������� ���������� ��������. ��������: 01.10.2025 - 30.10.2025"

    start_part = match.group(1).strip()
    end_part = match.group(2).strip()
    if not start_part or not end_part:
        return None, "���� � ����� ������ ���� ���������."

    start_parsed = _parse_drain_analysis_day(start_part, now_local)
    end_parsed = _parse_drain_analysis_day(end_part, now_local)
    if not start_parsed or not end_parsed:
        return None, "�� ������� ���������� ����. ��������: 01.10.2025 - 30.10.2025"

    start_dt = start_parsed[0]
    end_dt_start = end_parsed[0]
    if end_dt_start < start_dt:
        return None, "���� ��������� �� ����� ������ ���� ��������."

    max_span = MESSAGE_CACHE_MAX_PERIOD_DAYS
    days: List[Dict[str, Any]] = []
    current = start_dt
    total_allowed = max_span
    while current <= end_dt_start:
        days.append(
            {
                "start": current,
                "end": current + timedelta(days=1),
                "label": current.strftime("%d.%m.%Y"),
            }
        )
        if len(days) > total_allowed:
            return (
                None,
                f"������������ ������� ��������: �� ����� {MESSAGE_CACHE_MAX_PERIOD_DAYS} ���.",
            )
        current += timedelta(days=1)

    if not days:
        return None, "�� ������� ��������."

    range_label = (
        f"{days[0]['label']} \u2014 {days[-1]['label']}"
        if len(days) > 1
        else days[0]["label"]
    )

    return (
        {
            "start": start_dt,
            "end": end_dt_start + timedelta(days=1),
            "label": range_label,
            "days": days,
        },
        None,
    )


async def drain_analysis_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if _active_ui(context) == UI_ADMIN:
        text = (update.message.text if update.message and update.message.text else "").strip()
        if text == MENU_BUTTON_DRAIN_ANALYSIS:
            chat = update.effective_chat
            chat_id = chat.id if chat else None
            log.info("admin: to_drain user=%s -> set UI=OBJECTS", chat_id)
            panel_state = context.chat_data.get("admin_panel")
            if isinstance(panel_state, dict):
                panel_state.pop("awaiting", None)
            await _activate_objects_ui(context)
        else:
            chat = update.effective_chat
            log.debug("admin: ignore drain trigger user=%s", chat.id if chat else None)
            return ConversationHandler.END

    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN

    await _activate_objects_ui(context)

    chat = update.effective_chat
    chat_id = chat.id if chat else None
    if chat_id is None:
        return _objects_mode_to_state(context)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY, None)
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY, None)
    previous_prefetch = runtime.get(DRAIN_ANALYSIS_PREFETCH_TASK_KEY)
    if isinstance(previous_prefetch, asyncio.Task):
        previous_prefetch.cancel()
    runtime.pop(DRAIN_ANALYSIS_PREFETCH_TASK_KEY, None)
    runtime.clear()

    units_meta = _load_unit_snapshot_meta()
    if isinstance(units_meta, dict):
        runtime[DRAIN_ANALYSIS_UNITS_META_KEY] = dict(units_meta)
        dump_ts_meta = units_meta.get("dump_ts")
        if dump_ts_meta is not None:
            runtime[DRAIN_ANALYSIS_SNAPSHOT_TS_KEY] = dump_ts_meta
    else:
        runtime[DRAIN_ANALYSIS_UNITS_META_KEY] = {}
        runtime.pop(DRAIN_ANALYSIS_SNAPSHOT_TS_KEY, None)

    runtime["units_snapshot"] = {}
    try:
        runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    except Exception as exc:
        log.debug("drain_analysis: failed to list cached days: %s", exc)
        runtime["message_cache_days"] = []

    prompt_text, keyboard = _compose_drain_cache_prompt(runtime)

    status_msg: Optional[Message] = None
    if update.message:
        try:
            status_msg = await update.message.reply_text(prompt_text, reply_markup=keyboard)
        except Exception as exc:
            log.debug("drain_analysis: failed to send refresh prompt: %s", exc)
    if status_msg is None:
        try:
            status_msg = await context.bot.send_message(
                chat_id=chat_id, text=prompt_text, reply_markup=keyboard
            )
        except Exception as exc:
            log.debug("drain_analysis: failed to post refresh prompt: %s", exc)
            status_msg = None

    if status_msg:
        runtime[DRAIN_ANALYSIS_STATUS_KEY] = {
            "chat_id": status_msg.chat_id,
            "message_id": status_msg.message_id,
        }
    runtime[DRAIN_ANALYSIS_PROMPT_TEXT_KEY] = prompt_text
    runtime[DRAIN_ANALYSIS_PROMPT_MARKUP_KEY] = _serialize_markup(keyboard)
    _schedule_drain_analysis_prefetch(context, runtime)
    return STATE_DRAIN_PROMPT_REFRESH


def _schedule_drain_analysis_prefetch(context: ContextTypes.DEFAULT_TYPE, runtime: Dict[str, Any]) -> None:
    existing = runtime.get(DRAIN_ANALYSIS_PREFETCH_TASK_KEY)
    if isinstance(existing, asyncio.Task) and not existing.done():
        return

    async def _prefetch() -> None:
        try:
            snapshot, dump_ts = await asyncio.to_thread(_load_cached_unit_snapshot_only)
            runtime["units_snapshot"] = snapshot
            runtime[DRAIN_ANALYSIS_UNITS_META_KEY] = {
                "count": len(snapshot),
                "dump_ts": dump_ts,
                "updated_ts": int(time.time()),
            }
            try:
                _write_unit_snapshot_meta(runtime[DRAIN_ANALYSIS_UNITS_META_KEY])
            except Exception:
                pass
            if dump_ts is not None:
                runtime[DRAIN_ANALYSIS_SNAPSHOT_TS_KEY] = dump_ts
            try:
                message_days = await asyncio.to_thread(MESSAGE_CACHE.list_days)
            except Exception as exc:
                log.debug("drain_analysis: prefetch list_days failed: %s", exc)
                message_days = []
            runtime["message_cache_days"] = message_days
            await _update_drain_prompt_message(context, runtime)
        except Exception as exc:
            log.exception("drain_analysis: prefetch failed: %s", exc)
        finally:
            task_ref = runtime.get(DRAIN_ANALYSIS_PREFETCH_TASK_KEY)
            current = asyncio.current_task()
            if task_ref is current:
                runtime.pop(DRAIN_ANALYSIS_PREFETCH_TASK_KEY, None)

    application = getattr(context, "application", None)
    if application is not None:
        task = application.create_task(_prefetch())
    else:
        task = asyncio.create_task(_prefetch())
    runtime[DRAIN_ANALYSIS_PREFETCH_TASK_KEY] = task


async def _update_drain_prompt_message(context: ContextTypes.DEFAULT_TYPE, runtime: Dict[str, Any]) -> None:
    status_meta = runtime.get(DRAIN_ANALYSIS_STATUS_KEY)
    if not isinstance(status_meta, dict):
        return
    chat_id = status_meta.get("chat_id")
    message_id = status_meta.get("message_id")
    if chat_id is None or message_id is None:
        return
    previous_text = runtime.get(DRAIN_ANALYSIS_PROMPT_TEXT_KEY)
    previous_markup = runtime.get(DRAIN_ANALYSIS_PROMPT_MARKUP_KEY)
    prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
    serialized_markup = _serialize_markup(keyboard)
    if (
        isinstance(previous_text, str)
        and previous_text.startswith(("ℹ️", "✅", "❌"))
        and previous_markup == _serialize_markup(None)
    ):
        return
    if prompt_text == previous_text and serialized_markup == previous_markup:
        return
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=prompt_text,
            reply_markup=keyboard,
        )
    except BadRequest as exc:
        message = str(exc).lower()
        if "message is not modified" in message:
            runtime[DRAIN_ANALYSIS_PROMPT_TEXT_KEY] = prompt_text
            runtime[DRAIN_ANALYSIS_PROMPT_MARKUP_KEY] = serialized_markup
        else:
            log.debug("drain_analysis: prompt edit skipped: %s", exc)
        return
    except Exception as exc:
        log.debug("drain_analysis: prompt edit failed: %s", exc)
        return
    runtime[DRAIN_ANALYSIS_PROMPT_TEXT_KEY] = prompt_text
    runtime[DRAIN_ANALYSIS_PROMPT_MARKUP_KEY] = serialized_markup


async def _prompt_drain_analysis_date(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    reference_ts: Optional[int],
) -> None:
    examples = ["10.05.2025", "10 мая", "1 января 2024"]
    formatted_ts = _format_cache_timestamp(reference_ts)
    text = (
        "📅 Введите дату для анализа сливов.\n"
        "Поддерживаются форматы: "
        + ", ".join(examples)
        + ".\n"
        "Если год не указан, используется текущий, а для будущих дат — предыдущий год.\n"
        f"Текущие данные датчиков обновлены: {formatted_ts} мск."
    )
    await context.bot.send_message(chat_id=chat_id, text=text)


async def drain_analysis_cancel_cb(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return STATE_DRAIN_WAIT_DATE

    try:
        await _safe_answer_callback(query, "Останавливаю анализ…")
    except Exception:
        pass

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    cancel_event = runtime.get("cancel_event")
    if isinstance(cancel_event, asyncio.Event):
        cancel_event.set()
    else:
        new_event = asyncio.Event()
        new_event.set()
        runtime["cancel_event"] = new_event
        cancel_event = new_event

    cancelled_tasks, aborted_clients, pending_units = await _abort_active_drain_sessions(runtime)

    runtime["drain_analysis_last_cancel"] = {
        "tasks_cancelled": cancelled_tasks,
        "sessions_aborted": aborted_clients,
        "inflight_units": pending_units,
        "timestamp": time.time(),
    }
    drain_cache_log.info(
        "analysis_cancel_pressed tasks=%s sessions=%s units=%s caller=%s",
        cancelled_tasks,
        aborted_clients,
        pending_units,
        _stack_brief(),
    )
    cancel_log.info(
        "drain_analysis cancel_result tasks=%s clients=%s units=%s",
        cancelled_tasks,
        aborted_clients,
        pending_units,
    )
    log.info(
        "drain_analysis: cancel requested tasks_cancelled=%s sessions_aborted=%s units=%s",
        cancelled_tasks,
        aborted_clients,
        pending_units,
    )

    if not await _safe_edit_callback_text(query, "⏹️ Останавливаю анализ…"):
        log.debug("drain_analysis: cancel edit skipped or failed")

    return STATE_DRAIN_WAIT_DATE


async def drain_analysis_refresh_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query or not query.data:
        return STATE_DRAIN_PROMPT_REFRESH

    await _safe_answer_callback(query)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    status_meta = runtime.get(DRAIN_ANALYSIS_STATUS_KEY)
    if status_meta and isinstance(status_meta, dict):
        status_meta.update({"chat_id": query.message.chat_id, "message_id": query.message.message_id})
    else:
        runtime[DRAIN_ANALYSIS_STATUS_KEY] = {
            "chat_id": query.message.chat_id,
            "message_id": query.message.message_id,
        }
    existing_task = runtime.get(DRAIN_ANALYSIS_PREFETCH_TASK_KEY)

    choice = query.data.split(":")[-1]
    chat_id = query.message.chat_id

    if choice == "yes":
        if isinstance(existing_task, asyncio.Task):
            existing_task.cancel()
        runtime.pop(DRAIN_ANALYSIS_PREFETCH_TASK_KEY, None)
        edited = await _safe_edit_callback_text(query, "🔄 Обновляю данные объектов…")
        if edited:
            runtime[DRAIN_ANALYSIS_PROMPT_TEXT_KEY] = "🔄 Обновляю данные объектов…"
            runtime[DRAIN_ANALYSIS_PROMPT_MARKUP_KEY] = _serialize_markup(None)
        else:
            log.debug("drain_analysis: refresh message unchanged or failed during edit")

        client = await require_wialon_client(
            update,
            context,
            "Чтобы обновить данные, авторизуйтесь.",
        )
        if not client:
            return STATE_AUTH_WAIT_TOKEN

        try:
            count, total_loaded, duration = await run_blocking(rebuild_unit_snapshot_sync, client)
            log.info(
                "drain_analysis: unit cache rebuilt count=%s total_loaded=%s duration=%.2fs",
                count,
                total_loaded,
                duration,
            )
        except Exception as exc:
            log.exception("drain_analysis: cache rebuild failed: %s", exc)
            failure_text = f"❌ Не удалось обновить данные: {exc}\nБудут использованы текущие сведения."
            await _safe_edit_callback_text(query, failure_text, reply_markup=None)
            runtime[DRAIN_ANALYSIS_PROMPT_TEXT_KEY] = failure_text
            runtime[DRAIN_ANALYSIS_PROMPT_MARKUP_KEY] = _serialize_markup(None)
            _schedule_drain_analysis_prefetch(context, runtime)
        else:
            snapshot, dump_ts = _load_cached_unit_snapshot_only()
            runtime["units_snapshot"] = snapshot
            runtime[DRAIN_ANALYSIS_SNAPSHOT_TS_KEY] = dump_ts
            runtime[DRAIN_ANALYSIS_UNITS_META_KEY] = {
                "count": len(snapshot),
                "dump_ts": dump_ts,
                "updated_ts": int(time.time()),
            }
            try:
                runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
            except Exception as exc:
                log.debug("drain_analysis: failed to list cached days after refresh: %s", exc)
                runtime["message_cache_days"] = []
            prompt_text, _ = _compose_drain_cache_prompt(runtime)
            success_text = "✅ Данные объектов обновлены.\n\n" + prompt_text
            await _safe_edit_callback_text(query, success_text, reply_markup=None)
            runtime[DRAIN_ANALYSIS_PROMPT_TEXT_KEY] = success_text
            runtime[DRAIN_ANALYSIS_PROMPT_MARKUP_KEY] = _serialize_markup(None)
    else:
        try:
            runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
        except Exception as exc:
            log.debug("drain_analysis: failed to list cached days (use existing): %s", exc)
            runtime["message_cache_days"] = []
        prompt_text, _ = _compose_drain_cache_prompt(runtime)
        info_text = "ℹ️ Используем сохранённые данные.\n\n" + prompt_text
        await _safe_edit_callback_text(query, info_text, reply_markup=None)
        runtime[DRAIN_ANALYSIS_PROMPT_TEXT_KEY] = info_text
        runtime[DRAIN_ANALYSIS_PROMPT_MARKUP_KEY] = _serialize_markup(None)
        _schedule_drain_analysis_prefetch(context, runtime)

    await _prompt_drain_analysis_date(context, chat_id, runtime.get(DRAIN_ANALYSIS_SNAPSHOT_TS_KEY))
    return STATE_DRAIN_WAIT_DATE


async def drain_analysis_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.message
    if not message or not message.text:
        return STATE_DRAIN_WAIT_DATE

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    snapshot: Dict[int, Dict[str, Any]] = runtime.get("units_snapshot") or {}
    if not snapshot:
        # Try loading again in case файл был обновлён извне
        snapshot, dump_ts = _load_cached_unit_snapshot_only()
        runtime["units_snapshot"] = snapshot
        runtime[DRAIN_ANALYSIS_SNAPSHOT_TS_KEY] = dump_ts
        runtime[DRAIN_ANALYSIS_UNITS_META_KEY] = {
            "count": len(snapshot),
            "dump_ts": dump_ts,
            "updated_ts": int(time.time()),
        }
        try:
            _write_unit_snapshot_meta(runtime[DRAIN_ANALYSIS_UNITS_META_KEY])
        except Exception:
            pass

    now_local = datetime.now(MOSCOW_TZ)
    parsed = _parse_drain_analysis_day(message.text, now_local)
    if not parsed:
        await message.reply_text(
            "❌ Не удалось распознать дату. Пример: 10.05.2025 или 10 мая."
        )
        return STATE_DRAIN_WAIT_DATE

    start_dt, end_dt, label = parsed
    runtime[DRAIN_ANALYSIS_DATE_KEY] = label
    runtime[DRAIN_ANALYSIS_RANGE_KEY] = (start_dt, end_dt)

    message_cache_units: Optional[Dict[int, Dict[str, Any]]] = None
    day_key = start_dt.date().isoformat()
    cache_payload = MESSAGE_CACHE.load_day(day_key)
    if isinstance(cache_payload, dict):
        expected_start = int(start_dt.astimezone(timezone.utc).timestamp())
        expected_end = int(end_dt.astimezone(timezone.utc).timestamp())
        try:
            cache_start = int(cache_payload.get("start_ts", 0) or 0)
        except Exception:
            cache_start = None
        try:
            cache_end = int(cache_payload.get("end_ts", 0) or 0)
        except Exception:
            cache_end = None
        if cache_start == expected_start and cache_end == expected_end:
            message_cache_units = _prepare_message_cache_units(cache_payload.get("units"))
            if message_cache_units:
                runtime["message_cache_day"] = day_key
        else:
            log.debug(
                "message_cache: ignoring cached day=%s due to range mismatch cache=%s..%s expected=%s..%s",
                day_key,
                cache_start,
                cache_end,
                expected_start,
                expected_end,
            )

    client = await require_wialon_client(
        update,
        context,
        "Чтобы анализировать сливы, авторизуйтесь.",
    )
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    existing_job: Optional[asyncio.Task[Any]] = runtime.get("drain_analysis_job")  # type: ignore[assignment]
    if isinstance(existing_job, asyncio.Task) and not existing_job.done():
        await context.bot.send_message(
            chat_id=message.chat_id if message else update.effective_chat.id,
            text="Анализ уже выполняется, дождитесь завершения или остановите текущий процесс.",
        )
        return STATE_DRAIN_WAIT_DATE

    async def worker() -> None:
        try:
            await _execute_drain_analysis(
                update,
                context,
                client,
                snapshot,
                start_dt,
                end_dt,
                label,
                message_cache=message_cache_units,
            )
        except asyncio.CancelledError:
            cancel_log.info("drain_analysis worker cancelled day=%s", start_dt.date())
            raise
        except Exception as exc:  # pragma: no cover - defensive logging
            log.exception("drain_analysis: worker failed day=%s err=%s", start_dt.date(), exc)
            await context.bot.send_message(
                chat_id=message.chat_id if message else update.effective_chat.id,
                text=f"Не удалось выполнить анализ: {exc}",
            )
        finally:
            runtime.pop("drain_analysis_job", None)

    job_task = context.application.create_task(worker())
    runtime["drain_analysis_job"] = job_task
    cancel_log.info("drain_analysis worker scheduled day=%s", start_dt.date())
    return STATE_DRAIN_WAIT_DATE


async def drain_message_cache_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.message
    if not message or not message.text:
        return STATE_DRAIN_WAIT_MSG_DATE

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})

    range_label_ctx = runtime.get(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY)
    if not isinstance(range_label_ctx, str) or not range_label_ctx.strip():
        range_label_ctx = None

    now_local = datetime.now(MOSCOW_TZ)
    parsed = _parse_drain_analysis_day(message.text, now_local)
    if not parsed:
        await message.reply_text(
            "❌ Не удалось распознать дату. Пример: 10.05.2025 или 10 мая."
        )
        return STATE_DRAIN_WAIT_MSG_DATE

    start_dt, end_dt, label = parsed
    day_key = start_dt.date().isoformat()
    runtime[DRAIN_ANALYSIS_MSG_REQUEST_KEY] = {
        "day": day_key,
        "label": label,
        "start": start_dt,
        "end": end_dt,
    }

    runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    partial_info = MESSAGE_CACHE.get_partial_info(day_key)
    existing_payload = MESSAGE_CACHE.load_day(day_key)
    if isinstance(existing_payload, dict):
        expected_start = int(start_dt.astimezone(timezone.utc).timestamp())
        expected_end = int(end_dt.astimezone(timezone.utc).timestamp())
        try:
            cached_start = int(existing_payload.get("start_ts") or 0)
        except Exception:
            cached_start = None
        try:
            cached_end = int(existing_payload.get("end_ts") or 0)
        except Exception:
            cached_end = None
        if cached_start != expected_start or cached_end != expected_end:
            existing_payload = None
    formatted_day = _format_message_cache_day(day_key)

    if partial_info:
        meta = partial_info.get("meta") or {}
        total_units = meta.get("total_units")
        if not isinstance(total_units, int) or total_units <= 0:
            snapshot: Dict[int, Dict[str, Any]] = runtime.get("units_snapshot") or {}
            if not snapshot:
                snapshot, dump_ts = _load_cached_unit_snapshot_only()
                runtime["units_snapshot"] = snapshot
                runtime[DRAIN_ANALYSIS_SNAPSHOT_TS_KEY] = dump_ts
                runtime[DRAIN_ANALYSIS_UNITS_META_KEY] = {
                    "count": len(snapshot),
                    "dump_ts": dump_ts,
                    "updated_ts": int(time.time()),
                }
                try:
                    _write_unit_snapshot_meta(runtime[DRAIN_ANALYSIS_UNITS_META_KEY])
                except Exception:
                    pass
            total_units = len(snapshot)
        completed_units = len(partial_info.get("units") or [])
        failed_units = len(partial_info.get("failures") or [])
        status_text = str(meta.get("status") or "partial")
        runtime[DRAIN_ANALYSIS_MSG_PARTIAL_KEY] = {
            "day": day_key,
            "label": label,
            "start": start_dt,
            "end": end_dt,
            "completed": completed_units,
            "total": total_units,
            "failed": failed_units,
            "status": status_text,
        }
        lines = _compose_partial_prompt(
            formatted_day,
            "aborted" if status_text == "aborted" else "partial",
            completed_units,
            total_units,
            failed_units,
        )
        keyboard = _build_partial_message_keyboard()
        await context.bot.send_message(
            chat_id=message.chat_id,
            text="\n".join(lines),
            reply_markup=keyboard,
        )
        return STATE_DRAIN_WAIT_MSG_PARTIAL

    if existing_payload:
        runtime.pop(DRAIN_ANALYSIS_MSG_PARTIAL_KEY, None)
        runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
        prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
        await context.bot.send_message(
            chat_id=message.chat_id,
            text=f"✅ Сообщения за {formatted_day} уже сохранены.\n\n" + prompt_text,
            reply_markup=keyboard,
        )
        return STATE_DRAIN_PROMPT_REFRESH

    return await _run_message_cache_download(
        update,
        context,
        start_dt=start_dt,
        end_dt=end_dt,
        label=label,
        chat_id=message.chat_id,
    )


async def drain_message_cache_period_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.message
    if not message or not message.text:
        return STATE_DRAIN_WAIT_MSG_PERIOD

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY, None)
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY, None)

    now_local = datetime.now(MOSCOW_TZ)
    parsed, error = _parse_drain_analysis_period(message.text, now_local)
    if not parsed:
        await message.reply_text(error or "�� ������� ���������� ��������. ��������: 01.10.2025 - 30.10.2025")
        return STATE_DRAIN_WAIT_MSG_PERIOD

    days: List[Dict[str, Any]] = parsed["days"]
    if not days:
        await message.reply_text("�� ������� ��������. ��������, ������� �������� ��������.")
        return STATE_DRAIN_WAIT_MSG_PERIOD

    runtime.pop(DRAIN_ANALYSIS_MSG_PARTIAL_KEY, None)
    runtime[DRAIN_ANALYSIS_MSG_REQUEST_KEY] = {
        "day": days[0]["start"].date().isoformat(),
        "label": days[0]["label"],
        "start": days[0]["start"],
        "end": days[0]["end"],
    }
    runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    runtime["msg_action"] = "load_range"

    return await _run_message_cache_download_range(
        update,
        context,
        days=days,
        range_label=parsed["label"],
        chat_id=message.chat_id,
    )


async def _run_message_cache_download_range(
    trigger_update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    days: List[Dict[str, Any]],
    range_label: str,
    chat_id: int,
) -> int:
    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})

    pending = deque(days)
    if not pending:
        prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
        await context.bot.send_message(chat_id=chat_id, text=prompt_text, reply_markup=keyboard)
        return STATE_DRAIN_PROMPT_REFRESH

    snapshot: Dict[int, Dict[str, Any]] = runtime.get("units_snapshot") or {}
    if not snapshot:
        snapshot, dump_ts = _load_cached_unit_snapshot_only()
        runtime["units_snapshot"] = snapshot
        runtime[DRAIN_ANALYSIS_SNAPSHOT_TS_KEY] = dump_ts
        runtime[DRAIN_ANALYSIS_UNITS_META_KEY] = {
            "count": len(snapshot),
            "dump_ts": dump_ts,
            "updated_ts": int(time.time()),
        }
        try:
            _write_unit_snapshot_meta(runtime[DRAIN_ANALYSIS_UNITS_META_KEY])
        except Exception:
            pass
        runtime[DRAIN_ANALYSIS_UNITS_META_KEY] = {
            "count": len(snapshot),
            "dump_ts": dump_ts,
            "updated_ts": int(time.time()),
        }

    if not snapshot:
        await context.bot.send_message(
            chat_id=chat_id,
            text="? ��� �������� � ��������� ����. ������� �������� ������ ��������.",
        )
        prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
        await context.bot.send_message(chat_id=chat_id, text=prompt_text, reply_markup=keyboard)
        return STATE_DRAIN_PROMPT_REFRESH

    range_queue_snapshot = [
        {
            "start": entry["start"].isoformat() if isinstance(entry.get("start"), datetime) else entry.get("start"),
            "end": entry["end"].isoformat() if isinstance(entry.get("end"), datetime) else entry.get("end"),
            "label": entry.get("label"),
        }
        for entry in pending
    ]
    message_cache_store_log.info(
        "range_request_begin range=%s days=%s queue=%s snapshot_units=%s",
        range_label,
        len(range_queue_snapshot),
        range_queue_snapshot,
        len(snapshot),
    )

    client = await require_wialon_client(
        trigger_update,
        context,
        "����� ��������� � Wialon, �������������.",
    )
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    job: Optional[asyncio.Task[Any]] = runtime.get("message_cache_job")  # type: ignore[assignment]
    if isinstance(job, asyncio.Task) and not job.done():
        await context.bot.send_message(
            chat_id=chat_id,
            text="? �������� ��� �����������. ��������� ���������� ��� ������� ����.",
        )
        return STATE_DRAIN_WAIT_MSG_PARTIAL

    def _serialize_queue(q: Deque[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "start": entry["start"],
                "end": entry["end"],
                "label": entry["label"],
            }
            for entry in q
        ]

    runtime[DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY] = _serialize_queue(pending)
    runtime[DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY] = range_label
    runtime["msg_action"] = "load_range"

    total_days = len(pending)

    async def worker() -> None:
        try:
            processed_count = 0
            while pending:
                current = pending[0]
                start_dt: datetime = current["start"]
                end_dt: datetime = current["end"]
                label: str = current["label"]

                result = await _download_message_cache(
                    trigger_update,
                    context,
                    client,
                    snapshot,
                    start_dt,
                    end_dt,
                    label,
                    range_total_days=total_days,
                )
                if result is None:
                    pending.popleft()
                    processed_count += 1
                    runtime[DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY] = _serialize_queue(pending)
                    continue

                runtime["units_snapshot"] = snapshot
                runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
                runtime[DRAIN_ANALYSIS_MSG_REQUEST_KEY] = {
                    "day": result.day,
                    "label": label,
                    "start": start_dt,
                    "end": end_dt,
                }
                runtime.pop("msg_action", None)

                formatted_day = _format_message_cache_day(result.day)

                if result.status == "completed":
                    runtime.pop(DRAIN_ANALYSIS_MSG_PARTIAL_KEY, None)
                    pending.popleft()
                    processed_count += 1
                    runtime[DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY] = _serialize_queue(pending)

                    if pending:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=f"✅ Сообщения за {formatted_day} скачаны ({processed_count}/{total_days}).",
                        )
                        runtime["msg_action"] = "load_range"
                        continue

                    prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"✅ Сообщения за период {range_label} скачаны.\n\n" + prompt_text,
                        reply_markup=keyboard,
                    )
                    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY, None)
                    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY, None)
                    return

                runtime[DRAIN_ANALYSIS_MSG_PARTIAL_KEY] = {
                    "day": result.day,
                    "label": label,
                    "start": start_dt,
                    "end": end_dt,
                    "completed": result.completed,
                    "total": result.total,
                    "failed": result.failed,
                    "status": result.status,
                }
                lines = _compose_partial_prompt(
                    formatted_day,
                    result.status,
                    result.completed,
                    result.total,
                    result.failed,
                )
                keyboard = _build_partial_message_keyboard()
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="\n".join(lines),
                    reply_markup=keyboard,
                )
                runtime[DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY] = _serialize_queue(pending)
                runtime[DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY] = range_label
                runtime["msg_action"] = "load_range"
                return
        except asyncio.CancelledError:
            cancel_log.info("message_cache range worker cancelled range=%s", range_label)
            raise
        except Exception as exc:  # pragma: no cover - defensive fallback
            log.exception("message_cache: range worker failed range=%s", range_label, exc_info=exc)
            cancel_log.info("message_cache worker exception range=%s err=%s", range_label, exc)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"? ������ ��� �������� ���������: {exc}",
            )
        finally:
            runtime.pop("message_cache_job", None)
            cancel_log.info(
                "message_cache worker finished range=%s remaining_days=%s",
                range_label,
                len(pending),
            )

    job_task = context.application.create_task(worker())
    runtime["message_cache_job"] = job_task
    cancel_log.info(
        "message_cache worker scheduled range=%s days=%s",
        range_label,
        total_days,
    )
    return STATE_DRAIN_WAIT_MSG_PARTIAL


async def _run_message_cache_download(
    trigger_update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    start_dt: datetime,
    end_dt: datetime,
    label: str,
    chat_id: int,
) -> int:
    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    raw_range_queue = runtime.get(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY)
    if isinstance(raw_range_queue, list):
        range_queue_snapshot = list(raw_range_queue)
    else:
        range_queue_snapshot = []
    range_total_days = len(range_queue_snapshot)
    range_label_ctx = runtime.get(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY)
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY, None)
    if range_label_ctx is not None:
        runtime[DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY] = range_label_ctx

    snapshot: Dict[int, Dict[str, Any]] = runtime.get("units_snapshot") or {}
    if not snapshot:
        snapshot, dump_ts = _load_cached_unit_snapshot_only()
        runtime["units_snapshot"] = snapshot
        runtime[DRAIN_ANALYSIS_SNAPSHOT_TS_KEY] = dump_ts

    if not snapshot:
        await context.bot.send_message(
            chat_id=chat_id,
            text="❌ Нет объектов в локальном кэше. Сначала обновите данные объектов.",
        )
        prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
        await context.bot.send_message(chat_id=chat_id, text=prompt_text, reply_markup=keyboard)
        return STATE_DRAIN_PROMPT_REFRESH
    message_cache_store_log.info(
        "download_begin day=%s range_label=%s range_days=%s start_ts=%s end_ts=%s snapshot_units=%s",
        start_dt.date().isoformat(),
        range_label_ctx or "-",
        range_total_days,
        start_dt.isoformat(),
        end_dt.isoformat(),
        len(snapshot),
    )

    client = await require_wialon_client(
        trigger_update,
        context,
        "Чтобы загрузить сообщения, авторизуйтесь.",
    )
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    job: Optional[asyncio.Task[Any]] = runtime.get("message_cache_job")  # type: ignore[assignment]
    if isinstance(job, asyncio.Task) and not job.done():
        await context.bot.send_message(
            chat_id=chat_id,
            text="⏳ Загрузка уже выполняется. Дождитесь завершения или нажмите стоп.",
        )
        return STATE_DRAIN_WAIT_MSG_PARTIAL

    async def worker() -> None:
        try:
            result = await _download_message_cache(
                trigger_update,
                context,
                client,
                snapshot,
                start_dt,
                end_dt,
                label,
                range_total_days=range_total_days,
            )
        except asyncio.CancelledError:
            cancel_log.info("message_cache worker cancelled day=%s", start_dt.date())
            raise
        except Exception as exc:  # pragma: no cover - defensive fallback
            log.exception("message_cache: worker failed day=%s", start_dt.date(), exc_info=exc)
            cancel_log.info("message_cache worker exception day=%s err=%s", start_dt.date(), exc)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Ошибка при загрузке сообщений: {exc}",
            )
        else:
            if result is None:
                return

            runtime["units_snapshot"] = snapshot
            runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
            runtime[DRAIN_ANALYSIS_MSG_REQUEST_KEY] = {
                "day": result.day,
                "label": label,
                "start": start_dt,
                "end": end_dt,
            }
            runtime.pop("msg_action", None)

            formatted_day = _format_message_cache_day(result.day)

            if result.status == "completed":
                runtime.pop(DRAIN_ANALYSIS_MSG_PARTIAL_KEY, None)
                prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"✅ Сообщения за {formatted_day} сохранены.\n\n" + prompt_text,
                    reply_markup=keyboard,
                )
                return

            runtime[DRAIN_ANALYSIS_MSG_PARTIAL_KEY] = {
                "day": result.day,
                "label": label,
                "start": start_dt,
                "end": end_dt,
                "completed": result.completed,
                "total": result.total,
                "failed": result.failed,
                "status": result.status,
            }

            lines = _compose_partial_prompt(
                formatted_day,
                result.status,
                result.completed,
                result.total,
                result.failed,
            )
            keyboard = _build_partial_message_keyboard()
            await context.bot.send_message(chat_id=chat_id, text="\n".join(lines), reply_markup=keyboard)
        finally:
            runtime.pop("message_cache_job", None)
            cancel_log.info("message_cache worker finished day=%s", start_dt.date())

    job_task = context.application.create_task(worker())
    runtime["message_cache_job"] = job_task
    cancel_log.info("message_cache worker scheduled day=%s", start_dt.date())
    return STATE_DRAIN_WAIT_MSG_PARTIAL


async def _abort_active_message_sessions(runtime: Dict[str, Any]) -> Tuple[int, int, List[int]]:
    active = runtime.get(DRAIN_ANALYSIS_MSG_ACTIVE_SESSIONS_KEY)
    cancelled_tasks = 0
    aborted_clients = 0
    active_units: List[int] = []

    if not isinstance(active, dict):
        return cancelled_tasks, aborted_clients, active_units

    tasks = active.get("tasks")
    if isinstance(tasks, set):
        for task in list(tasks):
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
                cancelled_tasks += 1
            tasks.discard(task)

    clients = active.get("clients")
    if isinstance(clients, set):
        for client in list(clients):
            if client is None:
                continue
            abort_fn = getattr(client, "abort_pending_requests", None)
            clear_fn = getattr(client, "clear_cancel_predicate", None)
            if callable(abort_fn):
                try:
                    result = abort_fn()
                    if inspect.isawaitable(result):
                        await result
                    aborted_clients += 1
                except Exception as exc:
                    log.debug("message_cache: failed to abort session %r: %s", client, exc)
            if callable(clear_fn):
                with contextlib.suppress(Exception):
                    result = clear_fn()
                    if inspect.isawaitable(result):
                        await result
            clients.discard(client)

    units = active.get("units")
    if isinstance(units, set):
        for unit_id in list(units):
            if isinstance(unit_id, int):
                active_units.append(unit_id)
            units.discard(unit_id)

    log.info(
        "message_cache: abort_active_sessions tasks_cancelled=%s clients_aborted=%s units=%s",
        cancelled_tasks,
        aborted_clients,
        active_units,
    )
    cancel_log.info(
        "message_cache abort_active_sessions tasks=%s clients=%s units=%s",
        cancelled_tasks,
        aborted_clients,
        active_units,
    )
    return cancelled_tasks, aborted_clients, active_units


async def _abort_active_drain_sessions(runtime: Dict[str, Any]) -> Tuple[int, int, List[int]]:
    active = runtime.get(DRAIN_ANALYSIS_ACTIVE_SESSIONS_KEY)
    cancelled_tasks = 0
    aborted_clients = 0
    active_units: List[int] = []

    if not isinstance(active, dict):
        return cancelled_tasks, aborted_clients, active_units

    tasks = active.get("tasks")
    if isinstance(tasks, set):
        for task in list(tasks):
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
                cancelled_tasks += 1
            tasks.discard(task)

    clients = active.get("clients")
    if isinstance(clients, set):
        for client in list(clients):
            if client is None:
                continue
            clear_fn = getattr(client, "clear_cancel_predicate", None)
            aborted_clients += 1
            if callable(clear_fn):
                with contextlib.suppress(Exception):
                    result = clear_fn()
                    if inspect.isawaitable(result):
                        await result
            clients.discard(client)

    units = active.get("units")
    if isinstance(units, set):
        for unit_id in list(units):
            if isinstance(unit_id, int):
                active_units.append(unit_id)
            units.discard(unit_id)

    log.info(
        "drain_analysis: abort_active_sessions tasks_cancelled=%s clients_aborted=%s units=%s",
        cancelled_tasks,
        aborted_clients,
        active_units,
    )
    cancel_log.info(
        "drain_analysis abort_active_sessions tasks=%s clients=%s units=%s",
        cancelled_tasks,
        aborted_clients,
        active_units,
    )
    return cancelled_tasks, aborted_clients, active_units


async def drain_message_cache_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await _safe_answer_callback(query, "Останавливаю загрузку…", show_alert=False)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    cancelled_tasks = 0
    aborted_clients = 0
    pending_units: List[int] = []

    cancel_event = runtime.get(DRAIN_ANALYSIS_MSG_CANCEL_KEY)
    active = runtime.get(DRAIN_ANALYSIS_MSG_ACTIVE_SESSIONS_KEY, {})
    tasks_count = len(active.get("tasks", [])) if isinstance(active, dict) else 0
    clients_count = len(active.get("clients", [])) if isinstance(active, dict) else 0
    log.info(
        "message_cache: cancel button pressed tasks=%s clients=%s event_present=%s",
        tasks_count,
        clients_count,
        isinstance(cancel_event, asyncio.Event),
    )
    if isinstance(cancel_event, asyncio.Event):
        if cancel_event.is_set():
            log.info("message_cache: cancel event already set, skipping duplicate set()")
        else:
            cancel_event.set()
    else:
        cancel_event = asyncio.Event()
        cancel_event.set()
        runtime[DRAIN_ANALYSIS_MSG_CANCEL_KEY] = cancel_event
    cancelled_tasks, aborted_clients, pending_units = await _abort_active_message_sessions(runtime)

    runtime["message_cache_last_cancel"] = {
        "tasks_cancelled": cancelled_tasks,
        "sessions_aborted": aborted_clients,
        "inflight_units": pending_units,
        "timestamp": time.time(),
    }
    drain_log.info(
        "cancel pressed: tasks=%s sessions=%s units=%s",
        cancelled_tasks,
        aborted_clients,
        pending_units,
    )
    cancel_log.info(
        "message_cache cancel_result tasks=%s clients=%s units=%s",
        cancelled_tasks,
        aborted_clients,
        pending_units,
    )
    log.info(
        "message_cache: cancel requested tasks_cancelled=%s sessions_aborted=%s units=%s",
        cancelled_tasks,
        aborted_clients,
        pending_units,
    )

    return STATE_DRAIN_WAIT_MSG_DATE


async def drain_message_cache_partial_resume_cb(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return STATE_DRAIN_PROMPT_REFRESH
    await _safe_answer_callback(query)
    with contextlib.suppress(Exception):
        if query.message:
            await query.message.edit_reply_markup(None)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    request = runtime.get(DRAIN_ANALYSIS_MSG_REQUEST_KEY)
    if not isinstance(request, dict):
        await context.bot.send_message(
            chat_id=query.message.chat_id if query.message else update.effective_chat.id,
            text="❌ Не найдены параметры для докачки сообщений.",
        )
        prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
        await context.bot.send_message(
            chat_id=query.message.chat_id if query.message else update.effective_chat.id,
            text=prompt_text,
            reply_markup=keyboard,
        )
        return STATE_DRAIN_PROMPT_REFRESH

    start_dt = request.get("start")
    end_dt = request.get("end")
    label = request.get("label") or request.get("day") or "выбранный день"
    if not isinstance(start_dt, datetime) or not isinstance(end_dt, datetime):
        await context.bot.send_message(
            chat_id=query.message.chat_id if query.message else update.effective_chat.id,
            text="❌ Не удалось восстановить выбранный период.",
        )
        return STATE_DRAIN_PROMPT_REFRESH

    chat_id = query.message.chat_id if query.message else update.effective_chat.id

    pending_range = runtime.get(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY)
    if isinstance(pending_range, list) and pending_range:
        days: List[Dict[str, Any]] = []
        for entry in pending_range:
            start_val = entry.get("start")
            end_val = entry.get("end")
            label_val = entry.get("label")

            if isinstance(start_val, datetime):
                start_entry = start_val
            elif isinstance(start_val, str):
                try:
                    start_entry = datetime.fromisoformat(start_val)
                except Exception:
                    continue
            else:
                continue
            if start_entry.tzinfo is None:
                start_entry = start_entry.replace(tzinfo=MOSCOW_TZ)

            if isinstance(end_val, datetime):
                end_entry = end_val
            elif isinstance(end_val, str):
                try:
                    end_entry = datetime.fromisoformat(end_val)
                except Exception:
                    end_entry = start_entry + timedelta(days=1)
            else:
                end_entry = start_entry + timedelta(days=1)
            if end_entry.tzinfo is None:
                end_entry = end_entry.replace(tzinfo=MOSCOW_TZ)

            day_label = str(label_val or start_entry.strftime("%d.%m.%Y"))
            days.append(
                {
                    "start": start_entry,
                    "end": end_entry,
                    "label": day_label,
                }
            )

        if days:
            range_label = str(runtime.get(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY) or label)
            return await _run_message_cache_download_range(
                update,
                context,
                days=days,
                range_label=range_label,
                chat_id=chat_id,
            )

    return await _run_message_cache_download(
        update,
        context,
        start_dt=start_dt,
        end_dt=end_dt,
        label=str(label),
        chat_id=chat_id,
    )


async def drain_message_cache_partial_use_cb(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return STATE_DRAIN_PROMPT_REFRESH
    await _safe_answer_callback(query)
    with contextlib.suppress(Exception):
        if query.message:
            await query.message.edit_reply_markup(None)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY, None)
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY, None)
    partial = runtime.get(DRAIN_ANALYSIS_MSG_PARTIAL_KEY)
    if not isinstance(partial, dict):
        await context.bot.send_message(
            chat_id=query.message.chat_id if query.message else update.effective_chat.id,
            text="❌ Частичный кэш не найден.",
        )
        return STATE_DRAIN_PROMPT_REFRESH

    day_key = partial.get("day")
    total_units = partial.get("total")
    if not isinstance(day_key, str):
        await context.bot.send_message(
            chat_id=query.message.chat_id if query.message else update.effective_chat.id,
            text="❌ Не удалось определить дату частичного кэша.",
        )
        return STATE_DRAIN_PROMPT_REFRESH

    payload = MESSAGE_CACHE.finalize_partial(day_key, expected_total=total_units, mark_incomplete=True)
    if payload is None:
        await context.bot.send_message(
            chat_id=query.message.chat_id if query.message else update.effective_chat.id,
            text="❌ Не удалось сформировать кэш из частичных данных.",
        )
        return STATE_DRAIN_PROMPT_REFRESH

    runtime.pop(DRAIN_ANALYSIS_MSG_PARTIAL_KEY, None)
    runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
    formatted_day = _format_message_cache_day(day_key)
    await context.bot.send_message(
        chat_id=query.message.chat_id if query.message else update.effective_chat.id,
        text=f"✅ Частичный кэш за {formatted_day} сохранён.\n\n" + prompt_text,
        reply_markup=keyboard,
    )
    return STATE_DRAIN_PROMPT_REFRESH


async def drain_message_cache_partial_discard_cb(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return STATE_DRAIN_PROMPT_REFRESH
    await _safe_answer_callback(query)
    with contextlib.suppress(Exception):
        if query.message:
            await query.message.edit_reply_markup(None)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY, None)
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY, None)
    partial = runtime.pop(DRAIN_ANALYSIS_MSG_PARTIAL_KEY, None)
    day_key = partial.get("day") if isinstance(partial, dict) else None
    formatted_day = _format_message_cache_day(day_key) if isinstance(day_key, str) else "выбранный день"
    if isinstance(day_key, str):
        MESSAGE_CACHE.discard_partial(day_key)
    runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
    await context.bot.send_message(
        chat_id=query.message.chat_id if query.message else update.effective_chat.id,
        text=f"🗑️ Частичный кэш за {formatted_day} удалён.\n\n" + prompt_text,
        reply_markup=keyboard,
    )
    return STATE_DRAIN_PROMPT_REFRESH


async def drain_message_cache_partial_back_cb(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return STATE_DRAIN_PROMPT_REFRESH
    await _safe_answer_callback(query)
    with contextlib.suppress(Exception):
        if query.message:
            await query.message.edit_reply_markup(None)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY, None)
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY, None)
    runtime.pop(DRAIN_ANALYSIS_MSG_PARTIAL_KEY, None)
    prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
    await context.bot.send_message(
        chat_id=query.message.chat_id if query.message else update.effective_chat.id,
        text=prompt_text,
        reply_markup=keyboard,
    )
    return STATE_DRAIN_PROMPT_REFRESH


async def drain_message_cache_delete_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.message
    if not message or not message.text:
        return STATE_DRAIN_WAIT_MSG_DELETE

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    text = message.text.strip()
    chat_id = message.chat_id

    lowered = text.casefold()
    if lowered in {"отмена", "cancel"}:
        runtime.pop("msg_action", None)
        prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
        await context.bot.send_message(chat_id=chat_id, text=prompt_text, reply_markup=keyboard)
        return STATE_DRAIN_PROMPT_REFRESH

    if lowered in {"все", "всё", "all"}:
        removed = MESSAGE_CACHE.clear_all()
        runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
        runtime.pop("msg_action", None)
        prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🧹 Удалено дней: {removed}.\n\n" + prompt_text,
            reply_markup=keyboard,
        )
        return STATE_DRAIN_PROMPT_REFRESH

    now_local = datetime.now(MOSCOW_TZ)
    parsed = _parse_drain_analysis_day(text, now_local)
    if not parsed:
        await message.reply_text(
            "❌ Не удалось распознать дату. Пример: 10.05.2025 или 10 мая."
        )
        return STATE_DRAIN_WAIT_MSG_DELETE

    day_key = parsed[0].date().isoformat()
    formatted_day = _format_message_cache_day(day_key)
    if not MESSAGE_CACHE.delete_day(day_key):
        await message.reply_text(f"ℹ️ Кэш за {formatted_day} не найден.")
        return STATE_DRAIN_WAIT_MSG_DELETE

    runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    runtime.pop("msg_action", None)
    prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🧹 Кэш за {formatted_day} удалён.\n\n" + prompt_text,
        reply_markup=keyboard,
    )
    return STATE_DRAIN_PROMPT_REFRESH


def _build_wln_days_overview(entries: List[Dict[str, Any]]) -> str:
    ready_lines: List[str] = []
    partial_lines: List[str] = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        day_value = entry.get("day")
        if not isinstance(day_value, str):
            continue
        formatted = _format_message_cache_day(day_value)
        units_total = entry.get("units") or entry.get("total_units") or entry.get("completed_units")
        suffix = f" ({units_total} объектов)" if units_total else ""
        status = entry.get("status")
        if status == "ready" and not entry.get("partial"):
            ready_lines.append(f"- {formatted}{suffix}")
        else:
            partial_lines.append(f"- {formatted}{suffix}")
    lines: List[str] = []
    if ready_lines:
        lines.append("Готовые дни:")
        lines.extend(ready_lines)
    else:
        lines.append("Готовых выгрузок пока нет.")
    if partial_lines:
        lines.append("")
        lines.append("Незавершённые выгрузки:")
        lines.extend(partial_lines)
    return "\n".join(lines)


def _collect_unit_names(snapshot: Dict[int, Dict[str, Any]]) -> Dict[int, str]:
    result: Dict[int, str] = {}
    for unit_id, payload in (snapshot or {}).items():
        try:
            numeric_id = int(unit_id)
        except (TypeError, ValueError):
            continue
        name = None
        if isinstance(payload, dict):
            name = payload.get("nm") or payload.get("name")
        result[numeric_id] = name or f"id {numeric_id}"
    return result


def _build_wln_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⏹️ Остановить экспорт", callback_data="wln:cancel")]]
    )


async def _update_wln_status_message(
    context: ContextTypes.DEFAULT_TYPE,
    runtime: Dict[str, Any],
    text: str,
    *,
    remove_keyboard: bool = False,
) -> None:
    status_info = runtime.get("status_message")
    if not isinstance(status_info, dict):
        return
    chat_id = status_info.get("chat_id")
    message_id = status_info.get("message_id")
    if chat_id is None or message_id is None:
        return
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=None if remove_keyboard else _build_wln_cancel_keyboard(),
        )
    except BadRequest as exc:
        log.debug("wln_export: status message update ignored: %s", exc)
    except Exception as exc:
        log.debug("wln_export: failed to update status message: %s", exc)


async def wln_export_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if _active_ui(context) == UI_ADMIN:
        text = ""
        if update.message and update.message.text:
            text = update.message.text.strip()
        if text == MENU_BUTTON_WLN_EXPORT:
            chat = update.effective_chat
            chat_id = chat.id if chat else None
            log.info("admin: to_wln_export user=%s -> set UI=OBJECTS", chat_id)
            panel_state = context.chat_data.get("admin_panel")
            if isinstance(panel_state, dict):
                panel_state.pop("awaiting", None)
            await _activate_objects_ui(context)
        else:
            chat = update.effective_chat
            log.debug("admin: ignore wln export trigger user=%s", chat.id if chat else None)
            return ConversationHandler.END

    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN

    await _activate_objects_ui(context)
    await delete_anchor(context)
    await clear_stats_buttons(context)

    runtime: Dict[str, Any] = context.user_data.setdefault(WLN_EXPORT_KEY, {})
    runtime.clear()
    snapshot, dump_ts = _load_cached_unit_snapshot_only()
    runtime["units_snapshot"] = snapshot
    runtime["dump_ts"] = dump_ts

    days = MESSAGE_CACHE.list_days()
    runtime["message_cache_days"] = days
    overview = _build_wln_days_overview(days)

    prompt_lines = [
        "Экспорт WLN по дню кэша.",
        overview if overview else "Готовых выгрузок пока нет.",
        "",
        "Введите дату (например, 10.10.2025), чтобы сформировать ZIP архив.",
    ]
    message_text = "\n".join(line for line in prompt_lines if line)

    chat = update.effective_chat
    if update.message:
        await update.message.reply_text(message_text)
    elif chat is not None:
        await context.bot.send_message(chat_id=chat.id, text=message_text)

    context.user_data["mode"] = MODE_NONE
    return STATE_WLN_EXPORT_WAIT_DATE


async def wln_export_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN

    message = update.message
    if not message or not message.text:
        return STATE_WLN_EXPORT_WAIT_DATE

    query = message.text.strip()
    if not query:
        return STATE_WLN_EXPORT_WAIT_DATE

    runtime: Dict[str, Any] = context.user_data.setdefault(WLN_EXPORT_KEY, {})
    now_local = datetime.now(MOSCOW_TZ)
    parsed = _parse_drain_analysis_day(query, now_local)
    if not parsed:
        await message.reply_text("Не удалось распознать дату. Примеры: 10.10.2025 или 10 октября.")
        return STATE_WLN_EXPORT_WAIT_DATE

    start_dt, end_dt, label = parsed
    day_key = start_dt.date().isoformat()
    cache_payload = MESSAGE_CACHE.load_day(day_key)
    units_payload: Dict[str, Any] = {}
    if isinstance(cache_payload, dict):
        units_payload = cache_payload.get("units") or {}
    else:
        partial_info = MESSAGE_CACHE.get_partial_info(day_key)
        if partial_info:
            units_dir = MESSAGE_CACHE._partial_units_dir(day_key)  # type: ignore[attr-defined]
            if units_dir.exists():
                for path in sorted(units_dir.glob("*.json")):
                    if not path.is_file():
                        continue
                    try:
                        with open(path, "r", encoding="utf-8") as fh:
                            payload = json.load(fh)
                    except Exception as exc:
                        log.debug("wln_export: failed to read partial unit %s: %s", path, exc)
                        continue
                    units_payload[path.stem] = payload
        if not units_payload:
            await message.reply_text(f"Нет кэша за {label}.")
            return STATE_WLN_EXPORT_WAIT_DATE

    if not units_payload:
        await message.reply_text(f"В кэше за {label} нет данных для выгрузки.")
        return STATE_WLN_EXPORT_WAIT_DATE

    snapshot = runtime.get("units_snapshot")
    if not isinstance(snapshot, dict):
        snapshot, _ = _load_cached_unit_snapshot_only()
        runtime["units_snapshot"] = snapshot
    unit_names = _collect_unit_names(snapshot or {})

    total_units = len(units_payload)
    if total_units == 0:
        await message.reply_text(f"В кэше за {label} нет данных для выгрузки.")
        return STATE_WLN_EXPORT_WAIT_DATE

    cancel_event = runtime.get("cancel_event")
    if isinstance(cancel_event, asyncio.Event):
        cancel_event.clear()
    else:
        cancel_event = asyncio.Event()
        runtime["cancel_event"] = cancel_event

    active_registry: Dict[str, Any] = runtime.setdefault(
        DRAIN_ANALYSIS_ACTIVE_SESSIONS_KEY,
        {"tasks": set(), "clients": set(), "units": set()},
    )
    active_tasks: Set[asyncio.Task[Any]] = active_registry.setdefault("tasks", set())
    active_clients: Set[Any] = active_registry.setdefault("clients", set())
    inflight_units: Set[int] = active_registry.setdefault("units", set())
    cleanup_triggered = False

    async def cleanup_active_registry() -> None:
        nonlocal cleanup_triggered
        if cleanup_triggered:
            return
        cleanup_triggered = True
        with contextlib.suppress(Exception):
            runtime.pop(DRAIN_ANALYSIS_ACTIVE_SESSIONS_KEY, None)
        for task in list(active_tasks):
            active_tasks.discard(task)

        async def _clear_client(entry: Any) -> None:
            clear_fn = getattr(entry, "clear_cancel_predicate", None)
            if callable(clear_fn):
                with contextlib.suppress(Exception):
                    result = clear_fn()
                    if inspect.isawaitable(result):
                        await result

        for client_entry in list(active_clients):
            await _clear_client(client_entry)
            active_clients.discard(client_entry)
        inflight_units.clear()

    def _register_task(task: asyncio.Task[Any]) -> asyncio.Task[Any]:
        active_tasks.add(task)
        task.add_done_callback(lambda t: active_tasks.discard(t))
        cancel_log.info("drain_analysis task_registered active=%s", len(active_tasks))
        return task

    def _register_session(session_client: Optional[Any]) -> None:
        if session_client is None:
            return
        with contextlib.suppress(Exception):
            setter = getattr(session_client, "set_cancel_predicate", None)
            if callable(setter):
                setter(cancel_event.is_set)
        active_clients.add(session_client)
        cancel_log.info(
            "drain_analysis session_registered id=%s active=%s",
            id(session_client),
            len(active_clients),
        )

    def _unregister_session(session_client: Optional[Any]) -> None:
        if session_client is None:
            return
        with contextlib.suppress(Exception):
            clearer = getattr(session_client, "clear_cancel_predicate", None)
            if callable(clearer):
                clearer()
        active_clients.discard(session_client)
        cancel_log.info(
            "drain_analysis session_unregistered id=%s active=%s",
            id(session_client),
            len(active_clients),
        )

    async def wait_with_cancel(awaitable: Awaitable[T], *, timeout: Optional[float] = None) -> T:
        if cancel_event.is_set():
            cancel_log.info("drain_analysis wait_with_cancel pre-raise due_to_cancel")
            raise asyncio.CancelledError()
        task = _register_task(asyncio.create_task(awaitable))
        try:
            if timeout is not None:
                return await asyncio.wait_for(task, timeout=timeout)
            return await task
        except asyncio.CancelledError:
            cancel_event.set()
            if not task.done():
                task.cancel()
            cancel_log.info("drain_analysis wait_with_cancel cancelled awaitable=%s", awaitable)
            raise
        finally:
            active_tasks.discard(task)

    def _register_unit(unit_id: int) -> None:
        inflight_units.add(unit_id)
        cancel_log.info("drain_analysis unit_inflight_add unit=%s active=%s", unit_id, len(inflight_units))

    def _unregister_unit(unit_id: int) -> None:
        inflight_units.discard(unit_id)
        cancel_log.info("drain_analysis unit_inflight_remove unit=%s active=%s", unit_id, len(inflight_units))

    base_setter = getattr(client, "set_cancel_predicate", None)
    if callable(base_setter):
        try:
            base_setter(cancel_event.is_set)
            active_clients.add(client)
            cancel_log.info(
                "drain_analysis base_client_registered id=%s active=%s",
                id(client),
                len(active_clients),
            )
        except Exception as exc:
            log.debug("drain_analysis: failed to set cancel predicate on base client: %s", exc)

    active_registry: Dict[str, Any] = runtime.setdefault(
        DRAIN_ANALYSIS_ACTIVE_SESSIONS_KEY,
        {"tasks": set(), "clients": set(), "units": set()},
    )
    active_tasks: Set[asyncio.Task[Any]] = active_registry.setdefault("tasks", set())
    active_clients: Set[Any] = active_registry.setdefault("clients", set())
    inflight_units: Set[int] = active_registry.setdefault("units", set())
    cleanup_triggered = False

    async def cleanup_active_registry() -> None:
        nonlocal cleanup_triggered
        if cleanup_triggered:
            return
        cleanup_triggered = True
        log.info(
            "drain_analysis: cleanup_active_registry tasks=%s clients=%s units=%s",
            len(active_tasks),
            len(active_clients),
            len(inflight_units),
        )
        cancel_log.info(
            "drain_analysis cleanup tasks=%s clients=%s units=%s",
            len(active_tasks),
            len(active_clients),
            len(inflight_units),
        )

        for task in list(active_tasks):
            active_tasks.discard(task)

        async def _clear_client(client: Any) -> None:
            clear_fn = getattr(client, "clear_cancel_predicate", None)
            if callable(clear_fn):
                with contextlib.suppress(Exception):
                    result = clear_fn()
                    if inspect.isawaitable(result):
                        await result

        for client_entry in list(active_clients):
            await _clear_client(client_entry)
            active_clients.discard(client_entry)

        inflight_units.clear()
        runtime.pop(DRAIN_ANALYSIS_ACTIVE_SESSIONS_KEY, None)

    def _register_task(task: asyncio.Task[Any]) -> asyncio.Task[Any]:
        active_tasks.add(task)
        task.add_done_callback(lambda t: active_tasks.discard(t))
        cancel_log.info("drain_analysis task_registered active=%s", len(active_tasks))
        return task

    def _register_session(session_client: Optional[Any]) -> None:
        if session_client is None:
            return
        with contextlib.suppress(Exception):
            setter = getattr(session_client, "set_cancel_predicate", None)
            if callable(setter):
                setter(cancel_event.is_set)
        active_clients.add(session_client)
        cancel_log.info(
            "drain_analysis session_registered id=%s active=%s",
            id(session_client),
            len(active_clients),
        )
        log.debug(
            "drain_analysis: register session id=%s active=%s",
            id(session_client),
            len(active_clients),
        )

    def _unregister_session(session_client: Optional[Any]) -> None:
        if session_client is None:
            return
        with contextlib.suppress(Exception):
            clearer = getattr(session_client, "clear_cancel_predicate", None)
            if callable(clearer):
                clearer()
        active_clients.discard(session_client)
        cancel_log.info(
            "drain_analysis session_unregistered id=%s active=%s",
            id(session_client),
            len(active_clients),
        )
        log.debug(
            "drain_analysis: unregister session id=%s active=%s",
            id(session_client),
            len(active_clients),
        )

    async def wait_with_cancel(awaitable: Awaitable[T], *, timeout: Optional[float] = None) -> T:
        if cancel_event.is_set():
            cancel_log.info("drain_analysis wait_with_cancel pre-raise due_to_cancel")
            raise asyncio.CancelledError()
        task = _register_task(asyncio.create_task(awaitable))
        try:
            if timeout is not None:
                return await asyncio.wait_for(task, timeout=timeout)
            return await task
        except asyncio.CancelledError:
            cancel_event.set()
            if not task.done():
                task.cancel()
            cancel_log.info("drain_analysis wait_with_cancel cancelled awaitable=%s", awaitable)
            log.info("drain_analysis: wait_with_cancel cancelled awaitable=%s", awaitable)
            raise
        finally:
            active_tasks.discard(task)

    def _register_unit(unit_id: int) -> None:
        inflight_units.add(unit_id)
        cancel_log.info("drain_analysis unit_inflight_add unit=%s active=%s", unit_id, len(inflight_units))

    def _unregister_unit(unit_id: int) -> None:
        inflight_units.discard(unit_id)
        cancel_log.info("drain_analysis unit_inflight_remove unit=%s active=%s", unit_id, len(inflight_units))

    base_setter = getattr(client, "set_cancel_predicate", None)
    if callable(base_setter):
        try:
            base_setter(cancel_event.is_set)
            active_clients.add(client)
            cancel_log.info(
                "drain_analysis base_client_registered id=%s active=%s",
                id(client),
                len(active_clients),
            )
            log.debug(
                "drain_analysis: base client registered for cancel id=%s active=%s",
                id(client),
                len(active_clients),
            )
        except Exception as exc:
            log.debug("drain_analysis: failed to set cancel predicate on base client: %s", exc)

    status_text = f"⏳ Готовлю WLN за {label}...\nОбработано: 0 из {total_units}"
    status_msg = await message.reply_text(status_text, reply_markup=_build_wln_cancel_keyboard())
    runtime["status_message"] = {"chat_id": status_msg.chat_id, "message_id": status_msg.message_id}

    processed = 0
    summary = {"units": 0, "messages": 0, "empty_units": 0}
    cancelled = False
    zip_buffer = io.BytesIO()
    progress_step = max(1, total_units // 20)  # обновляем примерно 20 раз

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for unit_id_str, payload in sorted(units_payload.items(), key=lambda item: int(item[0])):
            if cancel_event.is_set():
                cancelled = True
                break

            try:
                unit_id = int(unit_id_str)
            except Exception:
                unit_id = None

            processed += 1
            content, stats = convert_unit_to_wln(payload or {})
            if content:
                name = unit_names.get(unit_id) if unit_id is not None else None
                if not name and isinstance(payload, dict):
                    name = payload.get("name") or payload.get("nm")
                safe_name = sanitize_filename(name or f"id_{unit_id_str}")
                zip_file.writestr(f"{safe_name}.wln", content)
                summary["units"] += 1
                summary["messages"] += stats.get("messages", 0)
            else:
                summary["empty_units"] += 1

            if processed == 1 or processed == total_units or processed % progress_step == 0:
                progress_text = (
                    f"⏳ Готовлю WLN за {label}...\n"
                    f"Обработано: {processed} из {total_units}\n"
                    f"С данными: {summary['units']}, пустых: {summary['empty_units']}"
                )
                await _update_wln_status_message(context, runtime, progress_text)

        if summary["units"] == 0:
            zip_file.writestr("README.txt", "Нет сообщений для выбранной даты.\n")

    buffer = io.BytesIO(zip_buffer.getvalue())
    buffer.seek(0)
    chat_id = message.chat_id

    final_status = "✅ WLN экспорт завершён." if not cancelled else "⚠️ WLN экспорт остановлен, отправляю то, что успел собрать."
    await _update_wln_status_message(context, runtime, final_status, remove_keyboard=True)

    runtime["status_message"] = None

    if summary["units"] == 0:
        if cancelled and processed == 0:
            await message.reply_text("Экспорт остановлен. Ни один объект не успели обработать.")
        else:
            await message.reply_text(f"Нет сообщений для выгрузки за {label}.")
        runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
        cancel_event.clear()
        return STATE_WLN_EXPORT_WAIT_DATE

    zip_filename = f"{day_key}.zip"
    try:
        log.info(
            "wln_export: sending archive day=%s units=%s messages=%s cancelled=%s",
            day_key,
            summary["units"],
            summary["messages"],
            cancelled,
        )
        await context.bot.send_document(
            chat_id=chat_id,
            document=InputFile(buffer, filename=zip_filename),
            caption=f"WLN выгрузка за {label}",
        )
    except TimedOut as exc:
        log.exception("wln_export: telegram timeout while sending archive day=%s", day_key, exc_info=exc)
        await message.reply_text(f"Не удалось отправить архив: {exc}")
        cancel_event.clear()
        return STATE_WLN_EXPORT_WAIT_DATE
    except Exception as exc:
        log.exception("wln_export: failed to send archive day=%s", day_key, exc_info=exc)
        await message.reply_text(f"Не удалось отправить архив: {exc}")
        cancel_event.clear()
        return STATE_WLN_EXPORT_WAIT_DATE

    units_count = summary.get("units", 0)
    messages_count = summary.get("messages", 0)
    empty_units = summary.get("empty_units", 0)
    summary_lines = [
        f"Файл {zip_filename} отправлен{' (частично)' if cancelled else ''}.",
        f"Обработано объектов: {units_count}, сообщений: {messages_count}.",
    ]
    if empty_units:
        summary_lines.append(f"Без данных: {empty_units}.")
    if cancelled:
        summary_lines.append("Экспорт был остановлен по запросу, в архиве только обработанные объекты.")
    summary_lines.append("Введите другую дату или воспользуйтесь кнопками меню.")
    await message.reply_text("\n".join(summary_lines))

    runtime["last_export_day"] = day_key
    runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    cancel_event.clear()

    return STATE_WLN_EXPORT_WAIT_DATE


async def wln_export_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await _safe_answer_callback(query, "Останавливаю экспорт…", show_alert=False)
    runtime: Dict[str, Any] = context.user_data.setdefault(WLN_EXPORT_KEY, {})
    cancel_event = runtime.get("cancel_event")
    if isinstance(cancel_event, asyncio.Event):
        cancel_event.set()
    await _update_wln_status_message(
        context,
        runtime,
        "⏳ Останавливаю экспорт…",
        remove_keyboard=False,
    )
    return STATE_WLN_EXPORT_WAIT_DATE


async def _download_message_cache(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    client: "WialonClient",
    unit_map: Dict[int, Dict[str, Any]],
    start_local: datetime,
    end_local: datetime,
    label: str,
    range_total_days: Optional[int] = None,
) -> Optional[MessageCacheDownloadResult]:
    chat = update.effective_chat
    chat_id = chat.id if chat else None
    if chat_id is None:
        return None

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})

    cancel_event = runtime.get(DRAIN_ANALYSIS_MSG_CANCEL_KEY)
    if isinstance(cancel_event, asyncio.Event):
        cancel_event.clear()
    else:
        cancel_event = asyncio.Event()
        runtime[DRAIN_ANALYSIS_MSG_CANCEL_KEY] = cancel_event

    cancel_keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("⏹️ Остановить загрузку", callback_data="drain:msg:cancel")]]
    )

    status_text = f"💾 Загружаю сообщения за {label}…"
    status_msg = await edit_or_post_status_message(
        context,
        runtime,
        chat_id,
        None,
        status_text,
        status_key=DRAIN_ANALYSIS_MSG_STATUS_KEY,
        logger=log,
        reply_markup=cancel_keyboard,
    )

    start_ts = int(start_local.astimezone(timezone.utc).timestamp())
    end_ts = int(end_local.astimezone(timezone.utc).timestamp())
    day_key = start_local.date().isoformat()

    units_sorted = sorted(
        unit_map.items(),
        key=lambda item: (
            str((item[1].get("nm") or item[1].get("name") or item[1].get("n") or "").strip()).casefold(),
            item[0],
        ),
    )

    valid_units: List[Tuple[int, Dict[str, Any]]] = []
    for unit_id_raw, unit_item_raw in units_sorted:
        try:
            unit_id = int(unit_id_raw)
        except Exception:
            continue
        if not isinstance(unit_item_raw, dict):
            continue
        copy_item = copy.deepcopy(unit_item_raw)
        copy_item.setdefault("id", unit_id)
        valid_units.append((unit_id, copy_item))

    total_units = len(valid_units)

    existing_info = MESSAGE_CACHE.get_partial_info(day_key)
    if not existing_info:
        MESSAGE_CACHE.ensure_partial(
            day_key,
            start_ts=start_ts,
            end_ts=end_ts,
            label=label,
            total_units=total_units,
        )
        existing_success: Set[int] = set()
        existing_failures: Set[int] = set()
    else:
        existing_success = set(existing_info.get("units") or [])
        existing_failures = set(existing_info.get("failures") or [])
        MESSAGE_CACHE.ensure_partial(
            day_key,
            start_ts=start_ts,
            end_ts=end_ts,
            label=label,
            total_units=total_units,
        )

    message_load_log.info(
        "download_start day=%s label=%s total_units=%s existing=%s failures=%s cancel=%s",
        day_key,
        label,
        total_units,
        len(existing_success),
        len(existing_failures),
        cancel_event.is_set(),
    )

    if total_units == 0:
        finalize_error = False
        payload = None
        try:
            payload = MESSAGE_CACHE.finalize_partial(day_key, expected_total=0, mark_incomplete=False)
        except Exception as exc:
            finalize_error = True
            log.exception("message_cache: finalize_partial failed day=%s", day_key)
            message_load_log.error(
                "finalize_error day=%s label=%s err=%s",
                day_key,
                label,
                getattr(exc, "message", None) or str(exc),
            )
        progress_updates_locked = True
        completion_text = build_success_message(0, 0)
        try:
            await edit_or_post_status_message(
                context,
                runtime,
                chat_id,
                status_msg,
                completion_text,
                status_key=DRAIN_ANALYSIS_MSG_STATUS_KEY,
                logger=log,
                reply_markup=None,
            )
        except Exception as exc:
            log.exception("message_cache: status post failed day=%s err=%s", day_key, exc)
        finish_status = "completed" if not finalize_error else "partial"
        message_load_log.info(
            "download_finish day=%s label=%s status=%s completed=%s total=%s failed=%s finalize_error=%s",
            day_key,
            label,
            finish_status,
            0,
            0,
            0,
            finalize_error,
        )
        return MessageCacheDownloadResult(
            status=finish_status,
            day=day_key,
            label=label,
            start_ts=start_ts,
            end_ts=end_ts,
            completed=0,
            total=0,
            failed=0,
            payload=payload,
        )

    MESSAGE_CACHE.update_partial_meta(
        day_key,
        status="in_progress",
        completed=len(existing_success),
        total=total_units,
        failed=len(existing_failures),
    )

    await run_blocking(_load_access_denied_cache_sync)
    cached_access_denied_units = await run_blocking(_get_access_denied_cache_snapshot)

    message_lock = asyncio.Lock()
    progress_lock = asyncio.Lock()
    status_update_lock = asyncio.Lock()
    tracker_state_lock = asyncio.Lock()
    progress_tracker_event = asyncio.Event()
    progress_count = len(existing_success)
    progress_updates_locked = False
    progress_tracker_state = {
        "current": progress_count,
        "last_sent": -1,
        "force": False,
        "stopped": False,
        "last_update_ts": time.perf_counter(),
        "task": None,
    }
    meta_write_lock = asyncio.Lock()
    session_failures: Set[int] = set()
    MAX_UNIT_RETRIES = 3
    active_tasks: Set[asyncio.Task[Any]] = set()
    active_sessions: Set[Any] = set()
    inflight_units: Set[int] = set()
    token_cursor = 0

    def _register_task(task: asyncio.Task[Any]) -> asyncio.Task[Any]:
        active_tasks.add(task)
        task.add_done_callback(lambda t: active_tasks.discard(t))
        return task

    def _register_session(session_client: Optional[Any]) -> None:
        if session_client is None:
            return
        setter = getattr(session_client, "set_cancel_predicate", None)
        if callable(setter):
            with contextlib.suppress(Exception):
                setter(cancel_event.is_set)
        active_sessions.add(session_client)

    def _unregister_session(session_client: Optional[Any]) -> None:
        if session_client is None:
            return
        clearer = getattr(session_client, "clear_cancel_predicate", None)
        if callable(clearer):
            with contextlib.suppress(Exception):
                clearer()
        active_sessions.discard(session_client)

    def _register_unit(unit_id: int) -> None:
        inflight_units.add(unit_id)
        cancel_log.info("message_cache unit_inflight_add unit=%s active=%s", unit_id, len(inflight_units))

    def _unregister_unit(unit_id: int) -> None:
        inflight_units.discard(unit_id)
        cancel_log.info("message_cache unit_inflight_remove unit=%s active=%s", unit_id, len(inflight_units))

    async def cleanup_active_registry() -> None:
        for task in list(active_tasks):
            active_tasks.discard(task)
        for session_client in list(active_sessions):
            _unregister_session(session_client)
        inflight_units.clear()

    async def _progress_tracker_request(value: int, *, force: bool = False) -> None:
        bounded = max(0, min(total_units, value))
        async with tracker_state_lock:
            progress_tracker_state["current"] = bounded
            if force:
                progress_tracker_state["force"] = True
            progress_tracker_event.set()

    async def _progress_tracker_flush() -> None:
        nonlocal status_msg, progress_updates_locked
        async with tracker_state_lock:
            current = progress_tracker_state["current"]
            last_sent = progress_tracker_state["last_sent"]
            force_flag = progress_tracker_state["force"] or progress_tracker_state["stopped"]
            progress_tracker_state["force"] = False
            last_update_ts = progress_tracker_state["last_update_ts"]
        if progress_updates_locked:
            async with tracker_state_lock:
                progress_tracker_state["last_sent"] = current
                progress_tracker_state["last_update_ts"] = time.perf_counter()
            return
        now = time.perf_counter()
        if not force_flag:
            if current <= last_sent:
                return
            delta = current - last_sent
            if (
                current < total_units
                and delta < DRAIN_ANALYSIS_PROGRESS_FORCE_STEP
                and now - last_update_ts < DRAIN_ANALYSIS_PROGRESS_REFRESH_SECONDS
            ):
                return
        else:
            if current < last_sent:
                current = last_sent
        delta = current - last_sent
        progress_text = (
            f"\U0001F4BE \u0417\u0430\u0433\u0440\u0443\u0436\u0430\u044e \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f \u0437\u0430 {label}\u2026\n"
            f"\u0413\u043e\u0442\u043e\u0432\u043e \u043e\u0431\u044a\u0435\u043a\u0442\u043e\u0432: {current}/{total_units}"
        )
        message_load_log.debug(
            "progress_tracker_update day=%s value=%s delta=%s force=%s",
            day_key,
            current,
            delta,
            force_flag,
        )
        async with status_update_lock:
            status_msg = await edit_or_post_status_message(
                context,
                runtime,
                chat_id,
                status_msg,
                progress_text,
                status_key=DRAIN_ANALYSIS_MSG_STATUS_KEY,
                logger=log,
                reply_markup=None if cancel_event.is_set() else cancel_keyboard,
            )
        async with tracker_state_lock:
            progress_tracker_state["last_sent"] = current
            progress_tracker_state["last_update_ts"] = now

    async def _progress_tracker_worker() -> None:
        while True:
            try:
                await asyncio.wait_for(
                    progress_tracker_event.wait(), timeout=DRAIN_ANALYSIS_PROGRESS_REFRESH_SECONDS
                )
            except asyncio.TimeoutError:
                async with tracker_state_lock:
                    if progress_tracker_state["stopped"]:
                        break
                    has_progress = progress_tracker_state["current"] > progress_tracker_state["last_sent"]
                if not has_progress:
                    continue
            else:
                progress_tracker_event.clear()
            await _progress_tracker_flush()
            async with tracker_state_lock:
                if (
                    progress_tracker_state["stopped"]
                    and progress_tracker_state["current"] <= progress_tracker_state["last_sent"]
                ):
                    break

    async def _progress_tracker_start() -> None:
        if progress_tracker_state["task"] is None:
            progress_tracker_state["task"] = asyncio.create_task(_progress_tracker_worker())

    async def _progress_tracker_stop(*, force: bool = False) -> None:
        if progress_tracker_state["task"] is None:
            return
        async with tracker_state_lock:
            progress_tracker_state["stopped"] = True
            if force:
                progress_tracker_state["force"] = True
        progress_tracker_event.set()
        try:
            await progress_tracker_state["task"]
        finally:
            progress_tracker_state["task"] = None

    await _progress_tracker_start()

    async def refresh_partial_meta(status: Optional[str] = None) -> None:
        async with meta_write_lock:
            await run_blocking(
                MESSAGE_CACHE.update_partial_meta,
                day_key,
                status=status,
                completed=progress_count,
                total=total_units,
                failed=len(session_failures),
            )

    def log_store_event(unit_id: int, **fields: Any) -> None:
        try:
            payload = {"day": day_key, "unit": unit_id}
            payload.update(fields)
            message_cache_store_log.info(
                "event=%s", json.dumps(payload, ensure_ascii=False)
            )
        except Exception:
            pass

    async def record_access_denied_result(unit_id: int, sensor_ids: List[int], cached: bool) -> None:
        entry = {
            "sensor_ids": list(sensor_ids or []),
            "series": {},
            "stats": {},
            "speed_series": [],
            "aux_series": {},
            "samples": [],
            "msg_count": 0,
            "index_from": 0,
            "index_to": 0,
            "width_hint": None,
            "had_sensors": bool(sensor_ids),
            "no_sensors": not sensor_ids,
            "no_data": True,
            "failed": False,
            "access_denied": True,
        }
        await run_blocking(MESSAGE_CACHE.store_partial_unit, day_key, unit_id, entry)
        await run_blocking(MESSAGE_CACHE.clear_partial_failure, day_key, unit_id)
        if cached:
            log_store_event(
                unit_id,
                reason="access_denied_cached",
                sensor_count=len(sensor_ids or []),
                sensor_ids=list(sensor_ids or []),
                cached=True,
            )
        tag = "cached" if cached else "new"
        message_load_log.info(
            "access_denied_%s day=%s unit=%s sensors=%s",
            tag,
            day_key,
            unit_id,
            len(sensor_ids or []),
        )
        await ACCESS_DENIED_ASYNC_LOCK.acquire()
        try:
            await run_blocking(_record_access_denied_cache_sync, unit_id)
        finally:
            ACCESS_DENIED_ASYNC_LOCK.release()

    def build_success_message(completed: int, failed: int) -> str:
        lines = [
            f"\u2705 \u041a\u044d\u0448 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439 \u0437\u0430 {label} \u0433\u043e\u0442\u043e\u0432. \u041e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u043d\u043e \u043e\u0431\u044a\u0435\u043a\u0442\u043e\u0432: {completed}"
        ]
        if failed:
            lines.append(f"\u26a0\ufe0f \u041e\u0448\u0438\u0431\u043a\u0438 \u043f\u0440\u0438 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0435: {failed}")
        return "\n".join(lines)

    async def update_progress(current: int, *, force: bool = False) -> None:
        if total_units == 0:
            return
        await _progress_tracker_request(current, force=force)

    async def advance_progress() -> None:
        nonlocal progress_count
        async with progress_lock:
            progress_count = min(total_units, progress_count + 1)
            current = progress_count
        await update_progress(current)

    auto_completed_units: List[int] = []
    if cached_access_denied_units:
        for unit_id, unit_item in valid_units:
            if unit_id in cached_access_denied_units and unit_id not in existing_success:
                try:
                    dut_sensors, _dut_summary, _ = _prepare_dut_for_analysis(unit_item or {})
                except Exception as exc:
                    log.debug("message_cache: dut prepare failed for cached access_denied unit=%s err=%s", unit_id, exc)
                    dut_sensors = []
                sensor_ids = [
                    meta.id
                    for meta in dut_sensors
                    if getattr(meta, "id", None) is not None
                ]
                await record_access_denied_result(unit_id, sensor_ids, cached=True)
                existing_success.add(unit_id)
                progress_count = min(total_units, progress_count + 1)
                auto_completed_units.append(unit_id)
        if auto_completed_units:
            await update_progress(progress_count, force=True)
            await run_blocking(
                MESSAGE_CACHE.update_partial_meta,
                day_key,
                status="in_progress",
                completed=len(existing_success),
                total=total_units,
                failed=len(existing_failures),
            )
            message_load_log.info(
                "access_denied_cached_summary day=%s count=%s",
                day_key,
                len(auto_completed_units),
            )

    pending_units = [(unit_id, unit_item) for unit_id, unit_item in valid_units if unit_id not in existing_success]

    if not pending_units and len(existing_success) >= total_units:
        finalize_error = False
        payload = None
        try:
            payload = MESSAGE_CACHE.finalize_partial(
                day_key,
                expected_total=total_units,
                mark_incomplete=bool(existing_failures),
            )
        except Exception as exc:
            finalize_error = True
            log.exception("message_cache: finalize_partial failed day=%s", day_key)
            message_load_log.error(
                "finalize_error day=%s label=%s err=%s",
                day_key,
                label,
                getattr(exc, "message", None) or str(exc),
            )
        completed_total = len(existing_success)
        failed_total = len(existing_failures)
        await update_progress(completed_total, force=True)
        progress_updates_locked = True
        completion_text = build_success_message(completed_total, failed_total)
        try:
            await edit_or_post_status_message(
                context,
                runtime,
                chat_id,
                status_msg,
                completion_text,
                status_key=DRAIN_ANALYSIS_MSG_STATUS_KEY,
                logger=log,
                reply_markup=None,
            )
        except Exception as exc:
            log.exception("message_cache: status post failed day=%s err=%s", day_key, exc)
        finish_status = "completed" if not finalize_error else "partial"
        message_load_log.info(
            "download_finish day=%s label=%s status=%s completed=%s total=%s failed=%s finalize_error=%s",
            day_key,
            label,
            finish_status,
            completed_total,
            total_units,
            failed_total,
            finalize_error,
        )
        return MessageCacheDownloadResult(
            status=finish_status,
            day=day_key,
            label=label,
            start_ts=start_ts,
            end_ts=end_ts,
            completed=completed_total,
            total=total_units,
            failed=failed_total,
            payload=payload,
        )

    @dataclass
    class TokenContext:
        label: str
        client: "WialonClient"
        owns_client: bool
        token_tail: str
        batches: int = 0
        processed_units: int = 0
        cooldown_until: float = 0.0
        last_error_status: Optional[int] = None
        batch_limit: int = WIALON_BATCH_INITIAL_LIMIT
        last_avg: float = 0.0
        last_stage: str = "init"
        slow_streak: int = 0
        busy: bool = False
        avg_samples: Deque[float] = field(default_factory=lambda: deque(maxlen=WIALON_TOKEN_AVG_WINDOW))

        def schedule_cooldown(self, duration: float) -> None:
            self.cooldown_until = max(self.cooldown_until, time.monotonic() + duration)
            self.last_error_status = None

        def schedule_error_cooldown(self, duration: float, status: Optional[int]) -> None:
            self.cooldown_until = max(self.cooldown_until, time.monotonic() + duration)
            self.last_error_status = status

        def adjust_after_batch(self, avg_unit: float) -> None:
            self.last_avg = avg_unit
            if self.avg_samples.maxlen != WIALON_TOKEN_AVG_WINDOW:
                self.avg_samples = deque(self.avg_samples, maxlen=WIALON_TOKEN_AVG_WINDOW)
            self.avg_samples.append(avg_unit)
            new_limit = self.batch_limit
            if avg_unit >= WIALON_BATCH_SLOW_THRESHOLD and self.batch_limit > WIALON_BATCH_MIN_SIZE:
                new_limit = max(WIALON_BATCH_MIN_SIZE, int(self.batch_limit * 0.7))
            elif avg_unit <= WIALON_BATCH_FAST_THRESHOLD and self.batch_limit < WIALON_TOKEN_ROTATE_AFTER_UNITS:
                growth = 1.2 if avg_unit > (WIALON_BATCH_FAST_THRESHOLD / 2) else 1.35
                new_limit = min(
                    WIALON_TOKEN_ROTATE_AFTER_UNITS,
                    max(WIALON_BATCH_MIN_SIZE, int(self.batch_limit * growth)),
                )
            if new_limit != self.batch_limit:
                token_rotation_log.info(
                    "token_adjust day=%s label=%s old_limit=%s new_limit=%s avg_unit=%.3fs",
                    day_key,
                    self.label,
                    self.batch_limit,
                    new_limit,
                    avg_unit,
                )
                self.batch_limit = new_limit

        def effective_avg(self) -> float:
            if self.avg_samples:
                return sum(self.avg_samples) / len(self.avg_samples)
            return self.last_avg

    @dataclass
    class UnitResult:
        unit_id: int
        status: str
        retryable: bool
        error_status: Optional[int] = None
        duration: float = 0.0

    units_queue: Deque[Tuple[int, Dict[str, Any]]] = deque(pending_units)
    retry_counters: Dict[int, int] = {}
    unit_payloads: Dict[int, Dict[str, Any]] = {unit_id: unit_item for unit_id, unit_item in pending_units}

    token_contexts: List[TokenContext] = []

    def _make_token_context(label: str, token_client: "WialonClient", owns: bool) -> TokenContext:
        token_tail = (getattr(token_client, "token", "") or "")[-4:]
        ctx = TokenContext(label=label, client=token_client, owns_client=owns, token_tail=token_tail)
        ctx.batch_limit = min(WIALON_TOKEN_ROTATE_AFTER_UNITS, WIALON_BATCH_INITIAL_LIMIT)
        return ctx

    primary_clone: Optional["WialonClient"] = None
    try:
        primary_clone = client.spawn_subsession()
    except Exception as exc:
        log.debug("message_cache: failed to spawn subsession for primary token: %s", exc)
        primary_clone = None

    if primary_clone is not None:
        token_contexts.append(_make_token_context("primary", primary_clone, True))
    else:
        token_contexts.append(_make_token_context("primary", client, False))

    primary_token_value = getattr(client, "token", None)
    for idx, extra_token in enumerate(WIALON_EXTRA_TOKENS, start=1):
        cleaned = extra_token.strip()
        if not cleaned:
            continue
        if primary_token_value and cleaned == primary_token_value:
            continue
        try:
            extra_client = WialonClient(client.host, cleaned)
        except Exception as exc:
            log.warning("message_cache: failed to initialize extra token %s: %s", idx, exc)
            continue
        token_contexts.append(_make_token_context(f"extra-{idx}", extra_client, True))

    if not token_contexts:
        log.error("message_cache: no available tokens for download")
        return MessageCacheDownloadResult(
            status="error",
            day=day_key,
            label=label,
            start_ts=start_ts,
            end_ts=end_ts,
            completed=len(existing_success),
            total=total_units,
            failed=len(existing_failures),
            payload=None,
        )

    total_batches_estimate = max(
        1, math.ceil(len(pending_units) / max(1, WIALON_TOKEN_ROTATE_AFTER_UNITS))
    )
    message_load_log.info(
        "download_plan day=%s label=%s pending=%s batches=%s tokens=%s rotate_after=%s",
        day_key,
        label,
        len(pending_units),
        total_batches_estimate,
        len(token_contexts),
        WIALON_TOKEN_ROTATE_AFTER_UNITS,
    )

    await update_progress(progress_count, force=True)

    derived_concurrency = len(token_contexts) * DRAIN_ANALYSIS_CONCURRENCY_PER_TOKEN
    target_concurrency = max(DRAIN_ANALYSIS_CONCURRENCY, derived_concurrency)
    target_concurrency = min(DRAIN_ANALYSIS_CONCURRENCY_MAX, target_concurrency)
    concurrency_limit = max(1, min(total_units, target_concurrency))
    parallel_token_limit = max(1, min(len(token_contexts), WIALON_PARALLEL_TOKEN_LIMIT))
    session_pool_size = max(
        1,
        min(
            concurrency_limit,
            DRAIN_ANALYSIS_SESSION_POOL_BASE
            + parallel_token_limit * DRAIN_ANALYSIS_SESSION_POOL_PER_TOKEN,
        ),
    )
    message_load_log.info(
        "concurrency_plan day=%s tokens=%s concurrency=%s session_pool=%s",
        day_key,
        len(token_contexts),
        concurrency_limit,
        session_pool_size,
    )
    message_load_log.info(
        "parallel_plan day=%s tokens=%s parallel_limit=%s warmup=%s unit_semaphore=%s",
        day_key,
        len(token_contexts),
        parallel_token_limit,
        WIALON_PARALLEL_TOKEN_WARMUP,
        concurrency_limit,
    )
    unit_semaphore = asyncio.Semaphore(concurrency_limit)

    def _pick_ready_token(now: float) -> Optional[TokenContext]:
        nonlocal token_cursor
        total = len(token_contexts)
        if total == 0:
            return None
        for _ in range(total):
            ctx = token_contexts[token_cursor]
            token_cursor = (token_cursor + 1) % total
            if ctx.busy:
                continue
            if now >= ctx.cooldown_until:
                ctx.busy = True
                return ctx
        return None

    def _next_token_wait(now: float) -> Optional[float]:
        candidates = [
            ctx.cooldown_until - now
            for ctx in token_contexts
            if not ctx.busy and ctx.cooldown_until > now
        ]
        if not candidates:
            return None
        return max(0.05, min(candidates))

    async def _sleep_until_token_ready() -> None:
        delay = _next_token_wait(time.monotonic())
        if delay is None:
            delay = 0.25
        token_rotation_log.info(
            "token_wait day=%s label=%s sleep=%.2fs",
            day_key,
            label,
            delay,
        )
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            pass

    async def run_batch_with_token(
        token_ctx: TokenContext,
        batch_units: List[Tuple[int, Dict[str, Any]]],
        batch_index: int,
    ) -> Dict[str, Any]:
        nonlocal progress_count

        batch_start_time = time.perf_counter()
        error_statuses: Set[int] = set()
        retry_units: List[Tuple[int, Dict[str, Any], Optional[int]]] = []
        failures: List[int] = []
        successes = 0

        batch_result_payload: Optional[Dict[str, Any]] = None
        try:
            token_ctx.last_stage = "batch_start"
            token_ctx.batches += 1
            token_ctx.processed_units += len(batch_units)

            token_rotation_log.info(
                "batch_start day=%s idx=%s label=%s token_tail=%s units=%s rotate_after=%s",
                day_key,
                batch_index,
                token_ctx.label,
                token_ctx.token_tail,
                len(batch_units),
                WIALON_TOKEN_ROTATE_AFTER_UNITS,
            )

            try:
                session_pool = await WialonSessionPool.create(token_ctx.client, session_pool_size)
            except Exception as exc:
                log.error(
                    "message_cache: failed to create session pool for token %s: %s",
                    token_ctx.label,
                    exc,
                )
                for unit_id, unit_item in batch_units:
                    retry_units.append((unit_id, unit_item, None))
                token_ctx.last_stage = "session_pool_error"
                result = {
                    "processed": 0,
                    "success": successes,
                    "retry_units": retry_units,
                    "failures": failures,
                    "error_statuses": {None},
                    "duration": time.perf_counter() - batch_start_time,
                }
                token_rotation_log.warning(
                    "batch_abort day=%s idx=%s label=%s reason=session_pool_error processed=%s retry=%s failures=%s",
                    day_key,
                    batch_index,
                    token_ctx.label,
                    result["processed"],
                    len(result["retry_units"]),
                    len(result["failures"]),
                )
                batch_result_payload = result
                return result
            token_ctx.last_stage = "session_pool_ready"

            async def process_unit(unit_id: int, unit_item: Dict[str, Any]) -> UnitResult:
                if cancel_event.is_set():
                    return UnitResult(unit_id=unit_id, status="cancelled", retryable=True, error_status=None)

                unit_start = time.perf_counter()
                had_sensors = False
                session_client: Optional["WialonClient"] = None
                client_broken = False
                load_result: Optional[
                    Tuple[
                        Dict[int, List[Tuple[int, float]]],
                        Dict[int, Dict[str, Any]],
                        int,
                        int,
                        int,
                        Optional[int],
                        List[Tuple[int, float]],
                        Dict[str, List[Tuple[int, float]]],
                        List[Dict[str, Any]],
                    ]
                ] = None

                try:
                    dut_sensors, dut_summary, _ = _prepare_dut_for_analysis(unit_item or {})
                except Exception as exc:
                    log.debug("message_cache: dut prepare failed unit=%s: %s", unit_id, exc)
                    await run_blocking(MESSAGE_CACHE.record_partial_failure, day_key, unit_id)
                    return UnitResult(
                        unit_id=unit_id,
                        status="failure",
                        retryable=False,
                        error_status=None,
                        duration=time.perf_counter() - unit_start,
                    )

                sensor_ids = [meta.id for meta in dut_sensors if getattr(meta, "id", None) is not None]
                if not sensor_ids:
                    entry = {
                        "sensor_ids": [],
                        "series": {},
                        "stats": {},
                        "speed_series": [],
                        "aux_series": {},
                        "samples": [],
                        "msg_count": 0,
                        "index_from": 0,
                        "index_to": 0,
                        "width_hint": None,
                        "had_sensors": False,
                        "no_sensors": True,
                        "no_data": False,
                        "failed": False,
                    }
                    await run_blocking(MESSAGE_CACHE.store_partial_unit, day_key, unit_id, entry)
                    await run_blocking(MESSAGE_CACHE.clear_partial_failure, day_key, unit_id)
                    return UnitResult(
                        unit_id=unit_id,
                        status="success",
                        retryable=False,
                        error_status=None,
                        duration=time.perf_counter() - unit_start,
                    )

                had_sensors = True
                target_map = dut_summary.get("target_map") if isinstance(dut_summary, dict) else None
                if not isinstance(target_map, dict):
                    target_map = None

                error_status: Optional[int] = None
                error_retryable = False

                try:
                    async with message_lock:
                        session_client = await session_pool.acquire()
                        _register_session(session_client)
                    try:
                        load_result = await asyncio.wait_for(
                            messages_loader.load_messages_and_series(
                                session_client,
                                unit_id,
                                sensor_ids,
                                start_ts,
                                end_ts,
                                dut_targets=target_map,
                                allow_remote_series=False,
                                request_timeout=DRAIN_ANALYSIS_MESSAGE_LOAD_TIMEOUT,
                            ),
                            timeout=DRAIN_ANALYSIS_MESSAGE_LOAD_TIMEOUT,
                        )
                    except messages_loader.AccessDeniedError as exc:
                        log_store_event(
                            unit_id,
                            reason="access_denied",
                            sensor_count=len(sensor_ids),
                            sensor_ids=sensor_ids,
                            note=str(exc),
                        )
                        await record_access_denied_result(unit_id, sensor_ids, cached=False)
                        return UnitResult(
                            unit_id=unit_id,
                            status="success",
                            retryable=False,
                            error_status=None,
                            duration=time.perf_counter() - unit_start,
                        )
                    except asyncio.TimeoutError:
                        client_broken = True
                        error_retryable = True
                        message_load_log.warning(
                            "load_failure day=%s unit=%s reason=timeout sensors=%s",
                            day_key,
                            unit_id,
                            len(sensor_ids),
                        )
                        await run_blocking(MESSAGE_CACHE.record_partial_failure, day_key, unit_id)
                        return UnitResult(
                            unit_id=unit_id,
                            status="failure",
                            retryable=True,
                            error_status=None,
                            duration=time.perf_counter() - unit_start,
                        )
                    except asyncio.CancelledError:
                        client_broken = True
                        raise
                    except Exception as exc:
                        client_broken = True
                        status_code = None
                        if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
                            status_code = exc.response.status_code
                        error_status = status_code
                        error_retryable = (
                            status_code in WIALON_TOKEN_ROTATE_HTTP_STATUSES
                            if status_code is not None
                            else isinstance(exc, httpx.HTTPError)
                        )
                        message_load_log.warning(
                            "load_failure day=%s unit=%s reason=exception:%s msg=%s sensors=%s",
                            day_key,
                            unit_id,
                            exc.__class__.__name__,
                            getattr(exc, "message", None) or str(exc),
                            len(sensor_ids),
                        )
                        await run_blocking(MESSAGE_CACHE.record_partial_failure, day_key, unit_id)
                        log.debug("message_cache: load failed unit=%s: %s", unit_id, exc)
                        return UnitResult(
                            unit_id=unit_id,
                            status="failure",
                            retryable=error_retryable,
                            error_status=status_code,
                            duration=time.perf_counter() - unit_start,
                        )
                finally:
                    if session_client is not None:
                        try:
                            if load_result is not None and not client_broken:
                                await asyncio.wait_for(
                                    run_blocking(session_client.unload_messages, unit_id),
                                    timeout=DRAIN_ANALYSIS_UNLOAD_TIMEOUT,
                                )
                        except Exception as exc:
                            log.debug("message_cache: unload failed unit=%s: %s", unit_id, exc)
                            client_broken = True
                        session_pool.recycle(session_client, broken=client_broken)
                        _unregister_session(session_client)

                if load_result is None:
                    return UnitResult(
                        unit_id=unit_id,
                        status="failure",
                        retryable=error_retryable,
                        error_status=error_status,
                        duration=time.perf_counter() - unit_start,
                    )

                (
                    series_by_sensor,
                    stats_by_sensor,
                    index_from,
                    index_to,
                    msg_count,
                    width_hint,
                    speed_series,
                    aux_series,
                    samples,
                ) = load_result

                if had_sensors:
                    entry = {
                        "sensor_ids": sensor_ids,
                        "series": {sid: series_by_sensor.get(sid, []) for sid in sensor_ids},
                        "stats": {sid: stats_by_sensor.get(sid, {}) for sid in sensor_ids},
                        "speed_series": speed_series,
                        "aux_series": aux_series,
                        "samples": samples,
                        "msg_count": msg_count,
                        "index_from": index_from,
                        "index_to": index_to,
                        "width_hint": width_hint,
                        "had_sensors": True,
                        "no_sensors": False,
                        "no_data": all(not series_by_sensor.get(sid) for sid in sensor_ids),
                        "failed": False,
                    }
                else:
                    entry = {
                        "sensor_ids": [],
                        "series": {},
                        "stats": {},
                        "speed_series": [],
                        "aux_series": {},
                        "samples": [],
                        "msg_count": 0,
                        "index_from": index_from,
                        "index_to": index_to,
                        "width_hint": width_hint,
                        "had_sensors": False,
                        "no_sensors": True,
                        "no_data": True,
                        "failed": False,
                    }

                await run_blocking(MESSAGE_CACHE.store_partial_unit, day_key, unit_id, entry)
                await run_blocking(MESSAGE_CACHE.clear_partial_failure, day_key, unit_id)
                return UnitResult(
                    unit_id=unit_id,
                    status="success",
                    retryable=False,
                    error_status=None,
                    duration=time.perf_counter() - unit_start,
                )

            async def run_unit(unit_id: int, unit_item: Dict[str, Any]) -> UnitResult:
                async with unit_semaphore:
                    if cancel_event.is_set():
                        return UnitResult(unit_id=unit_id, status="cancelled", retryable=True, error_status=None)
                    _register_unit(unit_id)
                    try:
                        return await process_unit(unit_id, unit_item)
                    finally:
                        _unregister_unit(unit_id)

            task_map: Dict[asyncio.Task[UnitResult], Tuple[int, Dict[str, Any]]] = {}
            all_tasks: List[asyncio.Task[UnitResult]] = []
            for unit_id, unit_item in batch_units:
                task = _register_task(asyncio.create_task(run_unit(unit_id, unit_item)))
                task_map[task] = (unit_id, unit_item)
                all_tasks.append(task)
            token_ctx.last_stage = "tasks_spawned"

            try:
                while task_map:
                    if cancel_event.is_set():
                        token_ctx.last_stage = "cancel_requested"
                        for pending_task in task_map:
                            if not pending_task.done():
                                pending_task.cancel()
                        break
                    token_ctx.last_stage = "processing"
                    done, _pending = await asyncio.wait(
                        list(task_map.keys()), return_when=asyncio.FIRST_COMPLETED
                    )
                    for completed_task in done:
                        meta = task_map.pop(completed_task, None)
                        if meta is None:
                            continue
                        unit_id, unit_item = meta
                        try:
                            result = completed_task.result()
                        except asyncio.CancelledError:
                            continue
                        except Exception as exc:  # pragma: no cover - defensive
                            log.debug(
                                "message_cache: run_unit crashed unit=%s err=%s",
                                unit_id,
                                exc,
                            )
                            session_failures.add(unit_id)
                            await advance_progress()
                            await refresh_partial_meta()
                            continue

                        if result.status == "success":
                            successes += 1
                            await advance_progress()
                            await refresh_partial_meta()
                        elif result.status == "failure":
                            if result.retryable and not cancel_event.is_set():
                                retry_units.append((unit_id, unit_item, result.error_status))
                                if result.error_status is not None:
                                    error_statuses.add(result.error_status)
                            else:
                                failures.append(unit_id)
                                session_failures.add(unit_id)
                                await advance_progress()
                                await refresh_partial_meta()
                                if result.error_status is not None:
                                    error_statuses.add(result.error_status)
                        elif result.status == "cancelled":
                            retry_units.append((unit_id, unit_item, None))

                if cancel_event.is_set():
                    for pending_task, meta in task_map.items():
                        unit_id, _unit_item = meta
                        if not pending_task.done():
                            pending_task.cancel()
                        session_failures.add(unit_id)
            finally:
                await asyncio.gather(*all_tasks, return_exceptions=True)
                await session_pool.aclose()

                if cancel_event.is_set():
                    token_ctx.last_stage = "cancelled"
                else:
                    token_ctx.last_stage = "loop_complete"

            duration = time.perf_counter() - batch_start_time
            avg_unit = duration / len(batch_units) if batch_units else 0.0
            token_ctx.adjust_after_batch(avg_unit)
            effective_avg = token_ctx.effective_avg()

            hard_cooldown = False
            hard_reason: Optional[str] = None
            soft_cooldown = False
            soft_reason: Optional[str] = None
            if batch_units and not cancel_event.is_set():
                is_slow = (
                    effective_avg >= WIALON_TOKEN_HARD_COOLDOWN_AVG_THRESHOLD
                    or duration >= WIALON_TOKEN_HARD_COOLDOWN_DURATION
                )
                if is_slow:
                    token_ctx.slow_streak += 1
                    token_rotation_log.debug(
                        "token_slow_batch day=%s idx=%s label=%s avg=%.3fs dur=%.2fs streak=%s limit=%s",
                        day_key,
                        batch_index,
                        token_ctx.label,
                        effective_avg,
                        duration,
                        token_ctx.slow_streak,
                        token_ctx.batch_limit,
                    )
                    if token_ctx.slow_streak >= WIALON_TOKEN_HARD_COOLDOWN_STREAK:
                        hard_cooldown = True
                        hard_reason = f"avg={effective_avg:.3f}s dur={duration:.1f}s streak={token_ctx.slow_streak}"
                        token_ctx.slow_streak = 0
                else:
                    token_ctx.slow_streak = 0
                if not hard_cooldown:
                    soft_cooldown = (
                        effective_avg >= WIALON_TOKEN_SOFT_COOLDOWN_AVG_THRESHOLD
                        or duration >= WIALON_TOKEN_SOFT_COOLDOWN_DURATION
                    )
                    if soft_cooldown:
                        soft_reason = f"avg={effective_avg:.3f}s dur={duration:.1f}s"
                        token_rotation_log.debug(
                            "token_soft_cooldown day=%s idx=%s label=%s reason=%s limit=%s",
                            day_key,
                            batch_index,
                            token_ctx.label,
                            soft_reason,
                            token_ctx.batch_limit,
                        )
            else:
                token_ctx.slow_streak = 0

            if hard_cooldown:
                token_rotation_log.warning(
                    "token_hard_cooldown day=%s idx=%s label=%s reason=%s limit=%s",
                    day_key,
                    batch_index,
                    token_ctx.label,
                    hard_reason,
                    token_ctx.batch_limit,
                )
                message_load_log.warning(
                    "token_hard_cooldown day=%s token=%s reason=%s duration=%.2fs avg_unit=%.3fs processed=%s",
                    day_key,
                    token_ctx.label,
                    hard_reason,
                    duration,
                    avg_unit,
                    len(batch_units),
                )

            token_rotation_log.info(
                "batch_end day=%s idx=%s label=%s processed=%s success=%s retry=%s failures=%s cancel=%s duration=%.2fs avg_per_unit=%.3fs eff_avg=%.3fs limit=%s soft=%s hard=%s",
                day_key,
                batch_index,
                token_ctx.label,
                len(batch_units),
                successes,
                len(retry_units),
                len(failures),
                cancel_event.is_set(),
                duration,
                avg_unit,
                effective_avg,
                token_ctx.batch_limit,
                soft_cooldown,
                hard_cooldown,
            )
            message_load_log.info(
                "batch_stats day=%s idx=%s token=%s processed=%s success=%s retry=%s failures=%s duration=%.2fs avg_unit=%.3fs eff_avg=%.3fs limit=%s completed=%s total=%s cancel=%s soft=%s hard=%s",
                day_key,
                batch_index,
                token_ctx.label,
                len(batch_units),
                successes,
                len(retry_units),
                len(failures),
                duration,
                avg_unit,
                effective_avg,
                token_ctx.batch_limit,
                progress_count,
                total_units,
                cancel_event.is_set(),
                soft_cooldown,
                hard_cooldown,
            )

            result = {
                "processed": len(batch_units),
                "success": successes,
                "retry_units": retry_units,
                "failures": failures,
                "error_statuses": error_statuses,
                "duration": duration,
                "hard_cooldown": hard_cooldown,
                "hard_reason": hard_reason,
                "soft_cooldown": soft_cooldown,
                "soft_reason": soft_reason,
                "effective_avg": effective_avg,
            }
            token_rotation_log.debug(
                "batch_return day=%s idx=%s label=%s processed=%s success=%s retry=%s failures=%s duration=%.2fs cancel=%s",
                day_key,
                batch_index,
                token_ctx.label,
                result["processed"],
                result["success"],
                len(result["retry_units"]),
                len(result["failures"]),
                result["duration"],
                cancel_event.is_set(),
            )
            token_ctx.last_stage = "return_success"
            batch_result_payload = result
            return result
        finally:
            token_rotation_log.debug(
                "batch_exit day=%s idx=%s label=%s stage=%s result_type=%s processed=%s",
                day_key,
                batch_index,
                token_ctx.label,
                token_ctx.last_stage,
                type(batch_result_payload).__name__ if batch_result_payload is not None else "NoneType",
                (batch_result_payload or {}).get("processed") if isinstance(batch_result_payload, dict) else None,
            )

    batch_index = 0
    completed_batches_total = 0
    active_batches: Dict[
        asyncio.Task[Dict[str, Any]],
        Tuple[TokenContext, List[Tuple[int, Dict[str, Any]]], int],
    ] = {}

    def current_parallel_limit() -> int:
        ramp = WIALON_PARALLEL_TOKEN_WARMUP
        if WIALON_PARALLEL_TOKEN_RAMP_BATCHES > 0:
            ramp += completed_batches_total // WIALON_PARALLEL_TOKEN_RAMP_BATCHES
        return max(1, min(parallel_token_limit, ramp))

    async def schedule_ready_batches() -> bool:
        nonlocal batch_index
        scheduled = False
        while (
            len(active_batches) < current_parallel_limit()
            and units_queue
            and not cancel_event.is_set()
        ):
            token_ctx = _pick_ready_token(time.monotonic())
            if token_ctx is None:
                break
            batch_units: List[Tuple[int, Dict[str, Any]]] = []
            batch_limit = max(1, token_ctx.batch_limit)
            if token_ctx.batches < WIALON_BATCH_WARMUP_BATCHES:
                batch_limit = min(batch_limit, WIALON_BATCH_WARMUP_LIMIT)
            while units_queue and len(batch_units) < batch_limit:
                batch_units.append(units_queue.popleft())
            if not batch_units:
                token_ctx.busy = False
                break
            token_ctx.last_stage = "scheduled"
            batch_index += 1
            batch_task = _register_task(
                asyncio.create_task(run_batch_with_token(token_ctx, batch_units, batch_index))
            )
            active_batches[batch_task] = (token_ctx, batch_units, batch_index)
            scheduled = True
        return scheduled

    try:
        while (units_queue or active_batches) and not cancel_event.is_set():
            scheduled = await schedule_ready_batches()
            if cancel_event.is_set():
                break
            if not active_batches:
                if not units_queue:
                    break
                if not scheduled:
                    await _sleep_until_token_ready()
                continue
            done, _pending = await asyncio.wait(
                list(active_batches.keys()), return_when=asyncio.FIRST_COMPLETED
            )
            for completed_task in done:
                token_ctx: Optional[TokenContext]
                batch_units: List[Tuple[int, Dict[str, Any]]]
                batch_no: int
                token_ctx, batch_units, batch_no = active_batches.pop(
                    completed_task, (None, [], 0)
                )
                if token_ctx is not None:
                    token_ctx.busy = False
                try:
                    batch_result = completed_task.result()
                except asyncio.CancelledError:
                    continue
                except Exception as exc:
                    log.exception(
                        "message_cache: batch task crashed day=%s idx=%s token=%s err=%s",
                        day_key,
                        batch_no,
                        token_ctx.label if token_ctx else "unknown",
                        exc,
                    )
                    for unit_id, unit_item in batch_units:
                        units_queue.appendleft((unit_id, unit_payloads.get(unit_id, unit_item)))
                    if token_ctx is not None:
                        token_ctx.schedule_error_cooldown(
                            WIALON_TOKEN_ERROR_COOLDOWN_SECONDS,
                            getattr(token_ctx, "last_error_status", None),
                        )
                    continue
                if not isinstance(batch_result, dict):
                    last_stage = getattr(token_ctx, "last_stage", "unknown") if token_ctx else "unknown"
                    token_label = token_ctx.label if token_ctx else "unknown"
                    token_rotation_log.error(
                        "batch_invalid_return day=%s idx=%s label=%s stage=%s type=%s units=%s",
                        day_key,
                        batch_no,
                        token_label,
                        last_stage,
                        type(batch_result).__name__,
                        len(batch_units),
                    )
                    message_load_log.error(
                        "batch_invalid_return day=%s idx=%s token=%s stage=%s type=%s units=%s",
                        day_key,
                        batch_no,
                        token_label,
                        last_stage,
                        type(batch_result).__name__,
                        len(batch_units),
                    )
                    raise RuntimeError(
                        f"run_batch_with_token returned {type(batch_result).__name__} "
                        f"for day={day_key} idx={batch_no} token={token_label} stage={last_stage}"
                    )
                for unit_id, unit_item, status_code in batch_result.get("retry_units", []):
                    retry_count = retry_counters.get(unit_id, 0) + 1
                    retry_counters[unit_id] = retry_count
                    if retry_count <= MAX_UNIT_RETRIES and not cancel_event.is_set():
                        units_queue.append((unit_id, unit_payloads.get(unit_id, unit_item)))
                    else:
                        session_failures.add(unit_id)
                        await advance_progress()
                if token_ctx is None:
                    continue
                error_statuses = batch_result.get("error_statuses") or set()
                if batch_result.get("hard_cooldown"):
                    token_ctx.schedule_error_cooldown(
                        WIALON_TOKEN_HARD_COOLDOWN_SECONDS,
                        None,
                    )
                elif error_statuses:
                    token_ctx.schedule_error_cooldown(
                        WIALON_TOKEN_ERROR_COOLDOWN_SECONDS,
                        next(iter(error_statuses)),
                    )
                elif batch_result.get("soft_cooldown"):
                    token_ctx.schedule_cooldown(WIALON_TOKEN_SOFT_COOLDOWN_SECONDS)
                else:
                    token_ctx.schedule_cooldown(WIALON_TOKEN_COOLDOWN_SECONDS)
                completed_batches_total += 1

    finally:
        if active_batches:
            for pending_task in list(active_batches.keys()):
                pending_task.cancel()
            await asyncio.gather(*active_batches.keys(), return_exceptions=True)
        if units_queue and cancel_event.is_set():
            for unit_id, _unit in list(units_queue):
                session_failures.add(unit_id)
        await cleanup_active_registry()
        await _progress_tracker_stop(force=True)
        for ctx in token_contexts:
            if ctx.owns_client and ctx.client is not client:
                with contextlib.suppress(Exception):
                    ctx.client.close()
        await refresh_partial_meta()

    final_info = MESSAGE_CACHE.get_partial_info(day_key) or {}
    completed_total = len(final_info.get("units") or [])
    failed_total = len(final_info.get("failures") or [])

    status_flag = "partial"
    if cancel_event.is_set():
        status_flag = "aborted"

    meta_status = "partial" if status_flag == "partial" else status_flag
    MESSAGE_CACHE.update_partial_meta(
        day_key,
        status=meta_status,
        completed=completed_total,
        total=total_units,
        failed=failed_total,
    )
    message_load_log.info(
        "download_status day=%s label=%s status=%s completed=%s total=%s failed=%s cancel=%s",
        day_key,
        label,
        status_flag,
        completed_total,
        total_units,
        failed_total,
        cancel_event.is_set(),
    )

    if not cancel_event.is_set() and completed_total >= total_units:
        finalize_error = False
        payload = None
        try:
            payload = MESSAGE_CACHE.finalize_partial(
                day_key,
                expected_total=total_units,
                mark_incomplete=bool(failed_total),
            )
        except Exception as exc:
            finalize_error = True
            log.exception("message_cache: finalize_partial failed day=%s", day_key)
            message_load_log.error(
                "finalize_error day=%s label=%s err=%s",
                day_key,
                label,
                getattr(exc, "message", None) or str(exc),
            )
        progress_updates_locked = True
        completion_text = build_success_message(completed_total, failed_total)
        try:
            await edit_or_post_status_message(
                context,
                runtime,
                chat_id,
                status_msg,
                completion_text,
                status_key=DRAIN_ANALYSIS_MSG_STATUS_KEY,
                logger=log,
                reply_markup=None,
            )
        except Exception as exc:
            log.exception("message_cache: status post failed day=%s err=%s", day_key, exc)
        finish_status = "completed" if not finalize_error else "partial"
        message_load_log.info(
            "download_finish day=%s label=%s status=%s completed=%s total=%s failed=%s finalize_error=%s",
            day_key,
            label,
            finish_status,
            completed_total,
            total_units,
            failed_total,
            finalize_error,
        )
        return MessageCacheDownloadResult(
            status=finish_status,
            day=day_key,
            label=label,
            start_ts=start_ts,
            end_ts=end_ts,
            completed=completed_total,
            total=total_units,
            failed=failed_total,
            payload=payload,
        )

    summary_lines = []
    if cancel_event.is_set():
        summary_lines.append("\u23F9\uFE0F \u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430 \u043e\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d\u0430.")
    else:
        summary_lines.append("\u2139\uFE0F \u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0430 \u0447\u0430\u0441\u0442\u0438\u0447\u043d\u043e.")
    summary_lines.append(f"\u0413\u043e\u0442\u043e\u0432\u043e \u043e\u0431\u044a\u0435\u043a\u0442\u043e\u0432: {completed_total}/{total_units}")
    if failed_total:
        summary_lines.append(f"\u26A0\uFE0F \u041e\u0448\u0438\u0431\u043a\u0438 \u043f\u0440\u0438 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0435: {failed_total}")
    if session_failures:
        summary_lines.append(
            "\u26A0\uFE0F \u041f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0443, \u0447\u0442\u043e\u0431\u044b \u0434\u043e\u043a\u0430\u0447\u0430\u0442\u044c \u043e\u0441\u0442\u0430\u0432\u0448\u0438\u0435\u0441\u044f \u043e\u0431\u044a\u0435\u043a\u0442\u044b."
        )

    progress_updates_locked = True
    await edit_or_post_status_message(
        context,
        runtime,
        chat_id,
        status_msg,
        "\n".join(summary_lines),
        status_key=DRAIN_ANALYSIS_MSG_STATUS_KEY,
        logger=log,
        reply_markup=None,
    )
    message_load_log.info(
        "download_finish day=%s label=%s status=%s completed=%s total=%s failed=%s cancel=%s",
        day_key,
        label,
        status_flag,
        completed_total,
        total_units,
        failed_total,
        cancel_event.is_set(),
    )

    return MessageCacheDownloadResult(
        status=status_flag,
        day=day_key,
        label=label,
        start_ts=start_ts,
        end_ts=end_ts,
        completed=completed_total,
        total=total_units,
        failed=failed_total,
        payload=None,
    )


async def drain_message_cache_load_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_DRAIN_PROMPT_REFRESH

    await _safe_answer_callback(query)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY, None)
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY, None)
    runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    runtime["msg_action"] = "load"

    if query.message:
        with contextlib.suppress(Exception):
            await query.message.edit_reply_markup(None)

    chat_id = query.message.chat_id if query.message else (update.effective_chat.id if update.effective_chat else None)
    if chat_id is None:
        return STATE_DRAIN_PROMPT_REFRESH

    reference_ts = runtime.get(DRAIN_ANALYSIS_SNAPSHOT_TS_KEY)
    reference_text = _format_cache_timestamp(reference_ts)
    text = (
        "📅 Введите дату, за которую нужно сохранить сообщения.\n"
        "Поддерживаются форматы: 10.05.2025, 10 мая и др.\n"
        f"Можно также указать период в формате 01.10.2025 - 30.10.2025 (не более {MESSAGE_CACHE_MAX_PERIOD_DAYS} дн.) или нажать кнопку ниже.\n"
        f"Текущие данные объектов обновлены: {reference_text} мск."
    )
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("📆 Выбрать период", callback_data="drain:msg:period")]]
        ),
    )
    return STATE_DRAIN_WAIT_MSG_DATE


async def drain_message_cache_period_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_DRAIN_WAIT_MSG_DATE

    await _safe_answer_callback(query)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_QUEUE_KEY, None)
    runtime.pop(DRAIN_ANALYSIS_MSG_RANGE_LABEL_KEY, None)
    runtime["msg_action"] = "load_period"

    if query.message:
        with contextlib.suppress(Exception):
            await query.message.edit_reply_markup(None)

    chat_id = query.message.chat_id if query.message else (update.effective_chat.id if update.effective_chat else None)
    if chat_id is None:
        return STATE_DRAIN_PROMPT_REFRESH

    reference_ts = runtime.get(DRAIN_ANALYSIS_SNAPSHOT_TS_KEY)
    reference_text = _format_cache_timestamp(reference_ts)
    text = (
        f"📆 Укажите период в формате 01.10.2025 - 30.10.2025 (не более {MESSAGE_CACHE_MAX_PERIOD_DAYS} дн.).\n"
        "Можно использовать и относительные даты: 1 октября - 3 октября.\n"
        f"Текущие данные объектов обновлены: {reference_text} мск."
    )
    await context.bot.send_message(chat_id=chat_id, text=text)
    return STATE_DRAIN_WAIT_MSG_PERIOD


async def drain_message_cache_clear_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_DRAIN_PROMPT_REFRESH

    await _safe_answer_callback(query)

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})
    runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    runtime["msg_action"] = "delete"

    if query.message:
        with contextlib.suppress(Exception):
            await query.message.edit_reply_markup(None)

    chat_id = query.message.chat_id if query.message else (update.effective_chat.id if update.effective_chat else None)
    if chat_id is None:
        return STATE_DRAIN_PROMPT_REFRESH

    if not runtime["message_cache_days"]:
        prompt_text, keyboard = _compose_drain_cache_prompt(runtime)
        await context.bot.send_message(
            chat_id=chat_id,
            text="ℹ️ Кэш сообщений пуст.\n\n" + prompt_text,
            reply_markup=keyboard,
        )
        return STATE_DRAIN_PROMPT_REFRESH

    days_list = runtime.get("message_cache_days") or []
    formatted_days = [
        _format_message_cache_day(entry.get("day"))
        for entry in days_list
        if isinstance(entry, dict) and isinstance(entry.get("day"), str)
    ]
    text_lines = [
        "🧹 Введите дату, за которую нужно удалить кэш сообщений.",
        "Можно написать 'все' для полной очистки.",
    ]
    if formatted_days:
        text_lines.append("Доступные дни: " + ", ".join(sorted(formatted_days)))
    await context.bot.send_message(chat_id=chat_id, text="\n".join(text_lines))
    return STATE_DRAIN_WAIT_MSG_DELETE


async def _execute_drain_analysis(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    client: "WialonClient",
    unit_map: Dict[int, Dict[str, Any]],
    start_local: datetime,
    end_local: datetime,
    label: str,
    *,
    message_cache: Optional[Dict[int, Dict[str, Any]]] = None,
) -> None:
    chat = update.effective_chat
    chat_id = chat.id if chat else None
    if chat_id is None:
        return

    runtime: Dict[str, Any] = context.user_data.setdefault(DRAIN_ANALYSIS_KEY, {})

    message_cache_by_unit: Dict[int, Dict[str, Any]] = {}
    if isinstance(message_cache, dict):
        for unit_key, entry in message_cache.items():
            try:
                unit_id = int(unit_key)
            except Exception:
                continue
            if isinstance(entry, dict):
                message_cache_by_unit[unit_id] = entry

    day_key = start_local.date().isoformat()

    status_text = f"📊 Анализирую сливы за {label}…"

    cancel_event = runtime.get("cancel_event")
    if isinstance(cancel_event, asyncio.Event):
        cancel_event.clear()
    else:
        cancel_event = asyncio.Event()
        runtime["cancel_event"] = cancel_event

    cancel_keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("⏹️ Завершить анализ", callback_data="drain:cancel")]]
    )

    status_msg = await edit_or_post_status_message(
        context,
        runtime,
        chat_id,
        None,
        status_text,
        status_key=DRAIN_ANALYSIS_STATUS_KEY,
        logger=log,
        reply_markup=cancel_keyboard,
    )

    start_ts = int(start_local.astimezone(timezone.utc).timestamp())
    end_ts = int(end_local.astimezone(timezone.utc).timestamp())

    units_sorted = sorted(
        unit_map.items(),
        key=lambda item: (
            str((item[1].get("nm") or item[1].get("name") or item[1].get("n") or "").strip()).casefold(),
            item[0],
        ),
    )

    valid_units: List[Tuple[int, Dict[str, Any]]] = []
    for unit_id_raw, unit_item_raw in units_sorted:
        try:
            unit_id = int(unit_id_raw)
        except Exception:
            continue
        if not isinstance(unit_item_raw, dict):
            continue
        copy_item = copy.deepcopy(unit_item_raw)
        copy_item.setdefault("id", unit_id)
        valid_units.append((unit_id, copy_item))

    total_units = len(valid_units)
    drain_cache_log.info(
        "analysis_prepare day=%s label=%s total_units=%s cached_units=%s caller=%s",
        start_local.date().isoformat(),
        label,
        total_units,
        len(message_cache_by_unit),
        _stack_brief(),
    )
    await run_blocking(
        MESSAGE_CACHE.ensure_partial,
        day_key,
        start_ts=start_ts,
        end_ts=end_ts,
        label=label,
        total_units=total_units,
    )
    with contextlib.suppress(Exception):
        runtime["message_cache_days"] = MESSAGE_CACHE.list_days()

    def commit_partial_status(status: str) -> None:
        try:
            info = MESSAGE_CACHE.get_partial_info(day_key) or {}
        except Exception:
            info = {}
        completed_total = len(info.get("units") or [])
        failed_total = len(info.get("failures") or [])
        try:
            MESSAGE_CACHE.update_partial_meta(
                day_key,
                status=status,
                completed=completed_total,
                total=total_units,
                failed=failed_total,
            )
        except Exception as exc:
            log.debug(
                "drain_analysis: failed to update partial meta day=%s status=%s: %s",
                day_key,
                status,
                exc,
            )
        else:
            with contextlib.suppress(Exception):
                runtime["message_cache_days"] = MESSAGE_CACHE.list_days()
    if total_units == 0:
        text = "ℹ️ Нет объектов в локальном кэше. Обновите данные и повторите попытку."
        if status_msg:
            try:
                await status_msg.edit_text(text)
            except Exception:
                await context.bot.send_message(chat_id=chat_id, text=text)
        else:
            await context.bot.send_message(chat_id=chat_id, text=text)
        return

    concurrency_limit = max(1, min(DRAIN_ANALYSIS_CONCURRENCY, total_units))
    session_pool_size = max(1, min(DRAIN_ANALYSIS_SESSION_POOL, concurrency_limit))
    try:
        session_pool = await AsyncWialonSessionPool.create_from_sync(client, session_pool_size)
    except Exception:
        await cleanup_active_registry()
        raise
    log.info(
        "drain_analysis: session pool requested=%s actual=%s owns=%s",
        session_pool_size,
        session_pool.size,
        session_pool.owns_clients,
    )

    message_lock = asyncio.Lock()

    last_status_update = time.perf_counter()

    async def update_progress(current: int) -> None:
        nonlocal status_msg, last_status_update
        if total_units == 0 or cancel_event.is_set():
            return
        now = time.perf_counter()
        if current < total_units and now - last_status_update < 2.0:
            return
        progress_text = f"📊 Анализирую сливы за {label}… {current}/{total_units}"
        reply_markup = None
        if current < total_units and not cancel_event.is_set():
            reply_markup = cancel_keyboard
        status_msg = await edit_or_post_status_message(
            context,
            runtime,
            chat_id,
            status_msg,
            progress_text,
            status_key=DRAIN_ANALYSIS_STATUS_KEY,
            logger=log,
            reply_markup=reply_markup,
        )
        last_status_update = now

    records: List[DrainEventRecord] = []
    units_with_sensors = 0
    units_no_sensors = 0
    units_no_data = 0
    units_failed: List[int] = []

    chunk_size = max(1, DRAIN_ANALYSIS_CHUNK_SIZE)
    processed = 0
    chunk_index = 0
    chunk_processed = 0
    chunk_records: List[DrainEventRecord] = []
    chunk_units_with_sensors = 0
    chunk_units_no_sensors = 0
    chunk_units_no_data = 0
    chunk_units_failed: List[int] = []

    def reset_chunk() -> None:
        nonlocal chunk_processed, chunk_units_with_sensors, chunk_units_no_sensors, chunk_units_no_data
        chunk_processed = 0
        chunk_units_with_sensors = 0
        chunk_units_no_sensors = 0
        chunk_units_no_data = 0
        chunk_records.clear()
        chunk_units_failed.clear()

    active_registry: Dict[str, Any] = runtime.setdefault(
        DRAIN_ANALYSIS_ACTIVE_SESSIONS_KEY,
        {"tasks": set(), "clients": set(), "units": set()},
    )
    active_tasks: Set[asyncio.Task[Any]] = active_registry.setdefault("tasks", set())
    active_clients: Set[Any] = active_registry.setdefault("clients", set())
    inflight_units: Set[int] = active_registry.setdefault("units", set())
    cleanup_triggered = False

    async def cleanup_active_registry() -> None:
        nonlocal cleanup_triggered
        if cleanup_triggered:
            return
        cleanup_triggered = True
        for task in list(active_tasks):
            active_tasks.discard(task)

        async def _clear_client(entry: Any) -> None:
            clear_fn = getattr(entry, "clear_cancel_predicate", None)
            if callable(clear_fn):
                with contextlib.suppress(Exception):
                    result = clear_fn()
                    if inspect.isawaitable(result):
                        await result

        for client_entry in list(active_clients):
            await _clear_client(client_entry)
            active_clients.discard(client_entry)
        inflight_units.clear()
        runtime.pop(DRAIN_ANALYSIS_ACTIVE_SESSIONS_KEY, None)

    def _register_task(task: asyncio.Task[Any]) -> asyncio.Task[Any]:
        active_tasks.add(task)
        task.add_done_callback(lambda t: active_tasks.discard(t))
        cancel_log.info("drain_analysis task_registered active=%s", len(active_tasks))
        return task

    def _register_session(session_client: Optional[Any]) -> None:
        if session_client is None:
            return
        with contextlib.suppress(Exception):
            setter = getattr(session_client, "set_cancel_predicate", None)
            if callable(setter):
                setter(cancel_event.is_set)
        active_clients.add(session_client)
        cancel_log.info(
            "drain_analysis session_registered id=%s active=%s",
            id(session_client),
            len(active_clients),
        )

    def _unregister_session(session_client: Optional[Any]) -> None:
        if session_client is None:
            return
        with contextlib.suppress(Exception):
            clearer = getattr(session_client, "clear_cancel_predicate", None)
            if callable(clearer):
                clearer()
        active_clients.discard(session_client)
        cancel_log.info(
            "drain_analysis session_unregistered id=%s active=%s",
            id(session_client),
            len(active_clients),
        )

    async def wait_with_cancel(awaitable: Awaitable[T], *, timeout: Optional[float] = None) -> T:
        if cancel_event.is_set():
            cancel_log.info("drain_analysis wait_with_cancel pre-raise due_to_cancel")
            raise asyncio.CancelledError()
        task = _register_task(asyncio.create_task(awaitable))
        try:
            if timeout is not None:
                return await asyncio.wait_for(task, timeout=timeout)
            return await task
        except asyncio.CancelledError:
            cancel_event.set()
            if not task.done():
                task.cancel()
            cancel_log.info("drain_analysis wait_with_cancel cancelled awaitable=%s", awaitable)
            raise
        finally:
            active_tasks.discard(task)

    def _register_unit(unit_id: int) -> None:
        inflight_units.add(unit_id)
        cancel_log.info("drain_analysis unit_inflight_add unit=%s active=%s", unit_id, len(inflight_units))

    def _unregister_unit(unit_id: int) -> None:
        inflight_units.discard(unit_id)
        cancel_log.info("drain_analysis unit_inflight_remove unit=%s active=%s", unit_id, len(inflight_units))

    base_setter = getattr(client, "set_cancel_predicate", None)
    if callable(base_setter):
        with contextlib.suppress(Exception):
            base_setter(cancel_event.is_set)
            active_clients.add(client)
            cancel_log.info(
                "drain_analysis base_client_registered id=%s active=%s",
                id(client),
                len(active_clients),
            )

    async def flush_chunk() -> None:
        nonlocal chunk_index
        if chunk_processed == 0:
            return

        chunk_index += 1
        start_idx = processed - chunk_processed + 1
        end_idx = processed

        report_summary: Dict[str, Any] = {
            "total_events": 0,
            "total_volume": 0.0,
            "unit_count": 0,
        }

        if chunk_records:
            try:
                filename, content, report_summary = build_drain_report(
                    chunk_records,
                    start_local,
                    end_local,
                )
                if filename.lower().endswith(".xlsx"):
                    filename = filename[:-5] + f"_part{chunk_index}.xlsx"
                document = InputFile(io.BytesIO(content), filename=filename)
                await context.bot.send_document(chat_id=chat_id, document=document)
            except Exception as exc:
                log.exception(
                    "drain_analysis: failed to send partial report part=%s: %s",
                    chunk_index,
                    exc,
                )
                error_text = (
                    f"❌ Не удалось отправить промежуточный отчёт №{chunk_index}: {exc}"
                )
                await context.bot.send_message(chat_id=chat_id, text=error_text)

        summary_lines = [
            f"📄 Промежуточный отчёт №{chunk_index}",
            f"День: {label}",
        ]

        events_count_raw = report_summary.get("total_events", 0)
        try:
            events_count = int(events_count_raw)
        except Exception:
            events_count = 0
        summary_lines.append(f"Событий: {events_count}")

        unit_count_raw = report_summary.get("unit_count", 0)
        try:
            unit_count = int(unit_count_raw)
        except Exception:
            unit_count = 0
        summary_lines.append(f"Объектов с событиями: {unit_count}")

        total_volume = report_summary.get("total_volume")
        if isinstance(total_volume, (int, float)):
            summary_lines.append(f"Суммарный объём: {float(total_volume):.1f} л")
        if events_count == 0:
            summary_lines.append("Сливы не обнаружены.")

        if start_idx == end_idx:
            summary_lines.append(f"Объект: {start_idx} из {total_units}")
        else:
            summary_lines.append(
                f"Объекты: {start_idx}–{end_idx} из {total_units}"
            )

        summary_lines.append(f"Обработано объектов: {chunk_processed}")
        summary_lines.append(
            f"Объектов с подходящими датчиками: {chunk_units_with_sensors}"
        )
        if chunk_units_no_sensors:
            summary_lines.append(
                f"Без подходящих датчиков: {chunk_units_no_sensors}"
            )
        if chunk_units_no_data:
            summary_lines.append(f"Без данных за период: {chunk_units_no_data}")
        if chunk_units_failed:
            summary_lines.append(
                f"Ошибки при обработке: {len(set(chunk_units_failed))}"
            )

        await context.bot.send_message(chat_id=chat_id, text="\n".join(summary_lines))

        reset_chunk()

            
    
    async def process_unit(unit_id: int, unit_item: Dict[str, Any]) -> Dict[str, Any]:
        had_sensors = False
        no_sensors = False
        no_data = False
        failed = False
        result_records: List[DrainEventRecord] = []

        if cancel_event.is_set():
            cancel_log.info("drain_analysis process_unit skipped unit=%s due_to_cancel", unit_id)
            return {
                "unit_id": unit_id,
                "records": result_records,
                "had_sensors": had_sensors,
                "no_sensors": no_sensors,
                "no_data": no_data,
                "failed": failed,
            }

        _register_unit(unit_id)
        log.debug("drain_analysis: start unit=%s inflight=%s", unit_id, len(inflight_units))

        async def _core() -> Dict[str, Any]:
            nonlocal had_sensors, no_sensors, no_data, failed

            try:
                dut_sensors, dut_summary, _ = _prepare_dut_for_analysis(unit_item or {})
            except Exception as exc:
                log.debug("drain_analysis: dut prepare failed unit=%s: %s", unit_id, exc)
                failed = True
                await run_blocking(MESSAGE_CACHE.record_partial_failure, day_key, unit_id)
                return {
                    "unit_id": unit_id,
                    "records": result_records,
                    "had_sensors": had_sensors,
                    "no_sensors": no_sensors,
                    "no_data": no_data,
                    "failed": failed,
                }

            sensor_ids = [meta.id for meta in dut_sensors if getattr(meta, "id", None) is not None]
            if not sensor_ids:
                no_sensors = True
                entry = {
                    "sensor_ids": [],
                    "series": {},
                    "stats": {},
                    "speed_series": [],
                    "aux_series": {},
                    "samples": [],
                    "msg_count": 0,
                    "index_from": 0,
                    "index_to": 0,
                    "width_hint": None,
                    "had_sensors": False,
                    "no_sensors": True,
                    "no_data": False,
                    "failed": False,
                }
                await run_blocking(MESSAGE_CACHE.store_partial_unit, day_key, unit_id, entry)
                await run_blocking(MESSAGE_CACHE.clear_partial_failure, day_key, unit_id)
                return {
                    "unit_id": unit_id,
                    "records": result_records,
                    "had_sensors": had_sensors,
                    "no_sensors": no_sensors,
                    "no_data": no_data,
                    "failed": failed,
                }

            had_sensors = True

            sensor_meta_by_id: Dict[Any, Any] = {meta.id: meta for meta in dut_sensors if getattr(meta, "id", None) is not None}
            sensor_meta_by_id["__summary__"] = dut_summary
            target_map = dut_summary.get("target_map") if isinstance(dut_summary, dict) else None
            if not isinstance(target_map, dict):
                target_map = None

            series_by_sensor: Dict[int, List[Tuple[int, float]]] = {}
            stats_by_sensor: Dict[int, Dict[str, Any]] = {}
            speed_series: List[Tuple[int, float]] = []
            aux_series: Dict[str, List[Tuple[int, float]]] = {}
            samples: List[Dict[str, Any]] = []

            session_client: Optional["WialonClient"] = None
            client_broken = False
            load_result: Optional[
                Tuple[
                    Dict[int, List[Tuple[int, float]]],
                    Dict[int, Dict[str, Any]],
                    int,
                    int,
                    int,
                    Optional[int],
                    List[Tuple[int, float]],
                    Dict[str, List[Tuple[int, float]]],
                    List[Dict[str, Any]],
                ]
            ] = None
            used_cache = False

            cache_entry = message_cache_by_unit.get(unit_id)
            if isinstance(cache_entry, dict):
                if cache_entry.get("no_sensors"):
                    no_sensors = True
                    entry = {
                        "sensor_ids": [],
                        "series": {},
                        "stats": {},
                        "speed_series": [],
                        "aux_series": {},
                        "samples": [],
                        "msg_count": 0,
                        "index_from": 0,
                        "index_to": 0,
                        "width_hint": cache_entry.get("width_hint"),
                        "had_sensors": False,
                        "no_sensors": True,
                        "no_data": cache_entry.get("no_data", False),
                        "failed": False,
                    }
                    await run_blocking(MESSAGE_CACHE.store_partial_unit, day_key, unit_id, entry)
                    await run_blocking(MESSAGE_CACHE.clear_partial_failure, day_key, unit_id)
                    return {
                        "unit_id": unit_id,
                        "records": result_records,
                        "had_sensors": False,
                        "no_sensors": True,
                        "no_data": entry["no_data"],
                        "failed": failed,
                    }
                if cache_entry.get("failed"):
                    failed = True
                    await run_blocking(MESSAGE_CACHE.record_partial_failure, day_key, unit_id)
                    return {
                        "unit_id": unit_id,
                        "records": result_records,
                        "had_sensors": had_sensors,
                        "no_sensors": no_sensors,
                        "no_data": no_data,
                        "failed": True,
                    }
                cached_series = cache_entry.get("series") if isinstance(cache_entry.get("series"), dict) else {}
                missing = [sid for sid in sensor_ids if sid not in cached_series]
                if not missing:
                    series_by_sensor = {sid: list(cached_series.get(sid, [])) for sid in sensor_ids}
                    cached_stats = cache_entry.get("stats") if isinstance(cache_entry.get("stats"), dict) else {}
                    stats_by_sensor = {sid: dict(cached_stats.get(sid) or {}) for sid in sensor_ids}
                    speed_series = [tuple(pair) for pair in (cache_entry.get("speed_series") or [])]
                    aux_raw = cache_entry.get("aux_series") if isinstance(cache_entry.get("aux_series"), dict) else {}
                    aux_series = {
                        str(key): [tuple(pair) for pair in value]
                        for key, value in aux_raw.items()
                        if isinstance(value, list)
                    }
                    samples = copy.deepcopy(cache_entry.get("samples") or [])
                    try:
                        index_from = int(cache_entry.get("index_from", 0) or 0)
                    except Exception:
                        index_from = 0
                    try:
                        index_to = int(cache_entry.get("index_to", 0) or 0)
                    except Exception:
                        index_to = 0
                    try:
                        msg_count = int(cache_entry.get("msg_count", 0) or 0)
                    except Exception:
                        msg_count = 0
                    width_hint = cache_entry.get("width_hint")
                    load_result = (
                        series_by_sensor,
                        stats_by_sensor,
                        index_from,
                        index_to,
                        msg_count,
                        width_hint,
                        speed_series,
                        aux_series,
                        samples,
                    )
                    used_cache = True
                    await run_blocking(MESSAGE_CACHE.clear_partial_failure, day_key, unit_id)
                    log.debug(
                        "drain_analysis: using cached messages unit=%s day=%s",
                        unit_id,
                        cache_day_label,
                    )

            if not used_cache:
                try:
                    async with message_lock:
                        session_client = await session_pool.acquire()
                    _register_session(session_client)
                    try:
                        load_result = await wait_with_cancel(
                            messages_loader.load_messages_and_series(
                                session_client,
                                unit_id,
                                sensor_ids,
                                start_ts,
                                end_ts,
                                dut_targets=target_map,
                                allow_remote_series=False,
                                request_timeout=DRAIN_ANALYSIS_MESSAGE_LOAD_TIMEOUT,
                            ),
                            timeout=DRAIN_ANALYSIS_MESSAGE_LOAD_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        client_broken = True
                        failed = True
                        log.warning(
                            "drain_analysis: load timeout unit=%s after %.1fs",
                            unit_id,
                            DRAIN_ANALYSIS_MESSAGE_LOAD_TIMEOUT,
                        )
                        await run_blocking(MESSAGE_CACHE.record_partial_failure, day_key, unit_id)
                        return {
                            "unit_id": unit_id,
                            "records": result_records,
                            "had_sensors": had_sensors,
                            "no_sensors": no_sensors,
                            "no_data": no_data,
                            "failed": failed,
                        }
                    except asyncio.CancelledError:
                        client_broken = True
                        raise
                    except Exception as exc:
                        client_broken = True
                        log.debug("drain_analysis: load failed unit=%s: %s", unit_id, exc)
                        failed = True
                        await run_blocking(MESSAGE_CACHE.record_partial_failure, day_key, unit_id)
                        return {
                            "unit_id": unit_id,
                            "records": result_records,
                            "had_sensors": had_sensors,
                            "no_sensors": no_sensors,
                            "no_data": no_data,
                            "failed": failed,
                        }
                finally:
                    if session_client is not None:
                        if load_result is not None and not client_broken:
                            try:
                                await wait_with_cancel(
                                    session_client.unload_messages(unit_id),
                                    timeout=DRAIN_ANALYSIS_UNLOAD_TIMEOUT,
                                )
                            except Exception as exc:
                                log.debug(
                                    "drain_analysis: unload failed unit=%s: %s",
                                    unit_id,
                                    exc,
                                )
                                client_broken = True
                        _unregister_session(session_client)
                        await session_pool.recycle(session_client, broken=client_broken)

            if load_result is None:
                failed = True
                await run_blocking(MESSAGE_CACHE.record_partial_failure, day_key, unit_id)
                return {
                    "unit_id": unit_id,
                    "records": result_records,
                    "had_sensors": had_sensors,
                    "no_sensors": no_sensors,
                    "no_data": no_data,
                    "failed": failed,
                }

            (
                series_by_sensor,
                _stats_by_sensor,
                _index_from,
                _index_to,
                _msg_count,
                _width_for_detector,
                speed_series,
                _aux_series,
                samples,
            ) = load_result

            detection_input = {
                sid: series_by_sensor.get(sid)
                for sid in sensor_ids
                if series_by_sensor.get(sid)
            }
            entry: Dict[str, Any]
            if not detection_input:
                no_data = True
                entry = {
                    "sensor_ids": sensor_ids,
                    "series": {sid: series_by_sensor.get(sid, []) for sid in sensor_ids},
                    "stats": {sid: {} for sid in sensor_ids},
                    "speed_series": speed_series,
                    "aux_series": aux_series,
                    "samples": samples,
                    "msg_count": sum(len(series_by_sensor.get(sid, [])) for sid in sensor_ids),
                    "index_from": _index_from,
                    "index_to": _index_to,
                    "width_hint": _width_for_detector,
                    "had_sensors": True,
                    "no_sensors": False,
                    "no_data": True,
                    "failed": False,
                }
                await run_blocking(MESSAGE_CACHE.store_partial_unit, day_key, unit_id, entry)
                await run_blocking(MESSAGE_CACHE.clear_partial_failure, day_key, unit_id)
                return {
                    "unit_id": unit_id,
                    "records": result_records,
                    "had_sensors": had_sensors,
                    "no_sensors": no_sensors,
                    "no_data": no_data,
                    "failed": failed,
                }

            try:
                drains = fuel_detector.detect_short_drains(
                    detection_input,
                    speed_series=speed_series,
                    sensor_meta_by_id=sensor_meta_by_id,
                    unit_summary=dut_summary,
                    samples=samples,
                )
            except Exception as exc:
                log.debug("drain_analysis: detect failed unit=%s: %s", unit_id, exc)
                failed = True
                await run_blocking(MESSAGE_CACHE.record_partial_failure, day_key, unit_id)
                return {
                    "unit_id": unit_id,
                    "records": result_records,
                    "had_sensors": had_sensors,
                    "no_sensors": no_sensors,
                    "no_data": no_data,
                    "failed": failed,
                }

            entry = {
                "sensor_ids": sensor_ids,
                "series": {sid: series_by_sensor.get(sid, []) for sid in sensor_ids},
                "stats": {sid: {} for sid in sensor_ids},
                "speed_series": speed_series,
                "aux_series": aux_series,
                "samples": samples,
                "msg_count": sum(len(series_by_sensor.get(sid, [])) for sid in sensor_ids),
                "index_from": _index_from,
                "index_to": _index_to,
                "width_hint": _width_for_detector,
                "had_sensors": True,
                "no_sensors": False,
                "no_data": False,
                "failed": False,
            }
            await run_blocking(MESSAGE_CACHE.store_partial_unit, day_key, unit_id, entry)
            await run_blocking(MESSAGE_CACHE.clear_partial_failure, day_key, unit_id)

            unit_name = (
                str(unit_item.get("nm") or unit_item.get("name") or unit_item.get("n") or f"id {unit_id}").strip()
                or f"id {unit_id}"
            )
            for event in drains:
                result_records.append(DrainEventRecord(unit_id=unit_id, unit_name=unit_name, event=event))

            return {
                "unit_id": unit_id,
                "records": result_records,
                "had_sensors": had_sensors,
                "no_sensors": no_sensors,
                "no_data": no_data,
                "failed": failed,
            }

        try:
            return await _core()
        finally:
            _unregister_unit(unit_id)
            log.debug("drain_analysis: finish unit=%s inflight=%s", unit_id, len(inflight_units))

    semaphore = asyncio.Semaphore(concurrency_limit)

    async def run_unit(unit_id: int, unit_item: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            if cancel_event.is_set():
                cancel_log.info("drain_analysis run_unit skipped unit=%s due_to_cancel", unit_id)
                return {
                    "unit_id": unit_id,
                    "records": [],
                    "had_sensors": False,
                    "no_sensors": False,
                    "no_data": False,
                    "failed": False,
                }
            try:
                return await process_unit(unit_id, unit_item)
            except asyncio.CancelledError:
                cancel_log.info("drain_analysis run_unit cancelled unit=%s", unit_id)
                raise
            except Exception:
                log.exception("drain_analysis: unexpected failure unit=%s", unit_id)
                return {
                    "unit_id": unit_id,
                    "records": [],
                    "had_sensors": False,
                    "no_sensors": False,
                    "no_data": False,
                    "failed": True,
                }

    tasks: List[asyncio.Task[Any]] = []
    for unit_id, unit_item in valid_units:
        if cancel_event.is_set():
            cancel_log.info("drain_analysis task_schedule skipped due_to_cancel unit=%s", unit_id)
            break
        tasks.append(_register_task(asyncio.create_task(run_unit(unit_id, unit_item))))

    await update_progress(0)

    cancel_requested = False
    try:
        for task in asyncio.as_completed(tasks):
            try:
                result = await task
            except asyncio.CancelledError:
                cancel_requested = True
                cancel_log.info("drain_analysis worker_task cancelled by user")
                break
            except Exception as exc:
                log.exception("drain_analysis: task failed: %s", exc)
                continue

            if cancel_event.is_set():
                cancel_requested = True
                break

            processed += 1
            chunk_processed += 1

            await update_progress(processed)

            if result.get("had_sensors"):
                units_with_sensors += 1
                chunk_units_with_sensors += 1
            if result.get("no_sensors"):
                units_no_sensors += 1
                chunk_units_no_sensors += 1
            if result.get("no_data"):
                units_no_data += 1
                chunk_units_no_data += 1
            if result.get("failed"):
                unit_failed_id = result.get("unit_id")
                try:
                    unit_failed_id_int = int(unit_failed_id)
                except Exception:
                    unit_failed_id_int = None
                if unit_failed_id_int is not None:
                    units_failed.append(unit_failed_id_int)
                    chunk_units_failed.append(unit_failed_id_int)
            for rec in result.get("records", []):
                records.append(rec)
                chunk_records.append(rec)

            if chunk_processed >= chunk_size:
                await flush_chunk()
        else:
            if cancel_event.is_set():
                cancel_requested = True
    finally:
        if cancel_event.is_set():
            cancel_requested = True
        if cancel_requested:
            for task in tasks:
                if not task.done():
                    task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(*tasks, return_exceptions=True)
        await session_pool.aclose()
        await cleanup_active_registry()

    if cancel_requested:
        drain_cache_log.info(
            "analysis_cancelled day=%s processed=%s units_with_data=%s units_failed=%s",
            start_local.date().isoformat(),
            processed,
            units_with_sensors,
            len(units_failed),
        )
        commit_partial_status("aborted")
        reset_chunk()
        records.clear()
        units_failed.clear()
        units_with_sensors = 0
        units_no_sensors = 0
        units_no_data = 0
        cancel_text = (
            f"⏹️ Анализ остановлен. Обработано: {processed} из {total_units}."
            "\nКэш сообщений очищен."
        )
        status_msg = await edit_or_post_status_message(
            context,
            runtime,
            chat_id,
            status_msg,
            cancel_text,
            status_key=DRAIN_ANALYSIS_STATUS_KEY,
            logger=log,
            reply_markup=None,
        )
        if not status_msg:
            await context.bot.send_message(chat_id=chat_id, text=cancel_text)
        await cleanup_active_registry()
        return

    if chunk_processed:
        await flush_chunk()

    if not records:
        summary_text = f"ℹ️ За {label} сливы не обнаружены."
        if units_no_sensors:
            summary_text += f"\nБез подходящих датчиков: {units_no_sensors}"
        if units_failed:
            summary_text += f"\nОшибки при обработке: {len(set(units_failed))}"
        if status_msg:
            try:
                await status_msg.edit_text(summary_text)
            except Exception:
                await context.bot.send_message(chat_id=chat_id, text=summary_text)
        else:
            await context.bot.send_message(chat_id=chat_id, text=summary_text)
        commit_partial_status("partial")
        await cleanup_active_registry()
        return

    records.sort(key=lambda rec: rec.event.get("start_ts") or 0)

    filename, content, report_summary = build_drain_report(
        records,
        start_local,
        end_local,
    )

    try:
        document = InputFile(io.BytesIO(content), filename=filename)
        await context.bot.send_document(chat_id=chat_id, document=document)
    except Exception as exc:
        error_text = f"❌ Не удалось отправить отчёт: {exc}"
        if status_msg:
            try:
                await status_msg.edit_text(error_text)
            except Exception:
                await context.bot.send_message(chat_id=chat_id, text=error_text)
        else:
            await context.bot.send_message(chat_id=chat_id, text=error_text)
        commit_partial_status("partial")
        await cleanup_active_registry()
        return

    summary_lines = [
        "✅ Отчёт по сливам готов.",
        f"День: {label}",
        f"Событий: {report_summary.get('total_events', 0)}",
        f"Объектов с событиями: {report_summary.get('unit_count', 0)}",
    ]
    total_volume = report_summary.get("total_volume")
    if isinstance(total_volume, (int, float)):
        summary_lines.append(f"Суммарный объём: {total_volume:.1f} л")
    summary_lines.append(f"Объектов с подходящими датчиками: {units_with_sensors}")
    summary_lines.append(f"Обработано объектов: {total_units}")
    if units_no_sensors:
        summary_lines.append(f"Без подходящих датчиков: {units_no_sensors}")
    if units_no_data:
        summary_lines.append(f"Без данных за период: {units_no_data}")
    if units_failed:
        summary_lines.append(f"Ошибки при обработке: {len(set(units_failed))}")

    summary_text = "\n".join(summary_lines)
    if status_msg:
        try:
            await status_msg.edit_text(summary_text)
        except Exception:
            await context.bot.send_message(chat_id=chat_id, text=summary_text)
    else:
        await context.bot.send_message(chat_id=chat_id, text=summary_text)

    commit_partial_status("partial")
    await cleanup_active_registry()


async def export_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if _active_ui(context) == UI_ADMIN:
        text = ""
        if update.message and update.message.text:
            text = update.message.text.strip()
        if text == MENU_BUTTON_EXPORT:
            chat = update.effective_chat
            chat_id = chat.id if chat else None
            log.info("admin: to_export user=%s -> set UI=OBJECTS", chat_id)
            panel_state = context.chat_data.get("admin_panel")
            if isinstance(panel_state, dict):
                panel_state.pop("awaiting", None)
            await _activate_objects_ui(context)
        else:
            chat = update.effective_chat
            log.debug("admin: ignore export trigger user=%s", chat.id if chat else None)
            return ConversationHandler.END
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _activate_objects_ui(context)
    await delete_anchor(context)
    await clear_stats_buttons(context)
    context.user_data["mode"] = MODE_EXPORT
    _set_objects_mode(context, OBJECTS_MODE_LIST)
    prompt_text = "Экспорт отчётов временно недоступен."
    markup = kb_export_menu()
    await _render_export_panel(
        context,
        prompt_text,
        markup,
        parse_mode=None,
        update_or_q=update,
    )
    return STATE_EXPORT_MENU


async def start_action_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return STATE_FIND_QUERY
    action = (q.data or "").partition(":")[2]
    await _safe_answer_callback(q)
    if action == "find":
        if not await ensure_authorized(update, context):
            return STATE_AUTH_WAIT_TOKEN
        return await _start_search_prompt_from_trigger(q, context)
    if action == "export":
        return await export_entry(update, context)
    return ConversationHandler.END


async def _restore_after_export(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: Optional[int] = None,
) -> int:
    _set_objects_mode(context, OBJECTS_MODE_LIST)
    if chat_id is None:
        anchor = context.user_data.get("anchor") or {}
        chat_id = anchor.get("chat_id")
    await _render_units_page(
        context,
        chat_id=chat_id,
        offset=context.user_data.get("search_units_offset"),
        reanchor=True,
    )
    return STATE_WAIT_UNIT


async def export_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return STATE_EXPORT_MENU
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _activate_objects_ui(context)
    chat_id = q.message.chat_id if q.message else (update.effective_chat.id if update.effective_chat else None)
    await _safe_answer_callback(q)
    context.user_data["mode"] = MODE_NONE
    await delete_export_anchor(context)
    if chat_id is not None:
        return await _restore_after_export(context, chat_id=chat_id)
    return STATE_FIND_QUERY


async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await global_restart(update, context)

# -------- Cancel / Back ----------
async def cancel_inline_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)
    mode = context.user_data.get("mode")

    if mode == MODE_REPORT:
        job = context.user_data.get("report_job")
        message = "\u041e\u0441\u0442\u0430\043d\u0430\u0432\u043b\u0438\u0432\u0430\u044e \u0444\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\043d\0438\u0435 \u043e\u0442\u0447\u0435\u0442\u0430."
        if isinstance(job, dict):
            event = job.get("cancel_event")
            if isinstance(event, threading.Event):
                event.set()
            job["final_message_sent"] = True
        await edit_anchor(context, message, markup=None, parse_mode=None)
        context.user_data.pop("anchor", None)
        await delete_stats_message(context)
        await delete_param_result_message(context)
        clear_param_runtime(context)
        context.user_data.pop("report_job", None)
        clear_report_context(context, cancel_job=False)
        if q.message:
            chat_id = q.message.chat_id
            await q.message.reply_text(message, reply_markup=reply_menu(chat_id=chat_id))
        return ConversationHandler.END

    if mode == MODE_CF:
        try:
            if context.user_data.get("allow_cancel_undo"):
                last = context.user_data.get("last_field")
                if last and isinstance(last, dict):
                    client = await require_wialon_client(
                        update,
                        context,
                        "Чтобы отменить последнее поле, авторизуйтесь.",
                    )
                    if not client:
                        return STATE_AUTH_WAIT_TOKEN
                    client.delete_custom_field(last["unit_id"], last["field_id"])
                    log.info(
                        "Отмена: удалено последнее поле %s (id=%s)",
                        last.get("name"),
                        last.get("field_id"),
                    )
        except Exception as e:
            log.warning(f"Не удалось откатить последнее поле при отмене: {e}")

        unit = context.user_data.get("chosen_unit")
        await delete_param_result_message(context)
        await delete_anchor(context)
        context.user_data.pop("allow_cancel_undo", None)
        context.user_data.pop("last_field", None)
        context.user_data.pop("field_name", None)
        context.user_data.pop("last_success_message", None)
        context.user_data.pop("cf_stage", None)
        context.user_data["mode"] = MODE_NONE

        if unit:
            try:
                unit_id = int(unit.get("id"))
            except (TypeError, ValueError):
                unit_id = None
            unit_name = str(unit.get("nm") or "").strip() or (f"id {unit_id}" if unit_id is not None else "")
            if unit_id is not None:
                client = None
                if not _pipeline_card_enabled():
                    client = await require_wialon_client(
                        update,
                        context,
                        "Чтобы просматривать объект, авторизуйтесь.",
                    )
                    if not client:
                        return STATE_AUTH_WAIT_TOKEN
                await delete_stats_message(context)
                return await show_stats_then_actions(
                    q,
                    context,
                    unit_id,
                    unit_name,
                    client,
                    via_callback=True,
                )

        await delete_stats_message(context)
        _clear_export_job(context, cancel=True)
        context.user_data.clear()
        return ConversationHandler.END

    await delete_anchor(context)
    await delete_stats_message(context)
    await delete_param_result_message(context)
    _clear_export_job(context, cancel=True)
    context.user_data.clear()
    return ConversationHandler.END

async def back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)
    code = (q.data or "").split(":", 1)[1]

    if code == "menu":
        await delete_anchor(context)
        await delete_stats_message(context)
        await delete_param_result_message(context)
        clear_param_runtime(context)
        clear_report_context(context, cancel_job=True)
        _reset_search_context(context)
        context.user_data["mode"] = MODE_NONE
        return STATE_MENU

    if code == "find":
        await delete_anchor(context)
        await delete_stats_message(context)
        await delete_param_result_message(context)
        clear_param_runtime(context)
        clear_report_context(context, cancel_job=True)
        _reset_search_context(context)
        _set_objects_mode(context, OBJECTS_MODE_LIST)
        msg = await q.message.reply_text(
            "Введите имя/госномер для поиска…",
            reply_markup=kb_search_prompt(context),
        )
        await set_anchor_on(msg, context)
        return STATE_FIND_QUERY

    if code == "unit":
        context.user_data.pop("cf_stage", None)
        unit = context.user_data.get("chosen_unit")
        if not unit:
            units = _filtered_units(context)
            if not units:
                stored = _get_search_units(context)
                if not stored:
                    anchor = context.user_data.get("anchor")
                    if anchor:
                        await edit_anchor(
                            context,
                            "Введите имя/госномер для поиска…",
                            kb_search_prompt(context),
                        )
                    elif q.message:
                        msg = await q.message.reply_text(
                            "Введите имя/госномер для поиска…",
                            reply_markup=kb_search_prompt(context),
                        )
                        await set_anchor_on(msg, context)
                    return STATE_FIND_QUERY
                context.user_data["search_units_offset"] = 0
                _set_objects_mode(context, OBJECTS_MODE_LIST)
                await _render_units_page(
                    context,
                    chat_id=q.message.chat_id if q.message else None,
                    offset=0,
                )
                return STATE_WAIT_UNIT
            offset = _normalize_units_offset(len(units), context.user_data.get("search_units_offset", 0) or 0)
            context.user_data["search_units_offset"] = offset
            _set_objects_mode(context, OBJECTS_MODE_LIST)
            await _render_units_page(
                context,
                chat_id=q.message.chat_id if q.message else None,
                offset=offset,
            )
            return STATE_WAIT_UNIT
        await delete_anchor(context)
        unit_id = int(unit["id"])
        unit_name = unit.get("nm") or f"id {unit_id}"
        context.user_data["mode"] = MODE_NONE
        client = None
        if not _pipeline_card_enabled():
            client = await require_wialon_client(
                update,
                context,
                "Чтобы просматривать объект, авторизуйтесь.",
            )
            if not client:
                return STATE_AUTH_WAIT_TOKEN
        return await show_stats_then_actions(q, context, unit_id, unit_name, client, via_callback=True)

    if code == "cmd_unit" and context.user_data.get("mode") == MODE_CMD:
        origin = context.user_data.get("cmd_return")
        await delete_anchor(context)
        if origin == "params":
            return await restore_params_after_cmd_return(q, context)

        unit = context.user_data.get("chosen_unit")
        if not unit:
            context.user_data["mode"] = MODE_NONE
            context.user_data.pop("cmd_return_payload", None)
            context.user_data.pop("cmd_return_runtime", None)
            context.user_data.pop("cmd_return", None)
            return ConversationHandler.END
        unit_id = int(unit["id"])
        unit_name = unit.get("nm") or f"id {unit_id}"
        context.user_data["mode"] = MODE_NONE
        context.user_data.pop("cmd_return_payload", None)
        context.user_data.pop("cmd_return_runtime", None)
        context.user_data.pop("cmd_return", None)
        client = None
        if not _pipeline_card_enabled():
            client = await require_wialon_client(
                update,
                context,
                "Чтобы просматривать объект, авторизуйтесь.",
            )
            if not client:
                return STATE_AUTH_WAIT_TOKEN
        return await show_stats_then_actions(q, context, unit_id, unit_name, client, via_callback=True)

    if code == "name" and context.user_data.get("mode") == MODE_CF:
        unit = context.user_data.get("chosen_unit")
        if not unit:
            await edit_anchor(context, "Объект не выбран. Диалог завершён.", markup=None)
            return ConversationHandler.END
        context.user_data["cf_stage"] = CF_STAGE_NAME
        await edit_anchor(
            context,
            f"Выбран объект: {unit['nm']} — id {unit['id']}\n\nУкажите фамилию.",
            kb_cf_controls("back:unit"),
            parse_mode="Markdown",
        )
        return STATE_CF_NAME

    if code == "value" and context.user_data.get("mode") == MODE_CF:
        name = context.user_data.get("field_name")
        if not name:
            return STATE_CF_NAME
        context.user_data["cf_stage"] = CF_STAGE_VALUE
        await edit_anchor(
            context,
            f"Поле: *{name}*\n\nОпишите выполненные работы.",
            kb_cf_controls("back:name"),
            parse_mode="Markdown",
        )
        return STATE_CF_VALUE

    return STATE_MENU

# ---- Кнопки под карточкой статистики ----
async def stats_actions_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    action = (q.data or "").split(":", 1)[1]
    log.info("stats_actions: action=%s", action)

    if action == "to_list":
        await _safe_answer_callback(q)
        await clear_stats_buttons(context)
        _clear_stats_generation(context)
        await _activate_objects_ui(context)
        units = _get_search_units(context)
        if units:
            _set_objects_mode(context, OBJECTS_MODE_LIST)
            await _render_units_page(
                context,
                chat_id=q.message.chat_id if q.message else None,
                offset=context.user_data.get("search_units_offset"),
                message=q.message,
                reanchor=True,
            )
            return STATE_WAIT_UNIT
        _reset_search_context(context)
        _set_objects_mode(context, OBJECTS_MODE_LIST)
        await _render_units_page(
            context,
            chat_id=q.message.chat_id if q.message else None,
            message=q.message,
            reanchor=True,
        )
        return STATE_FIND_QUERY

    if action == "add_dut":
        log.info("stats_actions: add_dut flow start")
        return await _start_add_dut_flow(q, context)

    if action == "sensors":
        return await _show_sensor_settings(q, context)

    if action == "dutcal":
        return await _show_dut_calibration(q, context)

    if action == "dutcal_refresh":
        return await _refresh_dut_calibration(q, context)

    if action == "clear_sensors":
        return await _handle_clear_unit_sensors(q, context)


    if action == "nearby":
        _set_objects_mode(context, OBJECTS_MODE_CARD_FLOW_NEARBY)
        unit = context.user_data.get("chosen_unit")
        if not unit:
            await _safe_answer_callback(q, "Сначала выберите объект", show_alert=True)
            _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
            return STATE_WAIT_UNIT
        await _safe_answer_callback(q)
        try:
            unit_id = int(unit["id"])
        except (TypeError, ValueError):
            await q.message.reply_text("Не удалось определить ID объекта.")
            _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
            return STATE_WAIT_UNIT
        unit_name = unit.get("nm") or f"id {unit_id}"

        if not _pipeline_card_enabled():
            await q.message.reply_text("Локальный источник данных не активирован: включите pipeline и повторите попытку.")
            _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
            return STATE_WAIT_UNIT

        return await _handle_nearby_pipeline(q, context, unit_id, unit_name)
    if action == "refresh":
        unit = context.user_data.get("chosen_unit")
        if not unit:
            return ConversationHandler.END
        unit_id = int(unit.get("id"))
        unit_name = unit.get("nm") or f"id {unit_id}"
        # Re-show stats (deletes old stats message and sends a fresh one)
        client = None
        if not _pipeline_card_enabled():
            client = await require_wialon_client(update, context, "Чтобы просматривать объект, авторизуйтесь.")
            if not client:
                return STATE_AUTH_WAIT_TOKEN
        return await show_stats_then_actions(q, context, unit_id, unit_name, client, via_callback=True)

    if action == "cf":
        context.user_data["mode"] = MODE_CF
        context.user_data["cf_stage"] = CF_STAGE_NAME
        _set_objects_mode(context, OBJECTS_MODE_CARD_FLOW_FIELDS)
        await clear_stats_buttons(context)  # оставляем карточку без кнопок
        unit = context.user_data.get("chosen_unit")
        if not unit:
            context.user_data.pop("cf_stage", None)
            _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
            return ConversationHandler.END
        await delete_anchor(context)
        msg = await q.message.reply_text(
            f"Выбран объект: {unit['nm']} — id {unit['id']}\n\nУкажите фамилию.",
            reply_markup=kb_cf_controls("back:unit"),
        )
        await set_anchor_on(msg, context)
        return STATE_CF_NAME

    if action == "cmd":
        unit = context.user_data.get("chosen_unit")
        return await _start_stats_cmd_flow(q, context, unit)

    if action == "report":
        unit = context.user_data.get("chosen_unit")
        if not unit:
            await _safe_answer_callback(q, "Объект не выбран", show_alert=True)
            return STATE_WAIT_UNIT
        client = await require_wialon_client(update, context, "Чтобы формировать отчёты, авторизуйтесь.")
        if not client:
            return STATE_AUTH_WAIT_TOKEN
        _set_objects_mode(context, OBJECTS_MODE_CARD_FLOW_REPORTS)
        result = await _begin_report_flow(
            q.message,
            context,
            client,
            keep_stats_message=True,
        )
        return result

    if action == "params":
        context.user_data["mode"] = MODE_PARAMS
        _set_objects_mode(context, OBJECTS_MODE_CARD_FLOW_SENSORS)
        await clear_stats_buttons(context)
        unit = context.user_data.get("chosen_unit")
        if not unit:
            _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
            return ConversationHandler.END
        names = _ordered_param_names(context)
        note = f"Найдено {len(names)} параметров." if names else "Нет данных о параметрах"
        await delete_param_result_message(context)
        await send_param_prompt(q, context, names, note)
        context.user_data["param_last_query"] = ""
        return STATE_PARAM_QUERY

    if action == "graph":
        unit = context.user_data.get("chosen_unit")
        if not unit:
            await _safe_answer_callback(q, "Объект не выбран", show_alert=True)
            return STATE_WAIT_UNIT
        await _safe_answer_callback(q)
        return await _start_graph_flow(q, context, unit)

    return STATE_MENU


async def _handle_nearby_pipeline(
    q: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    unit_id: int,
    unit_name: str,
) -> int:
    chat = q.message.chat if q.message else None  # type: ignore[attr-defined]
    chat_id = q.message.chat_id if q.message else None  # type: ignore[attr-defined]
    _ensure_settings_defaults(context, chat_id=chat_id)
    trace_id = f"{unit_id}-{uuid.uuid4().hex[:6]}"

    def _log(stage: str, **fields: Any) -> None:
        payload = [
            f"trace={trace_id}",
            f"chat={chat_id}",
            f"unit={unit_id}",
            f"stage={stage}",
        ]
        for key, value in fields.items():
            payload.append(f"{key}={value}")
        nearby_log.info(" | ".join(str(p) for p in payload))

    _log("start")
    storage_service = get_pipeline_storage_service()
    latest = storage_service.get_latest_metrics(unit_id) or {}
    lat = latest.get("lat")
    lon = latest.get("lon")
    if lat is None or lon is None:
        stats_item = context.user_data.get("stats_unit_item")
        if isinstance(stats_item, dict):
            pipeline_event = stats_item.get("pipeline_event") or {}
            if lat is None:
                lat = pipeline_event.get("latitude")
            if lon is None:
                lon = pipeline_event.get("longitude")

    def _finish(state: int = STATE_WAIT_UNIT) -> int:
        _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
        _log("finish", state=state)
        return state

    if lat is None or lon is None:
        _log("no_coords")
        await q.message.reply_text(
            "В локальном кэше нет координат — запустите поток или обновите карточку."
        )
        return _finish()

    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        _log("bad_coords", lat=lat, lon=lon)
        await q.message.reply_text("Координаты объекта повреждены, попробуйте обновить поток.")
        return _finish()

    radius_m = max(1, _get_nearby_radius(context, chat_id=chat_id))
    if NEARBY_MAX_KM > 0.0:
        radius_m = min(radius_m, int(NEARBY_MAX_KM * 1000))

    try:
        nearest = await run_blocking(
            storage_service.find_nearest_units,
            lat_f,
            lon_f,
            float(radius_m),
            NEARBY_LIMIT,
            exclude_unit_id=unit_id,
        )
    except Exception as exc:
        log.exception("pipeline nearby failed")
        _log("call_failed", error=repr(exc))
        await q.message.reply_text(
            f"Не удалось посчитать ближайших (pipeline): `{exc}`",
            parse_mode="Markdown",
        )
        return _finish()

    if not nearest:
        _log("empty", radius_m=radius_m)
        await q.message.reply_text(f"В радиусе {radius_m} м около {unit_name} никого нет.")
        return _finish()

    _log("result", radius_m=radius_m, count=len(nearest))
    header = (
        f"Ближайшие к <b>{html.escape(unit_name)}</b> — радиус {radius_m} м (локальные данные)."
    )
    lines = [header]
    for idx, entry in enumerate(nearest, 1):
        neighbor_name = str(entry.get("nm") or f"id {entry.get('id') or ''}").strip() or f"id {entry.get('id')}"
        dist_txt = _fmt_distance_m(entry.get("distance_m"))
        ts = entry.get("device_ts") or entry.get("received_ts")
        ago_dot, ago_txt = _human_ago(ts)
        coords_text = _format_coords(entry.get("lat"), entry.get("lon")) or "—"
        map_url = _build_map_url(lat=entry.get("lat"), lon=entry.get("lon"))
        base = f"{idx}) {neighbor_name} — {dist_txt} — {ago_dot} {ago_txt}"
        if coords_text != "—":
            decorated = f"{base} @ {coords_text}"
            if map_url:
                lines.append(_format_line_with_link(decorated, coords_text, map_url))
            else:
                lines.append(html.escape(decorated))
        else:
            lines.append(html.escape(base))

    await q.message.reply_text("\n".join(lines), parse_mode="HTML", disable_web_page_preview=True)
    return _finish()


async def graph_actions_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN

    data = q.data or ""
    try:
        _, command = data.split(":", 1)
    except ValueError:
        await _safe_answer_callback(q)
        return STATE_GRAPH_MENU

    await _safe_answer_callback(q)
    graph_ctx = _graph_context(context)
    unit = context.user_data.get("chosen_unit") or {}
    unit_id = graph_ctx.get("unit_id") or unit.get("id")
    unit_name = graph_ctx.get("unit_name") or (unit.get("nm") or "")
    if isinstance(unit_id, str) and unit_id.isdigit():
        unit_id = int(unit_id)

    if command == "back_card":
        context.user_data.pop("graph_context", None)
        context.user_data["mode"] = MODE_NONE
        _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
        await delete_anchor(context)
        await restore_stats_buttons(context)
        return STATE_WAIT_UNIT

    if command.startswith("period:"):
        if unit_id is None:
            await _send_drain_message(q, context, "Объект не выбран.")
            return STATE_WAIT_UNIT
        period_key = command.split(":", 1)[1]
        if period_key == "custom":
            graph_ctx["pending_period"] = "custom"
            await edit_anchor(
                context,
                "На какой день построить график топлива? Например: 15 мая или 13.08.2024.",
                kb_graph_periods(),
                parse_mode=None,
            )
            return STATE_GRAPH_CUSTOM_DATE
        client = await require_wialon_client(
            update,
            context,
            "Чтобы строить график, авторизуйтесь.",
        )
        if not client:
            return STATE_AUTH_WAIT_TOKEN
        await edit_anchor(context, "⏳ Строю график…", kb_graph_periods(), parse_mode=None)
        success = False
        try:
            success = await _handle_drain_summary(
                q,
                context,
                client,
                int(unit_id),
                str(unit_name or unit_id),
                period_key,
            )
        except Exception as exc:
            log.exception("graph: unexpected failure")
            await edit_anchor(
                context,
                "Произошла ошибка при построении графика.",
                kb_graph_periods(),
                parse_mode=None,
            )
            await _send_drain_message(q, context, "Не удалось выполнить анализ сливов. Попробуйте позже.")
        else:
            if success:
                await edit_anchor(
                    context,
                    "Готово! Можно выбрать другой период или вернуться к карточке.",
                    kb_graph_periods(),
                    parse_mode=None,
                )
            else:
                await edit_anchor(
                    context,
                    "Не удалось выполнить анализ сливов. Попробуйте другой период.",
                    kb_graph_periods(),
                    parse_mode=None,
                )
        return STATE_GRAPH_PERIOD

    return STATE_GRAPH_PERIOD


async def graph_custom_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_GRAPH_CUSTOM_DATE

    descriptor = _parse_custom_graph_day(update.message.text or "")
    if not descriptor:
        await update.message.reply_text(
            "Не удалось распознать дату. Например: 15 мая или 13.08.2024.",
        )
        return STATE_GRAPH_CUSTOM_DATE

    graph_ctx = _graph_context(context)
    unit = context.user_data.get("chosen_unit") or {}
    unit_id = graph_ctx.get("unit_id") or unit.get("id")
    unit_name = graph_ctx.get("unit_name") or (unit.get("nm") or "")

    if unit_id is None:
        await update.message.reply_text("Объект не выбран. Вернитесь к списку объектов.")
        return STATE_WAIT_UNIT

    client = await require_wialon_client(
        update,
        context,
        "Чтобы строить график, авторизуйтесь.",
    )
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    try:
        unit_id_int = int(unit_id)
    except Exception:
        await update.message.reply_text(
            "Не удалось определить выбранный объект. Попробуйте выбрать его заново.",
        )
        return STATE_WAIT_UNIT

    await edit_anchor(
        context,
        "⏳ Выполняю анализ сливов…",
        kb_graph_periods(),
        parse_mode=None,
    )

    success = False
    try:
        success = await _handle_drain_summary(
            update,
            context,
            client,
            unit_id_int,
            str(unit_name or unit_id_int),
            descriptor,
        )
    except Exception:
        log.exception("graph: unexpected failure (custom date)")
        await edit_anchor(
            context,
            "Произошла ошибка при анализе сливов.",
            kb_graph_periods(),
            parse_mode=None,
        )
        await _send_drain_message(
            update,
            context,
            "Не удалось выполнить анализ сливов. Попробуйте другой период.",
        )
        return STATE_GRAPH_CUSTOM_DATE
    else:
        if success:
            await edit_anchor(
                context,
                "Готово! Можно выбрать другой период или вернуться к карточке.",
                kb_graph_periods(),
                parse_mode=None,
            )
        else:
            await edit_anchor(
                context,
                "Нет данных для выбранного периода. Попробуйте другой период.",
                kb_graph_periods(),
                parse_mode=None,
            )

    graph_ctx.pop("pending_period", None)
    return STATE_GRAPH_PERIOD

# -------- Поиск (внутри сценария) ----------
async def find_query_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await _handle_objects_text(update, context)

async def choose_unit_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await _safe_answer_callback(q)
    data = q.data or ""
    if data == "units:page":
        return STATE_WAIT_UNIT
    if data.startswith("units:more:"):
        try:
            offset = int(data.split(":", 2)[2])
        except (IndexError, ValueError):
            offset = 0
        units = _filtered_units(context)
        if not units:
            _set_objects_mode(context, OBJECTS_MODE_LIST)
            await edit_anchor(
                context,
                _list_header(0),
                kb_search_prompt(context),
                parse_mode=None,
            )
            return STATE_FIND_QUERY
        normalized_offset = _normalize_units_offset(len(units), offset)
        context.user_data["search_units_offset"] = normalized_offset
        _set_objects_mode(context, OBJECTS_MODE_LIST)
        await _render_units_page(
            context,
            chat_id=q.message.chat_id if q.message else None,
            offset=normalized_offset,
        )
        return STATE_WAIT_UNIT

    client = None
    if not _pipeline_card_enabled():
        client = await require_wialon_client(update, context, "Чтобы просматривать объект, авторизуйтесь.")
        if not client:
            return STATE_AUTH_WAIT_TOKEN
    if not data.startswith("unit:"):
        await edit_anchor(context, "Некорректный выбор", kb_cancel("back:find"), parse_mode=None)
        return STATE_WAIT_UNIT

    unit_id_str = data.split(":", 1)[1]
    unit_id = int(unit_id_str)
    selected = context.user_data.get("search_results", {}).get(unit_id_str) or {"id": unit_id, "nm": f"id {unit_id}"}
    context.user_data["chosen_unit"] = {"id": int(selected["id"]), "nm": selected.get("nm", f"id {unit_id}")}

    await delete_anchor(context)
    context.user_data["last_card_callback_chat_id"] = q.message.chat_id if q.message else None
    context.user_data["last_card_message_id"] = q.message.message_id if q.message else None
    res = await show_stats_then_actions(
        q,
        context,
        unit_id,
        context.user_data["chosen_unit"]["nm"],
        client,
        via_callback=True,
        preserve_previous=True,
    )
    return res

async def show_stats_then_actions(
    update_or_q,
    context: ContextTypes.DEFAULT_TYPE,
    unit_id: int,
    unit_name: str,
    client: Optional["WialonClient"],
    via_callback: bool = False,
    *,
    preserve_previous: bool = False,
) -> int:
    if _pipeline_card_enabled():
        return await _show_pipeline_card_from_storage(
            update_or_q,
            context,
            unit_id,
            unit_name,
            preserve_previous=preserve_previous,
        )
    stats_token = _set_stats_generation(context, unit_id)
    prev_stats_meta = context.user_data.get("stats_msg") if preserve_previous else None
    prev_map_meta = context.user_data.get("stats_map_msg") if preserve_previous else None

    if card_log.isEnabledFor(logging.INFO):
        card_trace = f"{unit_id}-{uuid.uuid4().hex[:6]}"
        card_started = time.perf_counter()
        card_prev = card_started
        base_parts = [f"trace={card_trace}", f"unit={unit_id}"]
        if unit_name:
            base_parts.append(f"name={json.dumps(unit_name, ensure_ascii=False)}")

        def _card_stage(stage: str, **fields: Any) -> None:
            nonlocal card_prev
            now = time.perf_counter()
            delta_ms = int((now - card_prev) * 1000)
            total_ms = int((now - card_started) * 1000)
            card_prev = now
            parts = list(base_parts)
            parts.extend(
                [
                    f"stage={stage}",
                    f"delta_ms={delta_ms}",
                    f"total_ms={total_ms}",
                ]
            )
            if fields:
                for key, value in fields.items():
                    if isinstance(value, str):
                        value_repr = json.dumps(value, ensure_ascii=False)
                    elif isinstance(value, (dict, list, tuple)):
                        try:
                            value_repr = json.dumps(value, ensure_ascii=False)
                        except Exception:
                            value_repr = repr(value)
                    else:
                        value_repr = value
                    parts.append(f"{key}={value_repr}")
            card_log.info(" | ".join(str(part) for part in parts))
    else:

        def _card_stage(stage: str, **fields: Any) -> None:
            return

    _card_stage("start", via_callback=via_callback, preserve_previous=preserve_previous)
    def _restore_previous_stats_meta() -> None:
        if not preserve_previous:
            return
        if prev_stats_meta and "stats_msg" not in context.user_data:
            context.user_data["stats_msg"] = prev_stats_meta
        if prev_map_meta and "stats_map_msg" not in context.user_data:
            context.user_data["stats_map_msg"] = prev_map_meta

    if preserve_previous:
        await clear_stats_buttons(context)
        context.user_data.pop("stats_msg", None)
    else:
        await delete_stats_message(context)
        prev_map_meta = None

    try:
        item = await run_blocking(client.get_unit_full_for_stats, unit_id)
    except Exception as e:
        _card_stage("unit_load_error", error=type(e).__name__)
        _restore_previous_stats_meta()
        log.exception("Ошибка загрузки юнита (Stats)")
        target = update_or_q.message
        await target.reply_text(f"Не удалось загрузить объект: `{e}`", reply_markup=kb_cancel("back:find"))
        return STATE_FIND_QUERY

    current_token, current_unit = _current_stats_generation(context)
    if stats_token != current_token or current_unit != unit_id:
        _card_stage("abort_generation_mismatch")
        _restore_previous_stats_meta()
        return STATE_WAIT_UNIT

    if _active_ui(context) != UI_OBJECTS:
        _card_stage("abort_inactive_ui")
        _restore_previous_stats_meta()
        return STATE_WAIT_UNIT

    _card_stage("unit_loaded")

    if isinstance(item, dict):
        context.user_data["stats_unit_item"] = copy.deepcopy(item)
    else:
        context.user_data.pop("stats_unit_item", None)

    params_map, params_order = extract_params_from_item(item)
    context.user_data["params_map"] = params_map
    context.user_data["params_order"] = params_order
    clear_param_runtime(context)
    await delete_param_result_message(context)

    device_details: Dict[str, Any] = {}
    assigned_drivers: List[Dict[str, Any]] = []
    gather_tasks = [
        run_blocking(client.get_unit_device_details, unit_id),
        run_blocking(client.get_unit_assigned_drivers, unit_id),
    ]
    gathered = await asyncio.gather(*gather_tasks, return_exceptions=True)
    if gathered:
        device_result = gathered[0]
        if isinstance(device_result, Exception):
            log.debug("get_unit_device_details failed: %s", device_result)
        elif isinstance(device_result, dict):
            device_details = device_result
        assigned_result = gathered[1] if len(gathered) > 1 else []
        if isinstance(assigned_result, Exception):
            log.debug("get_unit_assigned_drivers failed: %s", assigned_result)
        elif isinstance(assigned_result, list):
            assigned_drivers = assigned_result

    _card_stage("extra_details_loaded", driver_count=len(assigned_drivers))

    extra_params: Dict[str, str] = {}
    device_uid = str(device_details.get("device_uid") or "").strip()
    if device_uid:
        extra_params.update(
            {
                "uid": device_uid,
                "unique_id": device_uid,
                "uniqueId": device_uid,
                "terminal_id": device_uid,
            }
        )
    device_type_name = str(device_details.get("device_type_name") or "").strip()
    if device_type_name:
        extra_params.update(
            {
                "device_type": device_type_name,
                "hardware": device_type_name,
                "hardware_name": device_type_name,
                "device": device_type_name,
                "hw_name": device_type_name,
                "terminal_type": device_type_name,
            }
        )
    device_type_id = device_details.get("device_type_id")
    if device_type_id is not None:
        extra_params.setdefault("hw", str(device_type_id))

    driver_display_text = ""
    if assigned_drivers:
        driver_names: List[str] = []
        for driver in assigned_drivers:
            if not isinstance(driver, dict):
                continue
            name = driver.get("name")
            code = driver.get("code")
            identifier = driver.get("id")
            formatted = None
            if isinstance(name, str) and name.strip():
                trimmed_name = name.strip()
                if isinstance(code, str) and code.strip() and code.strip() != trimmed_name:
                    formatted = f"{trimmed_name} ({code.strip()})"
                else:
                    formatted = trimmed_name
            elif isinstance(code, str) and code.strip():
                formatted = code.strip()
            elif isinstance(identifier, int):
                formatted = str(identifier)
            if formatted:
                driver_names.append(formatted)
        if driver_names:
            driver_display_text = ", ".join(driver_names)
            extra_params.setdefault("assigned_driver", driver_display_text)
            extra_params.setdefault("driver_name", driver_display_text)

    if extra_params:
        for key, value in extra_params.items():
            if value:
                params_map[key] = value

    pos = item.get("pos") or {}
    lmsg = item.get("lmsg") or {}
    last_ts = lmsg.get("t") or pos.get("t")
    sats = pos.get("sc")
    online = _is_online(last_ts)
    online_emoji = "🟢" if online else "🔴"
    online_text = f"{online_emoji} На связи" if online else f"{online_emoji} Нет связи"

    fuel_line = ""
    sensor = WialonClient._pick_fuel_sensor(item)
    fuel_task = None
    if sensor:
        sensor_id = int(sensor.get("id"))

        def _calc_fuel() -> Optional[float]:
            try:
                client.load_last_message(unit_id)
            except Exception:
                pass
            return client.calc_sensor_value(unit_id, sensor_id)

        fuel_task = asyncio.create_task(run_blocking(_calc_fuel))

    sat_dot, sat_ago = _human_ago(pos.get("t"))
    sats_text = f"{sats} спутн." if sats is not None else "—"
    lat_raw = pos.get("y") if isinstance(pos, dict) else None
    if lat_raw is None and isinstance(pos, dict):
        lat_raw = pos.get("lat")
    lon_raw = pos.get("x") if isinstance(pos, dict) else None
    if lon_raw is None and isinstance(pos, dict):
        lon_raw = pos.get("lon")
    try:
        lat_val = float(lat_raw) if lat_raw is not None else None
    except Exception:
        lat_val = None
    try:
        lon_val = float(lon_raw) if lon_raw is not None else None
    except Exception:
        lon_val = None

    address_text = _extract_address_from_pos(pos)

    target = update_or_q.message
    reverse_geocode_task = None
    geozone_task = None
    map_bytes_task = None
    if lat_val is not None and lon_val is not None:
        if not address_text:
            reverse_geocode_task = asyncio.create_task(
                run_blocking(client.reverse_geocode, lat_val, lon_val)
            )
        geozone_task = asyncio.create_task(
            run_blocking(client.get_unit_geozone_details, unit_id, lat_val, lon_val)
        )
        if target:
            map_bytes_task = asyncio.create_task(
                run_blocking(_download_yandex_static_map, lat_val, lon_val)
            )
    dut_lines_task = asyncio.create_task(run_blocking(_build_dut_lines2, client, item, unit_id))

    if reverse_geocode_task:
        try:
            geocoded = await reverse_geocode_task
        except Exception as exc:
            geocoded = None
            log.debug('reverse_geocode call failed: %s', exc)
        if geocoded:
            address_text = geocoded

    coords_text = _format_coords(lat_val, lon_val)
    if address_text and coords_text:
        location_display = f"{coords_text} — {address_text}"
    else:
        location_display = address_text or coords_text or ''
    location_map_url = _build_map_url(address=address_text, lat=lat_val, lon=lon_val)
    geozone_label = "Геозона"
    geozone_display = "вне зон"
    geozone_map_url: Optional[str] = None
    primary_zone: Optional[Dict[str, Any]] = None
    geozone_payload: Dict[str, Any] = {}
    if geozone_task:
        try:
            result = await geozone_task
        except Exception as exc:
            geozone_payload = {}
            geo_log.debug("get_unit_geozone_details failed: %s", exc)
        else:
            geozone_payload = result if isinstance(result, dict) else {}
    names_ordered: List[str] = []
    geozone_entries: List[Dict[str, Any]] = []
    if isinstance(geozone_payload, dict):
        raw_names = geozone_payload.get("names") or []
        if isinstance(raw_names, list):
            for name in raw_names:
                name_str = str(name).strip()
                if name_str:
                    names_ordered.append(name_str)
        raw_entries = geozone_payload.get("entries") or []
        if isinstance(raw_entries, list):
            geozone_entries = [dict(entry) for entry in raw_entries if isinstance(entry, dict)]

    if names_ordered:
        geozone_label = "Геозоны"
        display_parts: List[str] = [f"📍 {name}" for name in names_ordered[:GEO_MAX_NAMES]]
        if len(names_ordered) > GEO_MAX_NAMES:
            extra = len(names_ordered) - GEO_MAX_NAMES
            display_parts.append(f"… и ещё {extra}")
        geozone_display = ", ".join(display_parts)
        if geozone_entries:
            primary_zone = dict(geozone_entries[0])

    geo_log.debug(
        "zones_render: names=%s",
        ", ".join(names_ordered) if names_ordered else "—",
    )

    if primary_zone:
        g_lat = primary_zone.get("lat")
        g_lon = primary_zone.get("lon")
        if (g_lat is None or g_lon is None) and primary_zone.get("resource_id") and primary_zone.get("zone_id"):
            try:
                center = await run_blocking(
                    client.get_zone_center,
                    int(primary_zone["resource_id"]),
                    int(primary_zone["zone_id"]),
                )
            except Exception as exc:
                center = None
                geo_log.debug("get_zone_center failed: %s", exc)
            if center:
                g_lat, g_lon = center
                primary_zone["lat"], primary_zone["lon"] = center
        addr = str(primary_zone.get("name") or "").strip() or None
        if g_lat is not None and g_lon is not None:
            geozone_map_url = _build_map_url(address=addr, lat=g_lat, lon=g_lon)

    def _extract_text(value: Any) -> Optional[str]:
        if isinstance(value, str):
            text = value.strip()
            return text or None
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, dict):
            for key in ("n", "nm", "name", "value", "val", "title"):
                if key in value:
                    nested = _extract_text(value.get(key))
                    if nested:
                        return nested
            for nested in value.values():
                text = _extract_text(nested)
                if text:
                    return text
        if isinstance(value, (list, tuple)):
            for item_nested in value:
                text = _extract_text(item_nested)
                if text:
                    return text
        return None

    def _deep_search(source: Any, keys: List[str]) -> Optional[str]:
        if source is None:
            return None
        key_set = {key.casefold() for key in keys}
        stack: List[Any] = [source]
        seen: Set[int] = set()
        while stack:
            current = stack.pop()
            ident = id(current)
            if ident in seen:
                continue
            seen.add(ident)
            if isinstance(current, dict):
                for key, val in current.items():
                    key_cf = key.casefold() if isinstance(key, str) else None
                    if key_cf and key_cf in key_set:
                        candidate = _extract_text(val)
                        if candidate:
                            return candidate
                    stack.append(val)
            elif isinstance(current, (list, tuple)):
                stack.extend(current)
        return None

    def _get_from_params(params: Dict[str, str], keys: List[str]) -> Optional[str]:
        lowered_map = {k.casefold(): v for k, v in params.items()}
        for key in keys:
            value = lowered_map.get(key.casefold())
            if value:
                text = _extract_text(value)
                if text:
                    return text
        return None

    info_wialon_id = _get_from_params(
        params_map,
        ["monitoring_units_custom_row", "wialon_id", "unit_id", "object_id"],
    )
    if not info_wialon_id:
        info_wialon_id = _deep_search(
            item,
            [
                "monitoring_units_custom_row",
                "wialon_id",
                "unit_id",
                "object_id",
                "wialon_object_id",
            ],
        )
    if not info_wialon_id and unit_id:
        info_wialon_id = str(unit_id)

    info_device_type = _get_from_params(
        params_map,
        [
            "device_type",
            "hardware",
            "hardware_name",
            "device",
            "hw_name",
            "terminal_type",
        ],
    )
    if not info_device_type:
        info_device_type = _deep_search(
            item,
            [
                "device_type",
                "hardware",
                "hardware_name",
                "device",
                "hw_name",
                "terminal_type",
            ],
        )
    if not info_device_type:
        if device_type_name:
            info_device_type = device_type_name
        elif device_type_id is not None:
            info_device_type = str(device_type_id)

    info_terminal_id = _get_from_params(
        params_map,
        ["terminal_id", "terminal", "uid", "unique_id", "imei", "serial", "serial_number"],
    )
    if not info_terminal_id:
        info_terminal_id = _deep_search(
            item,
            ["terminal_id", "terminal", "uid", "unique_id", "imei", "serial", "serial_number"],
        )
    if not info_terminal_id and device_uid:
        info_terminal_id = device_uid

    info_driver = _get_from_params(
        params_map,
        [
            "driver_name",
            "assigned_driver",
            "driver",
            "driver_full_name",
            "driver_nm",
        ],
    )
    if not info_driver:
        info_driver = _deep_search(
            item,
            [
                "driver_name",
                "assigned_driver",
                "driver",
                "driver_full_name",
                "driver_nm",
                "driver_title",
            ],
        )
    if not info_driver and driver_display_text:
        info_driver = driver_display_text

    def _display_or_dash(value: Optional[str]) -> str:
        return value if value else "—"

    def _append_group(heading: str, entries: List[str]) -> List[int]:
        if not entries:
            return []
        if lines:
            lines.append("")
        lines.append(heading)
        start_idx = len(lines)
        lines.extend(entries)
        return list(range(start_idx, start_idx + len(entries)))
    if fuel_task:
        try:
            fuel_value = await fuel_task
        except Exception as exc:
            log.warning("Не удалось получить расчётное значение топлива по ДУТ: %s", exc)
        else:
            normalized = normalize_fuel_value(fuel_value)
            if normalized is not None and FUEL_MIN_VALID_L <= normalized <= FUEL_MAX_VALID_L:
                formatted = format_liters(normalized)
                if formatted is not None:
                    fuel_line = f"{formatted} л"


    lines: List[str] = []
    general_entries = [
        f"• Имя: {unit_name}",
        f"• Связь: {online_text}",
        f"• Спутники: {sat_dot} определены {sat_ago} назад ({sats_text})",
        f"• Последнее сообщение: {_fmt_ts_utc(last_ts)}",
        f"• ДУТ (уровень топлива): {_display_or_dash(fuel_line)}",
        f"• Назначенный водитель: {_display_or_dash(info_driver)}",
    ]
    _append_group("🧾 Общая информация", general_entries)

    location_entries = [f'• Координаты: {_display_or_dash(coords_text)}']
    address_hint: Optional[str] = None
    if snapshot_record:
        address_hint = snapshot_record.meta.get('address') or snapshot_record.meta.get('addr')
    if address_hint:
        location_entries.append(f'• Адрес: {address_hint}')
    location_indices = _append_group("🗺️ Местоположение", location_entries)

    device_dut_lines: List[str] = []
    try:
        dut_lines = await dut_lines_task
    except Exception as e:
        log.warning("build_dut_lines failed: %s", e)
    else:
        if isinstance(dut_lines, list) and dut_lines:
            processed: List[str] = []
            for raw_line in dut_lines:
                normalized_line = str(raw_line or "").strip()
                if normalized_line:
                    processed.append(normalized_line)
            if processed:
                device_dut_lines = processed
    if device_dut_lines:
        dut_label_prefix = "• ДУТ (уровень топлива):"
        device_dut_lines = [
            line
            for line in device_dut_lines
            if not line.strip().startswith(dut_label_prefix)
        ]

    device_entries = list(device_dut_lines)
    device_entries.append(f"• ID объекта Wialon: {_display_or_dash(info_wialon_id)}")
    device_entries.append(f"• Тип устройства: {_display_or_dash(info_device_type)}")
    device_entries.append(f"• ID терминала: {_display_or_dash(info_terminal_id)}")
    _append_group("🆔 Параметры устройства", device_entries)

    links_map: Dict[int, Tuple[str, str]] = {}
    if location_map_url and location_display != "—" and location_indices:
        links_map[location_indices[0]] = (location_display, location_map_url)
    if (
        geozone_map_url
        and geozone_display not in ("—", "вне зон")
        and len(location_indices) > 1
    ):
        links_map[location_indices[1]] = (geozone_display, geozone_map_url)

    message_html = _format_lines_with_links(lines, links_map)
    _card_stage(
        "payload_ready",
        line_count=len(lines),
        has_coordinates=bool(lat_val is not None and lon_val is not None),
    )
    map_caption = None
    map_caption_parse_mode = None

    try:
        msg = await target.reply_text(
            message_html,
            reply_markup=kb_stats_actions_with_refresh(),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as exc:
        _card_stage("card_send_error", error=type(exc).__name__)
        raise
    _card_stage("card_sent", message_id=msg.message_id, line_count=len(lines))
    context.user_data["stats_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
    _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)

    map_kind = "none"
    map_msg = None
    map_bytes_obtained = False
    map_bytes = None
    if map_bytes_task and target and lat_val is not None and lon_val is not None:
        try:
            map_bytes = await map_bytes_task
            map_bytes_obtained = bool(map_bytes)
        except Exception as exc:
            map_bytes = None
            log.debug("download_static_map failed: %s", exc)
        if map_bytes:
            photo_stream = io.BytesIO(map_bytes)
            photo_stream.name = "yandex_map.png"
            map_file = InputFile(photo_stream, filename="yandex_map.png")
            try:
                map_msg = await target.reply_photo(
                    photo=map_file,
                    caption=map_caption,
                    parse_mode=map_caption_parse_mode,
                )
                map_kind = "photo"
            except Exception as exc:
                log.debug("send_map_photo failed: %s", exc)
                map_msg = None
        if map_msg is None and lat_val is not None and lon_val is not None:
            try:
                map_msg = await target.reply_location(
                    latitude=float(lat_val),
                    longitude=float(lon_val),
                    disable_notification=True,
                )
                if map_msg is not None:
                    map_kind = "location"
            except Exception as exc:
                log.debug("send_location failed: %s", exc)
                map_msg = None
    if map_msg is not None:
        new_map_meta = {
            "chat_id": map_msg.chat_id,
            "message_id": map_msg.message_id,
        }
        context.user_data["stats_map_msg"] = new_map_meta
        if prev_map_meta and (
            prev_map_meta.get("chat_id") != map_msg.chat_id
            or prev_map_meta.get("message_id") != map_msg.message_id
        ):
            try:
                await context.bot.delete_message(
                    chat_id=prev_map_meta["chat_id"],
                    message_id=prev_map_meta["message_id"],
                )
            except Exception as exc:
                log.debug("cleanup old map failed: %s", exc)
        _card_stage("map_sent", map_kind=map_kind)
    else:
        _card_stage("map_skipped", candidate=map_bytes_obtained)
    _card_stage("done")
    return STATE_WAIT_UNIT

# -------- Ввод фамилии/работ (CF) ----------


def _build_pipeline_event_from_message(unit_id: int, payload: Mapping[str, Any]) -> Optional[Event]:
    if not isinstance(payload, Mapping):
        return None
    try:
        ts = int(payload.get("t") or payload.get("time") or 0)
    except Exception:
        ts = 0
    if ts <= 0:
        return None
    try:
        received_ts = int(payload.get("rt") or payload.get("serverTime") or ts)
    except Exception:
        received_ts = ts
    pos = payload.get("pos") or {}
    lat = (
        pos.get("y")
        or payload.get("y")
        or payload.get("lat")
        or payload.get("latitude")
    )
    lon = (
        pos.get("x")
        or payload.get("x")
        or payload.get("lon")
        or payload.get("longitude")
    )
    speed = pos.get("s") or payload.get("s") or payload.get("speed")
    course = pos.get("c") or payload.get("c") or payload.get("course")
    params_raw = payload.get("p") or payload.get("params") or {}
    params: Dict[str, Any] = dict(params_raw) if isinstance(params_raw, Mapping) else {}
    sat_count = pos.get("sc") or params.get("sats") or params.get("satellites")
    if sat_count is None:
        gps = params.get("sats_gps")
        glonass = params.get("sats_glonass")
        try:
            if gps is not None or glonass is not None:
                sat_count = int(gps or 0) + int(glonass or 0)
        except Exception:
            sat_count = None
    if sat_count is not None:
        try:
            params.setdefault("sats", int(sat_count))
        except Exception:
            pass
    try:
        lat_val = float(lat) if lat is not None else None
    except (TypeError, ValueError):
        lat_val = None
    try:
        lon_val = float(lon) if lon is not None else None
    except (TypeError, ValueError):
        lon_val = None
    try:
        speed_val = float(speed) if speed is not None else None
    except (TypeError, ValueError):
        speed_val = None
    try:
        course_val = float(course) if course is not None else None
    except (TypeError, ValueError):
        course_val = None
    return Event(
        unit_id=unit_id,
        device_ts=ts,
        received_ts=received_ts,
        latitude=lat_val,
        longitude=lon_val,
        speed=speed_val,
        course=course_val,
        params=params,
        source="bootstrap",
        raw_payload=dict(payload),
    )


async def _bootstrap_pipeline_storage_from_remote(
    unit_id: int,
    chat_id: Optional[int],
    storage_service: PipelineStorageService,
    stage_cb: Optional[Callable[..., None]] = None,
) -> bool:
    if chat_id is None:
        return False
    try:
        client = wialon_for_chat(chat_id)
    except (NotAuthorized, UserBlocked) as exc:
        log.debug("pipeline bootstrap: no token unit=%s chat=%s err=%s", unit_id, chat_id, exc)
        return False
    params = {
        "itemId": int(unit_id),
        "lastTime": 0,
        "lastCount": 16,
        "flags": 0,
        "flagsMask": 0,
    }
    try:
        resp = await run_blocking(client.request, "messages/load_last", params)
    except Exception as exc:
        log.warning("pipeline bootstrap: load_last failed unit=%s err=%s", unit_id, exc)
        return False
    messages = resp.get("messages") if isinstance(resp, Mapping) else resp
    if not isinstance(messages, list):
        log.info("pipeline bootstrap: no messages unit=%s", unit_id)
        return False
    events: List[Event] = []
    for payload in messages:
        if not isinstance(payload, Mapping):
            continue
        event = _build_pipeline_event_from_message(unit_id, payload)
        if event is not None:
            events.append(event)
    if not events:
        log.info("pipeline bootstrap: empty payload unit=%s", unit_id)
        return False
    events.sort(key=lambda item: item.device_ts)
    stored = storage_service.store_events(events)
    log.info("pipeline bootstrap: stored=%s unit=%s", stored, unit_id)
    if stage_cb:
        try:
            stage_cb("bootstrap_remote", stored=stored)
        except Exception:
            pass
    return stored > 0


async def _show_pipeline_card_from_storage(
    update_or_q,
    context: ContextTypes.DEFAULT_TYPE,
    unit_id: int,
    unit_name: str,
    *,
    preserve_previous: bool = False,
) -> int:
    perf_started = time.perf_counter()
    perf_prev = perf_started

    def _pipeline_stage(stage: str, **fields: Any) -> None:
        nonlocal perf_prev
        if not card_log.isEnabledFor(logging.INFO):
            return
        now = time.perf_counter()
        delta_ms = int((now - perf_prev) * 1000)
        total_ms = int((now - perf_started) * 1000)
        perf_prev = now
        parts = [
            "pipeline",
            f"unit={unit_id}",
            f"stage={stage}",
            f"delta_ms={delta_ms}",
            f"total_ms={total_ms}",
        ]
        for key, value in fields.items():
            if isinstance(value, str):
                value_repr = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, (dict, list, tuple)):
                try:
                    value_repr = json.dumps(value, ensure_ascii=False)
                except Exception:
                    value_repr = repr(value)
            else:
                value_repr = value
            parts.append(f"{key}={value_repr}")
        card_log.info(" | ".join(str(part) for part in parts))

    _pipeline_stage("start")
    storage_service = get_pipeline_storage_service()
    snapshot_service = get_unit_snapshot_service()
    unit_config: Optional[UnitConfig] = None
    try:
        unit_config = _load_unit_config(unit_id)
    except Exception:
        unit_config = None

    snapshot_record: Optional[UnitSnapshotRecord] = None
    try:
        snapshot_record = snapshot_service.get_unit(unit_id)
    except Exception as exc:
        log.debug("pipeline card: snapshot lookup failed unit=%s err=%s", unit_id, exc)
    if unit_config and unit_config.general and unit_config.general.name:
        unit_name = unit_config.general.name
    elif snapshot_record and snapshot_record.name:
        unit_name = snapshot_record.name
    # Fast path 0: metrics-only event (no file IO)
    record = storage_service.get_latest_event_from_metrics(unit_id)
    # Fast path 1: try latest_metrics day with bounded lookback, then fallback to slow scan
    if record is None:
        record = storage_service.find_latest_event_fast(unit_id)
    if record is None:
        record = storage_service.find_latest_event(unit_id)
    if record is None:
        chat_obj = getattr(update_or_q, "effective_chat", None)
        chat_id = getattr(chat_obj, "id", None)
        fetched = await _bootstrap_pipeline_storage_from_remote(
            unit_id, chat_id, storage_service, stage_cb=_pipeline_stage
        )
        if fetched:
            record = storage_service.find_latest_event(unit_id)
    target_message = getattr(update_or_q, "message", None) or getattr(
        update_or_q, "effective_message", None
    )
    if record is None:
        if target_message:
            await target_message.reply_text(
                "Нет локальных данных для этого объекта. Запустите загрузку сообщений."
            )
        _pipeline_stage("no_event")
        return STATE_FIND_QUERY

    _set_stats_generation(context, unit_id)
    _pipeline_stage("event_loaded", day=record.day)
    if preserve_previous:
        await clear_stats_buttons(context)
        context.user_data.pop("stats_msg", None)
    else:
        await delete_stats_message(context)
    context.user_data.pop("stats_map_msg", None)
    context.user_data["stats_unit_item"] = {
        "id": unit_id,
        "pipeline_event": record.event.as_dict(),
    }
    latest_metrics = storage_service.get_latest_metrics(unit_id)
    params_map = _merge_event_params(record.event.params, latest_metrics)
    context.user_data["params_map"] = params_map
    context.user_data["params_order"] = list(params_map.keys())
    clear_param_runtime(context)
    await delete_param_result_message(context)

    dot, ago_text = _human_ago(record.event.device_ts)
    timestamp = _fmt_ts_utc(record.event.device_ts)
    links_map: Dict[int, Tuple[str, str]] = {}
    lines: List[str] = []
    lat_val = record.event.latitude
    lon_val = record.event.longitude
    if lat_val is None and isinstance(latest_metrics, dict):
        lat_val = latest_metrics.get("lat")
    if lon_val is None and isinstance(latest_metrics, dict):
        lon_val = latest_metrics.get("lon")
    coords_text = _format_coords(lat_val, lon_val)

    def _display_or_dash(value: Optional[str]) -> str:
        return value if value else "—"

    def _append_group(heading: str, entries: List[str]) -> List[int]:
        if not entries:
            return []
        if lines:
            lines.append("")
        lines.append(heading)
        start_index = len(lines)
        lines.extend(entries)
        return list(range(start_index, start_index + len(entries)))

    def _get_param_value(keys: List[str]) -> Optional[str]:
        lowered = {str(k).casefold(): str(v) for k, v in params_map.items()}
        for key in keys:
            value = lowered.get(key.casefold())
            if value:
                return value.strip()
        return None

    sensor_values: Mapping[str, Tuple[Optional[float], Optional[float]]] = {}
    if unit_config and unit_config.sensors:
        try:
            sensor_values = _compute_unit_config_sensor_values(unit_config, params_map)
        except Exception as exc:
            log.warning("unit_config: sensor compute failed unit=%s err=%s", unit_id, exc)
            sensor_values = {}
    local_fuel_line = _pick_unit_config_fuel_display(unit_config, sensor_values)

    online = _is_online(record.event.device_ts)
    online_emoji = "🟢" if online else "🔴"
    online_text = f"{online_emoji} На связи" if online else f"{online_emoji} Нет связи"

    sat_ts = None
    if isinstance(latest_metrics, dict) and latest_metrics.get("device_ts"):
        sat_ts = latest_metrics.get("device_ts")
    if sat_ts is None:
        sat_ts = record.event.device_ts
    sat_dot, sat_ago = _human_ago(sat_ts)
    sats_raw = params_map.get("sats") or params_map.get("satellites")
    sats_display = "—"
    if sats_raw is not None:
        try:
            sats_value = int(float(sats_raw))
            sats_display = f"{sats_value} спутн."
        except (TypeError, ValueError):
            sats_display = str(sats_raw)

    def _pick_fuel_value() -> Optional[str]:
        if local_fuel_line:
            return local_fuel_line
        for key in ("fuel_level", "fuelLinear", "calc_fuel_level", "fuel"):
            raw = params_map.get(key)
            if raw is None:
                continue
            try:
                value = float(str(raw).replace(",", "."))
            except (TypeError, ValueError):
                continue
            normalized = normalize_fuel_value(value)
            if normalized is not None and FUEL_MIN_VALID_L <= normalized <= FUEL_MAX_VALID_L:
                formatted = format_liters(normalized)
                if formatted is not None:
                    return f"{formatted} л"
        return None

    fuel_line = _pick_fuel_value()
    driver_display = _get_param_value(
        ["driver_name", "assigned_driver", "driver", "driver_full_name", "driver_nm"]
    )
    if not driver_display and snapshot_record:
        driver_display = (
            snapshot_record.meta.get("driver_name")
            or snapshot_record.meta.get("driver")
            or snapshot_record.contacts.get("driver")
        )

    general_entries = [f"• Имя: {unit_name}"]
    if snapshot_record and snapshot_record.reg_number:
        general_entries.append(f"• Госномер: {snapshot_record.reg_number}")
    general_entries.extend(
        [
            f"• Связь: {online_text}",
            f"• Спутники: {sat_dot} определены {sat_ago} назад ({_display_or_dash(sats_display)})",
            f"• Последнее сообщение: {dot} {timestamp} ({ago_text})",
            f"• ДУТ (уровень топлива): {_display_or_dash(fuel_line)}",
            f"• Назначенный водитель: {_display_or_dash(driver_display)}",
        ]
    )
    _append_group("🧾 Общая информация", general_entries)

    config_general_entries = _build_unit_config_general_entries(unit_config)
    if config_general_entries:
        _append_group("⚙️ Свойства (конфиг)", config_general_entries)

    location_entries = [f'• Координаты: {_display_or_dash(coords_text)}']
    address_hint: Optional[str] = None
    if snapshot_record:
        address_hint = snapshot_record.meta.get('address') or snapshot_record.meta.get('addr')
    if address_hint:
        location_entries.append(f'• Адрес: {address_hint}')
    location_indices = _append_group("🗺️ Местоположение", location_entries)

    info_wialon_id = _get_param_value(
        ["monitoring_units_custom_row", "wialon_id", "unit_id", "object_id"]
    ) or str(unit_id)
    info_device_type = _get_param_value(
        ["device_type", "hardware", "hardware_name", "device", "hw_name", "terminal_type"]
    )
    if not info_device_type and isinstance(latest_metrics, dict):
        info_device_type = (
            str(latest_metrics.get("device_type") or latest_metrics.get("hardware") or "").strip() or None
        )
    if not info_device_type and snapshot_record:
        info_device_type = snapshot_record.device.hardware
    info_terminal_id = _get_param_value(
        ["terminal_id", "terminal", "uid", "unique_id", "imei", "serial", "serial_number"]
    )
    if not info_terminal_id and isinstance(latest_metrics, dict):
        info_terminal_id = str(latest_metrics.get("uid") or "").strip() or None
    if not info_terminal_id and snapshot_record:
        info_terminal_id = snapshot_record.device.uid

    device_entries = [
        f"• ID объекта Wialon: {_display_or_dash(info_wialon_id)}",
        f"• Тип устройства: {_display_or_dash(info_device_type)}",
        f"• ID терминала: {_display_or_dash(info_terminal_id)}",
    ]
    _append_group("🆔 Параметры устройства", device_entries)

    contact_entries: List[str] = []
    if snapshot_record and snapshot_record.contacts:
        for key, value in snapshot_record.contacts.items():
            if value:
                contact_entries.append(f"• {key}: {value}")
            if len(contact_entries) >= 5:
                break
    if contact_entries:
        _append_group("📞 Контакты", contact_entries)

    sensor_entries: List[str] = []
    if snapshot_record and snapshot_record.sensors:
        max_sensors = 6
        for sensor in snapshot_record.sensors[:max_sensors]:
            sensor_name = sensor.name or sensor.type or f"Сенсор {sensor.sensor_id or ''}".strip()
            details: List[str] = []
            if sensor.type and sensor.type != sensor_name:
                details.append(sensor.type)
            if sensor.units:
                details.append(sensor.units)
            if sensor.meta.get("last_value") is not None:
                details.append(str(sensor.meta["last_value"]))
            if details:
                sensor_entries.append(f"• {sensor_name} ({', '.join(details)})")
            else:
                sensor_entries.append(f"• {sensor_name}")
        remaining = len(snapshot_record.sensors) - max_sensors
        if remaining > 0:
            sensor_entries.append(f"… и ещё {remaining}")
    if sensor_entries:
        _append_group("🧪 Датчики", sensor_entries)

    config_sensor_entries = _build_unit_config_sensor_entries(
        unit_config,
        params_map,
        sensor_values=sensor_values,
    )
    if config_sensor_entries:
        _append_group("🧷 Датчики (UnitConfig)", config_sensor_entries)

    links_map: Dict[int, Tuple[str, str]] = {}
    if coords_text and location_indices:
        map_url = _build_map_url(address=None, lat=lat_val, lon=lon_val)
        if map_url:
            links_map[location_indices[0]] = (coords_text, map_url)

    used_keys = {
        k.casefold()
        for k in [
            "fuel_level",
            "fuel",
            "fuelLinear",
            "calc_fuel_level",
            "driver_name",
            "assigned_driver",
            "driver",
            "driver_full_name",
            "driver_nm",
            "sats",
            "satellites",
        ]
    }
    other_params: List[str] = []
    for key, value in params_map.items():
        if key.casefold() in used_keys:
            continue
        if len(other_params) >= 8:
            break
        other_params.append(f"{key}={value}")
    if other_params:
        _append_group("⚙️ Параметры", [", ".join(other_params)])

    snapshot_bundle = snapshot_service.load_bundle()
    snapshot_info: List[str] = []
    if snapshot_bundle.source_kind:
        snapshot_info.append(snapshot_bundle.source_kind)
    if snapshot_bundle.dump_ts:
        try:
            snapshot_dt = datetime.fromtimestamp(int(snapshot_bundle.dump_ts), tz=MOSCOW_TZ)
            snapshot_info.append(snapshot_dt.strftime("%d.%m.%Y %H:%M"))
        except Exception:
            snapshot_info.append(str(snapshot_bundle.dump_ts))
    if snapshot_info:
        lines.append("")
        lines.append(f"🗂 Снапшот: {' • '.join(snapshot_info)}")

    message_html = _format_lines_with_links(lines, links_map)
    if target_message is None:
        chat = update_or_q.effective_chat if hasattr(update_or_q, "effective_chat") else None
        if chat:
            target_message = await context.bot.send_message(chat_id=chat.id, text="…")
        else:
            raise RuntimeError("Unable to determine message target for pipeline card")

    msg = await target_message.reply_text(
        message_html,
        reply_markup=kb_stats_actions_with_refresh(),
        parse_mode="HTML",
        disable_web_page_preview=False,
    )
    context.user_data["stats_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
    _pipeline_stage("done", message_id=msg.message_id)
    _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
    return STATE_WAIT_UNIT


def _build_unit_config_general_entries(unit_config: Optional[UnitConfig]) -> List[str]:
    if unit_config is None:
        return []
    entries: List[str] = []
    general = unit_config.general
    if general:
        if general.uid:
            entries.append(f"• UID конфигурации: {general.uid}")
        if general.phone:
            entries.append(f"• Телефон SIM: {general.phone}")
        if general.phone2:
            entries.append(f"• Телефон SIM 2: {general.phone2}")
    hw_name = None
    if unit_config.hw_config and unit_config.hw_config.hardware:
        hw_name = unit_config.hw_config.hardware
    elif general and general.hardware:
        hw_name = general.hardware
    if hw_name:
        entries.append(f"• Тип устройства (конфиг): {hw_name}")
    if unit_config.hw_config and unit_config.hw_config.params:
        labels: List[str] = []
        for param in unit_config.hw_config.params[:3]:
            title = param.label or param.name
            if title:
                labels.append(title)
        if labels:
            entries.append(f"• Параметры HW: {', '.join(labels)}")
    if unit_config.counters and unit_config.counters.values:
        counter_pairs = []
        for key, value in list(unit_config.counters.values.items())[:4]:
            counter_pairs.append(f"{key}={value}")
        if counter_pairs:
            entries.append(f"• Счётчики: {', '.join(counter_pairs)}")
    return entries


def _build_unit_config_sensor_entries(
    unit_config: Optional[UnitConfig],
    params_map: Mapping[str, Any],
    limit: int = 5,
    *,
    sensor_values: Optional[Mapping[str, Tuple[Optional[float], Optional[float]]]] = None,
) -> List[str]:
    if unit_config is None or not unit_config.sensors:
        return []
    if sensor_values is None:
        sensor_values = _compute_unit_config_sensor_values(unit_config, params_map)
    entries: List[str] = []
    for sensor in unit_config.sensors[:limit]:
        info = sensor_values.get(sensor.name)
        display = _format_sensor_display(info, sensor)
        details: List[str] = []
        if sensor.type and sensor.type != sensor.name:
            details.append(sensor.type)
        units_text = _extract_sensor_units(sensor)
        if units_text:
            details.append(units_text)
        source = _guess_sensor_source_key(sensor.parameters)
        if source:
            details.append(source)
        suffix = f" ({', '.join(details)})" if details else ""
        entries.append(f"• {sensor.name}: {display}{suffix}")
    remaining = len(unit_config.sensors) - limit
    if remaining > 0:
        entries.append(f"… и ещё {remaining}")
    return entries


def _extract_sensor_units(sensor: SensorConfig) -> Optional[str]:
    units = (sensor.units or "").strip()
    if units:
        return units
    if isinstance(sensor.parameters, Mapping):
        value = sensor.parameters.get("m") or sensor.parameters.get("units")
        if isinstance(value, str):
            units = value.strip()
            if units:
                return units
    return None


def _compute_unit_config_sensor_values(
    config: UnitConfig,
    params_map: Mapping[str, Any],
) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
    sensor_by_name = {sensor.name: sensor for sensor in config.sensors if sensor.name}
    memo: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    visiting: set[str] = set()

    def compute(name: str) -> Tuple[Optional[float], Optional[float]]:
        if name in memo:
            return memo[name]
        if name in visiting:
            return (None, None)
        visiting.add(name)
        sensor = sensor_by_name.get(name)
        if sensor is None:
            raw = _get_numeric_param(params_map, name)
            memo[name] = (raw, raw)
            visiting.discard(name)
            return memo[name]
        raw_value = _evaluate_sensor_raw(sensor, params_map, compute, sensor_by_name)
        value = _apply_calibration_value(sensor, raw_value)
        memo[name] = (value if value is not None else raw_value, raw_value)
        visiting.discard(name)
        return memo[name]

    for sensor in config.sensors:
        compute(sensor.name)
    return memo


def _is_simple_param_expression(expr: str) -> bool:
    expr = expr.strip()
    if not expr:
        return False
    if any(ch in expr for ch in "[]{}()+-*/ \t"):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9_:.+-]+", expr))


def _evaluate_sensor_raw(
    sensor: SensorConfig,
    params_map: Mapping[str, Any],
    compute_fn: Callable[[str], Tuple[Optional[float], Optional[float]]],
    sensor_by_name: Mapping[str, SensorConfig],
) -> Optional[float]:
    expr = (sensor.expression or "").strip()
    if expr:
        if _is_simple_param_expression(expr):
            value = _get_numeric_param(params_map, expr)
            if value is not None:
                return value
        else:
            evaluated = _evaluate_sensor_expression(expr, params_map, compute_fn, sensor_by_name)
            if evaluated is not None:
                return evaluated
    param_key = _guess_sensor_source_key(sensor.parameters)
    if param_key:
        return _get_numeric_param(params_map, param_key)
    # fallback: try sensor name
    return _get_numeric_param(params_map, sensor.name)


def _evaluate_sensor_expression(
    expression: str,
    params_map: Mapping[str, Any],
    compute_fn: Callable[[str], Tuple[Optional[float], Optional[float]]],
    sensor_by_name: Mapping[str, SensorConfig],
) -> Optional[float]:
    refs = _extract_sensor_refs(expression)
    value_map: Dict[str, float] = {}
    for ref in refs:
        if ref in sensor_by_name:
            computed = compute_fn(ref)[0]
        else:
            computed = _get_numeric_param(params_map, ref)
        if computed is None:
            return None
        value_map[ref] = computed

    def replace(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        return str(value_map.get(key, 0))

    python_expr = re.sub(r"\[([^\]]+)\]", replace, expression)
    if not re.fullmatch(r"[0-9+\-*/(). \t]+", python_expr):
        return None
    try:
        result = eval(python_expr, {"__builtins__": None}, {})
    except Exception:
        return None
    try:
        return float(result)
    except (TypeError, ValueError):
        return None


def _guess_sensor_source_key(parameters: Mapping[str, Any]) -> Optional[str]:
    if not isinstance(parameters, Mapping):
        return None
    def _is_param_name(value: str) -> bool:
        if not value:
            return False
        if any(ch in value for ch in "[]{}()+-*/ "):
            return False
        return True

    p_value = parameters.get("p")
    if isinstance(p_value, str):
        candidate = p_value.strip()
        if _is_param_name(candidate):
            return candidate

    candidates = ["src", "source", "param", "p1", "user_param", "signal"]
    for key in candidates:
        value = parameters.get(key)
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
    skip_keys = {"m", "units", "unit", "f", "format", "min", "max"}
    for key, value in parameters.items():
        if key in skip_keys:
            continue
        if isinstance(value, str):
            candidate = value.strip()
            if candidate and _is_param_name(candidate):
                return candidate
    return None


def _get_numeric_param(params_map: Mapping[str, Any], key: str) -> Optional[float]:
    if not key:
        return None
    value = params_map.get(key)
    if value is None:
        value = params_map.get(key.lower())
    if value is None:
        value = params_map.get(key.upper())
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _apply_calibration_value(sensor: SensorConfig, raw_value: Optional[float]) -> Optional[float]:
    if raw_value is None:
        return None
    calibration = sensor.calibration
    if not calibration:
        return raw_value
    if calibration.segments:
        segments = sorted(calibration.segments, key=lambda seg: seg.start_x)
        candidate = None
        for seg in segments:
            if raw_value >= seg.start_x:
                candidate = seg
            else:
                break
        if candidate is None:
            candidate = segments[0]
        return candidate.a * raw_value + candidate.b
    if calibration.points:
        points = sorted(calibration.points, key=lambda pt: pt.raw)
        if raw_value <= points[0].raw:
            return points[0].value
        if raw_value >= points[-1].raw:
            return points[-1].value
        for left, right in zip(points, points[1:]):
            if left.raw <= raw_value <= right.raw:
                span = right.raw - left.raw
                if span == 0:
                    return left.value
                ratio = (raw_value - left.raw) / span
                return left.value + ratio * (right.value - left.value)
    return raw_value


def _format_sensor_display(
    value_info: Optional[Tuple[Optional[float], Optional[float]]],
    sensor: SensorConfig,
) -> str:
    if not value_info:
        return "—"
    value, raw = value_info
    sensor_type = (sensor.type or "").strip().lower()
    if sensor_type in {"custom", "произвольный датчик"}:
        raw_check = raw if raw is not None else value
        if raw_check is not None and not (1 <= raw_check <= 4096):
            raw_display = _format_number(raw_check) or str(raw_check)
            return f"не работает ({raw_display})"
    formatted_value = _format_number(value)
    formatted_raw = _format_number(raw)
    if sensor.type and sensor.type.lower() in {"custom", "произвольный датчик"}:
        if formatted_value is not None and formatted_raw is not None and formatted_value != formatted_raw:
            return f"{formatted_value} (raw: {formatted_raw})"
    if formatted_value is not None:
        if formatted_raw is not None and formatted_raw != formatted_value:
            return f"{formatted_value} (raw: {formatted_raw})"
        return formatted_value
    if formatted_raw is not None:
        return formatted_raw
    return "—"


def _format_number(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    try:
        return f"{value:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)


FUEL_SENSOR_TYPE_TOKENS = {
    "fuel level",
    "fuel_level",
    "fuel",
    "fuel level sensor",
    "fuel level impulse",
    "fuel level impulse sensor",
}


def _is_fuel_sensor_type(sensor_type: str) -> bool:
    normalized = sensor_type.strip().lower()
    if not normalized:
        return False
    if normalized in FUEL_SENSOR_TYPE_TOKENS:
        return True
    normalized = normalized.replace("_", " ")
    return "fuel" in normalized and "level" in normalized


def _is_fuel_sensor(sensor: SensorConfig) -> bool:
    name = (sensor.name or "").strip().lower()
    sensor_type = (sensor.type or "").strip().lower()
    if not name and not sensor_type:
        return False
    if _is_fuel_sensor_type(sensor_type) or sensor_type in {"dut", "дут"}:
        return True
    keywords = ("fuel", "дут", "уров", "level")
    return any(keyword in sensor_type for keyword in keywords) or any(keyword in name for keyword in keywords)


def _pick_unit_config_fuel_display(
    unit_config: Optional[UnitConfig],
    sensor_values: Mapping[str, Tuple[Optional[float], Optional[float]]],
) -> Optional[str]:
    if unit_config is None or not unit_config.sensors:
        return None
    primary: List[SensorConfig] = []
    secondary: List[SensorConfig] = []
    for sensor in unit_config.sensors:
        if _is_fuel_sensor_type(sensor.type or ""):
            primary.append(sensor)
        elif _is_fuel_sensor(sensor):
            secondary.append(sensor)

    def _pick_from(group: List[SensorConfig]) -> Optional[str]:
        for sensor in group:
            info = sensor_values.get(sensor.name)
            if not info:
                continue
            display = _format_sensor_display(info, sensor)
            if not display or display == "—":
                continue
            units = (sensor.units or "").strip()
            if units and not display.endswith(units):
                display = f"{display} {units}"
            return display
        return None

    result = _pick_from(primary)
    if result is not None:
        return result
    return _pick_from(secondary)
    return None


def _format_sensor_settings_blocks(
    config: UnitConfig,
    sensor_values: Mapping[str, Tuple[Optional[float], Optional[float]]],
) -> List[str]:
    blocks: List[str] = []
    for sensor in config.sensors:
        lines: List[str] = [f"<b>{html.escape(sensor.name)}</b>"]
        display = _format_sensor_display(sensor_values.get(sensor.name), sensor)
        units_text = _extract_sensor_units(sensor)
        if display and display != "—" and units_text and not display.endswith(units_text):
            display = f"{display} {units_text}"
        lines.append(f"Значение: {html.escape(display or '—')}")
        if sensor.type:
            lines.append(f"Тип: {html.escape(sensor.type)}")
        if units_text:
            lines.append(f"Единицы: {html.escape(units_text)}")
        source_key = _guess_sensor_source_key(sensor.parameters)
        if source_key:
            lines.append(f"Источник: <code>{html.escape(source_key)}</code>")
        if sensor.expression:
            lines.append(f"Формула: <code>{html.escape(sensor.expression)}</code>")
        if sensor.parameters:
            param_lines: List[str] = []
            for key, value in sensor.parameters.items():
                value_repr: Any = value
                if isinstance(value, (dict, list, tuple)):
                    try:
                        value_repr = json.dumps(value, ensure_ascii=False)
                    except Exception:
                        value_repr = str(value)
                param_lines.append(
                    f"• {html.escape(str(key))}: <code>{html.escape(str(value_repr))}</code>"
                )
            if param_lines:
                lines.append("Параметры:\n" + "\n".join(param_lines))
        if sensor.validation:
            validation = sensor.validation
            parts: List[str] = []
            if validation.mode:
                parts.append(f"mode={validation.mode}")
            if validation.min_value is not None:
                parts.append(f"min={validation.min_value}")
            if validation.max_value is not None:
                parts.append(f"max={validation.max_value}")
            if validation.hysteresis is not None:
                parts.append(f"hyst={validation.hysteresis}")
            if parts:
                lines.append("Валидация: " + ", ".join(html.escape(part) for part in parts))
        if sensor.calibration and sensor.calibration.points:
            lines.append(f"Тарировка: {len(sensor.calibration.points)} точек (CSV во вложении)")
        blocks.append("\n".join(lines))
    return blocks


def _chunk_sensor_settings_messages(header: str, blocks: Sequence[str], max_len: int = 3500) -> List[str]:
    messages: List[str] = []
    current = header
    for block in blocks:
        addition = ("\n\n" if current else "") + block
        if current and len(current) + len(addition) > max_len:
            messages.append(current)
            current = block
        else:
            current = (current + addition) if current else block
    if current:
        messages.append(current)
    return messages


async def _show_sensor_settings(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> int:
    unit = context.user_data.get("chosen_unit")
    if not unit:
        await _safe_answer_callback(q, "Сначала выберите объект", show_alert=True)
        return STATE_WAIT_UNIT
    try:
        unit_id = int(unit.get("id"))
    except (TypeError, ValueError):
        await _safe_answer_callback(q, "Некорректный ID объекта", show_alert=True)
        return STATE_WAIT_UNIT
    unit_name = unit.get("nm") or f"id {unit_id}"
    await _safe_answer_callback(q)
    config = _load_unit_config(unit_id)
    if not config or not config.sensors:
        await q.message.reply_text(
            "Для выбранного объекта нет локальных датчиков. Импортируйте конфигурацию или добавьте ДУТ."
        )
        return STATE_WAIT_UNIT
    params_map = context.user_data.get("params_map")
    if not isinstance(params_map, Mapping):
        params_map = {}
    try:
        sensor_values = _compute_unit_config_sensor_values(config, params_map)
    except Exception as exc:
        log.warning("sensor_settings: compute failed unit=%s err=%s", unit_id, exc)
        sensor_values = {}
    header = f"🔧 Настройки датчиков — <b>{html.escape(unit_name)}</b>\nВсего: {len(config.sensors)}"
    blocks = _format_sensor_settings_blocks(config, sensor_values)
    if not blocks:
        await q.message.reply_text(header + "\nНет датчиков в конфиге.", parse_mode="HTML")
        return STATE_WAIT_UNIT
    messages = _chunk_sensor_settings_messages(header, blocks)
    log.info("sensor_settings: start unit=%s chunks=%s", unit_id, len(messages))
    for text in messages:
        await q.message.reply_text(text, parse_mode="HTML", disable_web_page_preview=True)
    sent_csv = 0
    for sensor in config.sensors:
        path = _export_sensor_calibration_csv(unit_id, sensor)
        if not path:
            continue
        try:
            with path.open("rb") as fh:
                await q.message.reply_document(
                    document=InputFile(fh, filename=path.name),
                    caption=f"Тарировка: {sensor.name}",
                )
            sent_csv += 1
        except Exception as exc:
            log.warning("sensor_settings: failed to send calibration %s: %s", path, exc)
    log.info("sensor_settings: done unit=%s csv=%s", unit_id, sent_csv)
    return STATE_WAIT_UNIT


def _pick_custom_sensor_for_calibration(config: UnitConfig) -> Optional[SensorConfig]:
    customs = [
        sensor
        for sensor in config.sensors
        if (sensor.type or "").strip().lower() in {"custom", "произвольный датчик"}
    ]
    customs.sort(key=lambda s: (s.name or "").casefold())
    return customs[0] if customs else None


def _fmt_cal_number(value: float) -> str:
    try:
        return f"{value:.3f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)


async def _show_dut_calibration(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> int:
    unit = context.user_data.get("chosen_unit")
    if not unit:
        await _safe_answer_callback(q, "Сначала выберите объект", show_alert=True)
        return STATE_WAIT_UNIT
    try:
        unit_id = int(unit.get("id"))
    except (TypeError, ValueError):
        await _safe_answer_callback(q, "Некорректный ID объекта", show_alert=True)
        return STATE_WAIT_UNIT
    unit_name = unit.get("nm") or f"id {unit_id}"
    await _safe_answer_callback(q)
    context.user_data["last_dut_unit"] = unit_id
    context.user_data["last_dut_name"] = unit.get("nm")
    config = _load_unit_config(unit_id)
    sensor = _pick_custom_sensor_for_calibration(config) if config else None
    if not sensor:
        await q.message.reply_text("Тарировка недоступна: произвольные датчики не найдены.")
        return STATE_WAIT_UNIT
    context.user_data["last_dut_sensor"] = sensor.name
    log.info("dut_cal_local: unit=%s sensor=%s", unit_id, sensor.name)
    dut_cal_log.info(
        "local_view: unit=%s sensor=%s type=%s sensor_id=%s points=%s",
        unit_id,
        sensor.name,
        sensor.type,
        getattr(sensor, "sensor_id", None),
        len(sensor.calibration.points) if sensor.calibration and sensor.calibration.points else 0,
    )
    calibration = sensor.calibration
    header = [
        f"📈 Тарировка — <b>{html.escape(sensor.name)}</b>",
        f"Объект: {html.escape(unit_name)}",
    ]
    if sensor.type:
        header.append(f"Тип: {html.escape(sensor.type)}")
    if sensor.units:
        header.append(f"Единицы: {html.escape(sensor.units)}")
    source = None
    if isinstance(sensor.parameters, Mapping):
        source = sensor.parameters.get("p") or sensor.parameters.get("param") or sensor.parameters.get("src")
    if isinstance(source, str) and source.strip():
        header.append(f"Источник: <code>{html.escape(source.strip())}</code>")
    lines = header + [""]
    if calibration and calibration.points:
        lines.append("Точки тарировки:")
        for idx, point in enumerate(calibration.points, start=1):
            lines.append(f"{idx:>3}. {_fmt_cal_number(point.raw)} → {_fmt_cal_number(point.value)}")
        lines.append("")
        lines.append(f"Всего точек: {len(calibration.points)}")
    else:
        lines.append("Нет сохранённой тарировки для этого датчика.")
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔄 Обновить из СМТ", callback_data="stats:dutcal_refresh"),
                InlineKeyboardButton("❌ Закрыть", callback_data="action:cancel"),
            ]
        ]
    )
    await q.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=markup)
    log.info("dut_cal_local: response sent unit=%s points=%s", unit_id, len(calibration.points) if calibration and calibration.points else 0)
    return STATE_WAIT_UNIT


async def _refresh_dut_calibration(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> int:
    unit_id = context.user_data.get("last_dut_unit")
    sensor_name = context.user_data.get("last_dut_sensor")
    if not unit_id or not sensor_name:
        await _safe_answer_callback(q, "Сначала откройте тарировку через кнопку выше.", show_alert=True)
        return STATE_WAIT_UNIT
    unit_name = context.user_data.get("last_dut_name") or f"id {unit_id}"
    config = _ensure_unit_config_object(int(unit_id))
    local_sensor = None
    for sensor in config.sensors:
        if sensor.name == sensor_name:
            local_sensor = sensor
            break
    if local_sensor is None:
        await q.message.reply_text("Локальный конфиг не содержит этот датчик. Добавьте его заново.")
        return STATE_WAIT_UNIT
    await _safe_answer_callback(q)
    log.info("dut_cal_refresh: start unit=%s sensor=%s", unit_id, sensor_name)
    dut_cal_log.info(
        "refresh_start: unit=%s sensor=%s sensor_id=%s params=%s expr=%s",
        unit_id,
        sensor_name,
        getattr(local_sensor, "sensor_id", None),
        dict(local_sensor.parameters) if isinstance(local_sensor.parameters, Mapping) else local_sensor.parameters,
        local_sensor.expression,
    )
    chat = q.message.chat if q.message else None
    chat_id = chat.id if chat else None
    try:
        client = wialon_for_chat(chat_id)
    except NotAuthorized:
        await q.message.reply_text("Для обновления тарировки авторизуйтесь и отправьте токен Wialon.")
        return STATE_WAIT_UNIT
    except UserBlocked:
        await q.message.reply_text("Доступ заблокирован. Обратитесь к администратору.")
        return STATE_WAIT_UNIT

    try:
        sensors_api = await run_blocking(client.get_unit_sensors_detailed, int(unit_id))
        log.info("dut_cal_refresh: api sensors fetched unit=%s count=%s", unit_id, len(sensors_api))
        dut_cal_log.info("api_fetch: unit=%s count=%s", unit_id, len(sensors_api))
    except Exception as exc:
        log.warning("dut_cal_refresh: api fetch failed unit=%s err=%s", unit_id, exc)
        await q.message.reply_text(f"Не удалось запросить датчики: {exc}")
        dut_cal_log.info("api_error: unit=%s err=%s", unit_id, exc)
        return STATE_WAIT_UNIT

    def _normalize_key(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return text.casefold()

    sensors_by_id: Dict[int, Dict[str, Any]] = {}
    sensors_by_name: Dict[str, Dict[str, Any]] = {}
    sensors_by_param: Dict[str, List[Dict[str, Any]]] = {}
    for entry in sensors_api:
        entry_copy = dict(entry)
        try:
            sensor_id_val = int(entry_copy.get("id"))
        except Exception:
            sensor_id_val = None
        if sensor_id_val is not None:
            sensors_by_id[sensor_id_val] = entry_copy
        name_key = _normalize_key(entry_copy.get("n") or entry_copy.get("name"))
        if name_key:
            sensors_by_name[name_key] = entry_copy
        param_key = _normalize_key(entry_copy.get("p") or entry_copy.get("param") or entry_copy.get("src"))
        if param_key:
            sensors_by_param.setdefault(param_key, []).append(entry_copy)

    def _match_sensor_by_id() -> Optional[Dict[str, Any]]:
        try:
            sid = int(getattr(local_sensor, "sensor_id", None) or 0)
        except Exception:
            sid = 0
        if sid and sid in sensors_by_id:
            log.info("dut_cal_refresh: matched by id unit=%s sensor_id=%s", unit_id, sid)
            dut_cal_log.info("match_by_id: unit=%s sensor_id=%s", unit_id, sid)
            return sensors_by_id[sid]
        return None

    def _collect_local_param_keys() -> List[str]:
        keys: List[str] = []
        if isinstance(local_sensor.parameters, Mapping):
            for field in ("p", "param", "src"):
                value = local_sensor.parameters.get(field)
                if isinstance(value, str):
                    cleaned = value.strip()
                    if cleaned:
                        keys.append(cleaned)
        expr = local_sensor.expression
        if isinstance(expr, str) and expr.strip():
            keys.append(expr.strip())
        return keys

    def _match_sensor(target_name: str) -> Optional[Dict[str, Any]]:
        key = _normalize_key(target_name)
        if not key:
            return None
        return sensors_by_name.get(key)

    def _extract_rows(entry: Mapping[str, Any], visited_names: Set[str], visited_params: Set[str]) -> List[Dict[str, Any]]:
        rows = entry.get("tbl") or entry.get("calibration")
        if isinstance(rows, dict):
            rows_iter = rows.values()
        elif isinstance(rows, list):
            rows_iter = rows
        else:
            rows_iter = []
        collected = [dict(row) for row in rows_iter if isinstance(row, Mapping)]
        if collected:
            return collected

        expr = entry.get("p") or entry.get("param") or entry.get("expression")
        expr_str = str(expr or "").strip()
        ref_names = _extract_sensor_refs(expr_str) if expr_str else []
        for ref in ref_names:
            key = _normalize_key(ref)
            if key and key not in visited_names:
                visited_names.add(key)
                linked = sensors_by_name.get(key)
                if linked:
                    candidate = _extract_rows(linked, visited_names, visited_params)
                    if candidate:
                        return candidate

        param_key = _normalize_key(expr_str or entry.get("src"))
        if param_key and param_key not in visited_params:
            visited_params.add(param_key)
            for linked in sensors_by_param.get(param_key, []):
                if linked is entry:
                    continue
                candidate = _extract_rows(linked, visited_names, visited_params)
                if candidate:
                    return candidate
        return []

    api_sensor = _match_sensor_by_id()
    if not api_sensor:
        api_sensor = _match_sensor(sensor_name)
    if not api_sensor:
        for candidate_expr in _collect_local_param_keys():
            key = _normalize_key(candidate_expr)
            if not key:
                continue
            candidates = sensors_by_param.get(key)
            if candidates:
                api_sensor = candidates[0]
                log.info(
                    "dut_cal_refresh: fallback sensor by param unit=%s sensor=%s expr=%s",
                    unit_id,
                    sensor_name,
                    candidate_expr,
                )
                dut_cal_log.info(
                    "match_by_param: unit=%s sensor=%s expr=%s candidate=%s",
                    unit_id,
                    sensor_name,
                    candidate_expr,
                    api_sensor.get("n"),
                )
                break
    if not api_sensor:
        await q.message.reply_text("СМТ не вернула датчик с таким именем. Проверьте настройки.")
        dut_cal_log.info("match_failed: unit=%s sensor=%s reason=no_sensor", unit_id, sensor_name)
        return STATE_WAIT_UNIT
    rows = _extract_rows(
        api_sensor,
        {sensor_name.strip().casefold()},
        set(),
    )
    dut_cal_log.info(
        "rows_extracted: unit=%s sensor=%s rows=%s sample=%s",
        unit_id,
        sensor_name,
        len(rows),
        rows[:3],
    )
    points: List[CalibrationPoint] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        x = row.get("x")
        y = row.get("y")
        if y is None:
            a = row.get("a")
            b = row.get("b")
            try:
                if x is not None and a is not None and b is not None:
                    y = float(a) * float(x) + float(b)
            except (TypeError, ValueError):
                y = None
        if x is None or y is None:
            dut_cal_log.info(
                "row_skipped: unit=%s sensor=%s row=%s x=%s y=%s",
                unit_id,
                sensor_name,
                row,
                x,
                y,
            )
            continue
        try:
            raw_val = float(x)
            out_val = float(y)
            if math.isnan(raw_val) or math.isinf(raw_val) or math.isnan(out_val) or math.isinf(out_val):
                dut_cal_log.info(
                    "row_invalid: unit=%s sensor=%s row=%s raw=%s val=%s",
                    unit_id,
                    sensor_name,
                    row,
                    raw_val,
                    out_val,
                )
                continue
            points.append(CalibrationPoint(raw=raw_val, value=out_val))
        except Exception as exc:
            dut_cal_log.info(
                "row_error: unit=%s sensor=%s row=%s err=%s",
                unit_id,
                sensor_name,
                row,
                exc,
            )
            continue
    dut_cal_log.info("points_parsed: unit=%s sensor=%s count=%s", unit_id, sensor_name, len(points))
    if not points:
        await q.message.reply_text("СМТ вернула датчик без таблицы тарировки.")
        log.info("dut_cal_refresh: no calibration rows unit=%s sensor=%s", unit_id, sensor_name)
        dut_cal_log.info("no_points: unit=%s sensor=%s", unit_id, sensor_name)
        return STATE_WAIT_UNIT

    local_sensor.calibration = local_sensor.calibration or SensorCalibration(sensor_id=local_sensor.sensor_id or 0)
    local_sensor.calibration.points = points
    get_unit_config_service().save(config)
    log.info("dut_cal_refresh: saved unit=%s sensor=%s points=%s", unit_id, sensor_name, len(points))
    dut_cal_log.info("saved: unit=%s sensor=%s points=%s", unit_id, sensor_name, len(points))
    await q.message.reply_text(f"Тарировка обновлена. Точек: {len(points)}")
    return await _show_dut_calibration(q, context)


# ----- Добавление ДУТ в UnitConfig -----


async def _start_add_dut_flow(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> int:
    unit = context.user_data.get("chosen_unit")
    if not unit:
        await _safe_answer_callback(q, "Сначала выберите объект", show_alert=True)
        return STATE_WAIT_UNIT
    try:
        unit_id = int(unit["id"])
    except (TypeError, ValueError):
        await _safe_answer_callback(q, "Некорректный ID объекта", show_alert=True)
        return STATE_WAIT_UNIT
    unit_name = unit.get("nm") or f"id {unit_id}"
    context.user_data["add_dut_ctx"] = {"unit_id": unit_id, "unit_name": unit_name}
    log.info("add_dut: start unit=%s name=%s", unit_id, unit_name)
    await _safe_answer_callback(q)
    context.user_data["last_card_callback_chat_id"] = q.message.chat_id if q.message else None
    context.user_data["last_card_message_id"] = q.message.message_id if q.message else None
    await q.message.reply_text(
        "Введите имя датчика уровня топлива (как в системе Wialon).\n"
        "Отправьте 'Отмена', чтобы прервать добавление.",
        reply_markup=kb_cancel(),
    )
    return STATE_ADD_DUT_WAIT_NAME


async def add_dut_name_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    ctx = context.user_data.get("add_dut_ctx") or {}
    unit_id = ctx.get("unit_id")
    unit_name = ctx.get("unit_name") or (context.user_data.get("chosen_unit") or {}).get("nm")
    if not unit_id:
        await update.message.reply_text("Нет активной операции добавления ДУТ.")
        return STATE_WAIT_UNIT
    dut_name = (update.message.text or "").strip()
    if not dut_name:
        await update.message.reply_text("Имя не может быть пустым. Попробуйте ещё раз или отправьте 'Отмена'.")
        return STATE_ADD_DUT_WAIT_NAME
    if _text_is_cancel(dut_name):
        await update.message.reply_text("Добавление ДУТ отменено.")
        context.user_data.pop("add_dut_ctx", None)
        return STATE_WAIT_UNIT

    client = await require_wialon_client(update, context, "Чтобы получить параметры датчика, авторизуйтесь.")
    if not client:
        return STATE_AUTH_WAIT_TOKEN
    try:
        unit_item = await run_blocking(client.get_unit_full_for_stats, unit_id)
    except Exception as exc:
        await update.message.reply_text(f"❌ Не удалось загрузить объект: {exc}")
        return STATE_WAIT_UNIT

    try:
        detailed_sensors = await run_blocking(client.get_unit_sensors_detailed, unit_id)
    except Exception as exc:
        detailed_sensors = []
        log.debug("add_dut: extra sensors fetch failed unit=%s err=%s", unit_id, exc)
    else:
        _merge_unit_item_sensors(unit_item, detailed_sensors)

    try:
        result = await run_blocking(_persist_dut_from_unit_item, unit_id, dut_name, unit_item)
    except ValueError as exc:
        await update.message.reply_text(f"❌ {exc}\nПопробуйте другое имя или отправьте 'Отмена'.")
        return STATE_ADD_DUT_WAIT_NAME
    except Exception as exc:
        log.exception("add_dut failed: %s", exc)
        await update.message.reply_text(f"❌ Не удалось сохранить датчик: {exc}")
        return STATE_WAIT_UNIT
    finally:
        try:
            _load_unit_config.cache_clear()  # type: ignore[attr-defined]
        except AttributeError:
            pass

    created_names, csv_paths = result
    log.info("add_dut: saved unit=%s sensors=%s", unit_id, created_names)
    lines = ["✅ ДУТ добавлен в локальный конфиг."]
    if created_names:
        lines.append("Созданы/обновлены датчики: " + ", ".join(created_names))
    if csv_paths:
        lines.append("Тарировки сохранены в файлах:")
        lines.extend(f"• {path}" for path in csv_paths)
    await update.message.reply_text("\n".join(lines))

    context.user_data.pop("add_dut_ctx", None)
    unit = context.user_data.get("chosen_unit") or {"id": unit_id, "nm": unit_name or f"id {unit_id}"}
    client_for_refresh = None if _pipeline_card_enabled() else client
    await show_stats_then_actions(
        update,
        context,
        int(unit["id"]),
        unit.get("nm") or f"id {unit_id}",
        client_for_refresh,
        preserve_previous=True,
    )
    return STATE_WAIT_UNIT


def _text_is_cancel(value: str) -> bool:
    return value.strip().lower() in {"отмена", "cancel", "/cancel"}


def _persist_dut_from_unit_item(unit_id: int, dut_name: str, unit_item: Mapping[str, Any]) -> Tuple[List[str], List[Path]]:
    log.info("add_dut: persist unit=%s dut=%s", unit_id, dut_name)
    sensors_map = _collect_wialon_sensor_map(unit_item)
    if not sensors_map:
        raise ValueError("В объекте нет датчиков")
    target_name, target_entry = _resolve_sensor_by_name(sensors_map, dut_name)
    if target_entry is None:
        raise ValueError(f"Датчик '{dut_name}' не найден")
    sensor_type = str(target_entry.get("t") or "").strip().lower()
    if sensor_type not in {"fuel level", "fuel_level", "fuel"}:
        raise ValueError(f"Датчик '{target_name}' не является датчиком уровня топлива")

    working_map = {name: dict(entry) for name, entry in sensors_map.items()}
    sensor_copy = dict(target_entry)
    linked_entries, main_entry = _prepare_dut_sensor_entries(sensor_copy, working_map)
    all_entries: List[Mapping[str, Any]] = linked_entries + [main_entry]
    configs: List[SensorConfig] = []
    for entry in all_entries:
        config = sensor_config_from_entry(entry)
        if config:
            configs.append(config)
    if not configs:
        raise ValueError("Не удалось преобразовать датчики")

    config_obj = _ensure_unit_config_object(unit_id)
    existing_by_name = {sensor.name: sensor for sensor in config_obj.sensors}
    new_names: List[str] = []
    for cfg in configs:
        if cfg.name in existing_by_name:
            config_obj.sensors = [s for s in config_obj.sensors if s.name != cfg.name]
        config_obj.sensors.append(cfg)
        new_names.append(cfg.name)
    service = get_unit_config_service()
    service.save(config_obj)
    csv_paths = []
    for cfg in configs:
        path = _export_sensor_calibration_csv(unit_id, cfg)
        if path:
            csv_paths.append(path)
    return new_names, csv_paths


async def _handle_clear_unit_sensors(q: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not _pipeline_card_enabled():
        await _safe_answer_callback(q, "Функция доступна только при включённом локальном pipeline.", show_alert=True)
        return STATE_WAIT_UNIT
    unit = context.user_data.get("chosen_unit")
    if not unit:
        await _safe_answer_callback(q, "Сначала выберите объект", show_alert=True)
        return STATE_WAIT_UNIT
    try:
        unit_id = int(unit.get("id"))
    except (TypeError, ValueError):
        await _safe_answer_callback(q, "Некорректный ID объекта", show_alert=True)
        return STATE_WAIT_UNIT
    config = _ensure_unit_config_object(unit_id)
    sensors = list(config.sensors or [])
    if not sensors:
        await _safe_answer_callback(q)
        if q.message:
            await q.message.reply_text("Локальный конфиг уже пуст.")
        return STATE_WAIT_UNIT
    removed = len(sensors)
    config.sensors = []
    get_unit_config_service().save(config)
    try:
        _load_unit_config.cache_clear()  # type: ignore[attr-defined]
    except AttributeError:
        pass
    exports_dir = UNIT_CONFIG_DIR / "exports"
    removed_exports = 0
    if exports_dir.exists():
        for path in exports_dir.glob(f"{unit_id}_*.csv"):
            try:
                path.unlink()
                removed_exports += 1
            except Exception as exc:
                log.debug("clear_sensors: failed to delete %s err=%s", path, exc)
    log.info(
        "clear_sensors: unit=%s sensors_removed=%s exports_removed=%s",
        unit_id,
        removed,
        removed_exports,
    )
    await _safe_answer_callback(q)
    if q.message:
        await q.message.reply_text(f"🗑 Удалены все датчики ({removed}) из локального конфига.")
    await show_stats_then_actions(
        q,
        context,
        unit_id,
        unit.get("nm") or f"id {unit_id}",
        None,
        via_callback=True,
        preserve_previous=True,
    )
    return STATE_WAIT_UNIT


def _sensor_entry_score(entry: Mapping[str, Any]) -> Tuple[int, int]:
    sensor_type = str(entry.get("t") or "").strip().lower()
    is_fuel = _is_fuel_sensor_type(sensor_type)
    has_table = bool(entry.get("tbl"))
    return (1 if is_fuel else 0, 1 if has_table else 0)


def _collect_wialon_sensor_map(unit_item: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    sensors = _normalize_unit_sensors(unit_item.get("sens"))
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for entry in sensors.values():
        name = str(entry.get("n") or entry.get("name") or "").strip()
        if not name:
            continue
        key = name.casefold()
        grouped.setdefault(key, []).append(dict(entry))

    result: Dict[str, Dict[str, Any]] = {}
    for key, entries in grouped.items():
        best_entry = entries[0]
        best_score = _sensor_entry_score(best_entry)
        for candidate in entries[1:]:
            score = _sensor_entry_score(candidate)
            if score > best_score:
                best_entry = candidate
                best_score = score
        result[key] = best_entry
    return result


def _resolve_sensor_by_name(sensors_map: Dict[str, Dict[str, Any]], query: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    key = query.strip().casefold()
    entry = sensors_map.get(key)
    if entry is None:
        return query, None
    display_name = str(entry.get("n") or entry.get("name") or query).strip() or query
    return display_name, entry


def _prepare_dut_sensor_entries(sensor_entry: Dict[str, Any], sensors_map: Dict[str, Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    expression = str(sensor_entry.get("p") or sensor_entry.get("m") or "").strip()
    refs = _extract_sensor_refs(expression)
    created_entries: List[Dict[str, Any]] = []
    if not refs and expression:
        raw_name = _generate_custom_dut_ref_name(sensor_entry, sensors_map)
        raw_entry = dict(sensor_entry)
        raw_entry["n"] = raw_name
        raw_entry["name"] = raw_name
        raw_entry["t"] = "custom"
        raw_entry["type"] = "custom"
        raw_entry["kind"] = raw_entry.get("kind") or "custom"
        raw_entry["p"] = expression
        sensors_map[raw_name.casefold()] = raw_entry
        created_entries.append(raw_entry)
        sensor_entry = dict(sensor_entry)
        sensor_entry["tbl"] = []
        sensor_entry["p"] = f"[{raw_name}]"
        refs = [raw_name]
    referenced: List[Dict[str, Any]] = []
    for ref in refs:
        key, target = _resolve_sensor_by_name(sensors_map, ref)
        if target is None:
            raise ValueError(f"Ссылка на датчик '{ref}' не найдена")
        referenced.append(dict(target))
    payload: List[Dict[str, Any]] = []
    seen_names: Set[str] = set()
    for entry in created_entries + referenced:
        name = str(entry.get("n") or entry.get("name") or "").strip()
        key = name.casefold() if name else ""
        if key and key in seen_names:
            continue
        if key:
            seen_names.add(key)
        payload.append(entry)
    return payload, dict(sensor_entry)


def _extract_sensor_refs(expression: str) -> List[str]:
    pattern = re.compile(r"\[([^\]]+)\]")
    return [match.strip() for match in pattern.findall(expression)]


def _generate_unique_sensor_name(base_name: str, sensors_map: Dict[str, Dict[str, Any]]) -> str:
    slug = base_name.strip() or "sensor"
    candidate = slug
    index = 1
    names_cf = {name.casefold() for name in sensors_map.keys()}
    while candidate.casefold() in names_cf:
        index += 1
        candidate = f"{slug}_{index}"
    return candidate


def _generate_custom_dut_ref_name(sensor_entry: Mapping[str, Any], sensors_map: Dict[str, Dict[str, Any]]) -> str:
    base_name = str(sensor_entry.get("n") or sensor_entry.get("name") or "ДУТ").strip() or "ДУТ"
    prefix = re.sub(r"\d+$", "", base_name).strip()
    if not prefix:
        prefix = "ДУТ"
    names_cf = {name.casefold() for name in sensors_map.keys()}
    index = 1
    while True:
        candidate = f"{prefix}{index}"
        if candidate.casefold() not in names_cf:
            return candidate
        index += 1


def _merge_unit_item_sensors(unit_item: Dict[str, Any], extra_sensors: Sequence[Mapping[str, Any]]) -> None:
    if not extra_sensors:
        return
    base_map = _normalize_unit_sensors(unit_item.get("sens"))
    extra_map = _normalize_unit_sensors(extra_sensors)
    if not extra_map:
        return
    for key, extra_entry in extra_map.items():
        target = base_map.get(key)
        if target is None:
            base_map[key] = dict(extra_entry)
            continue
        for field, value in extra_entry.items():
            if value in (None, "", [], {}, ()):
                continue
            if not target.get(field):
                target[field] = value
            elif isinstance(value, dict):
                base_field = target.setdefault(field, {})
                if isinstance(base_field, dict):
                    for sub_key, sub_value in value.items():
                        if sub_value not in (None, "", [], {}, ()):
                            base_field.setdefault(sub_key, sub_value)
        base_map[key] = target
    unit_item["sens"] = list(base_map.values())


def _ensure_unit_config_object(unit_id: int) -> UnitConfig:
    config = _load_unit_config(unit_id)
    if config:
        return config
    snapshot_name = None
    try:
        record = get_unit_snapshot_service().get_unit(unit_id)
        if record and record.name:
            snapshot_name = record.name
    except Exception:
        snapshot_name = None
    general = GeneralConfig(name=snapshot_name or f"id {unit_id}")
    return UnitConfig(unit_id=unit_id, source_kind="local", general=general)


def _export_sensor_calibration_csv(unit_id: int, sensor: SensorConfig) -> Optional[Path]:
    calibration = sensor.calibration
    if not calibration or not calibration.points:
        return None
    exports_dir = UNIT_CONFIG_DIR / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    slug = re.sub(r"[^0-9A-Za-z_-]+", "_", sensor.name).strip("_") or "sensor"
    path = exports_dir / f"{unit_id}_{slug}.csv"
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["raw", "value"])
        for point in calibration.points:
            writer.writerow([point.raw, point.value])
    return path


def _merge_event_params(
    event_params: Optional[Dict[str, Any]],
    latest_metrics: Optional[Dict[str, Any]],
) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    if isinstance(event_params, dict):
        merged.update({str(k): str(v) for k, v in event_params.items()})
    if isinstance(latest_metrics, dict):
        params = latest_metrics.get("params")
        if isinstance(params, dict):
            for key, value in params.items():
                key_str = str(key)
                if key_str not in merged:
                    merged[key_str] = str(value)
    return merged

def _cf_current_state(context: ContextTypes.DEFAULT_TYPE) -> int:
    stage = context.user_data.get("cf_stage")
    if stage == CF_STAGE_VALUE:
        return STATE_CF_VALUE
    if stage == CF_STAGE_MORE:
        return STATE_CF_MORE
    return STATE_CF_NAME


def _format_custom_fields_message(
    unit_name: Optional[str],
    custom_fields: List[Dict[str, Any]],
    admin_fields: List[Dict[str, Any]],
) -> Tuple[str, str]:
    display_name = (unit_name or "").strip() or "Без имени"
    header = f"📄 Произвольные поля — <b>{html.escape(display_name)}</b>"
    lines: List[str] = [header]

    def _render_entries(entries: List[Dict[str, Any]]) -> List[str]:
        rendered: List[str] = []
        for entry in entries:
            raw_label = entry.get("name")
            label = str(raw_label).strip() if raw_label is not None else ""
            if not label:
                seq = entry.get("seq")
                label = f"Поле {seq}" if seq not in (None, "") else "Поле"
            raw_value = entry.get("value")
            if isinstance(raw_value, str):
                value = raw_value.strip()
            elif raw_value is None:
                value = ""
            else:
                value = str(raw_value)
            value_display = value if value else "—"
            label_html = html.escape(label)
            value_html = html.escape(value_display)
            rendered.append(f"• <b>{label_html}:</b> <i>{value_html}</i>")
        return rendered

    cf_lines = _render_entries(custom_fields)
    if cf_lines:
        lines.extend(cf_lines)
    else:
        lines.append("• Нет пользовательских полей.")

    admin_lines = _render_entries(admin_fields)
    if admin_lines:
        lines.append("")
        lines.append("🛡 Админ. поля")
        lines.extend(admin_lines)

    return "\n".join(lines), "HTML"


async def cf_name_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_CF:
        return STATE_MENU
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    text = (update.message.text or "").strip()
    if not text:
        await send_new_anchor_below(
            update,
            "Фамилия не может быть пустой",
            kb_cf_controls("back:unit"),
            context,
        )
        context.user_data["cf_stage"] = CF_STAGE_NAME
        return STATE_CF_NAME

    current_date = datetime.now().strftime("%y.%m.%d")
    formatted_name = f"{current_date} {text}"
    context.user_data["field_name"] = formatted_name
    context.user_data["cf_stage"] = CF_STAGE_VALUE

    await send_new_anchor_below(
        update,
        f"Поле: *{formatted_name}*\n\nОпишите выполненные работы.",
        kb_cf_controls("back:name"),
        context,
        parse_mode="Markdown",
    )
    return STATE_CF_VALUE

async def cf_value_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_CF:
        return STATE_MENU
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    client = await require_wialon_client(update, context, "Чтобы менять поля, авторизуйтесь.")
    if not client:
        return STATE_AUTH_WAIT_TOKEN
    value = (update.message.text or "").strip()
    if not value:
        await send_new_anchor_below(
            update,
            "Описание работ не может быть пустым",
            kb_cf_controls("back:name"),
            context,
        )
        context.user_data["cf_stage"] = CF_STAGE_VALUE
        return STATE_CF_VALUE

    unit = context.user_data.get("chosen_unit")
    name = context.user_data.get("field_name")
    if not unit or not name:
        await delete_anchor(context)
        _clear_export_job(context, cancel=True)
        context.user_data.clear()
        return ConversationHandler.END

    try:
        mode, field_id = client.update_custom_field(int(unit["id"]), name, value)

        unit_refreshed = client.get_unit_with_fields(int(unit["id"]))
        context.user_data["chosen_unit"] = unit_refreshed

        context.user_data["last_field"] = {
            "unit_id": int(unit["id"]),
            "field_id": int(field_id),
            "name": name,
            "value": value,
            "mode": mode,
        }
        context.user_data["allow_cancel_undo"] = True

        success_text = f"Поле *{name}* успешно создано.\n\nВнести ещё одно поле?"
        context.user_data["last_success_message"] = success_text

        await send_new_anchor_below(
            update,
            success_text,
            kb_more(),
            context,
            parse_mode="Markdown",
        )
        context.user_data["cf_stage"] = CF_STAGE_MORE
        return STATE_CF_MORE
    except PermissionError as pe:
        await send_new_anchor_below(
            update,
            f"Ошибка прав доступа: {pe}",
            kb_cf_controls("back:name"),
            context,
        )
        context.user_data["cf_stage"] = CF_STAGE_VALUE
        return STATE_CF_VALUE
    except Exception as e:
        log.exception("Ошибка update_custom_field")
        await send_new_anchor_below(
            update,
            f"Ошибка обновления поля: `{e}`",
            kb_cf_controls("back:name"),
            context,
        )
        context.user_data["cf_stage"] = CF_STAGE_VALUE
        return STATE_CF_VALUE


async def cf_dump_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_CF:
        return STATE_MENU
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)

    unit = context.user_data.get("chosen_unit")
    try:
        unit_id = int(unit.get("id")) if isinstance(unit, dict) else None
    except (TypeError, ValueError, AttributeError):
        unit_id = None
    if unit_id is None:
        return _cf_current_state(context)

    client = await require_wialon_client(
        update,
        context,
        "Чтобы просматривать объект, авторизуйтесь.",
    )
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    try:
        snapshot = await run_blocking(client.get_unit_custom_fields_snapshot, unit_id)
    except Exception as exc:
        log.debug("get_unit_custom_fields_snapshot failed: %s", exc)
        if q.message:
            await q.message.reply_text(
                f"Не удалось получить произвольные поля: `{exc}`",
                parse_mode="Markdown",
            )
        return _cf_current_state(context)

    unit_name = None
    custom_fields: List[Dict[str, Any]] = []
    admin_fields: List[Dict[str, Any]] = []
    if isinstance(snapshot, dict):
        unit_name = snapshot.get("unit_name") or (unit.get("nm") if isinstance(unit, dict) else None)
        raw_custom = snapshot.get("custom_fields")
        if isinstance(raw_custom, list):
            custom_fields = [dict(item) for item in raw_custom if isinstance(item, dict)]
        raw_admin = snapshot.get("admin_fields")
        if isinstance(raw_admin, list):
            admin_fields = [dict(item) for item in raw_admin if isinstance(item, dict)]

    text, parse_mode = _format_custom_fields_message(unit_name, custom_fields, admin_fields)

    chat_id = q.message.chat_id if q.message else (update.effective_chat.id if update.effective_chat else None)
    reply_to: Optional[int] = None
    stats_meta = context.user_data.get("stats_msg")
    if isinstance(stats_meta, dict):
        stats_chat_id = stats_meta.get("chat_id")
        stats_message_id = stats_meta.get("message_id")
        if stats_chat_id is not None:
            if chat_id is None:
                chat_id = stats_chat_id
            if chat_id == stats_chat_id and isinstance(stats_message_id, int):
                reply_to = stats_message_id

    if chat_id is None:
        if q.message:
            await q.message.reply_text(text, parse_mode=parse_mode, disable_web_page_preview=True)
        return _cf_current_state(context)

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
            reply_to_message_id=reply_to,
        )
    except Exception as exc:
        log.debug("send custom fields snapshot failed: %s", exc)
        if q.message:
            await q.message.reply_text(text, parse_mode=parse_mode, disable_web_page_preview=True)

    return _cf_current_state(context)


async def cf_more_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_CF:
        return STATE_MENU
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)

    if q.data == "more:yes":
        context.user_data["allow_cancel_undo"] = False
        unit = context.user_data.get("chosen_unit")
        if unit:
            context.user_data["cf_stage"] = CF_STAGE_NAME
            await edit_anchor(
                context,
                f"Объект: {unit['nm']}\n\nУкажите фамилию.",
                kb_cf_controls("back:unit"),
                parse_mode="Markdown",
            )
            return STATE_CF_NAME
        else:
            await delete_anchor(context)
            _clear_export_job(context, cancel=True)
            context.user_data.clear()
            return ConversationHandler.END
    else:
        context.user_data["allow_cancel_undo"] = False
        unit = context.user_data.get("chosen_unit")
        done_text = f"Готово. Поля внесены для объекта {unit['nm']}." if unit else "Готово. Диалог завершён."

        success_text = context.user_data.get("last_success_message")
        base_text = (success_text or q.message.text or "").replace("\n\nВнести ещё одно поле?", "").strip()
        final_text = f"{base_text}\n\n✅ Завершено.".strip()
        await edit_anchor(context, final_text, markup=None, parse_mode="Markdown")
        context.user_data.pop("anchor", None)
        context.user_data.pop("last_success_message", None)
        if q.message:
            await q.message.reply_text(
                done_text,
                reply_markup=reply_menu(chat_id=q.message.chat_id),
            )

        context.user_data["mode"] = MODE_NONE
        context.user_data.pop("cf_stage", None)
        return ConversationHandler.END

# -------- Отправка команды (CMD) ----------


def _normalize_unit_stub(unit: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(unit, dict):
        return None
    raw_id = unit.get("id")
    try:
        unit_id = int(raw_id)
    except (TypeError, ValueError):
        return None
    name = str(unit.get("nm") or "").strip() or f"id {unit_id}"
    return {"id": unit_id, "nm": name}


def _resolve_unit_for_cmd(
    context: ContextTypes.DEFAULT_TYPE,
    unit_id: Optional[int],
) -> Optional[Dict[str, Any]]:
    preferred_sources: List[Any] = [
        context.user_data.get("last_cmd_unit"),
        context.user_data.get("chosen_unit"),
    ]
    if unit_id is None:
        for candidate in preferred_sources:
            normalized = _normalize_unit_stub(candidate)
            if normalized:
                return normalized
        return None

    def _matches(candidate: Any) -> Optional[Dict[str, Any]]:
        normalized = _normalize_unit_stub(candidate)
        if normalized and normalized["id"] == unit_id:
            return normalized
        return None

    for candidate in preferred_sources:
        matched = _matches(candidate)
        if matched:
            return matched

    results_map = context.user_data.get("search_results") or {}
    matched = _matches(results_map.get(str(unit_id)))
    if matched:
        return matched

    for key in ("search_units", "search_units_all"):
        items = context.user_data.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            matched = _matches(item)
            if matched:
                return matched

    return {"id": unit_id, "nm": f"id {unit_id}"}


def _cmd_success_keyboard(unit_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "↩️ К карточке",
                    callback_data=f"cmdsuccess:return:{unit_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    "✉️ Ещё команда",
                    callback_data=f"cmdsuccess:again:{unit_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    "🔍 Новый поиск",
                    callback_data="cmdsuccess:search",
                )
            ],
        ]
    )


async def _start_stats_cmd_flow(
    update_or_q,
    context: ContextTypes.DEFAULT_TYPE,
    unit: Optional[Dict[str, Any]],
) -> int:
    normalized = _normalize_unit_stub(unit)
    if not normalized:
        _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
        return STATE_WAIT_UNIT

    unit_id = normalized["id"]
    unit_name = normalized["nm"]
    context.user_data["chosen_unit"] = dict(normalized)
    context.user_data["last_cmd_unit"] = dict(normalized)
    context.user_data["mode"] = MODE_CMD
    context.user_data["cmd_return"] = "stats"
    context.user_data.pop("cmd_return_payload", None)
    context.user_data.pop("cmd_return_runtime", None)
    _set_objects_mode(context, OBJECTS_MODE_CARD_FLOW_COMMAND)
    await clear_stats_buttons(context)
    await delete_anchor(context)
    target = getattr(update_or_q, "message", None)
    if not target:
        _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
        return STATE_WAIT_UNIT
    msg = await target.reply_text(
        f"Выбран объект: {unit_name} — id {unit_id}\n\n"
        f"Напишите TCP-команду для устройства.\n"
        f"_Команда отправляется только при активном TCP-соединении._",
        reply_markup=kb_cmd_entry_controls(),
    )
    await set_anchor_on(msg, context)
    return STATE_CMD_VALUE

async def cmd_value_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_CMD:
        return STATE_MENU
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    client = await require_wialon_client(update, context, "Чтобы отправлять команды, авторизуйтесь.")
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    text = (update.message.text or "")
    unit = context.user_data.get("chosen_unit")
    if not unit:
        await delete_anchor(context)
        _clear_export_job(context, cancel=True)
        context.user_data.clear()
        return ConversationHandler.END

    unit_id = int(unit["id"])
    unit_name = unit.get("nm") or f"id {unit_id}"

    try:
        _ = client.send_custom_tcp(unit_id, text, timeout=10)
        await delete_anchor(context)
        await update.message.reply_text(
            f"✅ Команда отправлена (TCP) на {unit_name}.\nТекст: `{text}`",
            parse_mode="Markdown",
            reply_markup=_cmd_success_keyboard(unit_id),
        )
        normalized_unit = _normalize_unit_stub({"id": unit_id, "nm": unit_name})
        if normalized_unit:
            context.user_data["last_cmd_unit"] = dict(normalized_unit)
        context.user_data.pop("cmd_return", None)
        context.user_data.pop("cmd_return_payload", None)
        context.user_data.pop("cmd_return_runtime", None)
        context.user_data["mode"] = MODE_NONE
        _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
        return STATE_WAIT_UNIT
    except Exception:
        await send_new_anchor_below(
            update,
            "❌ Не удалось отправить команду. Попробуйте ещё раз или нажмите «Новый поиск».",
            kb_cmd_entry_controls(),
            context,
            parse_mode=None,
        )
        return STATE_CMD_VALUE


async def cmd_success_action_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_WAIT_UNIT

    await _safe_answer_callback(query)
    data = (query.data or "").split(":")
    if len(data) < 2:
        return STATE_WAIT_UNIT

    action = data[1]
    unit_id: Optional[int] = None
    if len(data) > 2:
        try:
            unit_id = int(data[2])
        except (TypeError, ValueError):
            unit_id = None

    if action == "search":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return await _start_search_prompt_from_trigger(query, context)

    unit_info = _resolve_unit_for_cmd(context, unit_id)
    if not unit_info:
        await _safe_answer_callback(query, "Объект не найден.", show_alert=True)
        return STATE_WAIT_UNIT

    unit_id = unit_info["id"]
    unit_name = unit_info["nm"]

    if action == "return":
        client = None
        if not _pipeline_card_enabled():
            client = await require_wialon_client(
                update,
                context,
                "Чтобы просматривать объект, авторизуйтесь.",
            )
            if not client:
                return STATE_AUTH_WAIT_TOKEN
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        context.user_data["chosen_unit"] = dict(unit_info)
        context.user_data["last_cmd_unit"] = dict(unit_info)
        context.user_data["mode"] = MODE_NONE
        _set_objects_mode(context, OBJECTS_MODE_CARD_IDLE)
        return await show_stats_then_actions(
            query,
            context,
            unit_id,
            unit_name,
            client,
            via_callback=True,
        )

    if action == "again":
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        context.user_data["chosen_unit"] = dict(unit_info)
        context.user_data["last_cmd_unit"] = dict(unit_info)
        return await _start_stats_cmd_flow(query, context, unit_info)

    _pipeline_stage("done", message_id=msg.message_id)
    return STATE_WAIT_UNIT

# -------- Формирование отчётов ----------
def _match_saved_report_template(
    saved_template: Any, templates: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if not isinstance(saved_template, dict):
        return None
    try:
        saved_res = int(saved_template.get("resource_id"))
        saved_tpl = int(saved_template.get("template_id"))
    except (TypeError, ValueError):
        return None

    for tpl in templates:
        try:
            tpl_res = int(tpl.get("resource_id"))
            tpl_id = int(tpl.get("template_id"))
        except (TypeError, ValueError):
            continue
        if tpl_res == saved_res and tpl_id == saved_tpl:
            merged = dict(tpl)
            if "name" not in merged or not merged.get("name"):
                merged["name"] = saved_template.get("name")
            if "resource_name" not in merged or not merged.get("resource_name"):
                merged["resource_name"] = saved_template.get("resource_name")
            return merged
    return None


async def _begin_report_flow(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    client: "WialonClient",
    *,
    keep_stats_message: bool,
) -> int:
    if not message:
        return ConversationHandler.END

    try:
        await clear_stats_buttons(context)
        await delete_anchor(context)
        if not keep_stats_message:
            await delete_stats_message(context)
        await delete_param_result_message(context)
        clear_param_runtime(context)
        clear_report_context(context, cancel_job=True)
        context.user_data.pop("cmd_return", None)
        context.user_data.pop("cmd_return_payload", None)
        context.user_data.pop("cmd_return_runtime", None)

        unit = context.user_data.get("chosen_unit") or {}
        if unit.get("id") is None:
            await message.reply_text(
                "Сначала выберите объект через «🔍 Найти объект», затем повторите запрос отчёта.",
                reply_markup=reply_menu(chat_id=message.chat_id),
            )
            context.user_data["mode"] = MODE_NONE
            return ConversationHandler.END

        try:
            unit_id = int(unit.get("id"))
        except (TypeError, ValueError):
            await message.reply_text(
                "Не удалось определить выбранный объект. Повторите поиск и попробуйте снова.",
                reply_markup=reply_menu(chat_id=message.chat_id),
            )
            context.user_data["mode"] = MODE_NONE
            return ConversationHandler.END

        context.user_data["mode"] = MODE_REPORT
        context.user_data["report_unit_id"] = unit_id
        context.user_data["report_unit_meta"] = {"id": unit_id, "nm": unit.get("nm")}

        try:
            templates = client.list_report_templates()
        except Exception as e:
            log.exception("Ошибка получения списка шаблонов отчётов")
            await message.reply_text(
                f"Не удалось получить список отчётов: `{e}`",
                parse_mode="Markdown",
                reply_markup=reply_menu(chat_id=message.chat_id),
            )
            context.user_data["mode"] = MODE_NONE
            return ConversationHandler.END

        if not templates:
            await message.reply_text(
                "Шаблоны отчётов не найдены.", reply_markup=reply_menu(chat_id=message.chat_id)
            )
            context.user_data["mode"] = MODE_NONE
            return ConversationHandler.END

        context.user_data["report_templates"] = templates
        saved_template = _match_saved_report_template(
            context.user_data.get("report_last_template"), templates
        )

        if saved_template:
            context.user_data["report_selected_template"] = dict(saved_template)
            context.user_data["report_last_template"] = dict(saved_template)
            context.user_data.pop("report_matches", None)
            context.user_data.pop("report_offset", None)
            context.user_data.pop("report_candidates_text", None)
            label = _format_report_label(saved_template)
            await delete_anchor(context)
            msg = await message.reply_text(
                f"Отчёт: {label}\n\nВыберите период формирования:",
                reply_markup=kb_report_periods(),
                parse_mode=None,
            )
            await set_anchor_on(msg, context)
            return STATE_REPORT_PERIOD

        mask = context.user_data.get("report_search_mask")
        if mask is None:
            mask = context.user_data.get("report_last_query") or ""
        text, markup = _build_report_start_prompt(context, templates, mask)
        await delete_anchor(context)
        msg = await message.reply_text(
            text,
            reply_markup=markup,
            parse_mode=None,
        )
        await set_anchor_on(msg, context)
        return STATE_REPORT_NAME
    finally:
        try:
            client.http.close()
        except Exception:
            pass


async def reports_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    client = await require_wialon_client(update, context, "Чтобы формировать отчёты, авторизуйтесь.")
    if not client:
        return STATE_AUTH_WAIT_TOKEN
    return await _begin_report_flow(update.message, context, client, keep_stats_message=False)

async def cmd_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await reports_entry(update, context)


def _format_pipeline_summary(day: str, summary: Dict[str, Any], storage_root: Path) -> str:
    lines = [
        f"📊 Локальный отчёт за {day}",
        f"Событий: {summary.get('total_events', 0)}",
        f"Объектов: {summary.get('units', 0)}",
    ]
    span = summary.get("span")
    if span:
        lines.append(f"Диапазон: {span // 3600} ч {span % 3600 // 60} мин")
    first_ts = summary.get("first_ts")
    last_ts = summary.get("last_ts")
    if first_ts and last_ts:
        lines.append(f"UTC: {datetime.fromtimestamp(first_ts, tz=timezone.utc)} → {datetime.fromtimestamp(last_ts, tz=timezone.utc)}")
    fuel = summary.get("fuel") or {}
    if fuel:
        lines.append(
            f"Топливо: min {fuel.get('min'):.1f}, max {fuel.get('max'):.1f}, avg {fuel.get('avg'):.1f}, точек {fuel.get('samples')}"
        )
    else:
        lines.append("Топливо: нет подходящих параметров (нужен fuel_param).")
    lines.append(f"Источник: {storage_root}")
    return "\n".join(lines)


async def pipeline_summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update, context):
        return ConversationHandler.END

    message = update.effective_message
    args = getattr(context, "args", []) or []
    day = args[0] if args else datetime.now(timezone.utc).date().isoformat()
    try:
        datetime.fromisoformat(day)
    except ValueError:
        if message:
            await message.reply_text("Укажи дату в формате YYYY-MM-DD, например /pipeline_summary 2025-11-07.")
        return ConversationHandler.END

    storage_service = get_pipeline_storage_service()
    try:
        events = storage_service.fetch_day(day)
    except Exception as exc:
        log.exception("pipeline: failed to read storage for %s: %s", day, exc)
        if message:
            await message.reply_text(f"Не смогли прочитать локальное хранилище: {exc}")
        return ConversationHandler.END

    if not events:
        text = (
            f"⚠️ В локальном хранилище нет событий за {day}.\n"
            "Запусти загрузку (bootstrap или stream), чтобы наполнить кэш."
        )
        if message:
            await message.reply_text(text)
        return ConversationHandler.END

    calculator = SensorCalculator()
    summary = build_day_summary(events, calculator)
    reply_text = _format_pipeline_summary(day, summary, storage_service.storage_root)
    if message:
        await message.reply_text(reply_text)
    return ConversationHandler.END

def _ensure_report_templates(
    context: ContextTypes.DEFAULT_TYPE, client: "WialonClient"
) -> List[Dict[str, Any]]:
    templates = context.user_data.get("report_templates")
    if not templates:
        templates = client.list_report_templates()
        context.user_data["report_templates"] = templates
    return templates

async def report_name_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_REPORT:
        return STATE_MENU
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    client = await require_wialon_client(update, context, "Чтобы формировать отчёты, авторизуйтесь.")
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    query = (update.message.text or "").strip()
    if len(query) < 5:
        await send_new_anchor_below(
            update,
            "Название отчёта должно содержать минимум 5 символов. Попробуйте ещё раз.",
            kb_cancel(),
            context,
            parse_mode=None,
        )
        return STATE_REPORT_NAME

    try:
        templates = _ensure_report_templates(context, client)
    except Exception as e:
        log.exception("Ошибка обновления списка шаблонов отчётов")
        await send_new_anchor_below(
            update,
            f"Не удалось обновить список отчётов: `{e}`",
            kb_cancel(),
            context,
            parse_mode="Markdown",
        )
        return STATE_REPORT_NAME

    context.user_data["report_last_query"] = query
    context.user_data["report_search_mask"] = query

    query_cf = query.casefold()
    matches: List[Dict[str, Any]] = []
    for tpl in templates:
        name = (tpl.get("name") or "").strip()
        if not name:
            continue
        if query_cf in name.casefold():
            matches.append(tpl)

    exact = None
    for tpl in matches:
        name = (tpl.get("name") or "").strip()
        if name.casefold() == query_cf:
            exact = tpl
            break

    if exact:
        context.user_data["report_selected_template"] = dict(exact)
        context.user_data["report_last_template"] = dict(exact)
        context.user_data.pop("report_matches", None)
        context.user_data.pop("report_offset", None)
        context.user_data.pop("report_candidates_text", None)
        label = _format_report_label(exact)
        text = f"Выбран отчёт: {label}\n\nВыберите период формирования:".strip()
        await send_new_anchor_below(update, text, kb_report_periods(), context, parse_mode=None)
        return STATE_REPORT_PERIOD

    if matches:
        text = "Совпадения не найдены точно. Выберите отчёт из списка:"
        markup = _store_report_candidates(context, matches, text, offset=0)
        await send_new_anchor_below(update, text, markup, context, parse_mode=None)
        return STATE_REPORT_NAME

    if templates:
        text = "❌ Совпадений не найдено. Ниже доступные отчёты:"
        markup = _store_report_candidates(context, templates, text, offset=0)
        await send_new_anchor_below(update, text, markup, context, parse_mode=None)
    else:
        await send_new_anchor_below(
            update,
            "❌ Шаблоны отчётов отсутствуют. Попробуйте позже.",
            kb_cancel(),
            context,
            parse_mode=None,
        )
    return STATE_REPORT_NAME

async def report_choose_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_REPORT:
        return STATE_MENU
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)

    data = q.data or ""
    if data.startswith("report:more:"):
        try:
            _, _, raw_offset = data.split(":", 2)
            offset = int(raw_offset)
        except (ValueError, AttributeError):
            return STATE_REPORT_NAME
        matches = context.user_data.get("report_matches") or []
        if not matches:
            await _safe_answer_callback(q, "Список отчётов пуст", show_alert=True)
            return STATE_REPORT_NAME
        markup = _build_report_candidates_markup(context, offset=offset)
        text = context.user_data.get("report_candidates_text") or "Выберите отчёт из списка:"
        await edit_anchor(context, text, markup, parse_mode=None)
        return STATE_REPORT_NAME

    if not data.startswith("report:choose:"):
        return STATE_REPORT_NAME

    try:
        idx = int(data.split(":", 2)[2])
    except (ValueError, AttributeError):
        return STATE_REPORT_NAME

    matches = context.user_data.get("report_matches") or []
    if idx < 0 or idx >= len(matches):
        return STATE_REPORT_NAME

    selected = dict(matches[idx])
    context.user_data["report_selected_template"] = selected
    context.user_data["report_last_template"] = dict(selected)
    context.user_data.pop("report_matches", None)
    context.user_data.pop("report_offset", None)
    context.user_data.pop("report_candidates_text", None)
    label = _format_report_label(selected)
    text = (
        "Выбран отчёт: {label}\n\n"
        "Выберите период формирования или нажмите «🔁 Другой шаблон», чтобы вернуться к списку."
    ).format(label=label)
    await edit_anchor(context, text, kb_report_periods(), parse_mode=None)
    return STATE_REPORT_PERIOD


async def report_change_template_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_REPORT:
        return STATE_MENU

    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)

    client = await require_wialon_client(update, context, "Чтобы формировать отчёты, авторизуйтесь.")
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    context.user_data.pop("report_selected_template", None)
    context.user_data.pop("report_period", None)
    context.user_data.pop("report_format", None)

    try:
        templates = _ensure_report_templates(context, client)
    except Exception as e:
        log.exception("Ошибка обновления списка шаблонов отчётов при возврате к выбору")
        await edit_anchor(
            context,
            f"Не удалось обновить список отчётов: `{e}`",
            kb_cancel(),
            parse_mode="Markdown",
        )
        return STATE_REPORT_NAME

    if not templates:
        await edit_anchor(
            context,
            "Шаблоны отчётов недоступны. Попробуйте позже.",
            kb_cancel(),
            parse_mode=None,
        )
        return STATE_REPORT_NAME

    mask = context.user_data.get("report_search_mask")
    if mask is None:
        mask = context.user_data.get("report_last_query") or ""

    text, markup = _build_report_start_prompt(context, templates, mask)

    if context.user_data.get("anchor"):
        await edit_anchor(context, text, markup, parse_mode=None)
    else:
        await delete_anchor(context)
        if q.message:
            msg = await q.message.reply_text(text, reply_markup=markup, parse_mode=None)
            await set_anchor_on(msg, context)

    return STATE_REPORT_NAME


async def report_back_to_period_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_REPORT:
        return STATE_MENU

    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)

    template = context.user_data.get("report_selected_template")
    if not template:
        templates = context.user_data.get("report_templates") or []
        mask = context.user_data.get("report_search_mask") or ""
        text, markup = _build_report_start_prompt(context, templates, mask)
        await edit_anchor(context, text, markup, parse_mode=None)
        return STATE_REPORT_NAME

    label = _format_report_label(template)
    context.user_data.pop("report_period", None)
    text = f"Отчёт: {label}\n\nВыберите период формирования:".strip()
    await edit_anchor(context, text, kb_report_periods(), parse_mode=None)
    return STATE_REPORT_PERIOD


async def report_period_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_REPORT:
        return STATE_MENU
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)

    parts = (q.data or "").split(":")
    if len(parts) != 3:
        return STATE_REPORT_PERIOD
    key = parts[2]

    template = context.user_data.get("report_selected_template")
    if not template:
        await edit_anchor(context, "Сначала выберите отчёт.", kb_cancel(), parse_mode=None)
        return STATE_REPORT_NAME

    try:
        interval = build_report_interval(key)
    except ValueError:
        await _safe_answer_callback(q, "Неизвестный период", show_alert=True)
        return STATE_REPORT_PERIOD

    context.user_data["report_period"] = dict(interval)
    label = interval.get("label") or ""
    tpl_label = _format_report_label(template)
    text = (
        f"Отчёт: {tpl_label}\n"
        f"Период: {label}\n\n"
        "Выберите формат выгрузки:"
    )
    await edit_anchor(context, text, kb_report_formats(), parse_mode=None)
    return STATE_REPORT_FORMAT

async def report_format_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_REPORT:
        return STATE_MENU
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)

    client = await require_wialon_client(update, context, "Чтобы формировать отчёты, авторизуйтесь.")
    if not client:
        return STATE_AUTH_WAIT_TOKEN

    parts = (q.data or "").split(":")
    if len(parts) != 3:
        return STATE_REPORT_FORMAT
    fmt_key = parts[2]
    fmt_cf = fmt_key.lower()
    try:
        canonical_fmt, _, _ = WialonClient._resolve_format(fmt_cf)
    except ValueError:
        await _safe_answer_callback(q, "Неизвестный формат", show_alert=True)
        return STATE_REPORT_FORMAT

    template = context.user_data.get("report_selected_template")
    interval = context.user_data.get("report_period")
    if not template or not interval:
        await edit_anchor(context, "Сначала выберите отчёт и период.", kb_cancel(), parse_mode=None)
        return STATE_REPORT_NAME

    try:
        resource_id = int(template.get("resource_id"))
        template_id = int(template.get("template_id"))
    except (TypeError, ValueError):
        context.user_data.pop("report_selected_template", None)
        context.user_data.pop("report_last_template", None)
        await edit_anchor(
            context,
            "У выбранного шаблона некорректные параметры. Введите название отчёта ещё раз.",
            kb_cancel(),
            parse_mode=None,
        )
        return STATE_REPORT_NAME

    try:
        interval_from = int(interval.get("from"))
        interval_to = int(interval.get("to"))
    except (TypeError, ValueError):
        await edit_anchor(
            context,
            "Некорректный период отчёта. Выберите период ещё раз.",
            kb_report_periods(),
            parse_mode=None,
        )
        return STATE_REPORT_PERIOD

    unit_id_raw = context.user_data.get("report_unit_id")
    try:
        unit_id_val = int(unit_id_raw) if unit_id_raw is not None else None
    except (TypeError, ValueError):
        await edit_anchor(
            context,
            "Не удалось определить объект для отчёта. Нажмите «🔍 Найти объект» и выберите его заново.",
            markup=None,
            parse_mode=None,
        )
        clear_report_context(context, cancel_job=False)
        context.user_data["mode"] = MODE_NONE
        context.user_data.pop("anchor", None)
        if q.message:
            await q.message.reply_text(
                "Не удалось определить объект для отчёта. Нажмите «🔍 Найти объект».",
                reply_markup=reply_menu(chat_id=q.message.chat_id),
            )
        return STATE_MENU

    sanitized_template = dict(template)
    sanitized_template["resource_id"] = resource_id
    sanitized_template["template_id"] = template_id
    sanitized_interval = dict(interval)
    sanitized_interval["from"] = interval_from
    sanitized_interval["to"] = interval_to
    context.user_data["report_selected_template"] = sanitized_template
    context.user_data["report_period"] = sanitized_interval

    format_title = describe_report_format(canonical_fmt)
    tpl_label = _format_report_label(sanitized_template)
    period_label = sanitized_interval.get("label") or ""
    text = (
        f"Отчёт: {tpl_label}\n"
        f"Период: {period_label}\n"
        f"Формат: {format_title}\n\n"
        "⏳ Формирование отчёта, подождите…"
    )
    await edit_anchor(context, text, kb_report_wait(), parse_mode=None)

    cancel_event = threading.Event()
    unit_meta = context.user_data.get("report_unit_meta")
    job = {
        "template": dict(sanitized_template),
        "period": dict(sanitized_interval),
        "format": canonical_fmt,
        "chat_id": q.message.chat_id if q.message else (update.effective_chat.id if update.effective_chat else None),
        "cancel_event": cancel_event,
        "final_message_sent": False,
        "unit_id": unit_id_val,
        "unit": dict(unit_meta) if isinstance(unit_meta, dict) else None,
    }
    context.user_data["report_job"] = job

    async def _run() -> None:
        await report_generation_worker(context, job)

    task = context.application.create_task(_run())
    job["task"] = task

    try:
        client.http.close()
    except Exception:
        pass

    return STATE_REPORT_WAIT

async def report_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_REPORT:
        return STATE_MENU
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)

    job = context.user_data.get("report_job")
    if isinstance(job, dict):
        event = job.get("cancel_event")
        if isinstance(event, threading.Event):
            event.set()
        job["final_message_sent"] = True

    await edit_anchor(context, "❌ Формирование отчёта отменено.", markup=None, parse_mode=None)
    context.user_data.pop("anchor", None)
    await delete_stats_message(context)
    await delete_param_result_message(context)
    clear_param_runtime(context)
    context.user_data.pop("report_job", None)
    clear_report_context(context, cancel_job=False)
    if q.message:
        await q.message.reply_text(
            "Формирование отчёта отменено.",
            reply_markup=reply_menu(chat_id=q.message.chat_id),
        )
    return ConversationHandler.END

async def report_wait_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    job = context.user_data.get("report_job")
    if job and not job.get("final_message_sent"):
        await update.message.reply_text(
            "Отчёт ещё формируется. Нажмите «❌ Отмена», чтобы остановить процесс.",
            reply_markup=None,
        )
        return STATE_REPORT_WAIT

    context.user_data.pop("report_job", None)
    clear_report_context(context, cancel_job=False)
    context.user_data["mode"] = MODE_NONE
    await update.message.reply_text(
        "Диалог завершён. Нажмите «🔍 Найти объект», чтобы продолжить поиск.",
        reply_markup=reply_menu(chat_id=update.effective_chat.id if update.effective_chat else None),
    )
    return ConversationHandler.END

async def report_generation_worker(context: ContextTypes.DEFAULT_TYPE, job: Dict[str, Any]) -> None:
    template = job.get("template") or {}
    period = job.get("period") or {}
    fmt = job.get("format") or "pdf"
    cancel_event = job.get("cancel_event") if isinstance(job.get("cancel_event"), threading.Event) else threading.Event()
    chat_id = job.get("chat_id")
    unit_info = job.get("unit") if isinstance(job.get("unit"), dict) else {}
    unit_id_raw = job.get("unit_id")
    try:
        unit_id = int(unit_id_raw) if unit_id_raw is not None else None
    except (TypeError, ValueError):
        unit_id = None

    client: Optional[WialonClient] = None
    try:
        if chat_id is None:
            raise NotAuthorized("chat_id missing for report job")

        try:
            client = wialon_for_chat(int(chat_id))
        except UserBlocked:
            if not job.get("final_message_sent"):
                await edit_anchor(context, "❌ Пользователь заблокирован. Отчёт не сформирован.", markup=None, parse_mode=None)
                context.user_data.pop("anchor", None)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="🚫 Вы заблокированы. Обратитесь к администратору.",
                    reply_markup=reply_menu(chat_id=chat_id),
                )
            return
        except NotAuthorized:
            if not job.get("final_message_sent"):
                await edit_anchor(
                    context,
                    "❌ Не удалось сформировать отчёт: требуется авторизация.",
                    markup=None,
                    parse_mode=None,
                )
                context.user_data.pop("anchor", None)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Для формирования отчётов авторизуйтесь заново.",
                    reply_markup=reply_menu(chat_id=chat_id),
                )
            return

        result = await run_blocking(
            client.generate_report_file,
            int(template.get("resource_id", 0)),
            int(template.get("template_id", 0)),
            int(period.get("from", 0)),
            int(period.get("to", 0)),
            fmt,
            cancel_event,
            object_id=unit_id,
        )

        if job.get("final_message_sent"):
            return

        if not result:
            raise RuntimeError("Пустой результат отчёта")

        filename, content = result
        summary_lines: List[str] = []
        if unit_id is not None:
            unit_name = (unit_info.get("nm") or "").strip()
            if unit_name:
                summary_lines.append(f"Объект: {unit_name} (id {unit_id})")
            else:
                summary_lines.append(f"Объект id {unit_id}")
        summary_lines.extend(
            [
                f"Отчёт: {_format_report_label(template)}",
                f"Период: {period.get('label', '')}",
                f"Формат: {describe_report_format(fmt)}",
            ]
        )
        summary = "\n".join(summary_lines)
        has_unit = unit_id is not None
        await edit_anchor(
            context,
            f"{summary}\n\n✅ Отчёт готов.",
            markup=kb_report_finish(has_unit),
            parse_mode=None,
        )
        context.user_data.pop("anchor", None)

        file_obj = io.BytesIO(content)
        file_obj.name = filename
        file_obj.seek(0)
        if chat_id is not None:
            await context.bot.send_document(
                chat_id=chat_id,
                document=file_obj,
                caption=summary,
                reply_markup=reply_menu(chat_id=chat_id),
            )
    except ReportCancelledError:
        if not job.get("final_message_sent"):
            await edit_anchor(context, "❌ Формирование отчёта отменено.", markup=None, parse_mode=None)
            context.user_data.pop("anchor", None)
            if chat_id is not None:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Формирование отчёта отменено.",
                    reply_markup=reply_menu(chat_id=chat_id),
                )
    except Exception as e:
        log.exception("Ошибка формирования отчёта")
        if not job.get("final_message_sent"):
            error_text = str(e)
            await edit_anchor(
                context,
                f"Ошибка формирования отчёта: {error_text}",
                markup=None,
                parse_mode=None,
            )
            context.user_data.pop("anchor", None)
            if chat_id is not None:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Не удалось сформировать отчёт.",
                    reply_markup=reply_menu(chat_id=chat_id),
                )
    finally:
        if client:
            try:
                client.http.close()
            except Exception:
                pass
        context.user_data.pop("report_job", None)
        clear_report_context(context, cancel_job=False)
        job["final_message_sent"] = True

async def params_query_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.user_data.get("mode") != MODE_PARAMS:
        return STATE_MENU
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN

    query = (update.message.text or "").strip()
    params_map: Dict[str, str] = context.user_data.get("params_map") or {}
    if not params_map:
        await update_param_prompt(context, [], "Нет данных о параметрах", update)
        return STATE_PARAM_QUERY

    if len(query) < 2:
        await update_param_prompt(context, context.user_data.get("param_current_matches") or [],
                                  "Введите минимум 2 символа для поиска", update)
        return STATE_PARAM_QUERY

    matches = filter_params(context, query)
    context.user_data["param_last_query"] = query
    note = f"Найдено {len(matches)} параметров." if matches else "❌ Параметр не найден"

    params_map = context.user_data.get("params_map") or {}
    exact_matches = [name for name in matches if name.casefold() == query.casefold()]
    if exact_matches:
        await update_param_prompt(context, matches, note, update)
        name = exact_matches[0]
        value = params_map.get(name, "")
        await delete_param_result_message(context)
        text = f"{name} = {value}"
        msg = await update.message.reply_text(text, reply_markup=kb_param_results(), parse_mode=None)
        context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
        store_param_result_payload(context, text, None)
        return STATE_PARAM_QUERY

    if matches:
        await update_param_prompt(context, matches, note, update)
    else:
        await update_param_prompt(context, [], note, update)
    return STATE_PARAM_QUERY

async def params_buttons_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not await ensure_authorized(update, context):
        return STATE_AUTH_WAIT_TOKEN
    await _safe_answer_callback(q)

    data = (q.data or "")
    if not data.startswith("param:"):
        return STATE_PARAM_QUERY

    command = data.split(":", 1)[1]
    parts = command.split(":")
    action = parts[0]

    if action == "show":
        try:
            idx = int(parts[1])
        except (IndexError, ValueError):
            return STATE_PARAM_QUERY
        matches: List[str] = context.user_data.get("param_current_matches") or []
        if idx < 0 or idx >= len(matches):
            return STATE_PARAM_QUERY
        name = matches[idx]
        params_map: Dict[str, str] = context.user_data.get("params_map") or {}
        value = params_map.get(name, "")
        await delete_param_result_message(context)
        await delete_anchor(context)
        text = f"{name} = {value}"
        msg = await q.message.reply_text(text, reply_markup=kb_param_results(), parse_mode=None)
        context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
        store_param_result_payload(context, text, None)
        context.user_data["mode"] = MODE_PARAMS
        return STATE_PARAM_QUERY

    if action == "download":
        matches = context.user_data.get("param_current_matches") or []
        if not matches:
            await update_param_prompt(context, [], "❌ Параметр не найден", q)
            return STATE_PARAM_QUERY
        text, parse_mode = build_params_text(context, matches)
        await delete_param_result_message(context)
        await delete_anchor(context)
        msg = await q.message.reply_text(text, reply_markup=kb_param_results(), parse_mode=parse_mode)
        context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
        store_param_result_payload(context, text, parse_mode)
        context.user_data["mode"] = MODE_PARAMS
        return STATE_PARAM_QUERY

    if action == "show_all":
        names = _ordered_param_names(context)
        if names:
            text, parse_mode = build_params_text(context, names)
        else:
            text, parse_mode = "Нет данных о параметрах", None
        await delete_param_result_message(context)
        await delete_anchor(context)
        msg = await q.message.reply_text(text, reply_markup=kb_param_results(), parse_mode=parse_mode)
        context.user_data["param_result_msg"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
        store_param_result_payload(context, text, parse_mode)
        context.user_data["mode"] = MODE_PARAMS
        return STATE_PARAM_QUERY

    if action == "back_query":
        await delete_param_result_message(context)
        context.user_data.pop("param_result_payload", None)
        matches = context.user_data.get("param_current_matches") or _ordered_param_names(context)
        note = context.user_data.get("param_last_note") or None
        await send_param_prompt(q, context, matches, note)
        return STATE_PARAM_QUERY

    if action == "back_stats":
        await delete_param_result_message(context)
        context.user_data.pop("param_result_payload", None)
        await delete_anchor(context)
        clear_param_runtime(context)
        context.user_data["mode"] = MODE_NONE
        unit = context.user_data.get("chosen_unit")
        if not unit:
            return ConversationHandler.END
        unit_id = int(unit["id"])
        unit_name = unit.get("nm") or f"id {unit_id}"
        client = None
        if not _pipeline_card_enabled():
            client = await require_wialon_client(update, context, "Чтобы просматривать объект, авторизуйтесь.")
            if not client:
                return STATE_AUTH_WAIT_TOKEN
        return await show_stats_then_actions(q, context, unit_id, unit_name, client, via_callback=True)

    if action == "finish":
        await remove_reply_markup_safe(q.message)
        await remove_reply_markup_by_meta(context, context.user_data.pop("param_result_msg", None))
        context.user_data.pop("param_result_payload", None)
        context.user_data.pop("params_map", None)
        context.user_data.pop("params_order", None)
        context.user_data.pop("param_current_matches", None)
        context.user_data.pop("param_last_note", None)
        context.user_data.pop("param_last_query", None)
        context.user_data.pop("cmd_return", None)
        context.user_data.pop("cmd_return_payload", None)
        context.user_data.pop("cmd_return_runtime", None)
        clear_param_runtime(context)
        context.user_data["mode"] = MODE_NONE
        await delete_anchor(context)

        unit = context.user_data.get("chosen_unit")
        if not unit:
            if q.message:
                await q.message.reply_text(
                    "Диалог завершён.",
                    reply_markup=reply_menu(chat_id=q.message.chat_id),
                )
            return ConversationHandler.END

        try:
            unit_id = int(unit["id"])
        except (TypeError, ValueError):
            unit_id = None
        unit_name = unit.get("nm") if isinstance(unit, dict) else None
        if unit_id is None:
            if q.message:
                await q.message.reply_text(
                    "Диалог завершён.",
                    reply_markup=reply_menu(chat_id=q.message.chat_id),
                )
            return ConversationHandler.END

        client = None
        if not _pipeline_card_enabled():
            client = await require_wialon_client(
                update,
                context,
                "Чтобы просматривать объект, авторизуйтесь.",
            )
            if not client:
                return STATE_AUTH_WAIT_TOKEN

        await delete_stats_message(context)
        return await show_stats_then_actions(
            q,
            context,
            unit_id,
            unit_name or f"id {unit_id}",
            client,
            via_callback=True,
        )

    if action == "find_other":
        await delete_param_result_message(context)
        await delete_stats_message(context)
        _clear_export_job(context, cancel=True)
        context.user_data.clear()
        msg = await q.message.reply_text(
            "Введите минимум 3 символа для поиска объекта (например 676 или 676мт).",
            reply_markup=kb_cancel(),
        )
        await set_anchor_on(msg, context)
        return STATE_FIND_QUERY

    if action == "cmd":
        context.user_data["mode"] = MODE_CMD
        context.user_data["cmd_return"] = "params"
        payload_copy = context.user_data.get("param_result_payload")
        if isinstance(payload_copy, dict):
            context.user_data["cmd_return_payload"] = dict(payload_copy)
        else:
            context.user_data["cmd_return_payload"] = payload_copy
        context.user_data.pop("param_result_payload", None)
        runtime_snapshot = {
            "matches": list(context.user_data.get("param_current_matches") or []),
            "last_query": context.user_data.get("param_last_query"),
            "last_note": context.user_data.get("param_last_note"),
        }
        context.user_data["cmd_return_runtime"] = runtime_snapshot
        await delete_param_result_message(context)
        clear_param_runtime(context)
        await clear_stats_buttons(context)
        unit = context.user_data.get("chosen_unit")
        if not unit:
            return ConversationHandler.END
        await delete_anchor(context)
        msg = await q.message.reply_text(
            f"Выбран объект: {unit['nm']} — id {unit['id']}\n\n"
            f"Напишите TCP-команду для устройства.\n"
            f"_Команда отправляется только при активном TCP-соединении._",
            reply_markup=kb_cmd_entry_controls(),
        )
        await set_anchor_on(msg, context)
        return STATE_CMD_VALUE

    return STATE_PARAM_QUERY

def kb_stats_actions_with_refresh() -> InlineKeyboardMarkup:
    base = kb_stats_actions()
    try:
        rows = [list(row) for row in base.inline_keyboard]
    except Exception:
        rows = []
    rows.append(
        [
            InlineKeyboardButton("🔄 Обновить", callback_data="stats:refresh"),
            InlineKeyboardButton("↩️ К списку", callback_data="stats:to_list"),
        ]
    )
    return InlineKeyboardMarkup(rows)

# =================== MAIN ===================
def main() -> None:
    _start_unit_snapshot_daemon()
    _bootstrap_unit_snapshot_from_cache()
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Не указан TELEGRAM_BOT_TOKEN в .env")
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    from admin_panel import AdminPanel

    async def send_auth_prompt_to_chat(target_chat_id: int) -> None:
        try:
            await app.bot.send_message(chat_id=target_chat_id, text=AUTH_PROMPT_TEXT)
            await app.bot.send_message(
                chat_id=target_chat_id,
                text="Выберите действие:",
                reply_markup=kb_auth_actions(),
            )
        except Exception as exc:
            log.warning(
                "Не удалось отправить запрос авторизации пользователю %s: %s",
                target_chat_id,
                exc,
            )

    admin_panel = AdminPanel(
        token_store=token_store,
        verify_token_func=verify_wialon_token_sync,
        prompt_sender=send_auth_prompt_to_chat,
        admin_whitelist=list(ADMIN_WHITELIST),
    )

    async def admin_command_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        remember_user(update)
        if context.user_data.get("mode") == MODE_EXPORT:
            _clear_export_job(context, cancel=True)
            await delete_export_anchor(context)
            context.user_data["mode"] = MODE_NONE
        await _suppress_objects_keyboards(context)
        _set_active_ui(context, UI_ADMIN)
        chat = update.effective_chat
        log.info("admin: open_panel user=%s", chat.id if chat else None)
        await admin_panel.handle_command(update, context)

    async def admin_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if _active_ui(context) != UI_ADMIN:
            return
        remember_user(update)
        _set_active_ui(context, UI_ADMIN)
        await _suppress_objects_keyboards(context)
        handled = await admin_panel.handle_text(update, context)
        if not handled and update.message:
            chat = update.effective_chat
            log.debug("admin: ignore free text user=%s", chat.id if chat else None)
            await update.message.reply_text(
                "Не удалось распознать ввод для админ-панели."
            )

    app.add_handler(CommandHandler("admin", admin_command_entry), group=0)
    app.add_handler(
        MessageHandler(
            filters.Regex(f"^{re.escape(MENU_BUTTON_ADMIN)}$"),
            admin_command_entry,
            block=True,
        ),
        group=0,
    )
    async def admin_broadcast_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        remember_user(update)
        if _active_ui(context) != UI_ADMIN:
            return
        _set_active_ui(context, UI_ADMIN)
        await _suppress_objects_keyboards(context)
        await admin_panel.handle_broadcast_callback(update, context)

    async def admin_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        remember_user(update)
        if _active_ui(context) != UI_ADMIN:
            return
        _set_active_ui(context, UI_ADMIN)
        await _suppress_objects_keyboards(context)
        await admin_panel.handle_callback(update, context)

    app.add_handler(
        CallbackQueryHandler(
            admin_broadcast_router,
            pattern=r"^admin:broadcast:(confirm|cancel)$",
            block=False,
        ),
        group=0,
    )
    app.add_handler(
        CallbackQueryHandler(
            admin_callback_router,
            pattern=r"^adm:.*$",
            block=False,
        ),
        group=0,
    )
    admin_text_filter = (
        filters.TEXT
        & ~filters.COMMAND
        & ~filters.Regex(f"^{MENU_BUTTON_FIND}$")
        & ~filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$")
        & ~filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$")
        & ~filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$")
        & ~filters.Regex(f"^{re.escape(MENU_BUTTON_SETTINGS)}$")
    )
    app.add_handler(
        MessageHandler(
            admin_text_filter,
            admin_text_router,
            block=False,
        ),
        group=0,
    )

    app.add_handler(
        MessageHandler(
            filters.Regex(f"^{re.escape(MENU_BUTTON_SETTINGS)}$"),
            settings_entry,
            block=False,
        ),
        group=0,
    )
    app.add_handler(
        CallbackQueryHandler(
            settings_buttons_cb,
            pattern=r"^settings:.*$",
            block=False,
        ),
        group=0,
    )

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            CommandHandler("reports", cmd_reports),
            CommandHandler("pipeline_summary", pipeline_summary_cmd),
            MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
            MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
            MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
            MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
            CallbackQueryHandler(stats_actions_cb, pattern=STATS_ACTIONS_PATTERN),
        ],
        states={
            STATE_AUTH_WAIT_TOKEN: [
                CallbackQueryHandler(auth_callback, pattern=r"^auth:.*$"),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_SETTINGS)}$"), settings_entry),
                MessageHandler(filters.TEXT & ~filters.COMMAND, auth_token_text),
            ],
            STATE_MENU: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_SETTINGS)}$"), settings_entry),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:.*$"),
            ],
            STATE_FIND_QUERY: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_SETTINGS)}$"), settings_entry),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(menu|find)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, find_query_text),
            ],
            STATE_WAIT_UNIT: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                MessageHandler(filters.TEXT & ~filters.COMMAND, find_query_text),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(choose_unit_cb, pattern=r"^(unit:\d+|units:more:\d+)$"),
                CallbackQueryHandler(
                    stats_actions_cb,
                    pattern=STATS_ACTIONS_PATTERN,
                ),
                CallbackQueryHandler(
                    graph_actions_cb,
                    pattern=r"^graph:.*$",
                ),
                CallbackQueryHandler(
                    cmd_success_action_cb,
                    pattern=r"^cmdsuccess:(return|again|search)(?::\d+)?$",
                ),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(find)$"),
            ],
            STATE_CF_NAME: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(cf_dump_cb, pattern=r"^cf:dump$"),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(unit)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, cf_name_text),
            ],
            STATE_CF_VALUE: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(cf_dump_cb, pattern=r"^cf:dump$"),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(name)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, cf_value_text),
            ],
            STATE_CF_MORE: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(cf_dump_cb, pattern=r"^cf:dump$"),
                CallbackQueryHandler(cf_more_cb, pattern=r"^more:(yes|no)$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(value)$"),
            ],
            STATE_GRAPH_MENU: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(find)$"),
                CallbackQueryHandler(graph_actions_cb, pattern=r"^graph:.*$"),
            ],
            STATE_GRAPH_PERIOD: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(find)$"),
                CallbackQueryHandler(graph_actions_cb, pattern=r"^graph:.*$"),
            ],
            STATE_GRAPH_CUSTOM_DATE: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(find)$"),
                CallbackQueryHandler(graph_actions_cb, pattern=r"^graph:.*$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, graph_custom_date_text),
            ],
            STATE_DRAIN_PROMPT_REFRESH: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(drain_analysis_refresh_cb, pattern=r"^drain:refresh:(yes|no)$"),
                CallbackQueryHandler(drain_message_cache_load_cb, pattern=r"^drain:msg:load$"),
                CallbackQueryHandler(drain_message_cache_period_cb, pattern=r"^drain:msg:period$"),
                CallbackQueryHandler(drain_message_cache_clear_cb, pattern=r"^drain:msg:clear$"),
                CallbackQueryHandler(drain_message_cache_cancel_cb, pattern=r"^drain:msg:cancel$"),
            ],
            STATE_DRAIN_WAIT_DATE: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(drain_analysis_refresh_cb, pattern=r"^drain:refresh:(yes|no)$"),
                CallbackQueryHandler(drain_analysis_cancel_cb, pattern=r"^drain:cancel$"),
                CallbackQueryHandler(drain_message_cache_cancel_cb, pattern=r"^drain:msg:cancel$"),
                CallbackQueryHandler(drain_message_cache_load_cb, pattern=r"^drain:msg:load$"),
                CallbackQueryHandler(drain_message_cache_period_cb, pattern=r"^drain:msg:period$"),
                CallbackQueryHandler(drain_message_cache_clear_cb, pattern=r"^drain:msg:clear$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, drain_analysis_date_text),
            ],
            STATE_DRAIN_WAIT_MSG_DATE: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(drain_message_cache_load_cb, pattern=r"^drain:msg:load$"),
                CallbackQueryHandler(drain_message_cache_period_cb, pattern=r"^drain:msg:period$"),
                CallbackQueryHandler(drain_message_cache_clear_cb, pattern=r"^drain:msg:clear$"),
                CallbackQueryHandler(drain_message_cache_cancel_cb, pattern=r"^drain:msg:cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, drain_message_cache_date_text),
            ],
            STATE_DRAIN_WAIT_MSG_PERIOD: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(drain_message_cache_load_cb, pattern=r"^drain:msg:load$"),
                CallbackQueryHandler(drain_message_cache_period_cb, pattern=r"^drain:msg:period$"),
                CallbackQueryHandler(drain_message_cache_clear_cb, pattern=r"^drain:msg:clear$"),
                CallbackQueryHandler(drain_message_cache_cancel_cb, pattern=r"^drain:msg:cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, drain_message_cache_period_text),
            ],
            STATE_DRAIN_WAIT_MSG_PARTIAL: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(drain_message_cache_partial_resume_cb, pattern=r"^drain:msg:resume$"),
                CallbackQueryHandler(drain_message_cache_partial_use_cb, pattern=r"^drain:msg:use$"),
                CallbackQueryHandler(drain_message_cache_partial_discard_cb, pattern=r"^drain:msg:discard$"),
                CallbackQueryHandler(drain_message_cache_partial_back_cb, pattern=r"^drain:msg:back$"),
                CallbackQueryHandler(drain_message_cache_cancel_cb, pattern=r"^drain:msg:cancel$"),
            ],
            STATE_DRAIN_WAIT_MSG_DELETE: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(drain_message_cache_load_cb, pattern=r"^drain:msg:load$"),
                CallbackQueryHandler(drain_message_cache_period_cb, pattern=r"^drain:msg:period$"),
                CallbackQueryHandler(drain_message_cache_clear_cb, pattern=r"^drain:msg:clear$"),
                CallbackQueryHandler(drain_message_cache_cancel_cb, pattern=r"^drain:msg:cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, drain_message_cache_delete_text),
            ],
            STATE_WLN_EXPORT_WAIT_DATE: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(wln_export_cancel_cb, pattern=r"^wln:cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, wln_export_date_text),
            ],
            STATE_ADD_DUT_WAIT_NAME: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_dut_name_text),
            ],
            STATE_CMD_VALUE: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(cmd_unit)$"),
                CallbackQueryHandler(
                    cmd_success_action_cb,
                    pattern=r"^cmdsuccess:(return|again|search)(?::\d+)?$",
                ),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_value_text),
            ],
            STATE_PARAM_QUERY: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(back_cb, pattern=r"^back:(menu|find|unit)$"),
                CallbackQueryHandler(params_buttons_cb, pattern=r"^param:.*$"),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, params_query_text),
            ],
            STATE_REPORT_NAME: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(report_choose_cb, pattern=r"^report:(choose|more):\d+$"),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_name_text),
            ],
            STATE_REPORT_PERIOD: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(report_change_template_cb, pattern=r"^report:change$"),
                CallbackQueryHandler(report_period_cb, pattern=r"^report:period:(today|yesterday|7days|month)$"),
                CallbackQueryHandler(report_choose_cb, pattern=r"^report:(choose|more):\d+$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_name_text),
            ],
            STATE_REPORT_FORMAT: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(report_format_cb, pattern=r"^report:format:(pdf|excel)$"),
                CallbackQueryHandler(report_back_to_period_cb, pattern=r"^report:back:period$"),
                CallbackQueryHandler(report_period_cb, pattern=r"^report:period:(today|yesterday|7days|month)$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_name_text),
            ],
            STATE_REPORT_WAIT: [
                MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(report_cancel_cb, pattern=r"^report:cancel$"),
                CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_wait_text),
            ],
            STATE_EXPORT_MENU: [
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
                MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
                CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
                CallbackQueryHandler(export_cancel_cb, pattern=r"^export:cancel$"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(auth_callback, pattern=r"^auth:.*$"),
            CallbackQueryHandler(cancel_inline_cb, pattern=r"^action:cancel$"),
            CallbackQueryHandler(params_buttons_cb, pattern=r"^param:.*$"),
            CallbackQueryHandler(report_cancel_cb, pattern=r"^report:cancel$"),
            CallbackQueryHandler(stats_actions_cb, pattern=STATS_ACTIONS_PATTERN),
            CallbackQueryHandler(back_cb, pattern=r"^back:.*$"),
            CallbackQueryHandler(start_action_cb, pattern=r"^start:(find|export)$"),
            MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
            MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_EXPORT)}$"), export_entry),
            MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_DRAIN_ANALYSIS)}$"), drain_analysis_entry),
            MessageHandler(filters.Regex(f"^{re.escape(MENU_BUTTON_WLN_EXPORT)}$"), wln_export_entry),
            CallbackQueryHandler(wln_export_cancel_cb, pattern=r"^wln:cancel$"),
            CallbackQueryHandler(drain_analysis_cancel_cb, pattern=r"^drain:cancel$"),
        ],
        allow_reentry=False,  # КЛЮЧЕВОЕ: запрещаем ре-энтри, чтобы текст в CF/CMD не перезапускал диалог
    )

    app.add_handler(conv, group=1)

    log.info("Бот запущен: entry-point для текста активен, allow_reentry=False; команды и CF работают, дубли карточек устранены.")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

def _chunked(sequence: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    size = max(1, int(size))
    for idx in range(0, len(sequence), size):
        yield sequence[idx : idx + size]
