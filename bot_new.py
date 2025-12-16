"""
Телеграм-бот для SMT Telematics.

Архитектура v2 (SQL-First):
- Поиск объектов идёт прямым SQL-запросом в PostgreSQL.
- Карточка объекта собирается из latest_metrics (кэш) + БД (статические поля).
- Нет зависимости от файловых снапшотов (unit_snapshot.json.gz, unit_index и т.п.).
"""

from __future__ import annotations

import asyncio
import functools
import html
import os
import pathlib
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from dut_cache import (
    RE_ANY_DUT,
    extract_assigned_param_name,
    extract_param_hint_from_sensor,
    extract_params_from_item,
    format_liters,
    normalize_fuel_value,
    select_fuel_sensor,
)
from pipeline.config.defaults import load_from_env as load_pipeline_config
from pipeline.config.unit_config import UnitConfig
from pipeline.config.unit_config_service import UnitConfigService, UnitConfigValidationError
from pipeline.engine import SensorCalculator, build_day_summary
from pipeline.services.storage_service import get_pipeline_storage_service

from bot.constants import *
from bot.logging_setup import setup_logging
from bot.settings_store import SettingsStore
from bot.token_store import TokenStore
from bot.stores import ZoneFileStore
from bot.features import search as search_features

from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


# ---------------------------------------------------------------------------
# Глобальные настройки / директории
# ---------------------------------------------------------------------------

BASE_DIR = pathlib.Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
UNIT_CONFIG_DIR = DATA_DIR / "unit_configs"
UNIT_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
SENSOR_EXPORTS_DIR = UNIT_CONFIG_DIR / "exports"

loggers = setup_logging(LOG_DIR)
log = loggers.app
card_log = loggers.card
geo_log = loggers.geo
nearby_log = loggers.nearby
dut_cal_log = loggers.dut_cal

_settings_store = SettingsStore(DATA_DIR / "user_settings.json")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

WEB_BASE_URL = os.getenv("WEB_BASE_URL", "http://localhost:8000").strip()

EXECUTOR_WORKERS = int(os.getenv("EXECUTOR_WORKERS", "8") or 8)
EXECUTOR = ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS)

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

ONLINE_THRESHOLD_SEC = 600
GEO_CACHE_PATH = (BASE_DIR / "data/geozones.v1.json.gz").resolve()
ZONE_STORE = ZoneFileStore(GEO_CACHE_PATH)
GEO_MAX_NAMES = 5
NEARBY_LIMIT = 5
DEFAULT_NEARBY_RADIUS_M = 100
NEARBY_RADIUS_STEP_M = 50
NEARBY_RADIUS_MIN_M = 50
NEARBY_RADIUS_MAX_M = 5000

FUEL_MIN_VALID_L = 1.0
FUEL_MAX_VALID_L = 2000.0


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

async def _safe_answer_callback(query: Optional[CallbackQuery], *args: Any, **kwargs: Any) -> None:
    if query is None:
        return
    try:
        await query.answer(*args, **kwargs)
    except Exception:
        # В боте нам достаточно залогировать, но не падать
        log.debug("callback answer failed", exc_info=True)


@functools.lru_cache(maxsize=1)
def _pipeline_card_enabled() -> bool:
    # В новой архитектуре карточка всегда работает от pipeline/DB
    return True


@functools.lru_cache(maxsize=1)
def _pipeline_search_enabled() -> bool:
    return True


def _load_persisted_settings(chat_id: int) -> Dict[str, Any]:
    return _settings_store.load(chat_id)


def _persist_user_settings(chat_id: int, settings: Dict[str, Any]) -> None:
    _settings_store.save(chat_id, settings)


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
        _persist_user_settings(chat_id, settings)
    return value


# ---------------------------------------------------------------------------
# SQL-first доступ к данным (поиск и карточка)
# ---------------------------------------------------------------------------

def _search_units_locally(query: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Поиск юнитов по имени/UID через SQL."""

    query_norm = (query or "").strip()
    if not query_norm:
        return []

    storage = get_pipeline_storage_service()
    results: List[Dict[str, Any]] = []

    try:
        with storage._get_conn() as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT id, name, uid, hw_type
                    FROM units
                    WHERE COALESCE(is_deleted, FALSE) = FALSE
                      AND (name ILIKE %s OR uid ILIKE %s)
                    ORDER BY name ASC
                    LIMIT %s
                """
                pattern = f"%{query_norm}%"
                cur.execute(sql, (pattern, pattern, limit))
                for row in cur.fetchall():
                    results.append(
                        {
                            "id": row["id"],
                            "nm": row["name"],
                            "uid": row["uid"],
                            "hw": row["hw_type"],
                        }
                    )
    except Exception as exc:
        log.error("search_units_locally failed: %s", exc)

    return results


def _build_local_stats_item(unit_id: int) -> Dict[str, Any]:
    """Собрать статическую/динамическую часть карточки юнита из БД и LatestTelemetry."""

    storage = get_pipeline_storage_service()

    # Статическая часть
    static_info: Dict[str, Any] = {"id": unit_id, "nm": f"id {unit_id}"}
    try:
        with storage._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name, uid, hw_type FROM units WHERE id = %s", (unit_id,))
                row = cur.fetchone()
                if row:
                    static_info = {
                        "id": unit_id,
                        "nm": row["name"],
                        "uid": row["uid"],
                        "hw": row["hw_type"],
                    }
    except Exception as exc:
        log.debug("unit static load failed for %s: %s", unit_id, exc)

    # Динамическая часть (последняя телеметрия)
    latest = storage.get_latest_metrics(unit_id) or {}

    pos: Dict[str, Any] = {}
    if latest.get("lat") is not None:
        pos["y"] = latest["lat"]
    if latest.get("lon") is not None:
        pos["x"] = latest["lon"]
    if latest.get("course") is not None:
        pos["c"] = latest["course"]
    if latest.get("speed") is not None:
        pos["s"] = latest["speed"]

    ts = latest.get("device_ts") or latest.get("received_ts")
    if ts:
        pos["t"] = ts

    params = latest.get("params") or {}

    return {
        "id": unit_id,
        "nm": static_info["nm"],
        "uid": static_info.get("uid"),
        "hw": static_info.get("hw"),
        "pos": pos,
        "lmsg": {"t": ts} if ts else {},
        "prms": params,
        "params": params,
        "pipeline_event": latest,
        "sens": {},
    }


def _lookup_unit_snapshot(unit_id: int) -> Optional[Dict[str, Any]]:
    """Legacy‑совместимая обёртка, чтобы старые части кода могли получить псевдо‑snapshot."""

    try:
        return _build_local_stats_item(unit_id)
    except Exception as exc:
        log.error("lookup_unit_snapshot failed for %s: %s", unit_id, exc)
        return None


# ---------------------------------------------------------------------------
# UnitConfig (SQL-only)
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def get_unit_config_service() -> UnitConfigService:
    dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DEVICE_REGISTRY_DSN or DATABASE_URL is required for UnitConfigService")
    return UnitConfigService(dsn)


def _load_unit_config(unit_id: int) -> Optional[UnitConfig]:
    try:
        return get_unit_config_service().load(unit_id)
    except UnitConfigValidationError as exc:
        log.warning("unit_config validation failed for %s: %s", unit_id, exc)
    except Exception as exc:
        log.warning("unit_config load failed for %s: %s", unit_id, exc)
    return None


def _ensure_unit_config_object(unit_id: int) -> UnitConfig:
    cfg = _load_unit_config(unit_id)
    return cfg if cfg is not None else UnitConfig(unit_id=unit_id, source_kind="unknown")


def _clear_unit_config_cache() -> None:
    try:
        get_unit_config_service.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Legacy/Token store
# ---------------------------------------------------------------------------

token_store = TokenStore(DATA_DIR / "auth_tokens.sqlite3")


async def run_blocking(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(EXECUTOR, functools.partial(func, *args, **kwargs))


# ---------------------------------------------------------------------------
# Состояния диалога
# ---------------------------------------------------------------------------

STATE_AUTH_WAIT_TOKEN = 5
STATE_MENU = 10
STATE_FIND_QUERY = 20
STATE_WAIT_UNIT = 30

MODE_NONE = "none"

MENU_BUTTON_FIND = "🔍 Найти объект"
MENU_BUTTON_SETTINGS = "⚙️ Настройки"

ID_QUERY_RE = re.compile(r"^\s*id\s+(\d{1,10})\s*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Команды / меню
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text(
        "Привет! Я помогу найти объект.",
        reply_markup=reply_menu(),
    )
    return await _start_search_prompt_from_trigger(update, context)


async def global_restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await _start_search_prompt_from_trigger(update, context)


async def _start_search_prompt_from_trigger(update_or_q, context):
    await send_new_anchor_below(update_or_q, "Введите имя или ID...", kb_search_prompt(), context)
    return STATE_FIND_QUERY


def reply_menu(chat_id=None):
    return ReplyKeyboardMarkup(
        [[KeyboardButton(MENU_BUTTON_FIND), KeyboardButton(MENU_BUTTON_SETTINGS)]],
        resize_keyboard=True,
    )


def kb_search_prompt():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="action:cancel")]])


# ---------------------------------------------------------------------------
# Обработчик поиска
# ---------------------------------------------------------------------------

async def _handle_objects_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()

    # Поиск по ID
    id_match = ID_QUERY_RE.match(text)
    if id_match:
        unit_id = int(id_match.group(1))
        item = _build_local_stats_item(unit_id)
        context.user_data["chosen_unit"] = {"id": unit_id, "nm": item["nm"]}
        return await show_stats_then_actions(update, context, unit_id, item["nm"], None)

    if len(text) < 3:
        await update.message.reply_text("Минимум 3 символа.")
        return STATE_FIND_QUERY

    units = await run_blocking(_search_units_locally, text)
    if not units:
        await update.message.reply_text("Ничего не найдено.")
        return STATE_FIND_QUERY

    if len(units) == 1:
        u = units[0]
        context.user_data["chosen_unit"] = u
        return await show_stats_then_actions(update, context, u["id"], u["nm"], None)

    # Список
    search_features.set_units(context, units)
    await _render_units_page(context, chat_id=update.effective_chat.id, update=update, offset=0, reanchor=True)
    return STATE_WAIT_UNIT


async def choose_unit_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    data = q.data
    if data.startswith("unit:"):
        unit_id = int(data.split(":")[1])
        item = _build_local_stats_item(unit_id)
        context.user_data["chosen_unit"] = {"id": unit_id, "nm": item["nm"]}
        return await show_stats_then_actions(q, context, unit_id, item["nm"], None, via_callback=True)

    if data.startswith("units:more:"):
        offset = int(data.split(":")[2])
        await _render_units_page(context, chat_id=q.message.chat_id, message=q.message, offset=offset)
        return STATE_WAIT_UNIT

    return STATE_WAIT_UNIT


# ---------------------------------------------------------------------------
# Карточка объекта
# ---------------------------------------------------------------------------

async def show_stats_then_actions(
    update_or_q,
    context: ContextTypes.DEFAULT_TYPE,
    unit_id: int,
    unit_name: str,
    client: Any,
    via_callback: bool = False,
    preserve_previous: bool = False,
) -> int:
    await delete_anchor(context)

    item = _build_local_stats_item(unit_id)
    unit_config = _ensure_unit_config_object(unit_id)

    pos = item.get("pos", {})
    last_ts = pos.get("t")

    online = False
    if last_ts:
        online = (int(time.time()) - int(last_ts)) < ONLINE_THRESHOLD_SEC

    status_emoji = "🟢" if online else "🔴"
    last_seen_str = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%H:%M %d.%m") if last_ts else "—"
    speed = pos.get("s", 0)

    lines: List[str] = [
        f"<b>{html.escape(unit_name)}</b>",
        f"{status_emoji} Скорость: {speed} км/ч",
        f"🕒 {last_seen_str}",
    ]

    lat = pos.get("y")
    lon = pos.get("x")
    if lat is not None and lon is not None:
        maps_url = f"https://maps.google.com/?q={lat},{lon}"
        lines.append(f"📍 <a href='{maps_url}'>{lat:.5f}, {lon:.5f}</a>")

    params = item.get("prms", {})
    if params:
        lines.append("\n<b>Параметры (топ‑5):</b>")
        for k, v in list(params.items())[:5]:
            lines.append(f"• {k}: {v}")

    text = "\n".join(lines)

    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Обновить", callback_data=f"stats:refresh:{unit_id}")],
            [InlineKeyboardButton("↩️ К списку", callback_data="back:find")],
        ]
    )

    if via_callback:
        await update_or_q.message.edit_text(
            text,
            reply_markup=markup,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        msg = update_or_q.message
    else:
        msg = await update_or_q.message.reply_text(
            text,
            reply_markup=markup,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    context.user_data["anchor"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}
    return STATE_WAIT_UNIT


async def stats_actions_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    data = q.data
    if "refresh" in data:
        unit_id = int(data.split(":")[-1])
        item = _build_local_stats_item(unit_id)
        return await show_stats_then_actions(q, context, unit_id, item["nm"], None, via_callback=True)
    return STATE_WAIT_UNIT


# ---------------------------------------------------------------------------
# UI Helpers
# ---------------------------------------------------------------------------

async def send_new_anchor_below(update_or_q, text: str, markup, context, parse_mode: Optional[str] = None):
    if isinstance(update_or_q, Update):
        msg = update_or_q.message or update_or_q.callback_query.message
    elif isinstance(update_or_q, CallbackQuery):
        msg = update_or_q.message
    else:
        msg = update_or_q

    sent = await msg.reply_text(text, reply_markup=markup, parse_mode=parse_mode)
    context.user_data["anchor"] = {"chat_id": sent.chat_id, "message_id": sent.message_id}


async def delete_anchor(context: ContextTypes.DEFAULT_TYPE) -> None:
    anchor = context.user_data.pop("anchor", None)
    if anchor:
        try:
            await context.bot.delete_message(anchor["chat_id"], anchor["message_id"])
        except Exception:
            pass


async def _render_units_page(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    offset: int = 0,
    update: Optional[Update] = None,
    message=None,
    reanchor: bool = False,
) -> None:
    units = search_features.filtered_units(context)
    page_size = 10
    page = units[offset : offset + page_size]

    buttons: List[List[InlineKeyboardButton]] = []
    for u in page:
        buttons.append([InlineKeyboardButton(u["nm"], callback_data=f"unit:{u['id']}")])

    nav: List[InlineKeyboardButton] = []
    if offset > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"units:more:{offset-page_size}"))
    if offset + page_size < len(units):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"units:more:{offset+page_size}"))
    if nav:
        buttons.append(nav)

    markup = InlineKeyboardMarkup(buttons)
    text = f"Найдено {len(units)}. Страница {offset//page_size + 1}"

    if message:
        await message.edit_text(text, reply_markup=markup)
    else:
        sent = await context.bot.send_message(chat_id, text, reply_markup=markup)
        context.user_data["anchor"] = {"chat_id": sent.chat_id, "message_id": sent.message_id}


async def back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await cmd_start(update, context)


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN is missing; set it in environment")
        return

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            MessageHandler(filters.Regex(f"^{MENU_BUTTON_FIND}$"), global_restart),
        ],
        states={
            STATE_FIND_QUERY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_objects_text),
                CallbackQueryHandler(back_cb, pattern="action:cancel"),
            ],
            STATE_WAIT_UNIT: [
                CallbackQueryHandler(choose_unit_cb, pattern=r"^(unit:|units:)"),
                CallbackQueryHandler(stats_actions_cb, pattern=r"^stats:"),
                CallbackQueryHandler(back_cb, pattern="back:find"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_objects_text),
            ],
        },
        fallbacks=[CommandHandler("start", cmd_start)],
    )

    app.add_handler(conv)
    app.run_polling()


if __name__ == "__main__":
    main()

