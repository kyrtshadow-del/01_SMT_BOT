from __future__ import annotations

import asyncio
import json
import os
import uuid
import time
import logging
import logging.handlers
from dataclasses import asdict
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set

import redis
from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from pipeline.services.unit_snapshot_v2 import (
    get_unit_snapshot_v2_service,
    build_snapshot_v2_bundle,
)
from pipeline.services.storage_service import get_pipeline_storage_service
from pipeline.config.unit_config_service import load_default_service, UnitConfigService
from pipeline.config.unit_config import UnitConfig
from pipeline.config.defaults import load_from_env
from pipeline.engine.status import compute_status, get_online_threshold  # reuse helper
from pipeline.services.admin_storage import get_admin_storage, Node as AdminNode, UnitMeta as AdminUnitMeta
from pipeline.services.passwords import verify_password
from pipeline.services.passwords import hash_password
from pipeline.services.shadow_service import ShadowService

MAX_CARD_SENSORS = 8
MAX_CARD_PARAMS = 30
MAX_RECENT_EVENTS = 3

BASE_DIR = Path(__file__).resolve().parents[2]
STATIC_DIR = BASE_DIR / "web" / "static"
TEMPLATES_DIR = BASE_DIR / "web" / "templates"
SESSION_DIR = BASE_DIR / "data" / "web_v2_sessions"
SESSION_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
HEALTH_FILES = [
    LOG_DIR / "galileosky_ingest_health.json",
    LOG_DIR / "wialon_ips_ingest_health.json",
]
INGEST_HEALTH_TTL_SEC = int(os.getenv("INGEST_HEALTH_TTL_SEC", "900"))
IGNORE_HEALTH_TS = os.getenv("INGEST_HEALTH_IGNORE_TS", "0") == "1"
# feed: минимальная квантовка возраста статуса, чтобы не спамить дельтами каждый тик
FEED_AGE_BUCKET_SEC = float(os.getenv("FEED_AGE_BUCKET_SEC", "10"))
SNAPSHOT_V2_REFRESH_SEC = int(os.getenv("SNAPSHOT_V2_REFRESH_SEC", "300"))
DEVICE_REGISTRY_DSN = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")
SESSION_TTL = int(os.getenv("WEB_SESSION_TTL_SEC", str(86400 * 7)))
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter()

_monitor_log = logging.getLogger("web.monitoring")
_monitor_log.setLevel(logging.INFO)
_monitor_log.propagate = False
if not _monitor_log.handlers:
    handler = logging.handlers.RotatingFileHandler(
        LOG_DIR / "web_monitoring.log", maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    _monitor_log.addHandler(handler)

_cfg_service: Optional[UnitConfigService] = None
_static_version: Optional[str] = None
_static_mtime: float = 0.0
_feed_status_cache: Dict[int, tuple] = {}  # unit_id -> signature для отсечения неизменившихся статусов
_snapshot_updater_started = False
# cached singletons
_shadow_service: Optional[ShadowService] = None
_db_pool_lock = Lock()
# sessions (Redis)
_redis_sessions = redis.Redis.from_url(REDIS_URL or "redis://127.0.0.1:6379/0", decode_responses=True)
# порядок важен: DEFAULT_LIST_VIEW должен быть определён до DEFAULT_PANEL_SETTINGS
DEFAULT_LIST_VIEW = {
    "secondary": {"mode": "address", "max_length": 100},
    "icons": {
        "order": ["status", "fuel", "ignition", "alerts", "pin"],
        "enabled": {
            "status": True,
            "fuel": True,
            "ignition": True,
            "alerts": True,
            "pin": True,
            "online": False,
            "quick_track": False,
            "messages": False,
            "quick_report": False,
        },
        "max_visible": 4,
    },
    "actions": {"default_track_period": "today", "messages_period": "24h", "open_in_new_tab": False},
}
TRIP_DETECTOR_DEFAULTS: Dict[str, Any] = {
    "mode": "ignition",
    "min_speed_kph": 3.0,
    "min_parking_time_sec": 300,
    "min_trip_time_sec": 60,
    "min_trip_distance_m": 300,
    "max_gap_sec": 300,
    "max_gap_m": 10000,
}
DEFAULT_CARD_SECTIONS = {
    "status": True,
    "location": True,
    "sensors": True,
    "connectivity": True,
    "counters": True,
    "params": True,
    "profile": True,
    "custom_fields": True,
    "drivers": True,
    "trailers": True,
    "passengers": True,
}
DEFAULT_PANEL_SETTINGS = {
    "tabs": [
        {
            "id": "work",
            "name": "Рабочий",
            "filters": {"worklist": True},
        }
    ],
    "active_tab_id": "work",
    "card_sections": dict(DEFAULT_CARD_SECTIONS),
    "list_view": DEFAULT_LIST_VIEW,
}
ALLOWED_ICON_IDS = [
    "status",
    "online",
    "fuel",
    "ignition",
    "alerts",
    "pin",
    "quick_track",
    "messages",
    "quick_report",
]

# ----------------------------- Shadow/Registry models -----------------------------

class UnknownDevice(BaseModel):
    protocol: str
    uid: str
    last_seen_ts: int
    last_ip: Optional[str] = None
    params_seen: List[str] = Field(default_factory=list)
    lat: Optional[float] = None
    lon: Optional[float] = None


class BindExistingRequest(BaseModel):
    unit_id: int
    priority: int = 0


class CreateAndBindRequest(BaseModel):
    name: str
    priority: int = 0


class CreateUnitRequest(BaseModel):
    name: str
    node_id: int = 1


class BindUnitRequest(BaseModel):
    unit_id: int


class IgnoreRequest(BaseModel):
    reason: Optional[str] = None


class BindUnitItem(BaseModel):
    id: int
    name: str
    uid: Optional[str] = None
    reg_number: Optional[str] = None


class TripResponse(BaseModel):
    id: int
    unit_id: int
    start_ts: int
    end_ts: int
    type: str
    distance_m: int
    max_speed: int
    start_address: Optional[str] = None
    end_address: Optional[str] = None
    duration_str: str


class TrackPoint(BaseModel):
    lat: float
    lon: float
    ts: int
    speed: Optional[float] = 0


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _merge_trip_defaults(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Return effective trip detector config with defaults applied."""
    eff = dict(TRIP_DETECTOR_DEFAULTS)
    for k, v in (raw or {}).items():
        if v is None:
            continue
        eff[k] = v
    return eff


def _validate_trip_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate/normalize trip detector config (admin API)."""

    def num(val):
        try:
            if val is None:
                return None
            return float(val)
        except Exception:
            raise HTTPException(status_code=400, detail=f"invalid numeric value: {val}")

    def clamp(val: Optional[float], lo: float, hi: float, name: str) -> Optional[float]:
        if val is None:
            return None
        if val < lo or val > hi:
            raise HTTPException(status_code=400, detail=f"{name} must be between {lo} and {hi}")
        return val

    cleaned: Dict[str, Any] = {}
    mode = payload.get("mode")
    if mode is not None:
        if mode not in ("ignition", "speed"):
            raise HTTPException(status_code=400, detail="mode must be ignition|speed")
        cleaned["mode"] = mode

    min_speed = num(payload.get("min_speed_kph"))
    cleaned["min_speed_kph"] = clamp(min_speed, 0, 30, "min_speed_kph") if min_speed is not None else None

    min_parking = num(payload.get("min_parking_time_sec"))
    cleaned["min_parking_time_sec"] = clamp(min_parking, 0, 3600, "min_parking_time_sec") if min_parking is not None else None

    min_trip_time = num(payload.get("min_trip_time_sec"))
    cleaned["min_trip_time_sec"] = clamp(min_trip_time, 0, 7200, "min_trip_time_sec") if min_trip_time is not None else None

    min_trip_dist = num(payload.get("min_trip_distance_m"))
    cleaned["min_trip_distance_m"] = clamp(min_trip_dist, 0, 200000, "min_trip_distance_m") if min_trip_dist is not None else None

    max_gap_sec = num(payload.get("max_gap_sec"))
    cleaned["max_gap_sec"] = clamp(max_gap_sec, 0, 7200, "max_gap_sec") if max_gap_sec is not None else None

    max_gap_m = num(payload.get("max_gap_m"))
    cleaned["max_gap_m"] = clamp(max_gap_m, 0, 500000, "max_gap_m") if max_gap_m is not None else None

    # drop Nones to avoid cluttering configs
    return {k: v for k, v in cleaned.items() if v is not None}


def _normalize_list_view(raw: Any) -> ListViewSettings:
    base = DEFAULT_LIST_VIEW.copy()
    if isinstance(raw, BaseModel):
        raw = raw.dict()
    if not isinstance(raw, dict):
        raw = {}

    def _merge_dict(default: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        merged = default.copy()
        for k, v in (incoming or {}).items():
            if isinstance(v, dict) and isinstance(merged.get(k), dict):
                merged[k] = _merge_dict(merged[k], v)
            else:
                merged[k] = v
        return merged

    merged = _merge_dict(base, raw)

    # icons
    icons = merged.get("icons") or {}
    order = [i for i in icons.get("order", []) if i in ALLOWED_ICON_IDS]
    if not order:
        order = DEFAULT_LIST_VIEW["icons"]["order"]
    enabled = {k: bool(icons.get("enabled", {}).get(k, DEFAULT_LIST_VIEW["icons"]["enabled"].get(k, False))) for k in ALLOWED_ICON_IDS}
    max_visible = icons.get("max_visible", DEFAULT_LIST_VIEW["icons"]["max_visible"])
    merged["icons"] = {"order": order, "enabled": enabled, "max_visible": max(2, min(int(max_visible or 4), 12))}

    # secondary
    sec = merged.get("secondary") or {}
    mode = sec.get("mode") or DEFAULT_LIST_VIEW["secondary"]["mode"]
    if mode not in ("none", "address", "status", "driver"):
        mode = DEFAULT_LIST_VIEW["secondary"]["mode"]
    max_len = sec.get("max_length", DEFAULT_LIST_VIEW["secondary"]["max_length"])
    merged["secondary"] = {"mode": mode, "max_length": max(20, min(int(max_len or 100), 200))}

    # actions
    actions = merged.get("actions") or {}
    def _period(val: str) -> str:
        return val if val in ("today", "24h", "7d") else "today"
    merged["actions"] = {
        "default_track_period": _period(actions.get("default_track_period", DEFAULT_LIST_VIEW["actions"]["default_track_period"])),
        "messages_period": _period(actions.get("messages_period", DEFAULT_LIST_VIEW["actions"]["messages_period"])),
        "open_in_new_tab": bool(actions.get("open_in_new_tab", False)),
    }

    return ListViewSettings(**merged)


def _get_cfg_svc() -> UnitConfigService:
    global _cfg_service
    if _cfg_service is None:
        cfg = load_from_env()
        storage_root = Path(cfg.storage_root).resolve()
        unit_configs_dir = storage_root / "unit_configs"
        if not unit_configs_dir.exists():
            unit_configs_dir.mkdir(parents=True, exist_ok=True)
        _cfg_service = load_default_service(storage_root)
    return _cfg_service


def _get_snapshot_bundle():
    """Load snapshot v2, lazily rebuilding if empty/missing."""
    svc = get_unit_snapshot_v2_service()
    v2 = svc.load_bundle()
    if v2 and v2.units:
        return v2
    _monitor_log.info("web_v2: snapshot v2 missing/empty -> rebuilding")
    try:
        new_bundle = build_snapshot_v2_bundle()
        if new_bundle.units:
            svc.refresh(new_bundle.units.values(), source_kind=new_bundle.source_kind, dump_ts=new_bundle.dump_ts)
            return new_bundle
        # even if empty, persist to avoid repeated rebuilds per request
        svc.refresh([], source_kind="auto_empty", dump_ts=new_bundle.dump_ts)
        return new_bundle
    except Exception as exc:  # pragma: no cover - defensive
        _monitor_log.error("web_v2: auto-build snapshot failed: %s", exc)
        # return empty bundle instead of 503 to let UI load
        return svc.load_bundle()


def _get_static_version() -> str:
    """Return static cache-buster based on latest mtime of bundled assets.

    The value is recomputed when app.js/app.css mtime changes, so long-lived
    uvicorn workers pick up fresh frontend without full restart.
    """

    global _static_version, _static_mtime
    candidates = [
        STATIC_DIR / "v2" / "app.js",
        STATIC_DIR / "v2" / "app.css",
    ]
    latest = 0.0
    for path in candidates:
        try:
            latest = max(latest, path.stat().st_mtime)
        except OSError:
            continue
    if latest <= 0:
        latest = time.time()
    if _static_version is None or latest > _static_mtime:
        _static_version = str(int(latest))
        _static_mtime = latest
    return _static_version


def _get_shadow_service() -> ShadowService:
    global _shadow_service
    if _shadow_service is None:
        _shadow_service = ShadowService(redis_url=REDIS_URL)
    return _shadow_service


def _get_db_conn():
    import psycopg

    if not DEVICE_REGISTRY_DSN:
        raise HTTPException(status_code=500, detail="Registry DSN is not configured")
    # подключение по требованию; пул не делаем, т.к. операции редкие
    return psycopg.connect(DEVICE_REGISTRY_DSN)


def _ensure_device(cur, protocol: str, uid: str) -> int:
    cur.execute(
        "INSERT INTO devices (protocol, uid) VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING id",
        (protocol, uid),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur.execute("SELECT id FROM devices WHERE protocol=%s AND uid=%s", (protocol, uid))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="failed to upsert device")
    return int(row[0])


def _ensure_unit(cur, name: str) -> int:
    cur.execute("INSERT INTO units (name) VALUES (%s) ON CONFLICT DO NOTHING RETURNING id", (name,))
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur.execute("SELECT id FROM units WHERE name=%s LIMIT 1", (name,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="failed to upsert unit")
    return int(row[0])


def _link(cur, unit_id: int, device_id: int, priority: int) -> None:
    cur.execute(
        """
        INSERT INTO unit_device_links (unit_id, device_id, priority)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (unit_id, device_id, priority),
    )


async def _snapshot_v2_updater_loop() -> None:
    """Periodic background rebuild of snapshot v2."""
    while True:
        await asyncio.sleep(max(60, SNAPSHOT_V2_REFRESH_SEC))
        try:
            _monitor_log.info("web_v2: background snapshot rebuild started")
            loop = asyncio.get_running_loop()
            new_bundle = await loop.run_in_executor(None, build_snapshot_v2_bundle)
            svc = get_unit_snapshot_v2_service()
            svc.refresh(new_bundle.units.values(), source_kind="background_auto", dump_ts=new_bundle.dump_ts)
            _monitor_log.info("web_v2: background snapshot rebuilt units=%s", len(new_bundle.units))
        except Exception as exc:  # pragma: no cover - defensive
            _monitor_log.error("web_v2: background snapshot rebuild failed: %s", exc)


@router.on_event("startup")
async def _start_snapshot_updater() -> None:
    global _snapshot_updater_started
    if _snapshot_updater_started:
        return
    _snapshot_updater_started = True
    asyncio.create_task(_snapshot_v2_updater_loop())


def _sources_health_ok() -> bool:
    """Health is bad only if we have a *fresh* health file explicitly saying not-ok.
    Stale or missing files are treated as unknown → assume OK.
    """
    now = time.time()
    fresh_seen = False
    for path in HEALTH_FILES:
        try:
            mtime = path.stat().st_mtime
            fresh = now - mtime <= INGEST_HEALTH_TTL_SEC
            if not fresh:
                continue
            fresh_seen = True
            ok = True
            try:
                data = json.loads(path.read_text("utf-8"))
                ok = bool(data.get("ok", True))
                ts = data.get("ts")
                if ts and not IGNORE_HEALTH_TS:
                    ok = ok and (now - float(ts) <= INGEST_HEALTH_TTL_SEC)
            except Exception:
                ok = True
            if ok:
                return True
            # fresh but not ok → keep scanning, but mark as bad unless another fresh ok exists
            fresh_seen = True
        except OSError:
            continue
    if not fresh_seen:
        return True
    # we saw fresh files and none were ok → health bad
    return False


def _bucket(val: Optional[float], step: float) -> Optional[int]:
    """Quantize continuous values to reduce noisy feed updates."""
    if val is None:
        return None
    try:
        return int(float(val) // step)
    except Exception:
        return None


def _status_signature(
    status: Dict[str, Any], coords: Optional[tuple], offline_reason: Optional[str], speed: Optional[float]
) -> tuple:
    lat, lon = coords if coords else (None, None)
    return (
        status.get("online"),
        status.get("status"),
        status.get("status_label"),
        status.get("reason"),
        status.get("ignition"),
        status.get("last_ts"),
        _bucket(status.get("age_sec"), FEED_AGE_BUCKET_SEC),
        _bucket(status.get("stop_duration_s"), FEED_AGE_BUCKET_SEC),
        lat,
        lon,
        _bucket(speed, 1.0),
        status.get("has_fuel"),
        offline_reason,
    )


class TooltipData(BaseModel):
    online: bool = False
    status: Optional[str] = None
    status_label: Optional[str] = None
    last_ts: Optional[int] = None
    last_ts_age_sec: Optional[float] = None
    speed: Optional[float] = None
    address: Optional[str] = None
    geofences: List[str] = Field(default_factory=list)
    reason: Optional[str] = None


class CardData(BaseModel):
    status: Dict[str, Any] = Field(default_factory=dict)
    location: Dict[str, Any] = Field(default_factory=dict)
    counters: Dict[str, Any] = Field(default_factory=dict)
    sensors: List[Dict[str, Any]] = Field(default_factory=list)
    connectivity: Dict[str, Any] = Field(default_factory=dict)
    params: List[Dict[str, Any]] = Field(default_factory=list)
    profile: Dict[str, Any] = Field(default_factory=dict)
    custom_fields: List[Dict[str, Any]] = Field(default_factory=list)
    drivers: List[Dict[str, Any]] = Field(default_factory=list)
    trailers: List[Dict[str, Any]] = Field(default_factory=list)
    passengers: List[Dict[str, Any]] = Field(default_factory=list)


class UnitListItem(BaseModel):
    id: int
    name: str
    reg_number: Optional[str] = None
    uid: Optional[str] = None
    hw: Optional[str] = None
    region: Optional[str] = None
    address: Optional[str] = None
    online: bool = False
    status: str = "offline"
    status_label: str = "Нет связи"
    ignition: Optional[bool] = None
    speed: Optional[float] = None
    last_ts: Optional[int] = None
    last_ts_age_sec: Optional[int] = None
    stop_duration_s: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    has_fuel: bool = False
    offline_reason: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)
    sensor_tags: List[str] = Field(default_factory=list)
    tooltip_data: Optional[TooltipData] = None
    icon_kind: Optional[str] = None
    card_preview: Optional[Dict[str, Any]] = None
    reason: Optional[str] = None


class UnitFeedItem(BaseModel):
    id: int
    online: bool = False
    status: str = "offline"
    status_label: str = "Нет связи"
    ignition: Optional[bool] = None
    last_ts: Optional[int] = None
    last_ts_age_sec: Optional[int] = None
    stop_duration_s: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    speed: Optional[float] = None
    has_fuel: bool = False
    offline_reason: Optional[str] = None
    tooltip_data: Optional[TooltipData] = None
    reason: Optional[str] = None


class UnitFeedResponse(BaseModel):
    ts: float
    updates: List[UnitFeedItem] = Field(default_factory=list)
    reset: bool = False
    shadow_count: int = 0


class UnitDetail(BaseModel):
    item: UnitListItem
    snapshot: Optional[Dict[str, Any]] = None
    latest: Optional[Dict[str, Any]] = None
    unit_config: Optional[Dict[str, Any]] = None
    status_details: Optional[Dict[str, Any]] = None
    tooltip_data: Optional[TooltipData] = None
    card_data: Optional[CardData] = None
    filter_options: Optional["CardFilterOptions"] = None
    recent_events: List[Dict[str, Any]] = Field(default_factory=list)

class TripConfigPayload(BaseModel):
    mode: Optional[str] = None
    min_speed_kph: Optional[float] = None
    min_parking_time_sec: Optional[int] = None
    min_trip_time_sec: Optional[int] = None
    min_trip_distance_m: Optional[int] = None
    max_gap_sec: Optional[int] = None
    max_gap_m: Optional[int] = None


class TripConfigResponse(BaseModel):
    unit_id: int
    effective: Dict[str, Any]
    raw: Dict[str, Any]


class CardFilterOptions(BaseModel):
    sensors: List[str] = Field(default_factory=list)
    params: List[str] = Field(default_factory=list)
    statuses: List[str] = Field(default_factory=lambda: ["online", "offline"])


class WorklistPayload(BaseModel):
    unit_ids: List[int]


class ViewConfig(BaseModel):
    id: str
    name: str
    filters: Dict[str, Any] = Field(default_factory=dict)


class ListSecondarySettings(BaseModel):
    mode: str = "address"  # none|address|status|driver
    max_length: int = 100


class ListIconsSettings(BaseModel):
    order: List[str] = Field(default_factory=lambda: ["status", "fuel", "ignition", "alerts", "pin"])
    enabled: Dict[str, bool] = Field(
        default_factory=lambda: {
            "status": True,
            "fuel": True,
            "ignition": True,
            "alerts": True,
            "pin": True,
            "online": False,
            "quick_track": False,
            "messages": False,
            "quick_report": False,
        }
    )
    max_visible: int = 4


class ListActionsSettings(BaseModel):
    default_track_period: str = "today"
    messages_period: str = "24h"
    open_in_new_tab: bool = False


class ListViewSettings(BaseModel):
    secondary: ListSecondarySettings = Field(default_factory=ListSecondarySettings)
    icons: ListIconsSettings = Field(default_factory=ListIconsSettings)
    actions: ListActionsSettings = Field(default_factory=ListActionsSettings)


class PanelSettingsDTO(BaseModel):
    tabs: List[ViewConfig] = Field(default_factory=list)
    active_tab_id: Optional[str] = "work"
    card_sections: Dict[str, bool] = Field(default_factory=lambda: dict(DEFAULT_CARD_SECTIONS))
    list_view: ListViewSettings = Field(default_factory=ListViewSettings)


class ClientLogPayload(BaseModel):
    event: str
    message: Optional[str] = None
    detail: Dict[str, Any] = Field(default_factory=dict)


class LoginPayload(BaseModel):
    login: str
    password: str


class SessionInfo(BaseModel):
    session_id: str
    login: str
    display_name: str
    node_id: Optional[int]
    is_admin: bool


class NodeDTO(BaseModel):
    id: int
    parent_id: Optional[int]
    name: str
    order: int


class UserDTO(BaseModel):
    id: int
    login: str
    display_name: str
    node_id: Optional[int]
    is_admin: bool


class UnitMetaDTO(BaseModel):
    unit_id: int
    owner_node_id: Optional[int]
    is_deleted: bool


class AdminSummary(BaseModel):
    nodes: List[NodeDTO]
    users: List[UserDTO]
    unassigned_units: List[int]


class CreateUserPayload(BaseModel):
    login: str
    password: str
    display_name: Optional[str] = None
    node_id: Optional[int] = None
    is_admin: bool = False


class AssignOwnerPayload(BaseModel):
    unit_ids: List[int]
    node_id: int


class UpsertNodePayload(BaseModel):
    name: str
    parent_id: Optional[int] = None
    order: int = 0


def _default_work_tab() -> ViewConfig:
    return ViewConfig(id="work", name="Рабочий", filters={"worklist": True})


def _ensure_work_tab(tabs: List[ViewConfig]) -> List[ViewConfig]:
    items = list(tabs)
    if not any(tab.id == "work" for tab in items):
        items.insert(0, _default_work_tab())
    return items


def _session_path(session_id: str) -> Path:
    return SESSION_DIR / f"{session_id}.json"


def _session_key(session_id: str) -> str:
    return f"session:{session_id}"


def _load_session(session_id: str) -> Dict[str, Any]:
    # primary: Redis
    try:
        raw = _redis_sessions.get(_session_key(session_id))
        if raw:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
    except Exception:
        pass

    # fallback: legacy file (and migrate to Redis)
    p = _session_path(session_id)
    if not p.exists():
        raise HTTPException(status_code=401, detail="invalid session")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _save_session(session_id, data)
            return data
    except Exception:
        pass
    raise HTTPException(status_code=401, detail="invalid session")


def _save_session(session_id: str, payload: Dict[str, Any]) -> None:
    try:
        _redis_sessions.setex(_session_key(session_id), SESSION_TTL, json.dumps(payload, ensure_ascii=False))
    except Exception:
        # best-effort fallback to file if Redis not available
        p = _session_path(session_id)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp.replace(p)


def get_session_id(request: Request) -> str:
    sid = request.headers.get("X-Session-Id") or request.cookies.get("session_id") or request.query_params.get("session_id")
    if not sid:
        raise HTTPException(status_code=401, detail="session required")
    return sid


@router.post("/api/login")
async def login(payload: LoginPayload) -> Dict[str, Any]:
    """Authenticate user by login/password and create a bound session.

    В сессии сохраняются `user_id`, `user_node_id` и `is_admin`,
    которые затем используются при фильтрации видимости.
    """

    storage = get_admin_storage()
    user = storage.get_user_by_login(payload.login)
    if user is None:
        raise HTTPException(status_code=401, detail="invalid credentials")

    # Достаём password_hash напрямую из БД, чтобы не расширять User dataclass.
    with storage._conn() as conn:  # type: ignore[attr-defined]
        row = conn.execute("SELECT password_hash FROM users WHERE login = %s", (payload.login,)).fetchone()
    if row is None or "password_hash" not in row:
        raise HTTPException(status_code=401, detail="invalid credentials")
    stored_hash = row["password_hash"]
    if not isinstance(stored_hash, str) or not verify_password(payload.password, stored_hash):
        raise HTTPException(status_code=401, detail="invalid credentials")

    session_id = uuid.uuid4().hex
    _save_session(
        session_id,
        {
            "session_id": session_id,
            "created_ts": int(time.time()),
            "worklist": [],
            "user_id": user.id,
            "user_node_id": user.node_id,
            "is_admin": user.is_admin,
            "login": user.login,
            "display_name": user.display_name,
        },
    )
    return {
        "session_id": session_id,
        "login": user.login,
        "display_name": user.display_name,
        "is_admin": user.is_admin,
        "node_id": user.node_id,
    }


@router.post("/api/logout")
async def logout(session_id: str = Depends(get_session_id)) -> Dict[str, Any]:
    try:
        _redis_sessions.delete(_session_key(session_id))
    except Exception:
        pass
    p = _session_path(session_id)
    try:
        p.unlink()
    except FileNotFoundError:
        pass
    return {"status": "ok"}


@router.get("/api/health/status")
async def health_status(session_id: str = Depends(get_session_id)) -> Dict[str, Any]:
    """Lightweight health: DB lag + Redis ping.

    Требует авторизации (cookie/session). Возвращает состояние БД/Redis и лаг
    по данным (now - max(device_ts)).
    """

    status: Dict[str, Any] = {
        "db_ok": False,
        "redis_ok": False,
        "lag_seconds": None,
        "last_event_ts": None,
    }

    # DB check
    try:
        with _get_db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(device_ts))) AS lag, MAX(device_ts) FROM events"
            )
            row = cur.fetchone()
            if row:
                status["lag_seconds"] = float(row[0]) if row[0] is not None else None
                status["last_event_ts"] = row[1]
                status["db_ok"] = True
    except Exception as exc:
        status["db_error"] = str(exc)

    # Redis / Shadow check
    try:
        sh = _get_shadow_service()
        client = getattr(sh, "_client", None)
        if client:
            client.ping()
            status["redis_ok"] = True
            status["shadow_count"] = len(sh.list_keys(limit=50))
    except Exception as exc:
        status["redis_error"] = str(exc)

    return status


@router.get("/api/session", response_model=SessionInfo)
async def session_info(session_id: str = Depends(get_session_id)) -> SessionInfo:
    doc = _load_session(session_id)
    return SessionInfo(
        session_id=session_id,
        login=str(doc.get("login") or ""),
        display_name=str(doc.get("display_name") or doc.get("login") or ""),
        node_id=_safe_int(doc.get("user_node_id")),
        is_admin=bool(doc.get("is_admin")),
    )


# ---------------------- admin API (minimal) ----------------------


@router.get("/api/admin/summary", response_model=AdminSummary)
async def admin_summary(session_id: str = Depends(get_session_id)) -> AdminSummary:
    _require_admin(session_id)
    storage = get_admin_storage()
    nodes = [NodeDTO(id=n.id, parent_id=n.parent_id, name=n.name, order=n.order) for n in storage.list_nodes()]
    users = [
        UserDTO(
            id=u.id,
            login=u.login,
            display_name=u.display_name,
            node_id=u.node_id,
            is_admin=u.is_admin,
        )
        for u in storage.list_users()
    ]
    # unassigned units: those present in snapshot but without owner_node_id
    bundle = _get_snapshot_bundle()
    meta = storage.get_unit_meta_many(list(bundle.units.keys()))
    assigned = {m.unit_id for m in meta if m.owner_node_id is not None}
    unassigned = [uid for uid in bundle.units.keys() if uid not in assigned][:200]
    return AdminSummary(nodes=nodes, users=users, unassigned_units=unassigned)


@router.post("/api/admin/users", response_model=UserDTO)
async def admin_create_user(payload: CreateUserPayload, session_id: str = Depends(get_session_id)) -> UserDTO:
    _require_admin(session_id)
    storage = get_admin_storage()
    pwd_hash = hash_password(payload.password)
    user = storage.create_user(
        login=payload.login,
        password_hash=pwd_hash,
        display_name=payload.display_name or payload.login,
        node_id=payload.node_id,
        is_admin=payload.is_admin,
        can_manage_units=payload.is_admin,
        can_manage_users=payload.is_admin,
    )
    return UserDTO(
        id=user.id,
        login=user.login,
        display_name=user.display_name,
        node_id=user.node_id,
        is_admin=user.is_admin,
    )


@router.post("/api/admin/nodes", response_model=NodeDTO)
async def admin_create_node(payload: UpsertNodePayload, session_id: str = Depends(get_session_id)) -> NodeDTO:
    _require_admin(session_id)
    storage = get_admin_storage()
    node = storage.create_node(name=payload.name, parent_id=payload.parent_id, order=payload.order)
    return NodeDTO(id=node.id, parent_id=node.parent_id, name=node.name, order=node.order)


@router.post("/api/admin/units_meta", response_model=List[UnitMetaDTO])
async def admin_assign_owner(payload: AssignOwnerPayload, session_id: str = Depends(get_session_id)) -> List[UnitMetaDTO]:
    _require_admin(session_id)
    storage = get_admin_storage()
    results: List[UnitMetaDTO] = []
    for uid in payload.unit_ids:
        meta = storage.upsert_unit_meta(unit_id=uid, owner_node_id=payload.node_id)
        results.append(
            UnitMetaDTO(unit_id=meta.unit_id, owner_node_id=meta.owner_node_id, is_deleted=meta.is_deleted)
        )
    return results


@router.get("/api/admin/units/{unit_id}/trip-config", response_model=TripConfigResponse)
async def admin_get_trip_config(unit_id: int, session_id: str = Depends(get_session_id)) -> TripConfigResponse:
    _require_admin(session_id)
    cfg_svc = _get_cfg_svc()
    try:
        cfg = cfg_svc.load(unit_id)
    except FileNotFoundError:
        raw = {}
        effective = _merge_trip_defaults(raw)
        return TripConfigResponse(unit_id=unit_id, effective=effective, raw=raw)
    raw = {}
    try:
        raw = dict(cfg.advanced.get("trip_detector", {}) or {})
    except Exception:
        raw = {}
    effective = _merge_trip_defaults(raw)
    return TripConfigResponse(unit_id=unit_id, effective=effective, raw=raw)


@router.post("/api/admin/units/{unit_id}/trip-config", response_model=TripConfigResponse)
async def admin_set_trip_config(
    unit_id: int, payload: TripConfigPayload, session_id: str = Depends(get_session_id)
) -> TripConfigResponse:
    _require_admin(session_id)
    cfg_svc = _get_cfg_svc()
    try:
        cfg = cfg_svc.load(unit_id)
    except FileNotFoundError:
        cfg = UnitConfig(unit_id=unit_id, source_kind="unknown")

    raw = {}
    try:
        raw = dict(cfg.advanced.get("trip_detector", {}) or {})
    except Exception:
        raw = {}
    updates = _validate_trip_config(payload.dict(exclude_none=True))
    raw.update(updates)
    # очистим пустые значения
    cfg.advanced["trip_detector"] = {k: v for k, v in raw.items() if v is not None}
    cfg_svc.save(cfg)
    effective = _merge_trip_defaults(cfg.advanced.get("trip_detector"))
    return TripConfigResponse(unit_id=unit_id, effective=effective, raw=cfg.advanced.get("trip_detector"))


def _get_access_context(session_id: Optional[str]) -> tuple[Optional[int], bool]:
    """Resolve current user's node_id and admin flag from session.

    Until полноценная авторизация не внедрена, отсутствие user_id трактуем
    как «старый режим без ограничений» (is_admin=True).
    """

    if not session_id:
        return None, True
    try:
        doc = _load_session(session_id)
    except HTTPException:
        # Неизвестная сессия — лучше явно отдать 401 выше, чем молча резать доступ.
        raise
    user_id = doc.get("user_id")
    user_node_id = doc.get("user_node_id")
    is_admin = bool(doc.get("is_admin"))
    if user_id is None:
        # До внедрения login/auth все старые токен‑сессии считаем полноадминскими.
        return None, True
    # user_id уже будет привязан к users в admin_storage на следующем этапе;
    # пока ориентируемся на флаги в сессии.
    node_id: Optional[int] = None
    try:
        if user_node_id is not None:
            node_id = int(user_node_id)
    except (TypeError, ValueError):
        node_id = None
    return node_id, is_admin


def _require_admin(session_id: str) -> None:
    _, is_admin = _get_access_context(session_id)
    if not is_admin:
        raise HTTPException(status_code=403, detail="admin required")


def _build_descendant_node_ids(nodes: List[AdminNode], root_id: int) -> Set[int]:
    """Return all node ids in the subtree of root_id (including root)."""

    children: Dict[Optional[int], List[int]] = {}
    for n in nodes:
        children.setdefault(n.parent_id, []).append(n.id)
    stack = [root_id]
    result: Set[int] = set()
    while stack:
        nid = stack.pop()
        if nid in result:
            continue
        result.add(nid)
        stack.extend(children.get(nid, []))
    return result


def _compose(
    unit_id: int,
    snap: Dict[str, Any],
    latest: Dict[str, Any],
    status: Dict[str, Any],
    *,
    offline_reason: Optional[str] = None,
    last_ts_age_sec: Optional[int] = None,
    sensor_tags: Optional[List[str]] = None,
) -> UnitListItem:
    device = snap.get("device") or {}
    meta = snap.get("meta") if isinstance(snap.get("meta"), dict) else {}
    snap_lat = snap.get("lat") or snap.get("y") or (snap.get("pos") or {}).get("y")
    snap_lon = snap.get("lon") or snap.get("x") or (snap.get("pos") or {}).get("x")
    region = snap.get("region") or meta.get("region") or meta.get("area")
    name = snap.get("nm") or snap.get("name") or f"id {unit_id}"
    hw = snap.get("hw") or device.get("hardware") or device.get("d")
    params_raw = latest.get("params") if isinstance(latest.get("params"), dict) else {}
    preview_params = {k: params_raw[k] for k in list(params_raw.keys())[:8]} if params_raw else {}
    address = params_raw.get("address") or meta.get("address") or meta.get("addr")
    card_data_preview = _build_card_data(snap, latest, None, status).dict()
    card_preview = {
        "status": {
            "online": status.get("online"),
            "status": status.get("status"),
            "status_label": status.get("status_label"),
            "last_ts": status.get("last_ts"),
            "ignition": status.get("ignition"),
            "speed": latest.get("speed"),
        },
        "location": {"lat": snap_lat, "lon": snap_lon, "address": address},
        "connectivity": {"uid": snap.get("uid") or device.get("uid"), "hardware": hw},
        "params": [{"key": k, "value": v} for k, v in preview_params.items()],
        "counters": _build_counters(params_raw),
        "sensors": (_build_sensor_readings(latest, snap) or [])[:MAX_CARD_SENSORS],
        "card_data": card_data_preview,
        "latest": {
            "lat": latest.get("lat") if latest.get("lat") is not None else snap_lat,
            "lon": latest.get("lon") if latest.get("lon") is not None else snap_lon,
            "speed": latest.get("speed"),
            "params": params_raw,
            "last_ts": status.get("last_ts"),
        },
        "snapshot": {"lat": snap_lat, "lon": snap_lon, "meta": meta},
    }
    return UnitListItem(
        id=unit_id,
        name=name,
        reg_number=snap.get("reg_number") or snap.get("plate"),
        uid=snap.get("uid") or device.get("uid"),
        hw=hw,
        region=region,
        address=address,
        lat=latest.get("lat") if latest.get("lat") is not None else snap_lat,
        lon=latest.get("lon") if latest.get("lon") is not None else snap_lon,
        speed=latest.get("speed"),
        online=status.get("online", False),
        status=status.get("status", "offline"),
        status_label=status.get("status_label", "Нет связи"),
        ignition=status.get("ignition"),
        last_ts=status.get("last_ts"),
        last_ts_age_sec=status.get("age_sec", last_ts_age_sec),
        stop_duration_s=status.get("stop_duration_s"),
        has_fuel=status.get("has_fuel", False),
        offline_reason=offline_reason,
        params=preview_params,
        sensor_tags=sensor_tags or [],
        tooltip_data=_build_tooltip_data(status, latest, snap),
        icon_kind=_infer_icon_kind(name=name, hw=hw),
        card_preview=card_preview,
        reason=status.get("reason"),
    )


def _extract_snapshot_sensors(snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    sensors_block = snap.get("sensors") or snap.get("sens") or []
    if isinstance(sensors_block, dict):
        source = sensors_block.values()
    elif isinstance(sensors_block, list):
        source = sensors_block
    else:
        source = []
    sensors: List[Dict[str, Any]] = []
    for entry in source:
        if not isinstance(entry, dict):
            continue
        sensors.append(
            {
                "sensor_id": entry.get("sensor_id") or entry.get("id"),
                "name": entry.get("name") or entry.get("n") or entry.get("nm"),
                "type": entry.get("type") or entry.get("t"),
                "units": entry.get("units") or entry.get("m"),
                "description": entry.get("description") or entry.get("d"),
            }
        )
    return sensors


def _collect_sensor_tags(snap: Dict[str, Any], cfg_dict: Optional[Dict[str, Any]] = None) -> List[str]:
    sensors: List[Dict[str, Any]] = []
    cfg_sensors = (cfg_dict or {}).get("sensors")
    if isinstance(cfg_sensors, list) and cfg_sensors:
        sensors.extend(cfg_sensors)
    snapshot_sensors = _extract_snapshot_sensors(snap)
    if snapshot_sensors:
        sensors.extend(snapshot_sensors)
    tags: Set[str] = set()
    for sensor in sensors:
        name = sensor.get("name") or sensor.get("n")
        if isinstance(name, str) and name:
            tags.add(name)
        s_type = sensor.get("type") or sensor.get("t")
        if isinstance(s_type, str) and s_type:
            tags.add(s_type)
    return sorted(tags)[:24]


def _extract_params(latest: Dict[str, Any]) -> Dict[str, Any]:
    params = latest.get("params") or latest.get("prms")
    if isinstance(params, dict):
        return params
    return {}


def _infer_icon_kind(*, name: Optional[str], hw: Optional[str]) -> str:
    """Heuristic vehicle kind for icon selection.

    Categories: truck, tractor (агротехника/комбайн), car (по умолчанию/переносной терминал).
    """
    base = (name or "").lower()
    hw_l = (hw or "").lower()
    text = f"{base} {hw_l}"

    # Portable / переносной терминал → легковое
    if base.startswith("пт ") or base.startswith("pt ") or hw_l.startswith("adm"):
        return "car"

    # Vans / «буханка», микроавтобусы
    van_markers = [
        "уаз",
        "uaz",
        "буханка",
        "bukhanka",
        "газель",
        "gazelle",
        "sprinter",
        "transit",
        "ducato",
        "doblo",
        "trafic",
        "crafter",
    ]
    if any(token in text for token in van_markers):
        return "van"

    # Buses
    bus_markers = [
        "автобус",
        "bus",
        "паз",
        "маз 10",
        "лиаз",
        "yutong",
        "higer",
        "nevobus",
    ]
    if any(token in text for token in bus_markers):
        return "bus"

    # Trucks: КамАЗ и магистральные тягачи
    truck_markers = [
        "камаз",
        "daf",
        "man ",
        "scania",
        "volvo",
        "iveco",
        "actros",
        "xf ",
        "тягач",
        "faw",
        "howo",
        "шакман",
        "shacman",
    ]
    if any(token in text for token in truck_markers):
        return "truck"

    # Tractors / combines / агро
    agri_markers = [
        "mtz",
        "мтз",
        "belarus",
        "john deere",
        "new holland",
        "fendt",
        "claas",
        "acros",
        "torum",
        "vector",
        "комбайн",
        "terra dos",
        "holmer",
        "challenger",
    ]
    if any(token in text for token in agri_markers):
        # используем отдельный тип для комбайнов / уборочной
        if "acros" in text or "torum" in text or "vector" in text or "комбайн" in text or "terra dos" in text or "holmer" in text:
            return "combine"
        # телескопические/погрузчики JCB и похожие
        if "jcb" in text or "manitou" in text or "погрузчик" in text:
            return "loader"
        return "tractor"

    # Default: легковое
    return "car"


def _build_sensor_readings(latest: Dict[str, Any], snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    params = _extract_params(latest)
    readings: List[Dict[str, Any]] = []
    # Fuel RS-485
    for key, val in params.items():
        if key.startswith("rs485_fls") or key.startswith("rs485_fuel"):
            readings.append({"name": key, "value": val, "units": None, "type": "fuel"})
        if key.startswith("rs485_t"):
            readings.append({"name": key, "value": val, "units": "°C", "type": "temperature"})
    # Power
    if "pwr_ext" in params:
        val = params.get("pwr_ext")
        try:
            val = round(float(val), 2)
        except Exception:
            pass
        readings.append({"name": "Питание, борт", "value": val, "units": "В"})
    if "pwr_int" in params:
        val = params.get("pwr_int")
        try:
            val = round(float(val), 2)
        except Exception:
            pass
        readings.append({"name": "Питание, АКБ", "value": val, "units": "В"})
    # Temperature internal
    if "temp_int" in params:
        readings.append({"name": "Температура", "value": params.get("temp_int"), "units": "°C"})
    # Inputs/outputs bitmasks
    if "inputs_status" in params:
        readings.append({"name": "Входы", "value": params.get("inputs_status"), "units": "mask"})
    if "outputs_status" in params:
        readings.append({"name": "Выходы", "value": params.get("outputs_status"), "units": "mask"})
    # Use snapshot sensors list as metadata if available
    snap_sensors = snap.get("sensors") if isinstance(snap.get("sensors"), list) else []
    if snap_sensors:
        for s in snap_sensors[:8]:
            name = s.get("name") or s.get("n")
            if not name:
                continue
            key = s.get("param") or name
            val = params.get(key)
            readings.append(
                {
                    "name": name,
                    "value": val,
                    "units": s.get("units") or s.get("m"),
                    "type": s.get("type") or s.get("t"),
                }
            )
    # dedupe by name keep first non-null value
    seen = {}
    final = []
    for r in readings:
        nm = r.get("name")
        if nm in seen:
            if seen[nm].get("value") is None and r.get("value") is not None:
                seen[nm] = r
            continue
        seen[nm] = r
    for r in seen.values():
        final.append(r)
    return final[:MAX_CARD_SENSORS]


def _build_counters(params: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "mileage": params.get("mileage") or params.get("mileage_m") or params.get("odometer_m"),
        "engine_hours": params.get("engine_hours") or params.get("moto") or params.get("motohours"),
    }


def _derive_ignition(params: Dict[str, Any]) -> Optional[bool]:
    acc = params.get("acc_trigger")
    if isinstance(acc, str) and acc.isdigit():
        acc = int(acc)
    if isinstance(acc, (int, float)):
        return bool(acc)
    dev_status = params.get("dev_status")
    if isinstance(dev_status, str) and dev_status.isdigit():
        dev_status = int(dev_status)
    if isinstance(dev_status, int):
        return bool(dev_status & 0x1)
    return None


def _build_tooltip_data(status: Dict[str, Any], latest: Dict[str, Any], snap: Dict[str, Any]) -> TooltipData:
    meta = snap.get("meta") if isinstance(snap.get("meta"), dict) else {}
    address = latest.get("address") or snap.get("address") or meta.get("address") or meta.get("addr")
    geofences = latest.get("geofences")
    if not isinstance(geofences, list):
        geofences = []
    return TooltipData(
        online=bool(status.get("online")),
        status=status.get("status"),
        status_label=status.get("status_label"),
        last_ts=status.get("last_ts"),
        last_ts_age_sec=status.get("age_sec"),
        speed=latest.get("speed"),
        address=address,
        geofences=geofences,
        reason=status.get("reason"),
    )


def _build_card_data(snap: Dict[str, Any], latest: Dict[str, Any], cfg_dict: Optional[Dict[str, Any]], status: Dict[str, Any]) -> CardData:
    params = _extract_params(latest)
    meta = snap.get("meta") if isinstance(snap.get("meta"), dict) else {}
    address = latest.get("address") or snap.get("address") or meta.get("address") or meta.get("addr")
    geofences = latest.get("geofences") if isinstance(latest.get("geofences"), list) else []
    device = snap.get("device") or {}
    sensors = _build_sensor_readings(latest, snap)
    profile_block = {}
    for field in (cfg_dict or {}).get("profile") or []:
        name = field.get("name")
        if name:
            profile_block[name] = field.get("value")
    custom_fields = []
    extra = (cfg_dict or {}).get("extra")
    if isinstance(extra, dict):
        custom_fields = [{"key": str(k), "value": v} for k, v in list(extra.items())[:10]]
    # Показываем больше параметров, без скрытой отсечки.
    params_items = sorted(params.items())
    params_list = [{"key": k, "value": v} for k, v in params_items[:MAX_CARD_PARAMS]]
    counters = _build_counters(params)
    ignition = _derive_ignition(params)
    connectivity = {
        "uid": snap.get("uid") or device.get("uid") or params.get("imei"),
        "hardware": snap.get("hw") or device.get("hardware"),
        "firmware": params.get("firmware") or params.get("soft") or params.get("soft_version"),
        "phones": device.get("phones") if isinstance(device.get("phones"), list) else [],
    }
    moving = False
    try:
        moving = float(latest.get("speed") or status.get("speed") or 0) > 1.0
    except Exception:
        moving = status.get("status") == "moving"
    return CardData(
        status={
            "online": status.get("online"),
            "status": status.get("status"),
            "status_label": status.get("status_label"),
            "last_ts": status.get("last_ts"),
            "speed": latest.get("speed"),
            "ignition": ignition if ignition is not None else status.get("ignition"),
            "moving": moving,
        },
        location={
            "address": address,
            "lat": latest.get("lat") or snap.get("lat") or snap.get("y"),
            "lon": latest.get("lon") or snap.get("lon") or snap.get("x"),
            "geofences": geofences,
        },
        counters=counters,
        sensors=sensors,
        connectivity=connectivity,
        params=params_list,
        profile=profile_block,
        custom_fields=custom_fields,
        drivers=(cfg_dict or {}).get("drivers") or [],
        trailers=(cfg_dict or {}).get("trailers") or [],
        passengers=(cfg_dict or {}).get("passengers") or [],
    )


def _recent_events(raw_storage_service, unit_id: int, *, limit: int = 3) -> List[Dict[str, Any]]:
    """Return last N events for unit across available days (fast path)."""
    events: List[Dict[str, Any]] = []
    limit = min(limit, MAX_RECENT_EVENTS)
    days = sorted((p.name for p in raw_storage_service.storage_root.iterdir() if p.is_dir() and p.name[:4].isdigit()), reverse=True)
    for day in days:
        if len(events) >= limit:
            break
        evs = raw_storage_service.raw_storage.fetch(day, unit_id=unit_id)
        if not evs:
            continue
        for ev in reversed(evs):
            events.append(
                {
                    "device_ts": ev.device_ts,
                    "lat": ev.latitude,
                    "lon": ev.longitude,
                    "speed": ev.speed,
                    "course": ev.course,
                    "params": ev.params or {},
                }
            )
            if len(events) >= limit:
                break
    return events[:limit]


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    vs = sorted(values)
    n = len(vs)
    mid = n // 2
    if n % 2 == 1:
        return vs[mid]
    return (vs[mid - 1] + vs[mid]) / 2


def _adaptive_ignition(storage_service, unit_id: int, current_pwr: Optional[float]) -> Optional[bool]:
    """Estimate ignition threshold per unit from recent events (похож на Wialon авто-порог).

    - Собираем pwr_ext из последних ~200 событий за 2 дня.
    - Делим на кластеры по скорости: speed>3 -> "движение", speed<=0.5 -> "стоянка".
    - Если есть оба кластера и med_on > med_off + delta (env IGNITION_VOLTAGE_DELTA, default 1.0В),
      ставим пороги on/off = med_on, med_off и threshold=(med_on+med_off)/2.
    - Возвращаем bool(current_pwr > threshold) или None, если данных нет.
    """
    if current_pwr is None:
        return None
    try:
        delta_min = float(os.getenv("IGNITION_VOLTAGE_DELTA", "1.0"))
    except Exception:
        delta_min = 1.0
    days = sorted((p.name for p in storage_service.storage_root.iterdir() if p.is_dir() and p.name[:4].isdigit()), reverse=True)
    on_vals: List[float] = []
    off_vals: List[float] = []
    max_events = 200
    for day in days[:2]:  # смотрим максимум за 2 дня
        evs = storage_service.raw_storage.fetch(day, unit_id=unit_id)
        for ev in reversed(evs):
            if ev.params is None:
                continue
            pwr = ev.params.get("pwr_ext")
            try:
                pwr = float(pwr)
            except Exception:
                continue
            spd = ev.speed or 0.0
            if spd > 3:
                on_vals.append(pwr)
            elif spd <= 0.5:
                off_vals.append(pwr)
            if len(on_vals) + len(off_vals) >= max_events:
                break
        if len(on_vals) + len(off_vals) >= max_events:
            break
    med_on = _median(on_vals)
    med_off = _median(off_vals)
    if med_on is None or med_off is None:
        return None
    if med_on <= med_off + delta_min:  # неубедительно отличает «вкл» от «выкл»
        return None
    threshold = (med_on + med_off) / 2
    _cache_threshold(unit_id, med_on, med_off, threshold)
    return current_pwr > threshold


IGN_CACHE_PATH = Path("data/ignition_cache.json")
_IGN_CACHE: Optional[Dict[str, Any]] = None
_IGN_CACHE_LOCK = Lock()


def _load_ign_cache() -> Dict[str, Any]:
    global _IGN_CACHE
    if _IGN_CACHE is not None:
        return _IGN_CACHE
    if IGN_CACHE_PATH.exists():
        try:
            _IGN_CACHE = json.loads(IGN_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            _IGN_CACHE = {}
    else:
        _IGN_CACHE = {}
    return _IGN_CACHE


def _save_ign_cache() -> None:
    if _IGN_CACHE is None:
        return
    try:
        tmp = IGN_CACHE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(_IGN_CACHE, ensure_ascii=False), encoding="utf-8")
        tmp.replace(IGN_CACHE_PATH)
    except Exception:
        pass


def _cache_threshold(unit_id: int, med_on: float, med_off: float, threshold: float) -> None:
    cache = _load_ign_cache()
    cache[str(unit_id)] = {
        "med_on": med_on,
        "med_off": med_off,
        "threshold": threshold,
        "ts": int(time.time()),
    }
    _save_ign_cache()


def _get_cached_threshold(unit_id: int, *, max_age_sec: int = 7 * 86400) -> Optional[Dict[str, float]]:
    cache = _load_ign_cache()
    entry = cache.get(str(unit_id))
    if not isinstance(entry, dict):
        return None
    ts = entry.get("ts")
    try:
        if ts and int(time.time()) - int(ts) > max_age_sec:
            return None
    except Exception:
        return None
    return entry


def _apply_cached_ignition(unit_id: int, latest: Dict[str, Any], status: Dict[str, Any]) -> None:
    cache = _get_cached_threshold(unit_id)
    if not cache:
        return
    try:
        pwr = float(_extract_params(latest).get("pwr_ext"))
    except Exception:
        pwr = None
    if pwr is None:
        return
    th_on = cache.get("med_on")
    th_off = cache.get("med_off")
    th_mid = cache.get("threshold")
    ign = status.get("ignition")
    try:
        if th_on is not None and pwr >= th_on:
            ign = True
        elif th_off is not None and pwr <= th_off:
            ign = False
        elif th_mid is not None:
            ign = pwr > th_mid
    except Exception:
        pass
    if ign is None:
        return
    status["ignition"] = ign
    if not status.get("online"):
        return
    if status.get("status") == "stop":
        status["status"] = "park_ign_on" if ign else "park_ign_off"
        status["status_label"] = "Остановка, зажиг. вкл" if ign else "Остановка, зажиг. выкл"
    elif status.get("status") == "stopped":
        # Для длительной стоянки сохраняем статус (нужен для фильтра), но уточняем подпись.
        status["status_label"] = "Стоянка, зажиг. вкл" if ign else "Стоянка, зажиг. выкл"


def _has_manual_threshold(cfg_dict: Optional[Dict[str, Any]]) -> bool:
    adv = cfg_dict.get("advanced") if isinstance(cfg_dict, dict) else None
    if not isinstance(adv, dict):
        return False
    return "ignition_threshold_v_on" in adv and "ignition_threshold_v_off" in adv


@router.get("/monitoring/", response_class=HTMLResponse)
async def monitoring_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request, "static_version": _get_static_version()})


# ----------------------- Shadow / Registry UI API -----------------------


@router.get("/api/unknown_devices", response_model=List[UnknownDevice])
async def list_unknown_devices(session_id: str = Depends(get_session_id)) -> List[UnknownDevice]:
    _require_admin(session_id)
    sh = _get_shadow_service()
    # Self-healing: убираем из Shadow устройства, которые уже заведены в Registry/snapshot.
    bundle = _get_snapshot_bundle()
    known_pairs: set[tuple[str, str]] = set()
    for rec in bundle.units.values():
        try:
            device = rec.device if hasattr(rec, "device") else rec.get("device", {})
        except Exception:
            device = {}
        uid = device.get("uid")
        if uid:
            known_pairs.add(("wialon_ips", str(uid)))
            known_pairs.add(("galileosky", str(uid)))
            # без протокола — для безопасности
            known_pairs.add(("", str(uid)))
    devices: List[UnknownDevice] = []
    keys = sh.list_keys("shadow:device:*", limit=200)
    for key in keys:
        rec = sh.get(key) or {}
        try:
            _, _, protocol, uid = key.split(":", 3)
        except ValueError:
            continue
        proto_uid = (protocol.lower(), uid)
        proto_none = ("", uid)
        if proto_uid in known_pairs or proto_none in known_pairs:
            # фантом: устройство уже заведено, удаляем ключ
            sh.delete(key)
            continue
        params_seen = []
        try:
            params_seen = json.loads(rec.get("params_seen") or "[]")
        except Exception:
            params_seen = []
        last_sample = rec.get("last_sample")
        lat = lon = None
        if last_sample:
            try:
                sample = json.loads(last_sample)
                lat = sample.get("lat") or sample.get("latitude")
                lon = sample.get("lon") or sample.get("longitude")
            except Exception:
                pass
        devices.append(
            UnknownDevice(
                protocol=protocol,
                uid=uid,
                last_seen_ts=int(rec.get("last_seen_ts") or 0),
                last_ip=rec.get("last_ip") or None,
                params_seen=params_seen,
                lat=lat,
                lon=lon,
            )
        )
    devices.sort(key=lambda x: x.last_seen_ts, reverse=True)
    return devices


@router.post("/api/unknown_devices/{protocol}/{uid}/create")
async def create_and_bind(
    protocol: str,
    uid: str,
    body: CreateAndBindRequest,
    session_id: str = Depends(get_session_id),
) -> Dict[str, Any]:
    _require_admin(session_id)
    sh = _get_shadow_service()
    with _get_db_conn() as conn:
        cur = conn.cursor()
        device_id = _ensure_device(cur, protocol, uid)
        unit_id = _ensure_unit(cur, body.name)
        _link(cur, unit_id, device_id, body.priority)
        conn.commit()
    sh.delete(f"shadow:device:{protocol}:{uid}")
    # trigger snapshot v2 rebuild so юнит появился в основном списке
    try:
        bundle = build_snapshot_v2_bundle()
        svc = get_unit_snapshot_v2_service()
        svc.refresh(bundle.units.values(), source_kind=bundle.source_kind, dump_ts=bundle.dump_ts)
    except Exception as exc:
        _monitor_log.warning("shadow create: snapshot rebuild failed: %s", exc)
    return {"status": "ok", "unit_id": unit_id, "device_id": device_id}


@router.post("/api/unknown_devices/{protocol}/{uid}/bind")
async def bind_unknown_device(
    protocol: str,
    uid: str,
    body: BindExistingRequest,
    session_id: str = Depends(get_session_id),
) -> Dict[str, str]:
    _require_admin(session_id)
    sh = _get_shadow_service()
    with _get_db_conn() as conn:
        cur = conn.cursor()
        device_id = _ensure_device(cur, protocol, uid)
        _link(cur, body.unit_id, device_id, body.priority)
        conn.commit()
    sh.delete(f"shadow:device:{protocol}:{uid}")
    try:
        bundle = build_snapshot_v2_bundle()
        svc = get_unit_snapshot_v2_service()
        svc.refresh(bundle.units.values(), source_kind=bundle.source_kind, dump_ts=bundle.dump_ts)
    except Exception as exc:
        _monitor_log.warning("shadow bind: snapshot rebuild failed: %s", exc)
    return {"status": "ok"}


@router.post("/api/unknown_devices/{protocol}/{uid}/ignore")
async def ignore_unknown_device(
    protocol: str,
    uid: str,
    body: IgnoreRequest,
    session_id: str = Depends(get_session_id),
) -> Dict[str, str]:
    _require_admin(session_id)
    sh = _get_shadow_service()
    sh.delete(f"shadow:device:{protocol}:{uid}")
    return {"status": "ok"}


# ----------------------- Trips (history) -----------------------


@router.get("/api/history/trips", response_model=List[TripResponse])
async def get_trips(
    unit_id: int,
    from_ts: int,
    to_ts: int,
    session_id: str = Depends(get_session_id),
) -> List[TripResponse]:
    # TODO: добавить проверку видимости по node_id
    _get_access_context(session_id)

    query = """
        SELECT
            COALESCE(id, EXTRACT(EPOCH FROM start_ts)::bigint) AS id,
            unit_id,
            EXTRACT(EPOCH FROM start_ts)::bigint AS start_ts,
            EXTRACT(EPOCH FROM end_ts)::bigint AS end_ts,
            type,
            distance_m,
            max_speed,
            start_address,
            end_address
        FROM trips
        WHERE unit_id = %s
          AND start_ts >= to_timestamp(%s)
          AND start_ts <= to_timestamp(%s)
        ORDER BY start_ts DESC
        LIMIT 500
    """

    results: List[TripResponse] = []
    try:
        with _get_db_conn() as conn:
            cur = conn.cursor()
            cur.execute(query, (unit_id, from_ts, to_ts))
            for row in cur.fetchall():
                _id, uid, s_ts, e_ts, typ, dist, max_spd, s_addr, e_addr = row
                dur = max(0, int(e_ts - s_ts))
                h, rem = divmod(dur, 3600)
                m, _ = divmod(rem, 60)
                dur_str = f"{h}ч {m}м" if h else f"{m} мин"
                results.append(
                    TripResponse(
                        id=int(_id),
                        unit_id=int(uid),
                        start_ts=int(s_ts),
                        end_ts=int(e_ts),
                        type=str(typ),
                        distance_m=int(dist or 0),
                        max_speed=int(max_spd or 0),
                        start_address=s_addr,
                        end_address=e_addr,
                        duration_str=dur_str,
                    )
                )
    except Exception as exc:
        _monitor_log.error("trips API failed: %s", exc)
        raise HTTPException(status_code=500, detail="database error")

    return results


@router.get("/api/history/track", response_model=List[TrackPoint])
async def get_track(
    unit_id: int,
    from_ts: int,
    to_ts: int,
    session_id: str = Depends(get_session_id),
) -> List[TrackPoint]:
    # TODO: visibility check
    _get_access_context(session_id)

    query = """
        SELECT lat, lon, EXTRACT(EPOCH FROM device_ts)::bigint AS ts, speed
        FROM events
        WHERE unit_id = %s
          AND device_ts >= to_timestamp(%s)
          AND device_ts <= to_timestamp(%s)
          AND lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY device_ts ASC
        LIMIT 5000
    """
    points: List[TrackPoint] = []
    try:
        with _get_db_conn() as conn:
            cur = conn.cursor()
            cur.execute(query, (unit_id, from_ts, to_ts))
            for lat, lon, ts, speed in cur.fetchall():
                points.append(
                    TrackPoint(
                        lat=float(lat),
                        lon=float(lon),
                        ts=int(ts),
                        speed=float(speed) if speed is not None else 0,
                    )
                )
    except Exception as exc:
        _monitor_log.error("track API failed: %s", exc)
        raise HTTPException(status_code=500, detail="database error")
    return points


# ----------------------- Shadow direct (new create/bind) -----------------------


@router.post("/api/shadow/{protocol}/{uid}/create")
async def shadow_create_unit(
    protocol: str,
    uid: str,
    payload: CreateUnitRequest,
    session_id: str = Depends(get_session_id),
) -> Dict[str, Any]:
    _require_admin(session_id)
    sh = _get_shadow_service()
    with _get_db_conn() as conn:
        cur = conn.cursor()
        # create unit
        cur.execute("INSERT INTO units (name) VALUES (%s) RETURNING id", (payload.name,))
        unit_id = cur.fetchone()[0]
        # ensure device
        device_id = _ensure_device(cur, protocol, uid)
        # link (priority 0 default)
        _link(cur, unit_id, device_id, 0)
        # metadata
        cur.execute(
            """
            INSERT INTO units_meta (unit_id, owner_node_id)
            VALUES (%s, %s)
            ON CONFLICT (unit_id) DO UPDATE SET owner_node_id = EXCLUDED.owner_node_id
            """,
            (unit_id, payload.node_id),
        )
        conn.commit()
    sh.delete(f"shadow:device:{protocol}:{uid}")
    try:
        bundle = build_snapshot_v2_bundle()
        svc = get_unit_snapshot_v2_service()
        svc.refresh(bundle.units.values(), source_kind=bundle.source_kind, dump_ts=bundle.dump_ts)
    except Exception as exc:  # pragma: no cover - defensive
        _monitor_log.warning("shadow create (new api): snapshot rebuild failed: %s", exc)
    return {"status": "ok", "unit_id": unit_id}


@router.post("/api/shadow/{protocol}/{uid}/bind")
async def shadow_bind_existing(
    protocol: str,
    uid: str,
    payload: BindUnitRequest,
    session_id: str = Depends(get_session_id),
) -> Dict[str, Any]:
    _require_admin(session_id)
    sh = _get_shadow_service()
    with _get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM units WHERE id=%s", (payload.unit_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="unit not found")
        device_id = _ensure_device(cur, protocol, uid)
        # close previous links for this device (if any)
        cur.execute(
            "UPDATE unit_device_links SET valid_to = NOW() WHERE device_id = %s AND valid_to IS NULL",
            (device_id,),
        )
        _link(cur, payload.unit_id, device_id, 0)
        conn.commit()
    sh.delete(f"shadow:device:{protocol}:{uid}")
    try:
        bundle = build_snapshot_v2_bundle()
        svc = get_unit_snapshot_v2_service()
        svc.refresh(bundle.units.values(), source_kind=bundle.source_kind, dump_ts=bundle.dump_ts)
    except Exception as exc:  # pragma: no cover - defensive
        _monitor_log.warning("shadow bind (new api): snapshot rebuild failed: %s", exc)
    return {"status": "ok"}


@router.get("/api/admin/units", response_model=List[BindUnitItem])
async def search_units_admin(
    q: Optional[str] = None,
    limit: int = 50,
    session_id: str = Depends(get_session_id),
) -> List[BindUnitItem]:
    """Поиск юнитов для привязки UnknownDevice (Inbox). Основан на snapshot v2, чтобы не ходить в БД за каждым поиском."""
    bundle = _get_snapshot_bundle()
    if not q:
        return []
    ql = q.lower()
    items: List[BindUnitItem] = []
    for uid, rec in bundle.units.items():
        hay = [
            rec.name or "",
            rec.reg_number or "",
            rec.device.get("uid") or "",
            str(rec.unit_id),
        ]
        hay_lc = [str(h).lower() for h in hay if h is not None]
        if any(ql in h for h in hay_lc):
            items.append(
                BindUnitItem(
                    id=int(rec.unit_id),
                    name=rec.name or f"unit {rec.unit_id}",
                    uid=str(rec.device.get("uid")) if rec.device else None,
                    reg_number=rec.reg_number,
                )
            )
        if len(items) >= limit:
            break
    return items


@router.get("/api/units", response_model=List[UnitListItem])
async def list_units(
    q: Optional[str] = None,
    online: Optional[bool] = None,
    has_fuel: Optional[bool] = None,
    limit: Optional[int] = None,
    cursor: Optional[int] = None,
    session_id: str = Depends(get_session_id),
) -> List[UnitListItem]:
    bundle = _get_snapshot_bundle()
    storage = get_pipeline_storage_service()
    items: List[UnitListItem] = []
    q_lc = q.lower() if q else None
    totals = {
        "units_total": len(bundle.units),
        "online_total": 0,
        "offline_total": 0,
        "missing_latest": 0,
        "stale_over_threshold": 0,
    }
    filtered = {"units_matched": 0, "online": 0, "offline": 0}
    sample_offline: List[Dict[str, Any]] = []
    now = int(time.time())
    threshold = get_online_threshold()
    health_ok = _sources_health_ok()
    # Hierarchy / visibility: resolve current user's node and allowed units.
    node_id, is_admin = _get_access_context(session_id)
    admin_storage = get_admin_storage()
    unit_ids = list(bundle.units.keys())
    unit_meta_list = admin_storage.get_unit_meta_many(unit_ids)
    meta_by_unit: Dict[int, AdminUnitMeta] = {m.unit_id: m for m in unit_meta_list}
    nodes = admin_storage.list_nodes()
    allowed_nodes: Optional[Set[int]] = None
    if not is_admin and node_id is not None and nodes:
        allowed_nodes = _build_descendant_node_ids(nodes, node_id)

    def _unwrap(rec: Any) -> Dict[str, Any]:
        if hasattr(rec, "to_dict"):
            return rec.to_dict()  # UnitSnapshotV2Record or legacy record
        return dict(rec)

    for uid, rec in bundle.units.items():
        snap = _unwrap(rec)
        meta = meta_by_unit.get(uid)
        owner_node_id = meta.owner_node_id if meta else snap.get("owner_node_id")
        is_deleted = bool(meta.is_deleted) if meta else bool(snap.get("is_deleted"))
        # Visibility: hide deleted units for non-admins, and units outside subtree.
        if not is_admin and is_deleted:
            continue
        if allowed_nodes is not None:
            if owner_node_id is None or owner_node_id not in allowed_nodes:
                continue
        raw_latest = storage.get_latest_metrics(uid)
        latest = raw_latest or snap.get("latest") or {}
        status = compute_status(snap, latest, unit_config=None, unit_id=uid, now=now, health_ok=health_ok)
        if status.get("ignition") is None and not _has_manual_threshold(None):
            _apply_cached_ignition(uid, latest, status)
        age_sec = _age_seconds(status.get("last_ts"), now=now)
        has_latest = raw_latest is not None
        offline_reason = None
        if status.get("online"):
            totals["online_total"] += 1
        else:
            totals["offline_total"] += 1
            if not has_latest:
                totals["missing_latest"] += 1
            elif age_sec is not None and age_sec > threshold:
                totals["stale_over_threshold"] += 1
            offline_reason = _format_offline_reason(has_latest, age_sec, threshold)
            if len(sample_offline) < 3:
                sample_offline.append({"id": uid, "age_sec": age_sec, "reason": offline_reason})
        unit_name = snap.get("nm") or snap.get("name") or ""
        unit_uid = snap.get("uid") or (snap.get("device") or {}).get("uid") or ""
        if q_lc and q_lc not in unit_name.lower() and q_lc not in unit_uid.lower():
            continue
        unit_online = bool(status.get("online"))
        if online is not None and unit_online != online:
            continue
        unit_has_fuel = bool(status.get("has_fuel"))
        if has_fuel is not None and unit_has_fuel != has_fuel:
            continue
        filtered["units_matched"] += 1
        if status.get("online"):
            filtered["online"] += 1
        else:
            filtered["offline"] += 1
        tags = _collect_sensor_tags(snap, None)
        item = _compose(
            uid,
            snap,
            latest,
            status,
            offline_reason=offline_reason,
            last_ts_age_sec=age_sec,
            sensor_tags=tags,
        )
        items.append(item)
    items.sort(key=lambda u: (not u.online, u.name.lower()))
    _log_units_snapshot(
        {
            "event": "list_units",
            "filters": {"q": q, "online": online, "has_fuel": has_fuel},
            "totals": totals,
            "filtered": filtered,
            "sample_offline": sample_offline,
            "threshold_sec": threshold,
        }
    )
    return items


@router.get("/api/units/feed", response_model=UnitFeedResponse)
async def units_feed(
    since: Optional[float] = None,
    watch_ids: Optional[str] = None,
    session_id: str = Depends(get_session_id),
) -> UnitFeedResponse:
    storage = get_pipeline_storage_service()
    bundle = _get_snapshot_bundle()
    now_ts = time.time()
    watch_set: Optional[Set[int]] = None
    if watch_ids:
        try:
            watch_set = {int(x) for x in watch_ids.split(",") if x.strip().isdigit()}
        except Exception:
            watch_set = None
    node_id, is_admin = _get_access_context(session_id)
    admin_storage = get_admin_storage()
    unit_ids = list(bundle.units.keys())
    unit_meta_list = admin_storage.get_unit_meta_many(unit_ids)
    meta_by_unit: Dict[int, AdminUnitMeta] = {m.unit_id: m for m in unit_meta_list}
    nodes = admin_storage.list_nodes()
    allowed_nodes: Optional[Set[int]] = None
    if not is_admin and node_id is not None and nodes:
        allowed_nodes = _build_descendant_node_ids(nodes, node_id)
    now = int(time.time())
    threshold = get_online_threshold()
    health_ok = _sources_health_ok()
    updates: List[UnitFeedItem] = []
    for uid, rec in bundle.units.items():
        snap = rec.to_dict() if hasattr(rec, "to_dict") else dict(rec)
        meta = meta_by_unit.get(uid)
        owner_node_id = meta.owner_node_id if meta else None
        is_deleted = bool(meta.is_deleted) if meta else False
        if not is_admin and is_deleted:
            continue
        if allowed_nodes is not None:
            if owner_node_id is None or owner_node_id not in allowed_nodes:
                continue
        if watch_set is not None and uid not in watch_set:
            continue
        raw_latest = storage.get_latest_metrics(uid)
        latest = raw_latest or {}
        status = compute_status(snap, latest, unit_config=None, unit_id=uid, now=now, health_ok=health_ok)
        if status.get("ignition") is None and not _has_manual_threshold(None):
            _apply_cached_ignition(uid, latest, status)
        age_sec = _age_seconds(status.get("last_ts"), now=now)
        coords = _resolve_coords(latest, snap)
        offline_reason = None
        if not status.get("online"):
            offline_reason = _format_offline_reason(raw_latest is not None, age_sec, threshold)
        signature = _status_signature(status, coords, offline_reason, latest.get("speed"))
        prev = _feed_status_cache.get(uid)
        if prev == signature:
            continue
        _feed_status_cache[uid] = signature
        tooltip = _build_tooltip_data(status, latest, snap)
        updates.append(
            UnitFeedItem(
                id=uid,
                online=status.get("online", False),
                status=status.get("status", "offline"),
                status_label=status.get("status_label", "Нет связи"),
                ignition=status.get("ignition"),
                last_ts=status.get("last_ts"),
                last_ts_age_sec=age_sec,
                stop_duration_s=status.get("stop_duration_s"),
                lat=coords[0],
                lon=coords[1],
                speed=latest.get("speed"),
                has_fuel=status.get("has_fuel", False),
                offline_reason=offline_reason,
                tooltip_data=tooltip,
                reason=status.get("reason"),
            )
        )
    shadow_count = _get_shadow_service().get_count()
    return UnitFeedResponse(ts=now_ts, updates=updates, reset=False, shadow_count=shadow_count)


@router.get("/api/units/{unit_id}", response_model=UnitDetail)
async def unit_detail(unit_id: int, session_id: str = Depends(get_session_id)) -> UnitDetail:
    bundle = _get_snapshot_bundle()
    rec = bundle.units.get(unit_id)
    if not rec:
        raise HTTPException(status_code=404, detail="unit not found")
    node_id, is_admin = _get_access_context(session_id)
    admin_storage = get_admin_storage()
    meta_list = admin_storage.get_unit_meta_many([unit_id])
    meta = meta_list[0] if meta_list else None
    owner_node_id = meta.owner_node_id if meta else None
    is_deleted = bool(meta.is_deleted) if meta else False
    nodes = admin_storage.list_nodes()
    allowed_nodes: Optional[Set[int]] = None
    if not is_admin and node_id is not None and nodes:
        allowed_nodes = _build_descendant_node_ids(nodes, node_id)
    if not is_admin:
        if is_deleted:
            raise HTTPException(status_code=404, detail="unit not found")
        if allowed_nodes is not None and (owner_node_id is None or owner_node_id not in allowed_nodes):
            raise HTTPException(status_code=404, detail="unit not found")
    snap = rec.to_dict() if hasattr(rec, "to_dict") else dict(rec)
    storage = get_pipeline_storage_service()
    raw_latest = storage.get_latest_metrics(unit_id)
    latest = raw_latest or snap.get("latest") or {}
    cfg_dict = None
    try:
        cfg = _get_cfg_svc().load(unit_id)
        cfg_dict = asdict(cfg)
    except Exception:
        cfg_dict = None
    threshold = get_online_threshold()
    health_ok = _sources_health_ok()
    status = compute_status(snap, latest, unit_config=cfg_dict, unit_id=unit_id, health_ok=health_ok)
    age_sec = _age_seconds(status.get("last_ts"))
    offline_reason = None
    if not status.get("online"):
        offline_reason = _format_offline_reason(raw_latest is not None, age_sec, threshold)
    sensor_tags = _collect_sensor_tags(snap, cfg_dict)
    item = _compose(
        unit_id,
        snap,
        latest,
        status,
        offline_reason=offline_reason,
        last_ts_age_sec=age_sec,
        sensor_tags=sensor_tags,
    )
    tooltip = _build_tooltip_data(status, latest, snap)
    # Adaptive ignition threshold if not determined and manual thresholds absent
    if status.get("ignition") is None and not _has_manual_threshold(cfg_dict):
        pwr = None
        try:
            pwr = float(_extract_params(latest).get("pwr_ext"))
        except Exception:
            pwr = None
        adaptive_ign = _adaptive_ignition(storage, unit_id, pwr)
        if adaptive_ign is not None:
            status["ignition"] = adaptive_ign
            if status.get("status") == "stop":
                status["status"] = "park_ign_on" if adaptive_ign else "park_ign_off"
                status["status_label"] = "Остановка, зажиг. вкл" if adaptive_ign else "Остановка, зажиг. выкл"
            elif status.get("status") == "stopped":
                status["status_label"] = "Стоянка, зажиг. вкл" if adaptive_ign else "Стоянка, зажиг. выкл"
    card = _build_card_data(snap, latest, cfg_dict, status)
    filter_options = _build_filter_options(snap, latest, cfg_dict)
    recent = _recent_events(storage, unit_id, limit=3)
    return UnitDetail(
        item=item,
        snapshot=snap,
        latest=latest,
        unit_config=cfg_dict,
        status_details=status,
        tooltip_data=tooltip,
        card_data=card,
        filter_options=filter_options,
        recent_events=recent,
    )


def _load_worklist(session_id: str) -> List[int]:
    doc = _load_session(session_id)
    wl = doc.get("worklist") or []
    if not isinstance(wl, list):
        return []
    return [int(x) for x in wl if str(x).isdigit()]


def _save_worklist(session_id: str, ids: List[int]) -> List[int]:
    doc = _load_session(session_id)
    doc["worklist"] = sorted(set(int(x) for x in ids))
    _save_session(session_id, doc)
    return doc["worklist"]


@router.get("/api/worklist", response_model=List[int])
async def worklist_get(session_id: str = Depends(get_session_id)) -> List[int]:
    return _load_worklist(session_id)


@router.post("/api/worklist", response_model=List[int])
async def worklist_add(payload: WorklistPayload, session_id: str = Depends(get_session_id)) -> List[int]:
    bundle = _get_snapshot_bundle()
    current = _load_worklist(session_id)
    for uid in payload.unit_ids:
        if uid not in bundle.units:
            raise HTTPException(status_code=404, detail=f"unit {uid} not found")
        if uid not in current:
            current.append(uid)
    return _save_worklist(session_id, current)


@router.put("/api/worklist", response_model=List[int])
async def worklist_replace(payload: WorklistPayload, session_id: str = Depends(get_session_id)) -> List[int]:
    bundle = _get_snapshot_bundle()
    for uid in payload.unit_ids:
        if uid not in bundle.units:
            raise HTTPException(status_code=404, detail=f"unit {uid} not found")
    return _save_worklist(session_id, payload.unit_ids)


@router.delete("/api/worklist/{unit_id}", response_model=List[int])
async def worklist_delete(unit_id: int, session_id: str = Depends(get_session_id)) -> List[int]:
    current = [uid for uid in _load_worklist(session_id) if uid != unit_id]
    return _save_worklist(session_id, current)


@router.post("/api/units/{unit_id}/actions/{action}")
async def api_unit_action(unit_id: int, action: str) -> Dict[str, Any]:
    action = action.lower()
    storage = get_pipeline_storage_service()
    if action == "latest-event":
        stored = storage.get_latest_event_from_metrics(unit_id)
        if not stored:
            raise HTTPException(status_code=404, detail="No telemetry for unit")
        event = stored.event
        return {
            "status": "ok",
            "result": {
                "day": stored.day,
                "device_ts": event.device_ts,
                "received_ts": event.received_ts,
                "lat": event.latitude,
                "lon": event.longitude,
                "speed": event.speed,
            },
        }
    raise HTTPException(status_code=400, detail=f"Unknown action '{action}'")


def _age_seconds(last_ts: Any, *, now: Optional[int] = None) -> Optional[int]:
    if last_ts is None:
        return None
    try:
        ts = int(last_ts)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    if now is None:
        now = int(time.time())
    return max(0, now - ts)


def _format_offline_reason(has_latest: bool, age_sec: Optional[int], threshold: int) -> Optional[str]:
    if has_latest is False:
        return "Нет телеметрии (latest_metrics)"
    if age_sec is None:
        return "Нет данных о времени"
    if age_sec < 60:
        return "Нет данных <1 мин"
    minutes = age_sec // 60
    if minutes < 60:
        return f"Нет данных {minutes} мин"
    hours = age_sec / 3600
    if hours < 24:
        return f"Нет данных {hours:.1f} ч"
    days = hours / 24
    return f"Нет данных {days:.1f} дн"


def _log_units_snapshot(payload: Dict[str, Any]) -> None:
    try:
        _monitor_log.info(json.dumps(payload, ensure_ascii=False))
    except Exception:
        _monitor_log.info(str(payload))


def _resolve_coords(latest: Dict[str, Any], snap: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    lat = latest.get("lat")
    lon = latest.get("lon")
    if lat is None or lon is None:
        pos = snap.get("pos") or {}
        snap_lat = snap.get("lat") or snap.get("y") or pos.get("y")
        snap_lon = snap.get("lon") or snap.get("x") or pos.get("x")
        if lat is None:
            lat = snap_lat
    if lon is None:
        lon = snap_lon
    return lat, lon


def _build_filter_options(snap: Dict[str, Any], latest: Dict[str, Any], cfg_dict: Optional[Dict[str, Any]]) -> CardFilterOptions:
    sensors = _collect_sensor_tags(snap, cfg_dict)
    params = sorted(_extract_params(latest).keys())
    return CardFilterOptions(sensors=sensors[:20], params=params[:20])


def _get_panel_settings(session_id: str) -> PanelSettingsDTO:
    doc = _load_session(session_id)
    raw = doc.get("panel_settings")
    if not isinstance(raw, dict):
        raw = {}
    card_sections = {**DEFAULT_CARD_SECTIONS, **(raw.get("card_sections") or {})}
    tabs_payload: List[ViewConfig] = []
    for entry in raw.get("tabs") or []:
        if isinstance(entry, dict) and "id" in entry and "name" in entry:
            tabs_payload.append(ViewConfig(id=str(entry["id"]), name=str(entry["name"]), filters=entry.get("filters") or {}))
    tabs_payload = _ensure_work_tab(tabs_payload)
    active = raw.get("active_tab_id") or "work"
    if not any(tab.id == active for tab in tabs_payload):
        active = "work"
    list_view = _normalize_list_view(raw.get("list_view"))
    return PanelSettingsDTO(tabs=tabs_payload, active_tab_id=active, card_sections=card_sections, list_view=list_view)


def _save_panel_settings(session_id: str, payload: PanelSettingsDTO) -> PanelSettingsDTO:
    doc = _load_session(session_id)
    tabs = _ensure_work_tab(list(payload.tabs))
    payload.tabs = tabs
    if not payload.active_tab_id or not any(tab.id == payload.active_tab_id for tab in tabs):
        payload.active_tab_id = "work"
    payload.list_view = _normalize_list_view(payload.list_view.dict() if isinstance(payload.list_view, BaseModel) else payload.list_view)
    doc["panel_settings"] = payload.dict()
    _save_session(session_id, doc)
    return payload


@router.get("/api/settings/panel", response_model=PanelSettingsDTO)
async def api_get_panel_settings(session_id: str = Depends(get_session_id)) -> PanelSettingsDTO:
    return _get_panel_settings(session_id)


@router.put("/api/settings/panel", response_model=PanelSettingsDTO)
async def api_put_panel_settings(payload: PanelSettingsDTO, session_id: str = Depends(get_session_id)) -> PanelSettingsDTO:
    return _save_panel_settings(session_id, payload)


@router.post("/api/logs/client")
async def api_log_client(payload: ClientLogPayload, session_id: str = Depends(get_session_id)) -> Dict[str, Any]:
    _monitor_log.info(
        json.dumps(
            {
                "event": payload.event,
                "message": payload.message,
                "detail": payload.detail,
                "session_id": session_id,
            },
            ensure_ascii=False,
        )
    )
    return {"status": "ok"}
_static_version: Optional[str] = None
