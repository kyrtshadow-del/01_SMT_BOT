"""Next-gen unit snapshot built from local sources (UnitConfig + latest_metrics + admin meta).

This snapshot is the primary catalogue for Monitoring v2 and the bot.
It intentionally avoids any dependence on Wialon/legacy caches.
"""

from __future__ import annotations

import gzip
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pipeline.config.defaults import load_from_env
from pipeline.config.unit_config_service import UnitConfigService
from pipeline.services.admin_storage import AdminStorage, get_admin_storage, UnitMeta
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.storage.raw_storage import RawStorage

DEFAULT_SNAPSHOT_V2_PATH = Path("data/unit_snapshot.v2.json.gz")
LEGACY_SNAPSHOT_PATH = Path("data/unit_snapshot.json.gz")


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _ensure_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
    else:
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


@dataclass
class UnitSnapshotV2Record:
    unit_id: int
    name: str
    reg_number: Optional[str] = None
    owner_node_id: Optional[int] = None
    is_deleted: bool = False
    device: Dict[str, Any] = field(default_factory=dict)
    sensors: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    latest: Dict[str, Any] = field(default_factory=dict)
    status: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.unit_id,
            "name": self.name,
            "reg_number": self.reg_number,
            "owner_node_id": self.owner_node_id,
            "is_deleted": self.is_deleted,
            "device": self.device,
            "sensors": self.sensors,
            "meta": self.meta,
            "latest": self.latest,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "UnitSnapshotV2Record":
        return cls(
            unit_id=int(payload.get("id")),
            name=str(payload.get("name") or f"id {payload.get('id')}"),
            reg_number=payload.get("reg_number"),
            owner_node_id=_safe_int(payload.get("owner_node_id")),
            is_deleted=bool(payload.get("is_deleted")),
            device=_ensure_dict(payload.get("device")),
            sensors=list(payload.get("sensors") or []),
            meta=_ensure_dict(payload.get("meta")),
            latest=_ensure_dict(payload.get("latest")),
            status=_ensure_dict(payload.get("status")),
        )


@dataclass
class UnitSnapshotV2Bundle:
    units: Dict[int, UnitSnapshotV2Record]
    dump_ts: Optional[int]
    source_kind: str
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        return {
            "dump_ts": self.dump_ts,
            "source_kind": self.source_kind,
            "meta": self.meta,
            "items_by_id": {str(uid): rec.to_dict() for uid, rec in self.units.items()},
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "UnitSnapshotV2Bundle":
        items: Dict[int, UnitSnapshotV2Record] = {}
        for key, raw in (payload.get("items_by_id") or {}).items():
            try:
                uid = int(key)
            except Exception:
                continue
            if isinstance(raw, dict):
                items[uid] = UnitSnapshotV2Record.from_dict(raw)
        return cls(
            units=items,
            dump_ts=_safe_int(payload.get("dump_ts")),
            source_kind=str(payload.get("source_kind") or "unknown"),
            meta=_ensure_dict(payload.get("meta")),
        )

    @classmethod
    def empty(cls) -> "UnitSnapshotV2Bundle":
        return cls(units={}, dump_ts=None, source_kind="empty")


class UnitSnapshotV2Service:
    """Reader/writer for unit_snapshot.v2.json.gz with mtime caching."""

    def __init__(self, snapshot_path: Path | str = DEFAULT_SNAPSHOT_V2_PATH) -> None:
        self.snapshot_path = Path(snapshot_path)
        self._bundle: Optional[UnitSnapshotV2Bundle] = None
        self._lock = Lock()
        self._last_mtime: Optional[float] = None

    def load_bundle(self) -> UnitSnapshotV2Bundle:
        with self._lock:
            current_mtime = None
            try:
                current_mtime = self.snapshot_path.stat().st_mtime
            except OSError:
                current_mtime = None
            if self._bundle is None or (current_mtime and self._last_mtime and current_mtime > self._last_mtime):
                self._bundle = self._read_from_disk()
                self._last_mtime = current_mtime
            return self._bundle

    def refresh(self, records: Iterable[UnitSnapshotV2Record], *, source_kind: str, dump_ts: Optional[int] = None) -> None:
        bundle = UnitSnapshotV2Bundle(
            units={rec.unit_id: rec for rec in records},
            dump_ts=dump_ts or int(time.time()),
            source_kind=source_kind,
        )
        self._write_to_disk(bundle)
        with self._lock:
            self._bundle = bundle

    # internal ------------------------------------------------------

    def _read_from_disk(self) -> UnitSnapshotV2Bundle:
        payload = _read_json(self.snapshot_path)
        if isinstance(payload, dict):
            return UnitSnapshotV2Bundle.from_payload(payload)
        return UnitSnapshotV2Bundle.empty()

    def _write_to_disk(self, bundle: UnitSnapshotV2Bundle) -> None:
        payload = bundle.to_payload()
        _write_json(self.snapshot_path, payload)


_SERVICE: Optional[UnitSnapshotV2Service] = None
_SERVICE_LOCK = Lock()


def get_unit_snapshot_v2_service() -> UnitSnapshotV2Service:
    global _SERVICE
    if _SERVICE is not None:
        return _SERVICE
    with _SERVICE_LOCK:
        if _SERVICE is None:
            _SERVICE = UnitSnapshotV2Service()
    return _SERVICE


# -------------------------- builder --------------------------


def _load_unit_configs() -> Tuple[Dict[int, Dict[str, Any]], float]:
    """Return map unit_id -> config dict and max mtime."""
    cfg = load_from_env()
    storage_root = Path(cfg.storage_root).resolve()
    unit_configs_dir = storage_root / "unit_configs"
    use_legacy = False
    if not unit_configs_dir.exists() or not any(unit_configs_dir.glob("*.json")):
        legacy_root = Path("data") / "unit_configs"
        if legacy_root.exists() and any(legacy_root.glob("*.json")):
            unit_configs_dir = legacy_root
            use_legacy = True
    svc = UnitConfigService(unit_configs_dir)
    configs: Dict[int, Dict[str, Any]] = {}
    max_mtime = 0.0
    for summary in svc.list_configs():
        try:
            cfg_payload = json.loads(summary.path.read_text(encoding="utf-8"))
            configs[summary.unit_id] = cfg_payload
            max_mtime = max(max_mtime, summary.updated_ts)
        except Exception:
            continue
    if use_legacy and configs:
        max_mtime = max(max_mtime, unit_configs_dir.stat().st_mtime)
    return configs, max_mtime


def _load_latest_metrics() -> Tuple[Dict[int, Dict[str, Any]], float]:
    cfg = load_from_env()
    store = LatestTelemetryStore(Path(cfg.storage_root))
    data: Dict[int, Dict[str, Any]] = {}
    mtime = store.get_mtime()
    for uid, payload in store.iter_latest():
        data[uid] = payload
    return data, mtime


def _derive_latest_from_events(lookback_days: int = 7) -> Dict[int, Dict[str, Any]]:
    """Build latest-like map from recent events when latest_metrics.json is absent/empty."""
    cfg = load_from_env()
    storage_root = Path(cfg.storage_root)
    raw = RawStorage(storage_root)
    days = sorted((p.name for p in storage_root.iterdir() if p.is_dir() and p.name[:4].isdigit()), reverse=True)
    if lookback_days > 0:
        days = days[:lookback_days]
    latest: Dict[int, Dict[str, Any]] = {}
    for day in days:
        events = raw.fetch(day_key=day, unit_id=None)
        for ev in events:
            prev = latest.get(ev.unit_id)
            if prev and prev.get("device_ts", 0) >= ev.device_ts:
                continue
            entry = {
                "device_ts": ev.device_ts,
                "received_ts": ev.received_ts,
                "lat": ev.latitude,
                "lon": ev.longitude,
                "speed": ev.speed,
                "course": ev.course,
                "params": ev.params or {},
            }
            latest[ev.unit_id] = entry
    return latest


def _load_legacy_names() -> Dict[int, Dict[str, Any]]:
    """Optional helper: pull names/reg_number/device/sensors from legacy snapshot, without using it as data source."""
    path = LEGACY_SNAPSHOT_PATH
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return {}
    items = payload.get("items_by_id") or {}
    result: Dict[int, Dict[str, Any]] = {}
    for key, raw in items.items():
        try:
            uid = int(key)
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        result[uid] = raw
    return result


def _extract_from_config(payload: Dict[str, Any]) -> Tuple[str, Optional[str], Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    general = payload.get("general") or {}
    name = general.get("name") or f"id {payload.get('unit_id')}"
    reg_number = general.get("reg_number") or general.get("plate")
    hw = payload.get("hw_config") or {}
    device = {
        "uid": (hw.get("uid") or hw.get("unique_id") or hw.get("terminal_id")),
        "hardware": hw.get("hw_name") or hw.get("device_type") or hw.get("hw"),
    }
    sensors = []
    for entry in payload.get("sensors") or []:
        if not isinstance(entry, dict):
            continue
        sensors.append(
            {
                "sensor_id": entry.get("sensor_id") or entry.get("id"),
                "name": entry.get("name") or entry.get("n"),
                "type": entry.get("type") or entry.get("t"),
                "units": entry.get("units") or entry.get("m"),
                "description": entry.get("description") or entry.get("d"),
            }
        )
    meta = payload.get("extra") or {}
    return name, reg_number, device, sensors, meta


def build_snapshot_v2_bundle() -> UnitSnapshotV2Bundle:
    """Assemble snapshot v2 from local sources."""
    configs, cfg_mtime = _load_unit_configs()
    latest, latest_mtime = _load_latest_metrics()
    if not latest:
        latest = _derive_latest_from_events()
        latest_mtime = int(time.time())
    admin_storage: AdminStorage = get_admin_storage()
    legacy_names = _load_legacy_names()
    unit_ids = set(configs.keys()) | set(latest.keys())
    if not unit_ids:
        return UnitSnapshotV2Bundle.empty()
    unit_meta = admin_storage.get_unit_meta_many(sorted(unit_ids))
    meta_by_uid = {m.unit_id: m for m in unit_meta}

    records: Dict[int, UnitSnapshotV2Record] = {}
    now = int(time.time())
    for uid in sorted(unit_ids):
        cfg_payload = configs.get(uid)
        if cfg_payload:
            name, reg_number, device, sensors, extra_meta = _extract_from_config(cfg_payload)
        else:
            legacy = legacy_names.get(uid) or {}
            name = legacy.get("name") or legacy.get("nm") or f"unit {uid}"
            reg_number = legacy.get("reg_number") or legacy.get("plate")
            device = legacy.get("device") or {}
            sensors = legacy.get("sensors") or []
            extra_meta = legacy.get("meta") or {}
        latest_entry = latest.get(uid) or {}
        status = {
            "online": bool(latest_entry),
            "status_label": None,
            "last_ts": _safe_int(latest_entry.get("device_ts") or latest_entry.get("received_ts")),
            "has_fuel": False,
        }
        if status["last_ts"]:
            status["age_sec"] = max(0, now - status["last_ts"])
        meta_entry: Optional[UnitMeta] = meta_by_uid.get(uid)
        if meta_entry is None:
            # auto-create placeholder meta to make unit видимым админам
            meta_entry = admin_storage.upsert_unit_meta(unit_id=uid, owner_node_id=None)
            meta_by_uid[uid] = meta_entry
        rec = UnitSnapshotV2Record(
            unit_id=uid,
            name=name or f"unit {uid}",
            reg_number=reg_number,
            owner_node_id=meta_entry.owner_node_id if meta_entry else None,
            is_deleted=bool(meta_entry.is_deleted) if meta_entry else False,
            device=device,
            sensors=sensors,
            meta=extra_meta,
            latest=latest_entry,
            status=status,
        )
        records[uid] = rec

    dump_ts = int(max(cfg_mtime or 0, latest_mtime or 0, time.time()))
    return UnitSnapshotV2Bundle(units=records, dump_ts=dump_ts, source_kind="snapshot_v2")
