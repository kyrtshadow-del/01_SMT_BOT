"""Unit snapshot store and helpers for card/search data."""

from __future__ import annotations

import copy
import gzip
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional

DEFAULT_SNAPSHOT_PATH = Path("data/unit_snapshot.json.gz")
LEGACY_UNIT_CACHE_PATH = Path("data/units.v1.json.gz")


@dataclass
class UnitSensorMeta:
    sensor_id: Optional[int] = None
    type: Optional[str] = None
    name: Optional[str] = None
    units: Optional[str] = None
    calibration: Optional[List[Any]] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "UnitSensorMeta":
        return cls(
            sensor_id=_safe_int(payload.get("id")),
            type=(payload.get("type") or payload.get("t") or payload.get("kind")),
            name=(payload.get("name") or payload.get("n")),
            units=payload.get("units"),
            calibration=payload.get("calibration") or payload.get("segments"),
            meta={
                key: value
                for key, value in payload.items()
                if key
                not in {
                    "id",
                    "type",
                    "t",
                    "kind",
                    "name",
                    "n",
                    "units",
                    "calibration",
                    "segments",
                }
            },
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"id": self.sensor_id, "type": self.type, "name": self.name}
        if self.units is not None:
            payload["units"] = self.units
        if self.calibration is not None:
            payload["calibration"] = self.calibration
        if self.meta:
            payload.update(self.meta)
        return payload


@dataclass
class UnitDeviceMeta:
    uid: Optional[str] = None
    hardware: Optional[str] = None
    firmware: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "UnitDeviceMeta":
        return cls(
            uid=_safe_str(payload.get("uid") or payload.get("unique_id") or payload.get("terminal_id")),
            hardware=_safe_str(payload.get("hardware") or payload.get("device_type") or payload.get("hw")),
            firmware=_safe_str(payload.get("firmware") or payload.get("soft")),
            extra={
                key: value
                for key, value in payload.items()
                if key not in {"uid", "unique_id", "terminal_id", "hardware", "device_type", "hw", "firmware", "soft"}
            },
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if self.uid:
            payload["uid"] = self.uid
        if self.hardware:
            payload["hardware"] = self.hardware
        if self.firmware:
            payload["firmware"] = self.firmware
        if self.extra:
            payload.update(self.extra)
        return payload


@dataclass
class UnitSnapshotRecord:
    unit_id: int
    name: str
    reg_number: Optional[str] = None
    contacts: Dict[str, Any] = field(default_factory=dict)
    device: UnitDeviceMeta = field(default_factory=UnitDeviceMeta)
    sensors: List[UnitSensorMeta] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, unit_id: int, payload: Dict[str, Any]) -> "UnitSnapshotRecord":
        sensors_block = payload.get("sensors") or payload.get("sens") or []
        if isinstance(sensors_block, dict):
            sensors_iterable = sensors_block.values()
        else:
            sensors_iterable = sensors_block if isinstance(sensors_block, list) else []
        sensors: List[UnitSensorMeta] = []
        for raw_sensor in sensors_iterable:
            if isinstance(raw_sensor, dict):
                sensors.append(UnitSensorMeta.from_dict(raw_sensor))
        device_block = payload.get("device") or payload
        return cls(
            unit_id=unit_id,
            name=str(payload.get("nm") or payload.get("name") or f"id {unit_id}"),
            reg_number=_safe_str(payload.get("reg_number") or payload.get("plate")),
            contacts=_ensure_dict(payload.get("contacts")),
            device=UnitDeviceMeta.from_dict(device_block if isinstance(device_block, dict) else {}),
            sensors=sensors,
            meta={
                key: value
                for key, value in payload.items()
                if key
                not in {
                    "id",
                    "nm",
                    "name",
                    "reg_number",
                    "plate",
                    "contacts",
                    "device",
                    "sens",
                    "sensors",
                }
            },
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "id": self.unit_id,
            "nm": self.name,
        }
        if self.reg_number:
            payload["reg_number"] = self.reg_number
        if self.contacts:
            payload["contacts"] = copy.deepcopy(self.contacts)
        device_dict = self.device.to_dict()
        if device_dict:
            payload["device"] = device_dict
        if self.sensors:
            payload["sensors"] = [sensor.to_dict() for sensor in self.sensors]
        if self.meta:
            payload.update(self.meta)
        return payload


@dataclass
class UnitSnapshotBundle:
    units: Dict[int, UnitSnapshotRecord] = field(default_factory=dict)
    dump_ts: Optional[int] = None
    source_kind: str = "unknown"
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        return {
            "version": 2,
            "source_kind": self.source_kind,
            "dump_ts": self.dump_ts or int(time.time()),
            "meta": self.meta,
            "items_by_id": {str(uid): record.to_dict() for uid, record in self.units.items()},
        }

    @classmethod
    def from_payload(
        cls,
        payload: Dict[str, Any],
        *,
        source_kind: Optional[str] = None,
        dump_ts: Optional[int] = None,
    ) -> "UnitSnapshotBundle":
        items = payload.get("items_by_id")
        units: Dict[int, UnitSnapshotRecord] = {}
        if isinstance(items, dict):
            for key, raw in items.items():
                try:
                    unit_id = int(key)
                except Exception:
                    continue
                if not isinstance(raw, dict):
                    continue
                units[unit_id] = UnitSnapshotRecord.from_dict(unit_id, raw)
        return cls(
            units=units,
            dump_ts=_safe_int(payload.get("dump_ts")) or dump_ts,
            source_kind=str(payload.get("source_kind") or source_kind or "unknown"),
            meta=_ensure_dict(payload.get("meta")),
        )

    @classmethod
    def empty(cls) -> "UnitSnapshotBundle":
        return cls(units={}, dump_ts=None, source_kind="unknown")


class UnitSnapshotService:
    """Reader/writer for the unified unit snapshot file."""

    def __init__(
        self,
        snapshot_path: Path | str = DEFAULT_SNAPSHOT_PATH,
        legacy_path: Path | str = LEGACY_UNIT_CACHE_PATH,
    ) -> None:
        self.snapshot_path = Path(snapshot_path)
        self.legacy_path = Path(legacy_path)
        self._bundle: Optional[UnitSnapshotBundle] = None
        self._lock = Lock()

    def load_bundle(self) -> UnitSnapshotBundle:
        with self._lock:
            if self._bundle is None:
                self._bundle = self._read_from_disk()
            return self._bundle

    def get_unit(self, unit_id: int) -> Optional[UnitSnapshotRecord]:
        bundle = self.load_bundle()
        entry = bundle.units.get(unit_id)
        if entry is None:
            return None
        return copy.deepcopy(entry)

    def iter_units(self) -> Iterable[UnitSnapshotRecord]:
        bundle = self.load_bundle()
        for record in bundle.units.values():
            yield copy.deepcopy(record)

    def refresh(self, records: Iterable[UnitSnapshotRecord], *, source_kind: str, dump_ts: Optional[int] = None) -> None:
        bundle = UnitSnapshotBundle(
            units={record.unit_id: record for record in records},
            dump_ts=dump_ts or int(time.time()),
            source_kind=source_kind,
        )
        self._write_to_disk(bundle)
        with self._lock:
            self._bundle = bundle

    def _read_from_disk(self) -> UnitSnapshotBundle:
        if self.snapshot_path.exists():
            payload = _read_json(self.snapshot_path)
            if isinstance(payload, dict):
                return UnitSnapshotBundle.from_payload(payload)
        if self.legacy_path.exists():
            payload = _read_json(self.legacy_path)
            if isinstance(payload, dict):
                return UnitSnapshotBundle.from_payload(payload, source_kind="legacy")
        return UnitSnapshotBundle.empty()

    def _write_to_disk(self, bundle: UnitSnapshotBundle) -> None:
        payload = bundle.to_payload()
        _write_json(self.snapshot_path, payload)


_SERVICE: Optional[UnitSnapshotService] = None
_SERVICE_LOCK = Lock()


def get_unit_snapshot_service() -> UnitSnapshotService:
    global _SERVICE
    if _SERVICE is not None:
        return _SERVICE
    with _SERVICE_LOCK:
        if _SERVICE is None:
            _SERVICE = UnitSnapshotService()
    return _SERVICE


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _ensure_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _read_json(path: Path) -> Any:
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
