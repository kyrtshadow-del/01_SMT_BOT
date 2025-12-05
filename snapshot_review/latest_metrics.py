"""Store for the latest telemetry snapshot per unit."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Tuple, Dict, Any

from pipeline.events import Event
from pipeline.services.unit_snapshot_service import (
    get_unit_snapshot_service,
    UnitSnapshotRecord,
    UnitDeviceMeta,
)


class LatestTelemetryStore:
    """Tracks the latest telemetry payload for each unit."""

    def __init__(self, root: Path) -> None:
        self.path = Path(root) / "latest_metrics.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._payload: dict[str, dict] | None = None
        self._loaded_mtime: float | None = None
        import os
        self._ttl_sec = int(os.getenv("LATEST_METRICS_TTL_SEC", "172800"))  # default 2 days

    def _load(self) -> dict[str, dict]:
        reload_needed = False
        if self._payload is None:
            reload_needed = True
        else:
            try:
                current_mtime = self.path.stat().st_mtime
            except OSError:
                current_mtime = None
            if current_mtime is None:
                if self._loaded_mtime is None:
                    return self._payload
                reload_needed = True
            elif self._loaded_mtime is None or current_mtime > self._loaded_mtime:
                reload_needed = True
            if not reload_needed:
                return self._payload
        if not self.path.exists():
            self._payload = {}
            self._loaded_mtime = None
            return self._payload
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        self._payload = {str(k): v for k, v in data.items() if isinstance(v, dict)}
        try:
            self._loaded_mtime = self.path.stat().st_mtime
        except OSError:
            self._loaded_mtime = None
        return self._payload

    def _save(self) -> None:
        tmp_path = self.path.with_suffix(".tmp")
        payload = self._payload or {}
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(self.path)
        try:
            self._loaded_mtime = self.path.stat().st_mtime
        except OSError:
            self._loaded_mtime = None

    def update_from_events(self, events: Iterable[Event]) -> None:
        events = list(events)
        if not events:
            return
        # Bootstrap snapshot for new units before saving metrics
        self._bootstrap_units(events)
        with self._lock:
            data = self._load()
            updated = False
            for event in events:
                entry = {
                    "device_ts": event.device_ts,
                    "received_ts": event.received_ts,
                    "params": event.params or {},
                }
                if event.latitude is not None:
                    entry["lat"] = event.latitude
                if event.longitude is not None:
                    entry["lon"] = event.longitude
                if event.speed is not None:
                    entry["speed"] = event.speed
                if event.course is not None:
                    entry["course"] = event.course
                # ---- derive stop duration to distinguish остановка/стоянка ----
                prev = data.get(str(event.unit_id)) or {}
                last_move_ts = prev.get("last_moving_ts")
                try:
                    speed_val = float(event.speed) if event.speed is not None else None
                except Exception:
                    speed_val = None
                if speed_val is not None and speed_val >= 1.0:
                    # считается движением — сбрасываем таймер стоянки
                    last_move_ts = event.device_ts or event.received_ts
                    entry["stop_duration_s"] = 0
                else:
                    if last_move_ts:
                        try:
                            cur_ts = int(event.device_ts or event.received_ts or last_move_ts)
                            dur = max(0, cur_ts - int(last_move_ts))
                            entry["stop_duration_s"] = dur
                        except Exception:
                            pass
                if last_move_ts:
                    entry["last_moving_ts"] = last_move_ts
                data[str(event.unit_id)] = entry
                updated = True
            if updated:
                self._save()
                self._prune_expired()

    def _bootstrap_units(self, events: Iterable[Event]) -> None:
        """Ensure each unit from events exists in unit_snapshot immediately."""
        svc = get_unit_snapshot_service()
        created = 0
        for ev in events:
            if svc.has_unit(ev.unit_id):
                continue
            record = _build_snapshot_from_event(ev)
            svc.upsert_unit(record)
            created += 1
        if created:
            # no extra logging here; snapshot service can log if needed
            pass

    def get_latest(self, unit_id: int) -> Mapping[str, object] | None:
        with self._lock:
            data = self._load()
            entry = data.get(str(unit_id))
            if not isinstance(entry, dict):
                return None
            return dict(entry)

    def iter_latest(self) -> Iterator[Tuple[int, Dict[str, Any]]]:
        with self._lock:
            data = self._load()
            for key, entry in data.items():
                if not isinstance(entry, dict):
                    continue
                try:
                    unit_id = int(key)
                except (TypeError, ValueError):
                    continue
                yield unit_id, dict(entry)

    def get_mtime(self) -> float:
        """Expose the mtime of the underlying latest_metrics.json (0 if unknown)."""

        with self._lock:
            self._load()
            if self._loaded_mtime is None:
                return 0.0
            return float(self._loaded_mtime)

    def _prune_expired(self) -> None:
        """Remove entries older than ttl to keep latest_metrics lean."""
        if self._ttl_sec <= 0 or not self._payload:
            return
        import time
        cutoff = int(time.time()) - self._ttl_sec
        removed = False
        for key in list(self._payload.keys()):
            entry = self._payload.get(key) or {}
            if not isinstance(entry, dict):
                continue
            ts = entry.get("device_ts") or entry.get("received_ts")
            try:
                ts = int(ts)
            except Exception:
                ts = None
            if ts and ts < cutoff:
                self._payload.pop(key, None)
                removed = True
        if removed:
            self._save()


def _build_snapshot_from_event(ev: Event) -> UnitSnapshotRecord:
    params = ev.params or {}
    name = (
        params.get("name")
        or params.get("nm")
        or params.get("label")
        or params.get("unit_name")
        or params.get("alias")
        or params.get("uid")
        or params.get("imei")
        or f"id {ev.unit_id}"
    )
    reg = params.get("reg_number") or params.get("plate") or params.get("reg")
    hw = params.get("hardware") or params.get("hw") or params.get("device_type")
    uid = params.get("uid") or params.get("imei") or params.get("unique_id")
    meta: Dict[str, Any] = {}
    if ev.source:
        meta["source_kind"] = ev.source
    # Keep minimal meta to allow later enrichment
    return UnitSnapshotRecord(
        unit_id=ev.unit_id,
        name=str(name),
        reg_number=reg,
        device=UnitDeviceMeta(uid=uid, hardware=hw),
        sensors=[],
        meta=meta,
        unconfigured=True,
    )
