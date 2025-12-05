"""Store for the latest telemetry snapshot per unit (Redis-backed with file fallback)."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Tuple, Dict, Any, Optional

from pipeline.events import Event
from pipeline.services.unit_snapshot_service import (
    get_unit_snapshot_service,
    UnitSnapshotRecord,
    UnitDeviceMeta,
)

try:  # Optional dependency: redis-py
    import redis  # type: ignore
except Exception:  # pragma: no cover - defensive
    redis = None


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
    return UnitSnapshotRecord(
        unit_id=ev.unit_id,
        name=str(name),
        reg_number=reg,
        device=UnitDeviceMeta(uid=uid, hardware=hw),
        sensors=[],
        meta=meta,
        unconfigured=True,
    )


class LatestTelemetryStore:
    """Tracks the latest telemetry payload for each unit.

    Primary backend: Redis (hash per unit).
    Fallback: legacy JSON file if Redis unavailable.
    """

    def __init__(self, root: Path) -> None:
        # ---- Redis backend ----
        self._redis_ok = False
        self._r = None
        self._ttl_sec = int(os.getenv("LATEST_METRICS_TTL_SEC", "172800"))  # 2 days
        if redis is not None:
            try:
                url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
                self._r = redis.Redis.from_url(url, decode_responses=True)
                # lightweight check
                self._r.ping()
                self._redis_ok = True
            except Exception:
                self._redis_ok = False

        # ---- File fallback (legacy) ----
        self.path = Path(root) / "latest_metrics.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._payload: dict[str, dict] | None = None
        self._loaded_mtime: float | None = None

    # ---------------- Redis helpers ----------------
    def _inflate(self, raw: Dict[str, str]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in raw.items():
            if k in {"device_ts", "received_ts", "updated_at", "course", "last_moving_ts", "stop_duration_s"}:
                try:
                    out[k] = int(v)
                except Exception:
                    out[k] = 0
            elif k in {"lat", "lon", "speed"}:
                try:
                    out[k] = float(v)
                except Exception:
                    out[k] = 0.0
            elif k == "params":
                try:
                    out[k] = json.loads(v)
                except Exception:
                    out[k] = {}
            else:
                out[k] = v
        return out

    def _redis_update(self, events: Iterable[Event]) -> None:
        if not self._redis_ok or not self._r:
            return
        pipe = self._r.pipeline()
        now = int(time.time())
        for ev in events:
            key = f"lm:{ev.unit_id}"
            prev = self._r.hgetall(key) or {}
            last_move_ts = None
            try:
                last_move_ts = int(prev.get("last_moving_ts")) if prev.get("last_moving_ts") else None
            except Exception:
                last_move_ts = None

            entry: Dict[str, Any] = {
                "device_ts": ev.device_ts,
                "received_ts": ev.received_ts,
                "updated_at": now,
            }
            if ev.latitude is not None:
                entry["lat"] = ev.latitude
            if ev.longitude is not None:
                entry["lon"] = ev.longitude
            if ev.speed is not None:
                entry["speed"] = ev.speed
            if ev.course is not None:
                entry["course"] = ev.course
            if ev.params:
                entry["params"] = json.dumps(ev.params, ensure_ascii=False)

            # stop_duration_s + last_moving_ts (примерно как в legacy)
            try:
                speed_val = float(ev.speed) if ev.speed is not None else None
            except Exception:
                speed_val = None
            if speed_val is not None and speed_val >= 1.0:
                last_move_ts = ev.device_ts or ev.received_ts
                entry["stop_duration_s"] = 0
            elif last_move_ts:
                try:
                    cur_ts = int(ev.device_ts or ev.received_ts or last_move_ts)
                    entry["stop_duration_s"] = max(0, cur_ts - int(last_move_ts))
                except Exception:
                    pass
            if last_move_ts:
                entry["last_moving_ts"] = last_move_ts

            pipe.hset(key, mapping=entry)
            if self._ttl_sec > 0:
                pipe.expire(key, self._ttl_sec)
        try:
            pipe.execute()
        except Exception:
            # fallback to file if Redis fails at runtime
            self._redis_ok = False

    # ---------------- File fallback (legacy) ----------------
    def _load_file(self) -> dict[str, dict]:
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

    def _save_file(self) -> None:
        tmp_path = self.path.with_suffix(".tmp")
        payload = self._payload or {}
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(self.path)
        try:
            self._loaded_mtime = self.path.stat().st_mtime
        except OSError:
            self._loaded_mtime = None

    def _prune_expired_file(self) -> None:
        if self._ttl_sec <= 0 or not self._payload:
            return
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
            self._save_file()

    # ---------------- Public API ----------------
    def update_from_events(self, events: Iterable[Event]) -> None:
        events_list = list(events)
        if not events_list:
            return
        self._bootstrap_units(events_list)
        if self._redis_ok:
            self._redis_update(events_list)
            return

        # Fallback to legacy file mode
        with self._lock:
            data = self._load_file()
            updated = False
            for event in events_list:
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
                prev = data.get(str(event.unit_id)) or {}
                last_move_ts = prev.get("last_moving_ts")
                try:
                    speed_val = float(event.speed) if event.speed is not None else None
                except Exception:
                    speed_val = None
                if speed_val is not None and speed_val >= 1.0:
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
                self._save_file()
                self._prune_expired_file()

    def _bootstrap_units(self, events: Iterable[Event]) -> None:
        svc = get_unit_snapshot_service()
        for ev in events:
            if svc.has_unit(ev.unit_id):
                continue
            record = _build_snapshot_from_event(ev)
            svc.upsert_unit(record)

    def get_latest(self, unit_id: int) -> Mapping[str, object] | None:
        if self._redis_ok and self._r:
            try:
                raw = self._r.hgetall(f"lm:{unit_id}")
                if not raw:
                    return None
                return self._inflate(raw)
            except Exception:
                self._redis_ok = False
        with self._lock:
            data = self._load_file()
            entry = data.get(str(unit_id))
            if not isinstance(entry, dict):
                return None
            return dict(entry)

    def iter_latest(self) -> Iterator[Tuple[int, Dict[str, Any]]]:
        if self._redis_ok and self._r:
            try:
                cursor = 0
                while True:
                    cursor, keys = self._r.scan(cursor=cursor, match="lm:*", count=500)
                    if keys:
                        pipe = self._r.pipeline()
                        for key in keys:
                            pipe.hgetall(key)
                        results = pipe.execute()
                        for key, raw in zip(keys, results):
                            if not raw:
                                continue
                            try:
                                uid = int(key.split(":")[1])
                            except Exception:
                                continue
                            yield uid, self._inflate(raw)
                    if cursor == 0:
                        break
                return
            except Exception:
                self._redis_ok = False

        with self._lock:
            data = self._load_file()
            for key, entry in data.items():
                if not isinstance(entry, dict):
                    continue
                try:
                    unit_id = int(key)
                except (TypeError, ValueError):
                    continue
                yield unit_id, dict(entry)

    def get_mtime(self) -> float:
        if self._redis_ok:
            return float(time.time())
        with self._lock:
            self._load_file()
            if self._loaded_mtime is None:
                return 0.0
            return float(self._loaded_mtime)
