"""Helpers to access pipeline storage from application code (bot, CLI, etc.)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Sequence, Any

import math
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import os

from pipeline.config.defaults import PipelineConfig, load_from_env
from pipeline.events import Event
from pipeline.storage.raw_storage import RawStorage
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.services.unit_index import get_unit_index


@dataclass
class StoredEvent:
    day: str
    event: Event


@dataclass
class PipelineStorageService:
    """Lightweight wrapper around RawStorage with config awareness."""

    config: PipelineConfig
    raw_storage: RawStorage
    latest_store: LatestTelemetryStore

    @property
    def storage_root(self) -> Path:
        return self.raw_storage.root

    def fetch_day(self, day: str, unit_id: Optional[int] = None) -> Sequence[Event]:
        """Return events for a specific day (optionally filtered by unit)."""

        return self.raw_storage.fetch(day, unit_id=unit_id)

    def list_days(self) -> Sequence[str]:
        """List available day directories inside storage root."""

        entries: list[str] = []
        for item in sorted(self.storage_root.iterdir()):
            if item.is_dir():
                entries.append(item.name)
        return entries

    def find_latest_event(self, unit_id: int, max_days: int = 30) -> Optional[StoredEvent]:
        """Return the most recent event for a unit across available days."""

        days = sorted((p.name for p in self.storage_root.iterdir() if p.is_dir()), reverse=True)
        checked = 0
        for day in days:
            events = self.raw_storage.fetch(day, unit_id=unit_id)
            if events:
                return StoredEvent(day=day, event=events[-1])
            checked += 1
            if max_days and checked >= max_days:
                break
        return None

    def find_latest_event_fast(self, unit_id: int, *, lookback_days: int | None = None) -> Optional[StoredEvent]:
        """Fast path: start from latest_metrics day, then bounded lookback.

        - Uses the timestamp from latest_metrics.json to compute the most probable day
          and reads only that day's file.
        - If not found, walks back a limited number of days (default from env, else 7).
        - Falls back to None; caller may use slow path as a backup.
        """

        # Resolve lookback bound from env to keep this change low‑risk and configurable
        if lookback_days is None:
            try:
                lookback_days = int(os.getenv("PIPELINE_STORAGE_LOOKBACK_DAYS", "7"))
            except ValueError:
                lookback_days = 7

        latest = self.latest_store.get_latest(unit_id)
        if not isinstance(latest, dict):
            return None
        ts = latest.get("device_ts") or latest.get("received_ts")
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            return None
        # Walk chosen day, then bounded previous days
        base_day = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        for d in range(0, max(0, lookback_days)):
            day_key = (base_day - timedelta(days=d)).isoformat()
            events = self.raw_storage.fetch(day_key, unit_id=unit_id)
            if events:
                return StoredEvent(day=day_key, event=events[-1])
        return None

    def get_latest_event_from_metrics(self, unit_id: int) -> Optional[StoredEvent]:
        """Construct the latest Event directly from latest_metrics.json without reading files."""
        payload = self.latest_store.get_latest(unit_id)
        if not isinstance(payload, dict):
            return None
        try:
            device_ts = int(payload.get("device_ts") or 0)
            received_ts = int(payload.get("received_ts") or device_ts or 0)
        except (TypeError, ValueError):
            return None
        if device_ts <= 0:
            return None
        lat = payload.get("lat")
        lon = payload.get("lon")
        speed = payload.get("speed")
        course = payload.get("course")
        params = payload.get("params") or {}
        event = Event(
            unit_id=unit_id,
            device_ts=device_ts,
            received_ts=received_ts,
            latitude=lat,
            longitude=lon,
            speed=speed,
            course=course,
            params=params,
            source="latest_metrics",
            raw_payload=dict(params=params, device_ts=device_ts, received_ts=received_ts),
        )
        day_key = datetime.fromtimestamp(device_ts, tz=timezone.utc).date().isoformat()
        return StoredEvent(day=day_key, event=event)

    def get_latest_metrics(self, unit_id: int) -> Optional[dict]:
        payload = self.latest_store.get_latest(unit_id)
        if payload:
            return dict(payload)
        # fallback: derive from recent events if latest_metrics entry is missing
        fallback = self.find_latest_event_fast(unit_id)
        if fallback and fallback.event:
            ev = fallback.event
            return {
                "device_ts": ev.device_ts,
                "received_ts": ev.received_ts,
                "lat": ev.latitude,
                "lon": ev.longitude,
                "speed": ev.speed,
                "course": ev.course,
                "params": ev.params or {},
            }
        return None

    def find_nearest_units(
        self,
        lat: float,
        lon: float,
        max_distance_m: float = 100.0,
        limit: int = 5,
        *,
        exclude_unit_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if lat is None or lon is None:
            return []
        entries = list(self.latest_store.iter_latest())
        if not entries:
            return []
        index = get_unit_index()
        results: List[Dict[str, Any]] = []
        for unit_id, payload in entries:
            if exclude_unit_id is not None and unit_id == exclude_unit_id:
                continue
            u_lat = payload.get("lat")
            u_lon = payload.get("lon")
            dist_m = _haversine_m(lat, lon, u_lat, u_lon)
            if dist_m is None:
                continue
            if max_distance_m > 0 and dist_m > max_distance_m:
                continue
            info = index.get(unit_id) or {"id": unit_id, "nm": f"id {unit_id}"}
            enriched = dict(info)
            enriched["lat"] = u_lat
            enriched["lon"] = u_lon
            enriched["device_ts"] = payload.get("device_ts")
            enriched["distance_m"] = dist_m
            results.append(enriched)
        results.sort(key=lambda entry: entry["distance_m"])
        return results[:limit]

    def store_events(self, events: Sequence[Event]) -> int:
        """Persist events into raw storage and refresh latest metrics."""

        if not events:
            return 0
        # Optional: also write to Postgres (for trips/history) if DSN provided
        db_dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")

        grouped = defaultdict(list)
        for event in events:
            day_key = datetime.fromtimestamp(event.device_ts, tz=timezone.utc).date().isoformat()
            grouped[day_key].append(event)
        total = 0
        for day_key, chunk in grouped.items():
            total += self.raw_storage.append(day_key, chunk)

        if db_dsn:
            try:
                import psycopg

                rows = []
                for ev in events:
                    rows.append(
                        (
                            ev.unit_id,
                            ev.device_ts,
                            ev.received_ts or ev.device_ts,
                            ev.latitude,
                            ev.longitude,
                            ev.speed,
                            ev.course,
                            ev.params or {},
                            ev.raw_payload or {},
                        )
                    )
                if rows:
                    with psycopg.connect(db_dsn, autocommit=True) as conn:
                        cur = conn.cursor()
                        cur.executemany(
                            """
                            INSERT INTO events (unit_id, device_ts, received_ts, lat, lon, speed, course, params, raw)
                            VALUES (%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            rows,
                        )
            except Exception as exc:  # pragma: no cover
                _log.warning("raw_storage: failed to insert events into DB: %s", exc)

        self.latest_store.update_from_events(events)
        return total


_SERVICE: PipelineStorageService | None = None
_SERVICE_LOCK = Lock()


def get_pipeline_storage_service() -> PipelineStorageService:
    """Singleton-like accessor to avoid re-reading config in hot paths."""

    global _SERVICE
    if _SERVICE is not None:
        return _SERVICE
    with _SERVICE_LOCK:
        if _SERVICE is None:
            config = load_from_env()
            storage_root = Path(config.storage_root)
            raw_storage = RawStorage(storage_root)
            latest_store = LatestTelemetryStore(storage_root)
            _SERVICE = PipelineStorageService(
                config=config,
                raw_storage=raw_storage,
                latest_store=latest_store,
            )
    return _SERVICE


def _haversine_m(lat1: Any, lon1: Any, lat2: Any, lon2: Any) -> Optional[float]:
    try:
        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)
    except (TypeError, ValueError):
        return None
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    km = 6371.0 * c
    return km * 1000.0
