"""Helpers to access pipeline storage from application code (SQL-first)."""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.config.defaults import PipelineConfig, load_from_env
from pipeline.events import Event
from pipeline.services.unit_index import get_unit_index
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.storage.raw_storage import RawStorage

log = logging.getLogger("pipeline.storage")


@dataclass
class StoredEvent:
    day: str
    event: Event


@dataclass
class PipelineStorageService:
    """SQL-first storage access.

    Reads from PostgreSQL for speed.
    Writes to both PostgreSQL (hot) and RawStorage (cold backup).
    """

    config: PipelineConfig
    raw_storage: RawStorage
    latest_store: LatestTelemetryStore
    db_dsn: str

    @property
    def storage_root(self) -> Path:
        return self.raw_storage.root

    # ------------------------------------------------------------------ #
    # Low-level DB helpers                                               #
    # ------------------------------------------------------------------ #

    def _get_conn(self) -> psycopg.Connection:
        """Create a new DB connection.

        For now we rely on simple connections; if needed we can
        introduce a pool at the process level.
        """

        return psycopg.connect(self.db_dsn, row_factory=dict_row)

    # ------------------------------------------------------------------ #
    # Read API                                                           #
    # ------------------------------------------------------------------ #

    def fetch_period(self, unit_id: int, start_ts: int, end_ts: int) -> Sequence[Event]:
        """Fetch events for unit from DB between timestamps (inclusive)."""

        events: List[Event] = []
        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT
                      unit_id,
                      EXTRACT(EPOCH FROM device_ts)::bigint AS device_ts,
                      EXTRACT(EPOCH FROM received_ts)::bigint AS received_ts,
                      lat,
                      lon,
                      speed,
                      course,
                      params,
                      raw
                    FROM events
                    WHERE unit_id = %s
                      AND device_ts >= to_timestamp(%s)
                      AND device_ts <= to_timestamp(%s)
                    ORDER BY device_ts ASC
                    """,
                    (unit_id, start_ts, end_ts),
                )
                for row in cur.fetchall():
                    events.append(
                        Event(
                            unit_id=row["unit_id"],
                            device_ts=int(row["device_ts"]),
                            received_ts=int(row["received_ts"] or row["device_ts"]),
                            latitude=row["lat"],
                            longitude=row["lon"],
                            speed=row["speed"],
                            course=row["course"],
                            params=row.get("params") or {},
                            source="db",
                            raw_payload=row.get("raw"),
                        )
                    )
        except Exception as exc:  # pragma: no cover - defensive
            log.error("DB fetch_period error unit_id=%s err=%s", unit_id, exc)
        return events

    def fetch_day(self, day: str, unit_id: Optional[int] = None) -> Sequence[Event]:
        """Fetch history for a given day from Postgres.

        - With unit_id → read from DB (primary path).
        - Without unit_id → read all units for that day from DB.
        """

        try:
            dt_start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return []
        start_ts = int(dt_start.timestamp())
        end_ts = start_ts + 86400

        if unit_id is not None:
            return self.fetch_period(unit_id, start_ts, end_ts)

        events: List[Event] = []
        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT
                      unit_id,
                      EXTRACT(EPOCH FROM device_ts)::bigint AS device_ts,
                      EXTRACT(EPOCH FROM received_ts)::bigint AS received_ts,
                      lat,
                      lon,
                      speed,
                      course,
                      params,
                      raw
                    FROM events
                    WHERE device_ts >= to_timestamp(%s)
                      AND device_ts < to_timestamp(%s)
                    ORDER BY device_ts ASC
                    """,
                    (start_ts, end_ts),
                )
                for row in cur.fetchall():
                    events.append(
                        Event(
                            unit_id=row["unit_id"],
                            device_ts=row["device_ts"],
                            received_ts=row["received_ts"],
                            latitude=row["lat"],
                            longitude=row["lon"],
                            speed=row["speed"],
                            course=row["course"],
                            params=row.get("params") or {},
                            source="db",
                            raw_payload=row.get("raw"),
                        )
                    )
        except Exception as exc:  # pragma: no cover - defensive
            log.error("DB fetch_day(all units) error day=%s err=%s", day, exc)
        return events

    def list_days(self) -> Sequence[str]:
        """List distinct days that have events in Postgres."""

        days: List[str] = []
        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT DISTINCT to_char(device_ts::date, 'YYYY-MM-DD') AS day
                    FROM events
                    ORDER BY day
                    """
                )
                for row in cur.fetchall():
                    days.append(row["day"])
        except Exception as exc:  # pragma: no cover - defensive
            log.error("DB list_days error: %s", exc)
        return days

    def find_latest_event(self, unit_id: int, max_days: int = 30) -> Optional[StoredEvent]:  # noqa: ARG002
        """Return strict latest event for unit from DB."""

        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT
                      unit_id,
                      EXTRACT(EPOCH FROM device_ts)::bigint AS device_ts,
                      lat,
                      lon,
                      speed,
                      course,
                      params
                    FROM events
                    WHERE unit_id = %s
                    ORDER BY device_ts DESC
                    LIMIT 1
                    """,
                    (unit_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                ts = int(row["device_ts"])
                ev = Event(
                    unit_id=row["unit_id"],
                    device_ts=ts,
                    received_ts=ts,
                    latitude=row["lat"],
                    longitude=row["lon"],
                    speed=row["speed"],
                    course=row["course"],
                    params=row.get("params") or {},
                    source="db",
                )
                day_key = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
                return StoredEvent(day=day_key, event=ev)
        except Exception as exc:  # pragma: no cover - defensive
            log.error("DB latest fetch error unit_id=%s err=%s", unit_id, exc)
        return None

    def find_latest_event_fast(self, unit_id: int, *, lookback_days: int | None = None) -> Optional[StoredEvent]:  # noqa: ARG002
        """Fast path: in SQL version same as strict latest (indexed)."""

        return self.find_latest_event(unit_id)

    def get_latest_event_from_metrics(self, unit_id: int) -> Optional[StoredEvent]:
        """Construct the latest Event directly from latest_metrics cache."""

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
            raw_payload={"params": params, "device_ts": device_ts, "received_ts": received_ts},
        )
        day_key = datetime.fromtimestamp(device_ts, tz=timezone.utc).date().isoformat()
        return StoredEvent(day=day_key, event=event)

    def get_latest_metrics(self, unit_id: int) -> Optional[dict]:
        """Return cached latest metrics, falling back to DB if needed."""

        payload = self.latest_store.get_latest(unit_id)
        if payload:
            return dict(payload)

        last = self.find_latest_event(unit_id)
        if last and last.event:
            ev = last.event
            return {
                "device_ts": ev.device_ts,
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
        """Find units near the given coordinates using latest telemetry cache."""

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

    # ------------------------------------------------------------------ #
    # Write API                                                          #
    # ------------------------------------------------------------------ #

    def store_events(self, events: Sequence[Event]) -> int:
        """Persist events: hot to DB, cold to files, update latest cache."""

        if not events:
            return 0

        # 1) Cold archive: group by day and append to RawStorage
        grouped: Dict[str, List[Event]] = {}
        for ev in events:
            day_key = datetime.fromtimestamp(ev.device_ts, tz=timezone.utc).date().isoformat()
            grouped.setdefault(day_key, []).append(ev)

        for day_key, chunk in grouped.items():
            try:
                self.raw_storage.append(day_key, chunk)
            except Exception as exc:  # pragma: no cover - defensive
                log.error("RawStorage append failed day=%s err=%s", day_key, exc)

        # 2) Primary storage: insert into Postgres
        inserted = 0
        try:
            rows = []
            for ev in events:
                rows.append(
                    (
                        ev.unit_id,
                        datetime.fromtimestamp(ev.device_ts, tz=timezone.utc),
                        datetime.fromtimestamp(ev.received_ts or ev.device_ts, tz=timezone.utc),
                        ev.latitude,
                        ev.longitude,
                        ev.speed,
                        ev.course,
                        json.dumps(ev.params or {}, ensure_ascii=False),
                        json.dumps(ev.raw_payload or {}, ensure_ascii=False),
                    )
                )
            if rows:
                with self._get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO events (
                              unit_id,
                              device_ts,
                              received_ts,
                              lat,
                              lon,
                              speed,
                              course,
                              params,
                              raw
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (unit_id, device_ts) DO NOTHING
                            """,
                            rows,
                        )
                    inserted = len(rows)
        except Exception as exc:  # pragma: no cover - defensive
            log.error("DB insert failed for %s events: %s", len(events), exc)

        # 3) Update latest telemetry cache for online/nearby checks
        try:
            self.latest_store.update_from_events(events)
        except Exception as exc:  # pragma: no cover - defensive
            log.error("LatestTelemetryStore update failed: %s", exc)

        return inserted


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

            dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
            if not dsn:
                raise ValueError("DATABASE_URL or DEVICE_REGISTRY_DSN env var is required for SQL storage")

            raw_storage = RawStorage(storage_root)
            latest_store = LatestTelemetryStore(storage_root)
            _SERVICE = PipelineStorageService(
                config=config,
                raw_storage=raw_storage,
                latest_store=latest_store,
                db_dsn=dsn,
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
