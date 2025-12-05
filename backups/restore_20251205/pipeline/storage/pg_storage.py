"""Postgres / TimescaleDB storage for raw telemetry events."""

from __future__ import annotations

import json
import os
import time
import logging
from typing import Sequence

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover - defensive
    psycopg = None

from pipeline.events import Event

log = logging.getLogger(__name__)


class PgEventStorage:
    """Batch writer of events into Postgres/Timescale."""

    def __init__(self, dsn: str | None = None) -> None:
        if psycopg is None:
            log.warning("PgEventStorage disabled: psycopg not installed")
            self.enabled = False
            self.dsn = dsn
            return
        self.dsn = dsn or os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
        if not self.dsn:
            host = os.getenv("PGHOST", "127.0.0.1")
            user = os.getenv("PGUSER", "smt_user")
            pwd = os.getenv("PGPASSWORD", "smt_password")
            db = os.getenv("PGDATABASE", "smt_telematics")
            self.dsn = f"postgresql://{user}:{pwd}@{host}:5432/{db}"
        self.enabled = True

    def store_events(self, events: Sequence[Event]) -> int:
        """Insert a batch of events. Returns number of inserted rows."""

        if not events:
            return 0
        if not getattr(self, "enabled", False):
            return 0

        rows = []
        for e in events:
            # Convert unix ts to timestamptz strings acceptable by Postgres
            d_ts = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime(e.device_ts))
            r_ts = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime(e.received_ts))
            rows.append(
                (
                    d_ts,
                    r_ts,
                    e.unit_id,
                    e.latitude,
                    e.longitude,
                    e.speed,
                    e.course,
                    json.dumps(e.params or {}, ensure_ascii=False),
                    json.dumps(e.raw_payload or {}, ensure_ascii=False),
                )
            )

        try:
            with psycopg.connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO events
                        (device_ts, received_ts, unit_id, lat, lon, speed, course, params, raw)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        rows,
                    )
                conn.commit()
            return len(rows)
        except Exception as exc:  # pragma: no cover - defensive
            log.error("PgEventStorage: insert failed: %s", exc)
            return 0
