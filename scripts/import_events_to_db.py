"""Import existing events*.jsonl files into Postgres events table.

Usage:
    .venv/bin/python scripts/import_events_to_db.py

Env:
    DEVICE_REGISTRY_DSN or DATABASE_URL — Postgres DSN
"""

from __future__ import annotations

import glob
import json
import os
from pathlib import Path

import psycopg
from psycopg.types.json import Json

DSN = os.environ.get("DEVICE_REGISTRY_DSN") or os.environ.get("DATABASE_URL")
if not DSN:
    raise SystemExit("DEVICE_REGISTRY_DSN or DATABASE_URL is required")

paths = sorted(glob.glob("data/pipeline_storage/*/events*.jsonl"))
print(f"Found {len(paths)} files")

total = 0
with psycopg.connect(DSN, autocommit=True) as conn:
    cur = conn.cursor()
    for path in paths:
        p = Path(path)
        with p.open("r", encoding="utf-8") as fh:
            rows = [json.loads(line) for line in fh if line.strip()]
        if not rows:
            continue
        batch = [
            (
                r["unit_id"],
                r["device_ts"],
                r.get("received_ts") or r["device_ts"],
                r.get("latitude"),
                r.get("longitude"),
                r.get("speed"),
                r.get("course"),
                Json(r.get("params") or {}),
                Json(r.get("raw_payload") or {}),
            )
            for r in rows
        ]
        cur.executemany(
            """
            INSERT INTO events (unit_id, device_ts, received_ts, lat, lon, speed, course, params, raw)
            VALUES (%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            batch,
        )
        total += len(batch)
        print(f"imported {p} -> {len(batch)} rows")
print(f"total imported rows: {total}")
