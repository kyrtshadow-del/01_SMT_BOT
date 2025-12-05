#!/usr/bin/env python3
"""Import unit_snapshot.json.gz into Postgres with hybrid sensor config.

Usage:
  DATABASE_URL=postgresql://user:pass@localhost:5432/dbname \
  python scripts/migrate_snapshot_to_pg.py [--priority-default 50] [--protocol-default wialon_api] [--dry-run]

The script is idempotent: it upserts units/devices/links by (unit_id) and (protocol, uid).
It expects tables defined in pipeline/db/schema_pg.sql.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import psycopg
except ImportError:  # pragma: no cover - allows running dry-run without driver
    psycopg = None  # type: ignore

from pipeline.services.unit_snapshot_service import UnitSnapshotService, UnitSensorMeta, UnitSnapshotRecord

LOG = logging.getLogger("migrate_snapshot")
BASE_DIR = Path(__file__).resolve().parents[1]
SNAPSHOT_PATH = BASE_DIR / "data" / "unit_snapshot.json.gz"


@dataclass
class RegistryEntry:
    unit_id: int
    name: str
    reg_number: Optional[str]
    device_uid: Optional[str]
    hardware: Optional[str]
    firmware: Optional[str]
    sensors_config: Dict[str, Any]


# -------------------------- sensor conversion ---------------------------

def _normalize_pairs(raw: Any) -> List[Tuple[float, float]]:
    pairs: List[Tuple[float, float]] = []
    if not raw:
        return pairs
    if isinstance(raw, dict):
        # support legacy dict calibration {"x":..., "a":..., "b":...}
        if {"x", "a", "b"}.issubset(raw.keys()):
            x = float(raw["x"])
            y = float(raw["a"]) * x + float(raw["b"])
            pairs.append((x, y))
            return pairs
        # unknown shape
        return pairs
    for item in raw:
        try:
            if isinstance(item, dict) and {"x", "y"}.issubset(item.keys()):
                pairs.append((float(item["x"]), float(item["y"])))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                pairs.append((float(item[0]), float(item[1])))
        except Exception:
            continue
    return pairs


def _detect_formula_from_pairs(pairs: List[Tuple[float, float]]) -> Optional[str]:
    if len(pairs) != 2:
        return None
    (x0, y0), (x1, y1) = pairs
    if abs(x1 - x0) < 1e-6:
        return None
    # prefer origin at zero
    if abs(y0) < 1e-6:
        k = (y1 - y0) / (x1 - x0)
        return f"x * {k:.8f}".rstrip("0").rstrip(".")
    # generic linear with offset
    k = (y1 - y0) / (x1 - x0)
    b = y0 - k * x0
    return f"x * {k:.8f} + {b:.8f}".rstrip("0").rstrip(".")


def _detect_ignition_threshold(pairs: List[Tuple[float, float]]) -> Optional[str]:
    if len(pairs) < 2:
        return None
    # find first switch from 0 to 1
    sorted_pairs = sorted(pairs, key=lambda p: p[0])
    for i in range(1, len(sorted_pairs)):
        prev_y = sorted_pairs[i - 1][1]
        curr_y = sorted_pairs[i][1]
        if prev_y <= 0 < curr_y:
            threshold = sorted_pairs[i][0]
            return f"x > {threshold}"
    return None


def _extract_input_param(sensor: UnitSensorMeta) -> Optional[str]:
    for key in ("p", "param", "src", "source", "s"):
        if key in sensor.meta and sensor.meta[key]:
            return str(sensor.meta[key]).strip()
    return None


def convert_sensor(sensor: UnitSensorMeta) -> Optional[Dict[str, Any]]:
    input_param = _extract_input_param(sensor)
    if not input_param:
        return None

    name = sensor.name or f"sensor_{sensor.sensor_id or input_param}"
    sensor_type = (sensor.type or "").lower()
    pairs = _normalize_pairs(sensor.calibration)

    strategy = "table"
    params: Dict[str, Any] = {}

    if sensor_type in {"ignition", "ign", "bool"}:
        expr = _detect_ignition_threshold(pairs)
        if expr:
            strategy = "formula"
            params = {"expression": expr}
        else:
            strategy = "table"
            params = {"pairs": [{"x": x, "y": y} for x, y in pairs]}
    elif sensor_type in {"voltage", "temp", "temperature", "rpm", "weight"}:
        expr = _detect_formula_from_pairs(pairs)
        if expr:
            strategy = "formula"
            params = {"expression": expr}
        else:
            strategy = "table"
            params = {"pairs": [{"x": x, "y": y} for x, y in pairs]}
    elif sensor_type in {"fuel", "fuel_level", "dut"}:
        strategy = "table"
        params = {"pairs": [{"x": x, "y": y} for x, y in pairs]}
    else:
        # fallback: keep calibration if present
        if pairs:
            params = {"pairs": [{"x": x, "y": y} for x, y in pairs]}
        else:
            params = {}

    filters: Dict[str, Any] = {}
    if sensor.meta.get("skip_zero"):
        filters["skip_zero"] = True
    if sensor.meta.get("median"):
        try:
            filters["median"] = int(sensor.meta["median"])
        except Exception:
            pass

    sensor_id = sensor.sensor_id or f"w_{input_param}"
    return {
        "id": str(sensor_id),
        "name": name,
        "strategy": strategy,
        "input_param": input_param,
        "output_unit": sensor.units,
        "params": params,
        "filters": filters,
    }


def convert_record(record: UnitSnapshotRecord) -> RegistryEntry:
    sensors: List[Dict[str, Any]] = []
    for sensor in record.sensors:
        converted = convert_sensor(sensor)
        if converted:
            sensors.append(converted)

    sensors_config = {"version": 2, "sensors": sensors}

    return RegistryEntry(
        unit_id=record.unit_id,
        name=record.name,
        reg_number=record.reg_number,
        device_uid=record.device.uid,
        hardware=record.device.hardware,
        firmware=record.device.firmware,
        sensors_config=sensors_config,
    )


# ------------------------------- db io -------------------------------

def get_dsn(args: argparse.Namespace) -> str:
    if args.dsn:
        return args.dsn
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    dbname = os.getenv("PGDATABASE", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def ensure_driver():
    if psycopg is None:
        LOG.error("psycopg is not installed. Install psycopg[binary] or psycopg2 to use this script.")
        raise SystemExit(1)


def upsert_batch(conn, entries: Iterable[RegistryEntry], *, default_priority: int, default_protocol: str, dry_run: bool) -> None:
    cur = conn.cursor()
    for entry in entries:
        if entry.device_uid:
            protocol = default_protocol
        else:
            LOG.warning("unit %s skipped: no device UID", entry.unit_id)
            continue

        LOG.debug("upsert unit=%s uid=%s protocol=%s", entry.unit_id, entry.device_uid, protocol)
        if not dry_run:
            cur.execute(
                """
                INSERT INTO units (id, name, reg_number, owner_node_id, sensors_config)
                VALUES (%s, %s, %s, NULL, %s)
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    reg_number = EXCLUDED.reg_number,
                    sensors_config = EXCLUDED.sensors_config,
                    updated_at = NOW()
                """,
                (
                    entry.unit_id,
                    entry.name or str(entry.unit_id),
                    entry.reg_number,
                    json.dumps(entry.sensors_config),
                ),
            )
            cur.execute(
                """
                INSERT INTO devices (protocol, uid, hardware, firmware, is_active)
                VALUES (%s, %s, %s, %s, TRUE)
                ON CONFLICT (protocol, uid) DO UPDATE
                SET hardware = COALESCE(EXCLUDED.hardware, devices.hardware),
                    firmware = COALESCE(EXCLUDED.firmware, devices.firmware),
                    is_active = TRUE,
                    updated_at = NOW()
                RETURNING id
                """,
                (
                    protocol,
                    entry.device_uid,
                    entry.hardware,
                    entry.firmware,
                ),
            )
            device_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO unit_device_links (unit_id, device_id, priority)
                VALUES (%s, %s, %s)
                ON CONFLICT ON CONSTRAINT ux_links_device_active
                DO UPDATE SET unit_id = EXCLUDED.unit_id, priority = EXCLUDED.priority, updated_at = NOW()
                """,
                (entry.unit_id, device_id, default_priority),
            )
    if not dry_run:
        conn.commit()


# ------------------------------- cli -------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import unit_snapshot into Postgres (units/devices/links)")
    parser.add_argument("--dsn", help="Postgres DSN (default: env DATABASE_URL / PG* vars)")
    parser.add_argument("--priority-default", type=int, default=50, help="Default link priority for imported devices")
    parser.add_argument("--protocol-default", default="wialon_api", help="Protocol name to set for imported devices")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB, only show summary")
    parser.add_argument("--snapshot", default=str(SNAPSHOT_PATH), help="Path to unit_snapshot.json.gz")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def load_snapshot(path: Path) -> List[UnitSnapshotRecord]:
    service = UnitSnapshotService(snapshot_path=path)
    bundle = service.load_bundle()
    return list(bundle.units.values())


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    snap_path = Path(args.snapshot)
    if not snap_path.exists():
        raise SystemExit(f"Snapshot not found: {snap_path}")

    records = load_snapshot(snap_path)
    LOG.info("Loaded %s units from %s", len(records), snap_path)

    entries = [convert_record(rec) for rec in records]
    LOG.info("Prepared %s registry entries", len(entries))

    if args.dry_run:
        LOG.info("Dry-run mode: nothing will be written. Example entry: %s", entries[:1])
        return

    ensure_driver()
    dsn = get_dsn(args)
    LOG.info("Connecting to %s", dsn)
    with psycopg.connect(dsn) as conn:
        upsert_batch(conn, entries, default_priority=args.priority_default, default_protocol=args.protocol_default, dry_run=args.dry_run)
    LOG.info("Done")


if __name__ == "__main__":
    main()
