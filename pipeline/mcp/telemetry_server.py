from __future__ import annotations

"""MCP server exposing telemetry and health tools for SMT Local Bot.

Tools are deliberately read-only and operate on existing local artefacts:

- data/unit_snapshot.v2.json.gz  (primary catalogue for units)
- data/pipeline_storage/latest_metrics.json
- logs/*_ingest_health.json / logs/*_ingest.prom
- logs/stream_runner.log

The server is meant to be started via:

    PYTHONPATH=. python -m pipeline.mcp.telemetry_server

or packaged into a Docker image and used as an MCP server command.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from mcp.server.fastmcp import FastMCP

from pipeline.config.defaults import load_from_env
from pipeline.services.unit_snapshot_v2 import (
    UnitSnapshotV2Bundle,
    UnitSnapshotV2Record,
    UnitSnapshotV2Service,
)
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.services.shadow_service import ShadowService
from pipeline.services.device_registry import DeviceRegistry


log = logging.getLogger("pipeline.mcp.telemetry")

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
LOGS_DIR = REPO_ROOT / "logs"

SNAPSHOT_V2_PATH = (DATA_DIR / "unit_snapshot.v2.json.gz").resolve()
LATEST_METRICS_ROOT = (DATA_DIR / "pipeline_storage").resolve()
STREAM_LOG_PATH = (LOGS_DIR / "stream_runner.log").resolve()

WIALON_INGEST_HEALTH = (LOGS_DIR / "wialon_ips_ingest_health.json").resolve()
WIALON_INGEST_PROM = (LOGS_DIR / "wialon_ips_ingest.prom").resolve()
GAL_INGEST_HEALTH = (LOGS_DIR / "galileosky_ingest_health.json").resolve()
GAL_INGEST_PROM = (LOGS_DIR / "galileosky_ingest.prom").resolve()


def _ensure_storage_env() -> None:
    """Ensure PIPELINE_STORAGE_ROOT points to the repo-local storage by default.

    This keeps LatestTelemetryStore behaviour consistent regardless of the
    current working directory used to launch the MCP server.
    """

    if "PIPELINE_STORAGE_ROOT" not in os.environ:
        os.environ["PIPELINE_STORAGE_ROOT"] = str(LATEST_METRICS_ROOT)


def _load_snapshot_bundle() -> UnitSnapshotV2Bundle:
    service = UnitSnapshotV2Service(snapshot_path=SNAPSHOT_V2_PATH)
    try:
        bundle = service.load_bundle()
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("telemetry_mcp: failed to load snapshot_v2: %s", exc)
        return UnitSnapshotV2Bundle.empty()
    return bundle


def _load_latest_store() -> LatestTelemetryStore:
    _ensure_storage_env()
    cfg = load_from_env()
    return LatestTelemetryStore(cfg.storage_root)


def _read_json_file(path: Path) -> Optional[Mapping[str, Any]]:
    if not path.exists():
        return None
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
        return {"_raw": data}
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("telemetry_mcp: failed to read json %s: %s", path, exc)
        return None


def _read_text_tail(path: Path, max_lines: int) -> Tuple[int, str]:
    if max_lines <= 0:
        return 0, ""
    if not path.exists():
        return 0, ""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("telemetry_mcp: failed to read log %s: %s", path, exc)
        return 0, ""
    if not lines:
        return 0, ""
    tail = lines[-max_lines:]
    return len(tail), "\n".join(tail)


mcp = FastMCP("SMT Telemetry MCP")


@mcp.tool()
def get_unit_state(unit_id: int) -> Dict[str, Any]:
    """Return unit state from snapshot_v2 for the given unit_id.

    Response:
        {
            "found": bool,
            "snapshot_dump_ts": int | None,
            "unit": {...}  # UnitSnapshotV2Record.to_dict(), present when found
        }
    """

    bundle = _load_snapshot_bundle()
    record: Optional[UnitSnapshotV2Record] = bundle.units.get(int(unit_id)) if bundle.units else None
    if not record:
        return {
            "found": False,
            "snapshot_dump_ts": bundle.dump_ts,
            "unit": None,
        }
    return {
        "found": True,
        "snapshot_dump_ts": bundle.dump_ts,
        "unit": record.to_dict(),
    }


@mcp.tool()
def list_shadow_devices(limit: int = 200) -> Dict[str, Any]:
    """Return ShadowService devices (unknown protocol/uid pairs).

    Response:
        {
            "enabled": bool,
            "count": int,
            "devices": [
                {
                    "protocol": str,
                    "uid": str,
                    "last_seen_ts": int,
                    "last_ip": str | None,
                    "params_seen": list[str],
                    "lat": float | None,
                    "lon": float | None,
                    "linked_unit_id": int | None
                },
                ...
            ],
        }
    """

    shadow = ShadowService(redis_url=os.getenv("REDIS_URL"))
    if not shadow.enabled:
        return {"enabled": False, "count": 0, "devices": []}

    # try to load DeviceRegistry to mark already linked devices
    registry = None
    try:
        registry = DeviceRegistry()
        if not registry.enabled:
            registry = None
        else:
            registry.load()
    except Exception:
        registry = None

    keys = shadow.list_keys(limit=limit)
    devices: List[Dict[str, Any]] = []
    for key in keys:
        try:
            _, _, protocol, uid = key.split(":", 3)
        except ValueError:
            continue
        rec = shadow.get(key) or {}
        try:
            last_seen_ts = int(rec.get("last_seen_ts") or 0)
        except Exception:
            last_seen_ts = 0
        params_seen: List[str] = []
        try:
            raw_params = rec.get("params_seen") or "[]"
            params = json.loads(raw_params)
            if isinstance(params, list):
                params_seen = [str(p) for p in params][:50]
        except Exception:
            params_seen = []
        lat = lon = None
        last_sample = rec.get("last_sample")
        if last_sample:
            try:
                sample = json.loads(last_sample)
                if isinstance(sample, dict):
                    lat = sample.get("lat") or sample.get("latitude")
                    lon = sample.get("lon") or sample.get("longitude")
            except Exception:
                lat = lon = None

        linked_unit_id: Optional[int] = None
        if registry is not None:
            entry = registry.resolve(protocol, uid)
            if entry is not None:
                linked_unit_id = int(entry.unit_id)

        devices.append(
            {
                "protocol": protocol,
                "uid": uid,
                "last_seen_ts": last_seen_ts,
                "last_ip": rec.get("last_ip") or None,
                "params_seen": params_seen,
                "lat": lat,
                "lon": lon,
                "linked_unit_id": linked_unit_id,
            }
        )

    devices.sort(key=lambda d: d.get("last_seen_ts") or 0, reverse=True)
    return {"enabled": True, "count": len(devices), "devices": devices}


@mcp.tool()
def get_latest_metrics(unit_id: int) -> Dict[str, Any]:
    """Return latest_metrics entry for a unit_id (if present).

    Response:
        {
            "found": bool,
            "metrics_mtime": float,
            "metrics": {...} | None
        }
    """

    store = _load_latest_store()
    entry = store.get_latest(int(unit_id))
    return {
        "found": bool(entry),
        "metrics_mtime": float(store.get_mtime()),
        "metrics": dict(entry) if entry else None,
    }


@mcp.tool()
def get_ingest_health() -> Dict[str, Any]:
    """Return ingestion health snapshot for all known sources.

    Reads compact JSON health files and .prom text metrics for:
    - wialon_ips
    - galileosky
    """

    def _read_prom(path: Path) -> Optional[str]:
        if not path.exists():
            return None
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("telemetry_mcp: failed to read prom %s: %s", path, exc)
            return None

    return {
        "wialon_ips": {
            "health": _read_json_file(WIALON_INGEST_HEALTH),
            "prom": _read_prom(WIALON_INGEST_PROM),
        },
        "galileosky": {
            "health": _read_json_file(GAL_INGEST_HEALTH),
            "prom": _read_prom(GAL_INGEST_PROM),
        },
    }


@mcp.tool()
def tail_stream_log(lines: int = 200) -> Dict[str, Any]:
    """Return the tail of the main stream_runner log.

    Args:
        lines: maximum number of lines to return (default: 200).
    """

    count, tail = _read_text_tail(STREAM_LOG_PATH, max_lines=max(1, min(int(lines), 2000)))
    return {
        "path": str(STREAM_LOG_PATH),
        "lines": count,
        "content": tail,
    }


def main() -> None:
    """Entry point for running the MCP server."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log.info("telemetry_mcp: starting MCP server repo_root=%s", REPO_ROOT)
    _ensure_storage_env()
    mcp.run()


if __name__ == "__main__":
    main()
