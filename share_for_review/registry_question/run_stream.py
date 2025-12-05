"""Run the Wialon streaming ingestion loop."""

from __future__ import annotations

import argparse
import os
import asyncio
import contextlib
import gzip
import json
import logging
from logging.handlers import RotatingFileHandler
import signal
from dataclasses import replace
from pathlib import Path
from typing import List, Sequence

import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.config.defaults import load_from_env
from pipeline.services.unit_snapshot_service import UnitSnapshotService
from pipeline.sources import PollStrategy, SourceOverrides, get_source_provider
from pipeline.storage.raw_storage import RawStorage
from pipeline.storage.latest_metrics import LatestTelemetryStore
from pipeline.monitoring.health_http import start_health_http
from pipeline.services.device_registry import DeviceRegistry
from pipeline.services.shadow_service import ShadowService, ShadowPacket
from pipeline.events import Event, RawPacket
from pipeline.services.unit_snapshot_service import UnitSnapshotService
from pipeline.services.unit_snapshot_v2 import get_unit_snapshot_v2_service
LEGACY_UNIT_CACHE_PATH = (REPO_ROOT / "data" / "units.v1.json.gz").resolve()
SNAPSHOT_PATH = (REPO_ROOT / "data" / "unit_snapshot.json.gz").resolve()
DEFAULT_LOG_FILE = (REPO_ROOT / "logs" / "stream_runner.log").resolve()
def _resolve_unit_ids(spec: str) -> List[int]:
    text = (spec or "").strip()
    if not text:
        raise SystemExit("At least one unit ID is required.")
    lowered = text.lower()
    if lowered in {"cached", "all"}:
        unit_ids = _load_unit_ids_from_cache()
        if not unit_ids:
            raise SystemExit(
                "Unit cache is empty. Run the main bot once or refresh the units snapshot, "
                "then retry with '--units cached'."
            )
        log.info("stream_runner: resolved %s cached units", len(unit_ids))
        return unit_ids
    if lowered.startswith("@"):
        path = Path(text[1:]).expanduser()
        unit_ids = _load_unit_ids_from_file(path)
        log.info("stream_runner: resolved %s units from %s", len(unit_ids), path)
        return unit_ids
    try:
        unit_ids = [int(part) for part in text.split(",") if part.strip()]
    except ValueError as exc:
        raise SystemExit(f"Invalid unit ID in --units: {exc}") from exc
    if not unit_ids:
        raise SystemExit("At least one unit ID is required.")
    return unit_ids


def _load_unit_ids_from_file(path: Path) -> List[int]:
    if not path.exists():
        raise SystemExit(f"Units file not found: {path}")
    try:
        payload = path.read_text(encoding="utf-8")
    except Exception as exc:  # pragma: no cover - defensive
        raise SystemExit(f"Failed to read units file {path}: {exc}") from exc
    unit_ids: List[int] = []
    for chunk in payload.replace("\n", ",").split(","):
        token = chunk.strip()
        if not token:
            continue
        try:
            unit_ids.append(int(token))
        except ValueError as exc:
            raise SystemExit(f"Invalid unit ID '{token}' in {path}") from exc
    if not unit_ids:
        raise SystemExit(f"No unit IDs found in {path}")
    # Remove duplicates but preserve order
    seen = set()
    deduped: List[int] = []
    for uid in unit_ids:
        if uid in seen:
            continue
        seen.add(uid)
        deduped.append(uid)
    return deduped


def _load_unit_ids_from_cache() -> List[int]:
    unit_ids = _load_unit_ids_from_snapshot()
    if unit_ids:
        log.info("stream_runner: resolved %s units from unit_snapshot", len(unit_ids))
        return unit_ids
    return _load_unit_ids_from_legacy()


def _load_unit_ids_from_snapshot() -> List[int]:
    if not SNAPSHOT_PATH.exists():
        return []
    service = UnitSnapshotService(snapshot_path=SNAPSHOT_PATH, legacy_path=LEGACY_UNIT_CACHE_PATH)
    bundle = service.load_bundle()
    if not bundle.units:
        return []
    return sorted(bundle.units.keys())


def _load_unit_ids_from_legacy() -> List[int]:
    if not LEGACY_UNIT_CACHE_PATH.exists():
        raise SystemExit(
            f"Units cache {LEGACY_UNIT_CACHE_PATH} is missing. Launch the bot once to refresh it "
            "or provide --units manually."
        )
    try:
        with gzip.open(LEGACY_UNIT_CACHE_PATH, "rt", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as exc:  # pragma: no cover - defensive
        raise SystemExit(f"Failed to read {LEGACY_UNIT_CACHE_PATH}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"Unexpected payload in {LEGACY_UNIT_CACHE_PATH}: {type(payload).__name__}")
    items = payload.get("items_by_id")
    if not isinstance(items, dict):
        raise SystemExit("units.v1 cache does not contain items_by_id")
    unit_ids: List[int] = []
    for key in items.keys():
        try:
            unit_ids.append(int(key))
        except Exception:
            continue
    unit_ids.sort()
    return unit_ids


log = logging.getLogger("pipeline.stream_runner")

def _build_uid_map_fallback() -> dict[str, int]:
    """Collect uid->unit_id from legacy snapshot (v1) and new snapshot v2."""
    mapping: dict[str, int] = {}
    # legacy
    try:
        service = UnitSnapshotService(snapshot_path=SNAPSHOT_PATH, legacy_path=LEGACY_UNIT_CACHE_PATH)
        for rec in service.iter_units():
            if rec.device.uid:
                mapping[str(rec.device.uid)] = rec.unit_id
    except Exception:
        pass
    # v2
    try:
        v2 = get_unit_snapshot_v2_service().load_bundle()
        if v2 and v2.units:
            for uid, rec in v2.units.items():
                dev = rec.device or {}
                uid_val = dev.get("uid")
                if uid_val:
                    mapping[str(uid_val)] = int(rec.unit_id)
    except Exception:
        pass
    return mapping

def _synthetic_unit_id(uid: str) -> int:
    import zlib
    return zlib.crc32(uid.encode()) & 0x7FFFFFFF


def _setup_logging(log_file: Path | None, verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    log_file = log_file or DEFAULT_LOG_FILE
    log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logging.basicConfig(
        level=level,
        handlers=[stream_handler, file_handler],
        force=True,
    )
    log.info("stream_runner: logging to %s level=%s", log_file, logging.getLevelName(level))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Wialon messages into local storage")
    parser.add_argument(
        "--units",
        required=False,
        help=(
            "Comma-separated unit IDs, '@path/to/file' to read IDs from file, "
            "or 'cached'/'all' (Wialon only). For direct push sources (galileosky) "
            "parameter can be omitted."
        ),
    )
    parser.add_argument("--interval", type=float, default=5.0, help="Poll interval in seconds")
    parser.add_argument("--token", help="Override Wialon token (defaults to env)")
    parser.add_argument("--host", help="Override Wialon host (defaults to env)")
    parser.add_argument("--storage-root", help="Override storage root path")
    parser.add_argument("--metrics-interval", type=int, default=60, help="Seconds between health logs")
    parser.add_argument("--log-file", help="Path to stream runner log file (defaults to logs/stream_runner.log)")
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging to console/log file")
    parser.add_argument(
        "--source-kind",
        help="Override pipeline source kind (defaults to PIPELINE_SOURCE_KIND env var)",
    )
    parser.add_argument(
        "--registry-dsn",
        help="Postgres DSN for DeviceRegistry (optional, falls back to env DEVICE_REGISTRY_DSN/DATABASE_URL)",
    )
    parser.add_argument(
        "--enable-registry",
        action="store_true",
        help="Load DeviceRegistry at startup for future ingestion path",
    )
    parser.add_argument(
        "--redis-url",
        help="Redis URL for ShadowService (fallback to REDIS_URL env)",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    log_file = Path(args.log_file).expanduser() if args.log_file else DEFAULT_LOG_FILE
    _setup_logging(log_file, verbose=args.verbose)

    config = load_from_env()
    # Поддержка мульти-источника: PIPELINE_SOURCE_LIST=ips,galileosky
    if args.source_kind:
        source_kinds = [args.source_kind.lower()]
    else:
        env_list = os.getenv("PIPELINE_SOURCE_LIST")
        if env_list:
            source_kinds = [k.strip().lower() for k in env_list.split(",") if k.strip()]
        else:
            source_kinds = [config.source_kind.lower()]

    storage_root = Path(args.storage_root) if args.storage_root else config.storage_root
    raw_storage = RawStorage(storage_root)
    latest_store = LatestTelemetryStore(storage_root)

    need_units = any(k == "wialon" for k in source_kinds)
    if need_units:
        if not args.units:
            raise SystemExit("--units is required for source_kind=wialon")
        unit_ids = _resolve_unit_ids(args.units)
    else:
        unit_ids = tuple()

    fallback_uid_map: dict[str, int] = _build_uid_map_fallback()

    registry = None
    if args.enable_registry or os.getenv("DEVICE_REGISTRY_ENABLED") == "1":
        registry = DeviceRegistry(dsn=args.registry_dsn or os.getenv("DEVICE_REGISTRY_DSN"))
        try:
            loaded = registry.load()
            log.info("stream_runner: device registry enabled entries=%s", loaded)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("stream_runner: registry load failed: %s", exc)
            registry = None

    log.info(
        "stream_runner: starting poll loop sources=%s units=%s interval=%ss storage_root=%s metrics_interval=%ss latest_metrics=%s",
        source_kinds,
        len(unit_ids),
        args.interval,
        storage_root,
        args.metrics_interval,
        latest_store.path if latest_store else "disabled",
    )

    overrides = SourceOverrides(token=args.token or None, host=args.host or None)
    poll_strategy = PollStrategy(
        active_age_sec=config.poll_active_age,
        warm_age_sec=config.poll_warm_age,
        active_interval_sec=config.poll_active_interval,
        warm_interval_sec=config.poll_warm_interval,
        cold_interval_sec=config.poll_cold_interval,
        idle_interval_sec=config.poll_idle_interval,
        empty_backoff_sec=config.poll_empty_backoff,
        jitter=max(0.0, min(0.49, config.poll_jitter)),
        max_batch_units=max(1, config.poll_batch_size),
    )

    adapters = []
    for kind in source_kinds:
        cfg = replace(config, source_kind=kind)
        provider = get_source_provider(cfg, overrides)
        adapter = provider.make_stream_adapter(
            raw_storage=raw_storage,
            unit_ids=unit_ids,
            poll_interval=args.interval,
            metrics_interval=args.metrics_interval,
            latest_store=latest_store,
            poll_strategy=poll_strategy,
        )
        adapters.append(adapter)

    shadow = ShadowService(redis_url=args.redis_url or os.getenv("REDIS_URL"))

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows event loop: rely on KeyboardInterrupt
            pass

    async def consume_adapter(adapter_idx: int, adapter_inst) -> None:
        try:
            async for msg in adapter_inst.subscribe():
                if stop_event.is_set():
                    break
                if isinstance(msg, Event):
                    raw_storage.append(raw_storage.day_key(msg.device_ts), [msg])
                    if latest_store:
                        latest_store.update_from_events([msg])
                    continue
                if isinstance(msg, RawPacket):
                    # resolve via registry (preferred) or fallback to legacy/v2 snapshot map
                    entry = registry.resolve(msg.protocol, msg.uid) if registry else None
                    unit_id = entry.unit_id if entry else fallback_uid_map.get(msg.uid)
                    params = dict(msg.params)
                    if unit_id is None:
                        # last-resort: synthetic unit, and register in shadow if available
                        unit_id = _synthetic_unit_id(msg.uid)
                        params.setdefault("synthetic_unit", True)
                        params.setdefault("imei", msg.uid)
                        shadow.register(
                            ShadowPacket(
                                protocol=msg.protocol,
                                uid=msg.uid,
                                ip=msg.ip,
                                payload=msg.raw_payload or msg.params,
                                params_keys=set(msg.params.keys()),
                                received_ts=msg.received_ts,
                            )
                        )
                    ev = Event(
                        unit_id=unit_id,
                        device_ts=msg.device_ts,
                        received_ts=msg.received_ts,
                        latitude=msg.latitude,
                        longitude=msg.longitude,
                        speed=msg.speed,
                        course=msg.course,
                        params=params,
                        source=msg.protocol,
                        raw_payload=msg.raw_payload,
                    )
                    raw_storage.append(raw_storage.day_key(ev.device_ts), [ev])
                    if latest_store:
                        latest_store.update_from_events([ev])
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("stream_runner: adapter %s crashed: %s", adapter_idx, exc)
            stop_event.set()

    tasks = [asyncio.create_task(consume_adapter(i, ad)) for i, ad in enumerate(adapters)]

    # optional HTTP health/metrics
    health_runner = None
    if os.getenv("STREAM_HEALTH_HTTP", "0") == "1":
        health_runner = await start_health_http(
            host=os.getenv("STREAM_HEALTH_HOST", "127.0.0.1"),
            port=int(os.getenv("STREAM_HEALTH_PORT", "9100")),
            health_file=os.getenv("STREAM_HEALTH_FILE", "logs/wialon_ips_ingest_health.json"),
            prom_files=[
                Path("logs/wialon_ips_ingest.prom"),
                Path("logs/galileosky_ingest.prom"),
            ],
        )
        log.info("stream_runner: health HTTP started on %s:%s", os.getenv("STREAM_HEALTH_HOST", "127.0.0.1"), os.getenv("STREAM_HEALTH_PORT", "9100"))
    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        if health_runner:
            await health_runner.cleanup()
        log.info("stream_runner: stopped")


if __name__ == "__main__":
    asyncio.run(main())
