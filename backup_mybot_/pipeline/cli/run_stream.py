"""Run the Wialon streaming ingestion loop."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import gzip
import json
import logging
import signal
from dataclasses import replace
from pathlib import Path
from typing import List, Sequence

from pipeline.config.defaults import load_from_env
from pipeline.services.unit_snapshot_service import UnitSnapshotService
from pipeline.sources import SourceOverrides, get_source_provider
from pipeline.storage.raw_storage import RawStorage
from pipeline.storage.latest_metrics import LatestTelemetryStore

REPO_ROOT = Path(__file__).resolve().parents[2]
LEGACY_UNIT_CACHE_PATH = (REPO_ROOT / "data" / "units.v1.json.gz").resolve()
SNAPSHOT_PATH = (REPO_ROOT / "data" / "unit_snapshot.json.gz").resolve()
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Wialon messages into local storage")
    parser.add_argument(
        "--units",
        required=True,
        help=(
            "Comma-separated unit IDs, '@path/to/file' to read IDs from file, "
            "or 'cached'/'all' to grab every known unit from unit_snapshot.json.gz "
            "(fallback to legacy data/units.v1.json.gz)."
        ),
    )
    parser.add_argument("--interval", type=float, default=5.0, help="Poll interval in seconds")
    parser.add_argument("--token", help="Override Wialon token (defaults to env)")
    parser.add_argument("--host", help="Override Wialon host (defaults to env)")
    parser.add_argument("--storage-root", help="Override storage root path")
    parser.add_argument("--metrics-interval", type=int, default=60, help="Seconds between health logs")
    parser.add_argument(
        "--source-kind",
        help="Override pipeline source kind (defaults to PIPELINE_SOURCE_KIND env var)",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    config = load_from_env()
    requested_kind = (args.source_kind or config.source_kind or "wialon").lower()
    if requested_kind != "wialon":
        log.info(
            "stream_runner: overriding source_kind=%s to 'wialon' for ingestion",
            requested_kind,
        )
        config = replace(config, source_kind="wialon")
    elif args.source_kind:
        config = replace(config, source_kind=args.source_kind)

    storage_root = Path(args.storage_root) if args.storage_root else config.storage_root
    raw_storage = RawStorage(storage_root)
    latest_store = LatestTelemetryStore(storage_root)

    unit_ids = _resolve_unit_ids(args.units)

    overrides = SourceOverrides(
        token=args.token or None,
        host=args.host or None,
    )
    provider = get_source_provider(config, overrides)
    adapter = provider.make_stream_adapter(
        raw_storage=raw_storage,
        unit_ids=unit_ids,
        poll_interval=args.interval,
        metrics_interval=args.metrics_interval,
        latest_store=latest_store,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows event loop: rely on KeyboardInterrupt
            pass

    async def consume() -> None:
        try:
            async for _ in adapter.subscribe():
                if stop_event.is_set():
                    break
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("stream_runner: crashed: %s", exc)
            stop_event.set()

    task = asyncio.create_task(consume())
    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        log.info("stream_runner: stopped")


if __name__ == "__main__":
    asyncio.run(main())
