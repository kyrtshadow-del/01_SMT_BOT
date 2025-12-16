"""Run ingestion (push/poll) with SQL-first storage.

- Loads registry (devices/units) from Postgres via DeviceRegistry.
- Push sources (Galileosky/Wialon IPS) validate UID against registry; unknown go to Shadow.
- Poll source (Wialon API) can optionally take --units list; no snapshot fallback.
- All telemetry is persisted via PipelineStorageService (Postgres + RawStorage + latest_metrics).
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
from logging.handlers import RotatingFileHandler
import os
import signal
from dataclasses import replace
from pathlib import Path
from typing import List
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.config.defaults import load_from_env
from pipeline.sources import PollStrategy, SourceOverrides, get_source_provider
from pipeline.monitoring.health_http import start_health_http
from pipeline.services.device_registry import DeviceRegistry
from pipeline.services.shadow_service import ShadowService, ShadowPacket
from pipeline.services.storage_service import get_pipeline_storage_service
from pipeline.events import Event, RawPacket

DEFAULT_LOG_FILE = (REPO_ROOT / "logs" / "stream_runner.log").resolve()
log = logging.getLogger("pipeline.stream_runner")


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
    logging.basicConfig(level=level, handlers=[stream_handler, file_handler], force=True)
    log.info("stream_runner: logging to %s level=%s", log_file, logging.getLevelName(level))


def _resolve_unit_ids(spec: str) -> List[int]:
    text = (spec or "").strip()
    if not text:
        raise SystemExit("--units is required for source_kind=wialon (poll)")
    if text.startswith("@"):
        path = Path(text[1:]).expanduser()
        if not path.exists():
            raise SystemExit(f"Units file not found: {path}")
        payload = path.read_text(encoding="utf-8")
        tokens = payload.replace("\n", ",").split(",")
    else:
        tokens = text.split(",")
    unit_ids: List[int] = []
    for tok in tokens:
        tok = tok.strip()
        if not tok:
            continue
        try:
            unit_ids.append(int(tok))
        except ValueError as exc:
            raise SystemExit(f"Invalid unit ID '{tok}' in --units") from exc
    if not unit_ids:
        raise SystemExit("No unit IDs provided")
    # dedupe preserve order
    seen = set()
    out: List[int] = []
    for uid in unit_ids:
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stream ingestion runner (no snapshots)")
    p.add_argument("--units", help="Comma-separated IDs or @file (only for wialon poll)")
    p.add_argument("--interval", type=float, default=5.0, help="Poll interval seconds (wialon)")
    p.add_argument("--token", help="Override Wialon token")
    p.add_argument("--host", help="Override Wialon host")
    p.add_argument("--storage-root", help="Override storage root")
    p.add_argument("--metrics-interval", type=int, default=60, help="Health log period sec")
    p.add_argument("--log-file", help="Custom log file path")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--source-kind", help="Override source kind; or PIPELINE_SOURCE_LIST env")
    p.add_argument("--registry-dsn", help="Postgres DSN (fallback DEVICE_REGISTRY_DSN/DATABASE_URL)")
    p.add_argument("--redis-url", help="Redis URL (fallback REDIS_URL)")
    return p.parse_args()


async def main() -> None:
    args = parse_args()
    _setup_logging(Path(args.log_file).expanduser() if args.log_file else DEFAULT_LOG_FILE, args.verbose)

    # Respect --storage-root override by updating env before reading config
    if args.storage_root:
        os.environ["PIPELINE_STORAGE_ROOT"] = str(Path(args.storage_root).expanduser())

    cfg = load_from_env()
    if args.source_kind:
        source_kinds = [args.source_kind.lower()]
    else:
        env_list = os.getenv("PIPELINE_SOURCE_LIST")
        source_kinds = [k.strip().lower() for k in env_list.split(",") if k.strip()] if env_list else [cfg.source_kind.lower()]

    # Unified SQL-first storage service (DB + RawStorage + latest_metrics)
    storage_service = get_pipeline_storage_service()
    raw_storage = storage_service.raw_storage
    latest_store = storage_service.latest_store

    # Poll-only needs explicit units
    need_units = any(k == "wialon" for k in source_kinds)
    unit_ids = _resolve_unit_ids(args.units) if need_units else []

    # Registry (shares DSN with main DB in typical deployments)
    registry_dsn = args.registry_dsn or os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
    registry: DeviceRegistry | None = None
    if registry_dsn:
        registry = DeviceRegistry(dsn=registry_dsn)
        try:
            registry.load()
        except Exception as exc:
            log.error("registry load failed: %s", exc)
            registry = None
    else:
        log.warning("DEVICE_REGISTRY_DSN not set; unknown devices will go to shadow")

    overrides = SourceOverrides(token=args.token or None, host=args.host or None)
    poll_strategy = PollStrategy(
        active_age_sec=cfg.poll_active_age,
        warm_age_sec=cfg.poll_warm_age,
        active_interval_sec=cfg.poll_active_interval,
        warm_interval_sec=cfg.poll_warm_interval,
        cold_interval_sec=cfg.poll_cold_interval,
        idle_interval_sec=cfg.poll_idle_interval,
        empty_backoff_sec=cfg.poll_empty_backoff,
        jitter=max(0.0, min(0.49, cfg.poll_jitter)),
        max_batch_units=max(1, cfg.poll_batch_size),
    )

    adapters = []
    for kind in source_kinds:
        cfg_kind = replace(cfg, source_kind=kind)
        provider = get_source_provider(cfg_kind, overrides)
        adapters.append(
            provider.make_stream_adapter(
                raw_storage=raw_storage,
                unit_ids=unit_ids,
                poll_interval=args.interval,
                metrics_interval=args.metrics_interval,
                latest_store=latest_store,
                poll_strategy=poll_strategy,
            )
        )

    shadow = ShadowService(redis_url=args.redis_url or os.getenv("REDIS_URL"))

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    async def reload_registry_loop():
        if not registry:
            return
        interval = int(os.getenv("REGISTRY_RELOAD_SEC", "60"))
        while not stop_event.is_set():
            await asyncio.sleep(interval)
            try:
                registry.load()
            except Exception as exc:
                log.warning("registry reload failed: %s", exc)

    async def consume_adapter(idx: int, adapter_inst) -> None:
        try:
            async for msg in adapter_inst.subscribe():
                if stop_event.is_set():
                    break
                if isinstance(msg, Event):
                    # Single point of persistence: SQL + RawStorage + latest_metrics
                    storage_service.store_events([msg])
                    continue
                if isinstance(msg, RawPacket):
                    entry = registry.resolve(msg.protocol, msg.uid) if registry else None
                    if registry and entry is None:
                        # one immediate refresh to reduce shadow boomerangs
                        try:
                            registry.load()
                            entry = registry.resolve(msg.protocol, msg.uid)
                        except Exception:
                            entry = None
                    if entry is None:
                        log.info("shadow register unknown device protocol=%s uid=%s", msg.protocol, msg.uid)
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
                        continue
                    ev = Event(
                        unit_id=entry.unit_id,
                        device_ts=msg.device_ts,
                        received_ts=msg.received_ts,
                        latitude=msg.latitude,
                        longitude=msg.longitude,
                        speed=msg.speed,
                        course=msg.course,
                        params=dict(msg.params),
                        source=msg.protocol,
                        raw_payload=msg.raw_payload,
                    )
                    storage_service.store_events([ev])
        except Exception as exc:
            log.exception("adapter %s crashed: %s", idx, exc)
            stop_event.set()

    tasks = [asyncio.create_task(consume_adapter(i, ad)) for i, ad in enumerate(adapters)]
    if registry:
        tasks.append(asyncio.create_task(reload_registry_loop()))

    health_runner = None
    if os.getenv("STREAM_HEALTH_HTTP", "0") == "1":
        health_runner = await start_health_http(
            host=os.getenv("STREAM_HEALTH_HOST", "127.0.0.1"),
            port=int(os.getenv("STREAM_HEALTH_PORT", "9100")),
            health_file=os.getenv("STREAM_HEALTH_FILE", "logs/ingest_health.json"),
            prom_files=[Path("logs/wialon_ips_ingest.prom"), Path("logs/galileosky_ingest.prom")],
        )

    # lightweight HTTP hook to reload registry on-demand
    async def reload_registry_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        path = ""
        try:
            data = await reader.read(1024)
            path = data.decode(errors="ignore").split(" ")[1]
            if path == "/api/reload_registry":
                if registry:
                    try:
                        registry.load()
                        resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
                    except Exception as exc:  # pragma: no cover - defensive
                        resp = f"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".encode()
                    writer.write(resp)
                else:
                    writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
            else:
                writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
        finally:
            writer.close()

    mini_http_server = None
    if registry:
        port = int(os.getenv("STREAM_RELOAD_HTTP_PORT", "9101"))
        mini_http_server = await asyncio.start_server(reload_registry_handler, host="127.0.0.1", port=port)
        log.info("stream_runner: reload HTTP on 127.0.0.1:%s", port)

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
        if mini_http_server:
            mini_http_server.close()
            await mini_http_server.wait_closed()
        log.info("stream_runner: stopped")


if __name__ == "__main__":
    asyncio.run(main())
