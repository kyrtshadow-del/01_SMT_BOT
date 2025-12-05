"""Lightweight HTTP server to expose health JSON and Prometheus metrics files."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Callable, Dict, Any

from aiohttp import web


async def start_health_http(
    *,
    host: str = "127.0.0.1",
    port: int = 9100,
    health_file: Path | str = "logs/wialon_ips_ingest_health.json",
    prom_files: list[Path | str] | None = None,
) -> web.AppRunner:
    app = web.Application()

    async def handle_health(_: web.Request) -> web.Response:
        try:
            data = json.loads(Path(health_file).read_text(encoding="utf-8"))
        except Exception:
            data = {"status": "unknown"}
        return web.json_response(data)

    async def handle_metrics(_: web.Request) -> web.Response:
        lines = []
        for f in prom_files or []:
            p = Path(f)
            if not p.exists():
                continue
            try:
                lines.append(p.read_text(encoding="utf-8"))
            except Exception:
                continue
        return web.Response(text="\n".join(lines), content_type="text/plain; version=0.0.4")

    app.add_routes([web.get("/health", handle_health), web.get("/metrics", handle_metrics)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner
