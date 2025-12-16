"""One-command launcher for bot + streaming + web API."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import signal
import sys
from pathlib import Path
from typing import Iterable, List


PYTHON = sys.executable or "python3"
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Telegram bot, streaming ingestion and web API in one terminal"
    )
    parser.add_argument("--units", default="cached", help="Unit set for run_stream (cached/all/@file/id list)")
    parser.add_argument("--no-bot", action="store_true", help="Disable Telegram bot")
    parser.add_argument("--no-stream", action="store_true", help="Disable streaming ingestion")
    parser.add_argument("--no-trips", action="store_true", help="Disable trip detector worker")
    parser.add_argument("--no-web", action="store_true", help="Disable web API/UI (uvicorn)")
    parser.add_argument(
        "--web-port",
        type=int,
        # 8000 — порт, который стабильно работает в текущем окружении/стеке.
        default=int(os.getenv("WEB_PORT", "8000") or 8000),
        help="Port for uvicorn web API (default 8000)",
    )
    return parser.parse_args()


async def stream_subprocess(proc: asyncio.subprocess.Process, name: str) -> None:
    async def pipe(stream: asyncio.StreamReader, prefix: str) -> None:
        if stream is None:
            return
        chunk_size = 4096
        buffer = b""
        while True:
            data = await stream.read(chunk_size)
            if not data:
                if buffer:
                    sys.stdout.write(f"[{prefix}] {buffer.decode(errors='replace')}\n")
                    sys.stdout.flush()
                break
            buffer += data
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                sys.stdout.write(f"[{prefix}] {line.decode(errors='replace')}\n")
                sys.stdout.flush()

    await asyncio.gather(
        pipe(proc.stdout, name),
        pipe(proc.stderr, f"{name}!"),
    )


async def launch_process(name: str, cmd: Iterable[str]) -> asyncio.subprocess.Process:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(REPO_ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    asyncio.create_task(stream_subprocess(proc, name))
    return proc


async def run_all() -> None:
    args = parse_args()
    stop_event = asyncio.Event()

    async def shutdown(*_: object) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown()))
        except NotImplementedError:
            pass

    procs: List[asyncio.subprocess.Process] = []

    web = None
    if not args.no_web:
        web_cmd = [
            PYTHON,
            "-m",
            "uvicorn",
            "pipeline.api.web_app:app",
            "--host",
            "0.0.0.0",
            "--port",
            str(args.web_port),
            "--log-level",
            "info",
        ]
        web = await launch_process("web", web_cmd)
        procs.append(web)
    else:
        print("[run_all] web disabled")

    bot = None
    if not args.no_bot:
        bot_cmd = [PYTHON, "bot_new.py"]
        bot = await launch_process("bot", bot_cmd)
        procs.append(bot)
    else:
        print("[run_all] bot disabled")

    stream_task: asyncio.Task | None = None
    if not args.no_stream:
        stream_cmd = [PYTHON, "pipeline/cli/run_stream.py", "--units", args.units]
        stream = await launch_process("stream", stream_cmd)
        procs.append(stream)
    else:
        print("[run_all] streaming disabled")

    # Trip detector worker
    if not args.no_trips:
        trip_cmd = [PYTHON, "pipeline/cli/run_trip_worker.py"]
        trip = await launch_process("trip", trip_cmd)
        procs.append(trip)
    else:
        print("[run_all] trip worker disabled")

    try:
        await stop_event.wait()
    finally:
        for proc in procs:
            if proc.returncode is None:
                proc.terminate()
        await asyncio.gather(*(proc.wait() for proc in procs), return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(run_all())
