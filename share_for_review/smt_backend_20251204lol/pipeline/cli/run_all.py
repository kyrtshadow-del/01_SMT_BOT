"""One-command launcher for bot + streaming + snapshot refresh."""

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
        description="Run Telegram bot, streaming ingestion and snapshot refresh in one terminal"
    )
    parser.add_argument("--units", default="cached", help="Unit set for run_stream (cached/all/@file/id list)")
    parser.add_argument("--no-bot", action="store_true", help="Disable Telegram bot")
    parser.add_argument(
        "--snapshot-interval",
        type=int,
        default=int(os.getenv("SNAPSHOT_INTERVAL_SEC", "300") or 300),
        help="Seconds between snapshot refresh runs (0 disables snapshot loop)",
    )
    parser.add_argument("--no-stream", action="store_true", help="Disable streaming ingestion")
    parser.add_argument("--no-snapshot", action="store_true", help="Disable snapshot refresh loop")
    parser.add_argument("--no-web", action="store_true", help="Disable web API/UI (uvicorn)")
    parser.add_argument(
        "--web-port",
        type=int,
        default=int(os.getenv("WEB_PORT", "8080") or 8080),
        help="Port for uvicorn web API (default 8080)",
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


async def snapshot_loop(interval: int, stop_event: asyncio.Event) -> None:
    if interval <= 0:
        print("[snapshot] disabled")
        return
    # Prefer new snapshot v2; fallback to legacy builder for compatibility.
    cmd = [PYTHON, "-m", "pipeline.cli.build_snapshot_v2"]
    while not stop_event.is_set():
        print("[snapshot] running …")
        proc = await launch_process("snapshot", cmd)
        await proc.wait()
        if stop_event.is_set():
            break
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            # normal wake-up to run the next snapshot iteration
            continue


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

    snapshot_task: asyncio.Task | None = None
    if not args.no_snapshot and args.snapshot_interval > 0:
        snapshot_task = asyncio.create_task(snapshot_loop(args.snapshot_interval, stop_event))
    else:
        print("[run_all] snapshot loop disabled")

    try:
        await stop_event.wait()
    finally:
        for proc in procs:
            if proc.returncode is None:
                proc.terminate()
        await asyncio.gather(*(proc.wait() for proc in procs), return_exceptions=True)
        if snapshot_task:
            snapshot_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await snapshot_task


if __name__ == "__main__":
    asyncio.run(run_all())
