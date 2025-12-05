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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Telegram bot, streaming ingestion and snapshot refresh in one terminal"
    )
    parser.add_argument("--units", default="cached", help="Unit set for run_stream (cached/all/@file/id list)")
    parser.add_argument(
        "--snapshot-interval",
        type=int,
        default=int(os.getenv("SNAPSHOT_INTERVAL_SEC", "300") or 300),
        help="Seconds between snapshot refresh runs (0 disables snapshot loop)",
    )
    parser.add_argument("--no-stream", action="store_true", help="Disable streaming ingestion")
    parser.add_argument("--no-snapshot", action="store_true", help="Disable snapshot refresh loop")
    return parser.parse_args()


async def stream_subprocess(proc: asyncio.subprocess.Process, name: str) -> None:
    async def pipe(stream: asyncio.StreamReader, prefix: str) -> None:
        if stream is None:
            return
        while not stream.at_eof():
            line = await stream.readline()
            if not line:
                break
            sys.stdout.write(f"[{prefix}] {line.decode(errors='replace')}"
                             )
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
    cmd = [PYTHON, "_refresh_snapshot.py"]
    while not stop_event.is_set():
        print("[snapshot] running …")
        proc = await launch_process("snapshot", cmd)
        await proc.wait()
        if stop_event.is_set():
            break
        await asyncio.wait_for(stop_event.wait(), timeout=interval)


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

    bot_cmd = [PYTHON, "bot_new.py"]
    bot = await launch_process("bot", bot_cmd)
    procs.append(bot)

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
