#!/usr/bin/env python3
"""Simple watchdog for pipeline run_stream.

Checks freshness of logs/pipeline_health.json and optionally restarts the stream.
Meant to be called from cron/systemd timers.
"""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_HEALTH_FILE = Path("logs/pipeline_health.json")
DEFAULT_LOG = Path("logs/run_stream_watchdog.log")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor run_stream health and optionally restart it.")
    parser.add_argument(
        "--health-file",
        type=Path,
        default=DEFAULT_HEALTH_FILE,
        help="Path to pipeline health JSON (default: logs/pipeline_health.json)",
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=300,
        help="Maximum allowed age of health timestamp in seconds (default: 300)",
    )
    parser.add_argument(
        "--process-match",
        help="Substring to search for in `ps -eo pid,command` output to confirm stream is running",
        default="pipeline/cli/run_stream.py",
    )
    parser.add_argument(
        "--start-cmd",
        help="Command to execute if stream is unhealthy (example: \"python -m pipeline.cli.run_stream --units cached\")",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=DEFAULT_LOG,
        help="Write watchdog logs to this file (default: logs/run_stream_watchdog.log)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not execute start command, only log actions")
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging")
    return parser.parse_args()


def setup_logging(path: Path, verbose: bool = False) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    level = logging.DEBUG if verbose else logging.INFO
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | watchdog | %(message)s")
    handlers = []
    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    handlers.append(stream)
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(fmt)
    handlers.append(file_handler)
    logging.basicConfig(level=level, handlers=handlers, force=True)


def _read_health_ts(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, f"health file missing: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, f"failed to parse health file: {exc}"
    ts_raw = payload.get("ts")
    if not ts_raw:
        return False, "health file lacks 'ts'"
    try:
        ts = datetime.fromisoformat(ts_raw)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except Exception as exc:
        return False, f"invalid ts format: {exc}"
    age = (datetime.now(timezone.utc) - ts).total_seconds()
    return True, age


def _process_running(pattern: str | None) -> tuple[bool, str]:
    if not pattern:
        return True, "process check disabled"
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid,command"],
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception as exc:
        return False, f"ps call failed: {exc}"
    for line in result.stdout.splitlines():
        if pattern in line and "run_stream_watchdog" not in line:
            return True, f"found match: {line.strip()}"
    return False, f"no process matching '{pattern}'"


def _start_command(cmd: str, dry_run: bool) -> bool:
    try:
        args = shlex.split(cmd)
    except ValueError as exc:
        logging.error("watchdog: unable to parse start command: %s", exc)
        return False
    if dry_run:
        logging.info("watchdog: DRY RUN would execute: %s", cmd)
        return True
    try:
        subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logging.info("watchdog: start command launched: %s", cmd)
        return True
    except Exception as exc:
        logging.error("watchdog: failed to execute start command: %s", exc)
        return False


def main() -> int:
    args = parse_args()
    setup_logging(args.log_file, verbose=args.verbose)
    healthy, health_info = _read_health_ts(args.health_file)
    if healthy:
        age_seconds = float(health_info)
        if age_seconds <= args.max_age:
            logging.info("watchdog: health ok (age %.0fs ≤ %ss)", age_seconds, args.max_age)
            process_ok, proc_info = _process_running(args.process_match)
            if process_ok:
                logging.debug("watchdog: process check ok (%s)", proc_info)
                return 0
            logging.warning("watchdog: process check failed (%s)", proc_info)
            healthy = False
            health_info = proc_info
        else:
            healthy = False
            health_info = f"health age {age_seconds:.0f}s exceeds {args.max_age}s"
    else:
        logging.warning("watchdog: %s", health_info)

    logging.error("watchdog: stream unhealthy (%s)", health_info)
    if args.start_cmd:
        if _start_command(args.start_cmd, args.dry_run):
            return 0
        return 2
    logging.error("watchdog: no start command configured; exiting with error")
    return 1


if __name__ == "__main__":
    sys.exit(main())
