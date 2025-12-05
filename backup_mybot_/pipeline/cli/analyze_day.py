"""CLI to build simple summaries from stored events."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.config.defaults import load_from_env
from pipeline.engine import SensorCalculator, build_day_summary
from pipeline.storage.raw_storage import RawStorage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze stored events for a day")
    parser.add_argument("--day", required=True, help="ISO date (YYYY-MM-DD)")
    parser.add_argument("--storage-root", help="Override storage root")
    parser.add_argument("--fuel-param", default="fuel_raw")
    parser.add_argument("--fuel-scale", type=float, default=1.0)
    parser.add_argument("--fuel-offset", type=float, default=0.0)
    parser.add_argument("--fuel-expression", help="Expression using params, e.g. \"rs485_fls12+rs485_fls22\"")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_from_env()
    storage_root = Path(args.storage_root) if args.storage_root else config.storage_root
    raw_storage = RawStorage(storage_root)
    events = raw_storage.fetch(args.day)
    calculator = SensorCalculator(
        fuel_param=args.fuel_param,
        fuel_scale=args.fuel_scale,
        fuel_offset=args.fuel_offset,
        fuel_expression=args.fuel_expression,
    )
    summary = build_day_summary(events, calculator)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
