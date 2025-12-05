"""CLI helper that loads a day of data into the new pipeline storage."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from pipeline.config.defaults import PipelineConfig, load_from_env
from pipeline.sources import SourceOverrides, get_source_provider
from pipeline.storage.raw_storage import RawStorage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap day into pipeline storage")
    parser.add_argument("--day", help="ISO date (YYYY-MM-DD)", required=True)
    parser.add_argument(
        "--units",
        help="Comma-separated unit IDs (defaults to all from Wialon cache)",
        required=True,
    )
    parser.add_argument("--token", help="Wialon token (defaults to PIPELINE_WIALON_TOKEN)")
    parser.add_argument("--host", help="Wialon host (defaults to PIPELINE_WIALON_HOST)")
    parser.add_argument(
        "--storage-root",
        help="Override storage root path",
    )
    parser.add_argument(
        "--source-kind",
        help="Override pipeline source kind (defaults to PIPELINE_SOURCE_KIND env var)",
    )
    return parser.parse_args()


def _day_window(day: str) -> tuple[int, int]:
    dt = datetime.fromisoformat(day)
    start = int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())
    end = int((datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc) + timedelta(days=1)).timestamp())
    return start, end


async def main() -> None:
    args = parse_args()
    start_ts, end_ts = _day_window(args.day)
    config: PipelineConfig = load_from_env()
    if args.source_kind:
        config = replace(config, source_kind=args.source_kind)
    storage_root = Path(args.storage_root) if args.storage_root else config.storage_root
    raw_storage = RawStorage(storage_root)
    overrides = SourceOverrides(
        token=args.token or None,
        host=args.host or None,
    )
    adapter = get_source_provider(config, overrides).make_history_adapter(raw_storage)
    unit_ids: List[int] = [int(part) for part in args.units.split(",") if part.strip()]
    events = await adapter.fetch_history(unit_ids, start_ts, end_ts)
    print(f"Stored {len(events)} events for day {args.day}")


if __name__ == "__main__":
    asyncio.run(main())
