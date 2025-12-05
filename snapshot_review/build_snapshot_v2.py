"""Build unit_snapshot.v2.json.gz from local sources."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.services.unit_snapshot_v2 import (
    DEFAULT_SNAPSHOT_V2_PATH,
    build_snapshot_v2_bundle,
    get_unit_snapshot_v2_service,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build unit_snapshot.v2.json.gz from UnitConfig + latest_metrics + admin meta")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_SNAPSHOT_V2_PATH,
        help=f"Path to snapshot file (default: {DEFAULT_SNAPSHOT_V2_PATH})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    svc = get_unit_snapshot_v2_service()
    bundle = build_snapshot_v2_bundle()
    svc.snapshot_path = args.output
    svc.refresh(bundle.units.values(), source_kind=bundle.source_kind, dump_ts=bundle.dump_ts)
    print(f"snapshot_v2: units={len(bundle.units)} dump_ts={bundle.dump_ts} path={svc.snapshot_path}")
    if not bundle.units:
        print("WARN: snapshot v2 is empty. Ensure unit_configs or events/latest_metrics exist.")


if __name__ == "__main__":
    main()
