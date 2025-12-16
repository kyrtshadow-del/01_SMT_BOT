"""CLI helper to convert Wialon .wlp exports into UnitConfig storage entries."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from pipeline.adapters.wialon.unit_config_adapter import WialonUnitConfigAdapter
from pipeline.config.unit_config_service import load_default_service


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wlp", required=True, help="Path to Wialon .wlp export")
    parser.add_argument("--unit-id", required=True, type=int, help="Numeric unit ID")
    parser.add_argument("--dump-ts", type=int, help="Override dump timestamp (unix seconds)")
    parser.add_argument(
        "--source-kind",
        default="wialon",
        help="Override source kind label stored in UnitConfig",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    adapter = WialonUnitConfigAdapter(source_kind=args.source_kind)
    config = adapter.from_wlp_file(Path(args.wlp), unit_id=args.unit_id, dump_ts=args.dump_ts)

    service = load_default_service()
    service.save(config)
    print(f"[ok] saved unit config for unit_id={args.unit_id} to PostgreSQL")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
