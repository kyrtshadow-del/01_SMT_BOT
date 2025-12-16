"""CLI helper to import Wialon .wlp files into UnitConfigService (PostgreSQL)."""

from __future__ import annotations

import argparse
from pathlib import Path
import os

from pipeline.adapters.wialon_wlp_import import import_wlp_to_service
from pipeline.config.unit_config_service import UnitConfigService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Wialon .wlp export into UnitConfigService storage.")
    parser.add_argument("--unit-id", type=int, required=True, help="Numeric unit ID.")
    parser.add_argument("--input", required=True, help="Path to .wlp file.")
    parser.add_argument(
        "--source-kind",
        default="wialon",
        help="Source kind label stored inside UnitConfig (default: wialon).",
    )
    parser.add_argument(
        "--schema",
        help="Optional path to UnitConfig JSON schema. Defaults to bundled schema.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dsn = os.getenv("DEVICE_REGISTRY_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise SystemExit("DEVICE_REGISTRY_DSN or DATABASE_URL env var is required")
    service = UnitConfigService(dsn, schema_path=Path(args.schema) if args.schema else None)
    input_path = Path(args.input)
    import_wlp_to_service(
        input_path,
        unit_id=args.unit_id,
        service=service,
        source_kind=args.source_kind,
    )
    print(f"UnitConfig for unit_id={args.unit_id} saved to PostgreSQL")


if __name__ == "__main__":
    main()
