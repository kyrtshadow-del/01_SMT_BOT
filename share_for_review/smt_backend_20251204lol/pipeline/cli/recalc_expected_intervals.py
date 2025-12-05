#!/usr/bin/env python3
"""Recalculate expected message intervals per unit."""

from __future__ import annotations

import argparse
import sys

from pipeline.services.expected_interval_service import recalc_expected_intervals


def main() -> int:
    parser = argparse.ArgumentParser(description="Recalculate expected telemetry intervals per unit")
    parser.add_argument("--window-days", type=int, default=2, help="Days of history to analyze (default: 2)")
    parser.add_argument("--recalc-period-hours", type=int, default=12, help="Min hours between recalcs for a unit")
    parser.add_argument("--base-expected-sec", type=float, default=300.0, help="Fallback expected interval (sec)")
    parser.add_argument("--max-units", type=int, default=200, help="Max units to process per run")
    parser.add_argument("--min-samples", type=int, default=20, help="Minimum good intervals to accept estimation")
    parser.add_argument("--alpha", type=float, default=0.7, help="Smoothing factor with previous value (0-1)")
    parser.add_argument(
        "--online-threshold-sec",
        type=float,
        default=None,
        help="If set, keep only intervals where gaps <= threshold (emulate online slices). Default: MONITORING_ONLINE_SEC",
    )
    args = parser.parse_args()

    data = recalc_expected_intervals(
        window_days=args.window_days,
        recalc_period_hours=args.recalc_period_hours,
        base_expected_sec=args.base_expected_sec,
        max_units=args.max_units,
        min_samples=args.min_samples,
        alpha=args.alpha,
        online_threshold_sec=args.online_threshold_sec,
    )
    print(f"Recalculated/loaded expected intervals for {len(data)} units")
    return 0


if __name__ == "__main__":
    sys.exit(main())
