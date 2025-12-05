#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate 2>/dev/null || true
PYTHONPATH=. python3 -m pipeline.cli.recalc_expected_intervals "$@"
