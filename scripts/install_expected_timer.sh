#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="recalc_expected_intervals"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
TIMER_PATH="/etc/systemd/system/${SERVICE_NAME}.timer"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [[ $EUID -ne 0 ]]; then
  echo "Please run as root (sudo) to install systemd units" >&2
  exit 1
fi

cp "${ROOT_DIR}/scripts/recalc_expected_intervals.service.sample" "${SERVICE_PATH}"
cp "${ROOT_DIR}/scripts/recalc_expected_intervals.timer.sample" "${TIMER_PATH}"
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}.timer"
echo "Installed and started ${SERVICE_NAME}.timer (every 30 minutes)"
