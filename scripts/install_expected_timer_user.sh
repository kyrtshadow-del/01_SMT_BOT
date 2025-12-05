#!/usr/bin/env bash
# Install and start user-level systemd timer for expected-interval recalc.
# Works без sudo (пишет в ~/.config/systemd/user).

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
UNIT_NAME="recalc_expected_intervals"
USER_SYSTEMD_DIR="${HOME}/.config/systemd/user"
SERVICE_PATH="${USER_SYSTEMD_DIR}/${UNIT_NAME}.service"
TIMER_PATH="${USER_SYSTEMD_DIR}/${UNIT_NAME}.timer"

mkdir -p "${USER_SYSTEMD_DIR}"

cat > "${SERVICE_PATH}" <<EOF
[Unit]
Description=Recalculate expected telemetry intervals (user)
After=network.target

[Service]
Type=oneshot
WorkingDirectory=${ROOT_DIR}
Environment="PYTHONPATH=."
ExecStart=${ROOT_DIR}/scripts/recalc_expected_intervals.sh --window-days 2 --recalc-period-hours 12 --max-units 200 --online-threshold-sec \${MONITORING_ONLINE_SEC:-600}
Nice=10

[Install]
WantedBy=default.target
EOF

cat > "${TIMER_PATH}" <<EOF
[Unit]
Description=Recalculate expected telemetry intervals every 30 minutes (user)

[Timer]
OnCalendar=*:0/30
Persistent=true
Unit=${UNIT_NAME}.service

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now "${UNIT_NAME}.timer"

echo "User-level timer installed and started (systemctl --user status ${UNIT_NAME}.timer)"
