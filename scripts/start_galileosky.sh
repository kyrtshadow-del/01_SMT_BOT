#!/usr/bin/env bash
set -e

PORT=8088
VPS_HOST=77.232.134.54
VPS_USER=root
KEY="${HOME}/.ssh/id_ed25519"  # поправь, если ключ другой
LOG=/tmp/run_stream.log

echo "[start] killing old streamer and tunnels..."
pkill -f "pipeline/cli/run_stream.py" 2>/dev/null || true
pkill -f "ssh .*${PORT}" 2>/dev/null || true
# дополнительно убиваем процессы, держащие порт
PIDS=$(lsof -tiTCP:${PORT} -sTCP:LISTEN 2>/dev/null || true)
if [ -n "${PIDS}" ]; then
  echo "[info] freeing port ${PORT}, killing: ${PIDS}"
  kill -9 ${PIDS} 2>/dev/null || true
fi

echo "[start] launching streamer on localhost:${PORT}..."
cd "${HOME}/project/mybot"
PIPELINE_SOURCE_KIND=galileosky PIPELINE_GALILEOSKY_PORT=${PORT} PYTHONPATH=. \
python3 pipeline/cli/run_stream.py --source-kind galileosky \
  --interval 5 --metrics-interval 60 \
  > "${LOG}" 2>&1 &

sleep 2

if ! lsof -iTCP:${PORT} -sTCP:LISTEN >/dev/null; then
  echo "[error] streamer is not listening on ${PORT}. See ${LOG}"
  exit 1
fi
echo "[ok] streamer listening on localhost:${PORT}"

echo "[start] launching tunnel to ${VPS_HOST}:${PORT}..."
if command -v autossh >/dev/null 2>&1; then
  SSH_CMD="autossh -f -M 0 -i \"${KEY}\" \
    -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
    -N -R 0.0.0.0:${PORT}:localhost:${PORT} ${VPS_USER}@${VPS_HOST}"
else
  SSH_CMD="ssh -f -i \"${KEY}\" \
    -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
    -N -R 0.0.0.0:${PORT}:localhost:${PORT} ${VPS_USER}@${VPS_HOST}"
fi
# если нужно использовать пароль и установлен sshpass — подставим его; иначе ssh запросит пароль интерактивно
if [ -n "${SSH_PASS:-}" ] && command -v sshpass >/dev/null 2>&1; then
  SSH_CMD="sshpass -p \"${SSH_PASS}\" ${SSH_CMD}"
fi
eval ${SSH_CMD}

# проверяем, что порт поднялся на локальной стороне туннеля (ssh/sshd оставляет слушатель на VPS)
sleep 2
if ! pgrep -af "0.0.0.0:${PORT}:localhost:${PORT}" >/dev/null; then
  echo "[error] tunnel failed to start (ssh process not found)"
  exit 1
fi
echo "[ok] tunnel command executed -> ${VPS_HOST}:${PORT}"

echo "[tail] recent ingest log:"
tail -n 5 logs/galileosky_ingest.log || true
