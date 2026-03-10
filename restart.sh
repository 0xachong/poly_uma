#!/bin/sh
set -eu

# ── 配置（优先读环境变量，也可直接在这里填写）────────────────────────────────
WSS_URL="${POLYGON_WSS_URL:-}"
RPC_URL="${POLYGON_RPC_URL:-}"
SQLITE_PATH="${SQLITE_PATH:-uma_oo_events.sqlite}"
API_ADDR="${API_ADDR:-0.0.0.0:7002}"
MYSQL_DSN="${MYSQL_DSN:-}"
HTTP_PROXY_URL="${HTTP_PROXY:-}"
LOG_FILE="${LOG_FILE:-uma-sync.log}"
PID_FILE="${PID_FILE:-.uma-sync.pid}"

cd "$(dirname "$0")"

# ── 校验必填 ────────────────────────────────────────────────────────────────
if [ -z "$WSS_URL" ]; then
  echo "[ERROR] 请设置环境变量 POLYGON_WSS_URL" >&2
  exit 1
fi

# ── 停止旧进程 ───────────────────────────────────────────────────────────────
if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "[INFO] 停止旧进程 PID=$OLD_PID ..."
    kill "$OLD_PID"
    i=0
    while [ $i -lt 10 ]; do
      kill -0 "$OLD_PID" 2>/dev/null || break
      sleep 1
      i=$((i + 1))
    done
    if kill -0 "$OLD_PID" 2>/dev/null; then
      kill -9 "$OLD_PID" || true
    fi
  fi
  rm -f "$PID_FILE"
fi

# ── git pull ─────────────────────────────────────────────────────────────────
echo "[INFO] git pull ..."
git pull --ff-only

# ── 构建 ─────────────────────────────────────────────────────────────────────
echo "[INFO] go build ..."
go build -o uma-sync ./cmd/uma-sync

# ── 拼装启动参数 ──────────────────────────────────────────────────────────────
CMD_ARGS="-wss $WSS_URL -sqlite $SQLITE_PATH -api-addr $API_ADDR"
if [ -n "$RPC_URL" ];       then CMD_ARGS="$CMD_ARGS -rpc $RPC_URL";             fi
if [ -n "$MYSQL_DSN" ];     then CMD_ARGS="$CMD_ARGS -mysql $MYSQL_DSN";         fi
if [ -n "$HTTP_PROXY_URL" ];then CMD_ARGS="$CMD_ARGS -proxy $HTTP_PROXY_URL";    fi

# ── 后台启动 ──────────────────────────────────────────────────────────────────
echo "[INFO] 启动 uma-sync，日志 → $LOG_FILE"
# shellcheck disable=SC2086
nohup ./uma-sync $CMD_ARGS >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "[INFO] 已启动 PID=$(cat "$PID_FILE")"
