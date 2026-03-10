#!/usr/bin/env bash
set -euo pipefail

# ── 配置（优先读环境变量，也可直接在这里填写）────────────────────────────────
WSS_URL="${POLYGON_WSS_URL:-}"
RPC_URL="${POLYGON_RPC_URL:-}"
SQLITE_PATH="${SQLITE_PATH:-uma_oo_events.sqlite}"
API_ADDR="${API_ADDR:-0.0.0.0:7002}"
MYSQL_DSN="${MYSQL_DSN:-}"
HTTP_PROXY="${HTTP_PROXY:-}"
LOG_FILE="${LOG_FILE:-uma-sync.log}"
PID_FILE="${PID_FILE:-.uma-sync.pid}"

cd "$(dirname "$0")"

# ── 校验必填 ────────────────────────────────────────────────────────────────
if [[ -z "$WSS_URL" ]]; then
  echo "[ERROR] 请设置环境变量 POLYGON_WSS_URL" >&2
  exit 1
fi

# ── 停止旧进程 ───────────────────────────────────────────────────────────────
if [[ -f "$PID_FILE" ]]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "[INFO] 停止旧进程 PID=$OLD_PID ..."
    kill "$OLD_PID"
    # 等待最多 10 秒
    for i in $(seq 1 10); do
      kill -0 "$OLD_PID" 2>/dev/null || break
      sleep 1
    done
    kill -0 "$OLD_PID" 2>/dev/null && kill -9 "$OLD_PID" || true
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
ARGS=(-wss "$WSS_URL" -sqlite "$SQLITE_PATH" -api-addr "$API_ADDR")
[[ -n "$RPC_URL"    ]] && ARGS+=(-rpc    "$RPC_URL")
[[ -n "$MYSQL_DSN"  ]] && ARGS+=(-mysql  "$MYSQL_DSN")
[[ -n "$HTTP_PROXY" ]] && ARGS+=(-proxy  "$HTTP_PROXY")

# ── 后台启动 ──────────────────────────────────────────────────────────────────
echo "[INFO] 启动 uma-sync，日志 → $LOG_FILE"
nohup ./uma-sync "${ARGS[@]}" >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "[INFO] 已启动 PID=$(cat "$PID_FILE")"
