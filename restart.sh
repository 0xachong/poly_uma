#!/bin/sh
set -eu

cd "$(dirname "$0")"

# ── 配置文件（可选）────────────────────────────────────────────────────────────
# 你可以在脚本同级目录放置一个 `.uma-sync.env`，内容为 shell 形式的 key=value：
# POLYGON_WSS_URL=...
# POLYGON_RPC_URL=...
# API_ADDR=0.0.0.0:7002
if [ -f ".uma-sync.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "./.uma-sync.env"
  set +a
fi

# ── 配置（优先读环境变量）────────────────────────────────────────────────────
WSS_URL="${POLYGON_WSS_URL:-}"
RPC_URL="${POLYGON_RPC_URL:-}"
SQLITE_PATH="${SQLITE_PATH:-uma_oo_events.sqlite}"
API_ADDR="${API_ADDR:-0.0.0.0:7002}"
HTTP_PROXY_URL="${HTTP_PROXY:-}"
LOG_FILE="${LOG_FILE:-uma-sync.log}"
PID_FILE="${PID_FILE:-.uma-sync.pid}"

# ── 校验必填 ────────────────────────────────────────────────────────────────
if [ -z "$WSS_URL" ]; then
  echo "[ERROR] 请设置 POLYGON_WSS_URL（可通过 `.uma-sync.env` 配置，或直接在环境变量中设置）" >&2
  exit 1
fi

# ── git pull ─────────────────────────────────────────────────────────────────
echo "[INFO] git pull ..."
git pull --ff-only

# ── 先构建新二进制（失败则不影响正在跑的进程）──────────────────────────────
echo "[INFO] go build (to uma-sync.new) ..."
go build -o uma-sync.new ./cmd/uma-sync

# 构建成功后再替换二进制，准备切换进程
mv uma-sync.new uma-sync

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

# ── 拼装启动参数 ──────────────────────────────────────────────────────────────
CMD_ARGS="-wss $WSS_URL -sqlite $SQLITE_PATH -api-addr $API_ADDR"
if [ -n "$RPC_URL" ];       then CMD_ARGS="$CMD_ARGS -rpc $RPC_URL";             fi
if [ -n "$HTTP_PROXY_URL" ];then CMD_ARGS="$CMD_ARGS -proxy $HTTP_PROXY_URL";    fi

# ── 后台启动 ──────────────────────────────────────────────────────────────────
echo "[INFO] 启动 uma-sync，日志 → $LOG_FILE"
# shellcheck disable=SC2086
nohup ./uma-sync $CMD_ARGS >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "[INFO] 已启动 PID=$(cat "$PID_FILE")"
