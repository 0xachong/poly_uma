#!/usr/bin/env python3
import argparse
import asyncio
import contextlib
import math
import os
import random
import ssl
import statistics
import sys
import time
from dataclasses import dataclass
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed


def _fd_count_linux() -> Optional[int]:
    if sys.platform != "linux":
        return None
    try:
        return len(os.listdir("/proc/self/fd"))
    except Exception:
        return None


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return float("nan")
    if p <= 0:
        return sorted_values[0]
    if p >= 100:
        return sorted_values[-1]
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return d0 + d1


def _fmt_ms(x: float) -> str:
    if math.isnan(x):
        return "NaN"
    if x >= 1000:
        return f"{x/1000.0:.3f}s"
    return f"{x:.2f}ms"


@dataclass
class ConnStats:
    connect_ms: float
    bytes_in: int
    bytes_out: int
    messages_in: int
    messages_out: int
    connected_at: float
    last_msg_at: float
    last_send_at: float


class Counter:
    def __init__(self) -> None:
        self.ok = 0
        self.failed = 0
        self.closed = 0
        self.errors = 0
        self.fail_reasons: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def inc(self, name: str, reason: Optional[str] = None) -> None:
        async with self._lock:
            setattr(self, name, getattr(self, name) + 1)
            if reason:
                self.fail_reasons[reason] = self.fail_reasons.get(reason, 0) + 1


async def _one_connection(
    *,
    url: str,
    open_timeout_s: float,
    ping_interval_s: float,
    ping_timeout_s: float,
    ssl_ctx: Optional[ssl.SSLContext],
    counter: Counter,
    stats_out: dict[int, ConnStats],
    idx: int,
    client_send_bps: int,
    client_frame_bytes: int,
    expect_echo: bool,
) -> None:
    t0 = time.perf_counter()
    try:
        async with websockets.connect(
            url,
            open_timeout=open_timeout_s,
            close_timeout=2.0,
            ping_interval=ping_interval_s if ping_interval_s > 0 else None,
            ping_timeout=ping_timeout_s if ping_timeout_s > 0 else None,
            ssl=ssl_ctx,
            max_size=None,
        ) as ws:
            t1 = time.perf_counter()
            await counter.inc("ok")

            s = ConnStats(
                connect_ms=(t1 - t0) * 1000.0,
                bytes_in=0,
                bytes_out=0,
                messages_in=0,
                messages_out=0,
                connected_at=time.time(),
                last_msg_at=time.time(),
                last_send_at=time.time(),
            )
            stats_out[idx] = s

            frame = ("x" * client_frame_bytes) if client_frame_bytes > 0 else ""

            async def sender() -> None:
                if client_send_bps <= 0 or client_frame_bytes <= 0:
                    return
                tick_s = 0.01
                budget = 0.0
                budget_per_tick = float(client_send_bps) * tick_s
                frame_size = float(client_frame_bytes)
                while True:
                    await asyncio.sleep(tick_s)
                    budget += budget_per_tick
                    while budget >= frame_size:
                        try:
                            await ws.send(frame)
                        except ConnectionClosed:
                            return
                        s.messages_out += 1
                        s.bytes_out += client_frame_bytes
                        s.last_send_at = time.time()
                        budget -= frame_size

            send_task = asyncio.create_task(sender())
            try:
                async for msg in ws:
                    s.messages_in += 1
                    if isinstance(msg, (bytes, bytearray)):
                        s.bytes_in += len(msg)
                    else:
                        s.bytes_in += len(msg.encode("utf-8", errors="ignore"))
                    s.last_msg_at = time.time()
                    if expect_echo and isinstance(msg, str) and client_frame_bytes > 0:
                        _ = len(msg)
            finally:
                send_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await send_task

    except (websockets.InvalidStatus, websockets.InvalidURI, websockets.InvalidHandshake) as e:
        await counter.inc("failed", reason=type(e).__name__)
    except (asyncio.TimeoutError, TimeoutError) as e:
        await counter.inc("failed", reason=type(e).__name__)
    except OSError as e:
        await counter.inc("failed", reason=f"OSError:{getattr(e, 'errno', 'na')}")
    except websockets.ConnectionClosed as e:
        await counter.inc("closed", reason=f"ConnectionClosed:{getattr(e, 'code', 'na')}")
    except Exception as e:
        await counter.inc("errors", reason=type(e).__name__)
    finally:
        # 连接结束后，从“当前存活”集合移除
        stats_out.pop(idx, None)


async def main_async() -> None:
    ap = argparse.ArgumentParser(description="WS/WSS 最大连接数与吞吐压测（带精确限速）")
    ap.add_argument("--url", required=True, help="WS/WSS URL")
    ap.add_argument("--target", type=int, default=5000, help="目标连接数上限")
    ap.add_argument("--ramp", type=int, default=200, help="每秒新建连接数")
    ap.add_argument("--stable", type=float, default=60.0, help="稳定保持时长（秒）")
    ap.add_argument("--open-timeout", type=float, default=10.0, help="握手超时（秒）")
    ap.add_argument("--ping-interval", type=float, default=20.0, help="ping 间隔（秒），0 禁用")
    ap.add_argument("--ping-timeout", type=float, default=20.0, help="ping 超时（秒），0 禁用")
    ap.add_argument("--client-send-bps", type=int, default=0, help="客户端上行速率（字节/秒），0 不发")
    ap.add_argument("--client-frame-bytes", type=int, default=256, help="客户端单帧大小（字节）")
    ap.add_argument("--expect-echo", action="store_true", help="期望服务端回显（bench echo 场景）")
    ap.add_argument("--insecure", action="store_true", help="wss 跳过证书校验（仅测试）")
    ap.add_argument("--max-fail-rate", type=float, default=0.02, help="建连失败率阈值（超过停止拉升）")
    ap.add_argument("--report-interval", type=float, default=1.0, help="状态打印间隔（秒）")
    ap.add_argument("--seed", type=int, default=42, help="随机种子")
    args = ap.parse_args()

    ssl_ctx: Optional[ssl.SSLContext] = None
    if args.url.startswith("wss://"):
        ssl_ctx = ssl.create_default_context()
        if args.insecure:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE

    counter = Counter()
    # stats 是“当前存活连接”的集合（连接关闭会 pop）
    stats: dict[int, ConnStats] = {}
    tasks: list[asyncio.Task] = []
    rng = random.Random(args.seed)
    start = time.time()
    target_reached = False

    async def reporter() -> None:
        last_in = 0
        last_out = 0
        last_t = time.time()
        while True:
            await asyncio.sleep(args.report_interval)
            now = time.time()
            alive_now = len(stats)
            attempted = counter.ok + counter.failed + counter.errors + counter.closed
            fail_total = counter.failed + counter.errors
            fail_rate = (fail_total / attempted) if attempted > 0 else 0.0

            total_in = sum(s.bytes_in for s in stats.values())
            total_out = sum(s.bytes_out for s in stats.values())
            dt = now - last_t
            in_bps = (total_in - last_in) / dt if dt > 0 else 0.0
            out_bps = (total_out - last_out) / dt if dt > 0 else 0.0
            last_in = total_in
            last_out = total_out
            last_t = now

            fd = _fd_count_linux()
            fd_str = f" fd={fd}" if fd is not None else ""
            print(
                f"t+{now-start:.0f}s alive={alive_now} attempted={attempted} "
                f"ok={counter.ok} failed={counter.failed} closed={counter.closed} errors={counter.errors} "
                f"fail_rate={fail_rate*100:.2f}% in={in_bps/1024:.1f}KB/s out={out_bps/1024:.1f}KB/s{fd_str}",
                flush=True,
            )

    rep_task = asyncio.create_task(reporter())
    idx = 0
    try:
        while True:
            alive_now = len(stats)
            if alive_now >= args.target:
                target_reached = True
                break

            attempted = counter.ok + counter.failed + counter.errors + counter.closed
            fail_total = counter.failed + counter.errors
            fail_rate = (fail_total / attempted) if attempted > 0 else 0.0
            if attempted >= 200 and fail_rate > args.max_fail_rate:
                break

            batch = min(args.ramp, args.target - alive_now)
            for _ in range(batch):
                idx += 1
                if rng.random() < 0.05:
                    await asyncio.sleep(0)
                t = asyncio.create_task(
                    _one_connection(
                        url=args.url,
                        open_timeout_s=args.open_timeout,
                        ping_interval_s=args.ping_interval,
                        ping_timeout_s=args.ping_timeout,
                        ssl_ctx=ssl_ctx,
                        counter=counter,
                        stats_out=stats,
                        idx=idx,
                        client_send_bps=args.client_send_bps,
                        client_frame_bytes=args.client_frame_bytes,
                        expect_echo=args.expect_echo,
                    )
                )
                tasks.append(t)
            await asyncio.sleep(1.0)

        stable_start = time.time()
        while time.time() - stable_start < args.stable:
            await asyncio.sleep(0.5)

    finally:
        rep_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await rep_task

        # 这里只统计“当前仍存活”的连接的建连耗时分位数（更贴近现状）
        conn_ms = [s.connect_ms for s in stats.values()]
        conn_ms.sort()
        if conn_ms:
            avg = statistics.fmean(conn_ms)
            p50 = _percentile(conn_ms, 50)
            p95 = _percentile(conn_ms, 95)
            p99 = _percentile(conn_ms, 99)
            mx = conn_ms[-1]
        else:
            avg = p50 = p95 = p99 = mx = float("nan")

        attempted = counter.ok + counter.failed + counter.errors + counter.closed
        fail_total = counter.failed + counter.errors
        fail_rate = (fail_total / attempted) if attempted > 0 else 0.0

        total_in = sum(s.bytes_in for s in stats.values())
        total_out = sum(s.bytes_out for s in stats.values())

        print("\n=== Summary ===")
        print(f"url={args.url}")
        print(f"target={args.target} ramp={args.ramp}/s stable={args.stable}s open_timeout={args.open_timeout}s")
        print(f"attempted={attempted} ok={counter.ok} failed={counter.failed} closed={counter.closed} errors={counter.errors}")
        print(f"fail_rate={fail_rate*100:.2f}% target_reached={target_reached}")
        print(f"alive_at_end={len(stats)}")
        print(
            "connect_latency(current_alive): "
            f"avg={_fmt_ms(avg)} p50={_fmt_ms(p50)} p95={_fmt_ms(p95)} p99={_fmt_ms(p99)} max={_fmt_ms(mx)}"
        )
        print(f"bytes_total(current_alive): in={total_in} out={total_out}")
        if counter.fail_reasons:
            top = sorted(counter.fail_reasons.items(), key=lambda kv: kv[1], reverse=True)[:10]
            print("top_fail_reasons=" + ", ".join([f"{k}={v}" for k, v in top]))

        for t in tasks:
            t.cancel()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()

