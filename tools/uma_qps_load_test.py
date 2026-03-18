#!/usr/bin/env python3
import argparse
import asyncio
import contextlib
import math
import random
import statistics
import time
from dataclasses import dataclass
from typing import Iterable, Optional

import aiohttp


def _now_ts() -> int:
    return int(time.time())


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


@dataclass
class RunResult:
    target_qps: int
    duration_s: float
    total_requests: int
    ok: int
    non_2xx: int
    timeouts: int
    errors: int
    achieved_qps: float
    lat_ms_avg: float
    lat_ms_p50: float
    lat_ms_p90: float
    lat_ms_p95: float
    lat_ms_p99: float
    lat_ms_max: float


async def _one_request(
    session: aiohttp.ClientSession,
    url: str,
    timeout_s: float,
    sem: Optional[asyncio.Semaphore],
) -> tuple[bool, bool, bool, float]:
    """
    Returns: (ok_2xx, non_2xx, timeout, latency_ms)
    Any other exception is counted as errors by caller.
    """
    t0 = time.perf_counter()
    try:
        async with (contextlib.nullcontext() if sem is None else sem):
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
                await resp.read()
                t1 = time.perf_counter()
                lat_ms = (t1 - t0) * 1000.0
                if 200 <= resp.status < 300:
                    return True, False, False, lat_ms
                return False, True, False, lat_ms
    except asyncio.TimeoutError:
        t1 = time.perf_counter()
        return False, False, True, (t1 - t0) * 1000.0


async def _run_qps(
    *,
    url: str,
    target_qps: int,
    duration_s: float,
    warmup_s: float,
    timeout_s: float,
    max_in_flight: int,
    jitter_ms: float,
    conn_limit: int,
    seed: int,
) -> RunResult:
    rng = random.Random(seed + target_qps)
    sem = asyncio.Semaphore(max_in_flight) if max_in_flight > 0 else None
    connector = aiohttp.TCPConnector(limit=conn_limit, ttl_dns_cache=300)

    latencies_ms: list[float] = []
    ok = non_2xx = timeouts = errors = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        if warmup_s > 0:
            warmup_end = time.perf_counter() + warmup_s
            while time.perf_counter() < warmup_end:
                try:
                    _, _, _, _ = await _one_request(session, url, timeout_s, sem)
                except Exception:
                    pass

        start = time.perf_counter()
        end = start + duration_s
        interval_s = 1.0 / float(target_qps)
        next_at = start

        tasks: list[asyncio.Task] = []

        while True:
            now = time.perf_counter()
            if now >= end:
                break

            sleep_s = next_at - now
            if sleep_s > 0:
                await asyncio.sleep(sleep_s)

            jitter_s = (rng.random() * 2.0 - 1.0) * (jitter_ms / 1000.0)
            next_at += interval_s + jitter_s

            tasks.append(asyncio.create_task(_one_request(session, url, timeout_s, sem)))

        results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
        for item in results:
            if isinstance(item, Exception):
                errors += 1
                continue
            r_ok, r_non2xx, r_to, r_lat = item
            latencies_ms.append(r_lat)
            if r_ok:
                ok += 1
            elif r_non2xx:
                non_2xx += 1
            elif r_to:
                timeouts += 1
            else:
                errors += 1

        wall = time.perf_counter() - start
        total = ok + non_2xx + timeouts + errors
        achieved = (total / wall) if wall > 0 else 0.0

        lat_sorted = sorted(latencies_ms)
        lat_avg = statistics.fmean(lat_sorted) if lat_sorted else float("nan")

        return RunResult(
            target_qps=target_qps,
            duration_s=wall,
            total_requests=total,
            ok=ok,
            non_2xx=non_2xx,
            timeouts=timeouts,
            errors=errors,
            achieved_qps=achieved,
            lat_ms_avg=lat_avg,
            lat_ms_p50=_percentile(lat_sorted, 50),
            lat_ms_p90=_percentile(lat_sorted, 90),
            lat_ms_p95=_percentile(lat_sorted, 95),
            lat_ms_p99=_percentile(lat_sorted, 99),
            lat_ms_max=(lat_sorted[-1] if lat_sorted else float("nan")),
        )


def _build_url(base: str) -> str:
    now = _now_ts()
    from_ts = now - 2 * 60 * 60
    joiner = "&" if "?" in base else "?"
    return f"{base}{joiner}from_ts={from_ts}&to_ts={now}"


def _fmt_ms(x: float) -> str:
    if math.isnan(x):
        return "NaN"
    if x >= 1000:
        return f"{x/1000.0:.3f}s"
    return f"{x:.2f}ms"


def _print_table(results: Iterable[RunResult]) -> None:
    header = (
        "target_qps  achieved_qps  total  ok  non_2xx  timeouts  errors  "
        "avg    p50    p90    p95    p99    max"
    )
    print(header)
    for r in results:
        print(
            f"{r.target_qps:10d}  "
            f"{r.achieved_qps:11.1f}  "
            f"{r.total_requests:5d}  "
            f"{r.ok:2d}  "
            f"{r.non_2xx:7d}  "
            f"{r.timeouts:8d}  "
            f"{r.errors:6d}  "
            f"{_fmt_ms(r.lat_ms_avg):>6}  "
            f"{_fmt_ms(r.lat_ms_p50):>6}  "
            f"{_fmt_ms(r.lat_ms_p90):>6}  "
            f"{_fmt_ms(r.lat_ms_p95):>6}  "
            f"{_fmt_ms(r.lat_ms_p99):>6}  "
            f"{_fmt_ms(r.lat_ms_max):>6}"
        )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="对指定 UMA API 进行固定 QPS 压测，输出各档位延迟分位数/错误率。"
    )
    ap.add_argument(
        "--base-url",
        default="http://43.154.60.204:8011/uma/v1/proposed?limit=50",
        help="基础 URL。脚本会自动追加 from_ts=now-2h、to_ts=now。",
    )
    ap.add_argument("--qps", default="10,100,500,1000", help="逗号分隔的 QPS 档位。")
    ap.add_argument("--duration", type=float, default=30.0, help="每个档位压测时长（秒）。")
    ap.add_argument("--warmup", type=float, default=2.0, help="每个档位预热时长（秒）。")
    ap.add_argument("--timeout", type=float, default=10.0, help="单请求超时（秒）。")
    ap.add_argument(
        "--max-in-flight",
        type=int,
        default=2000,
        help="在途请求上限（防止本机爆内存/FD）。<=0 表示不限制。",
    )
    ap.add_argument(
        "--conn-limit",
        type=int,
        default=2000,
        help="aiohttp 连接池上限（TCPConnector limit）。",
    )
    ap.add_argument(
        "--jitter-ms",
        type=float,
        default=1.0,
        help="发包间隔 jitter（毫秒），避免严格整点脉冲。0 表示无 jitter。",
    )
    ap.add_argument("--seed", type=int, default=42, help="随机种子（用于 jitter）。")
    args = ap.parse_args()

    url = _build_url(args.base_url)
    qps_list = [int(x.strip()) for x in args.qps.split(",") if x.strip()]
    print(f"URL: {url}")
    print(f"QPS: {qps_list} | duration={args.duration}s warmup={args.warmup}s timeout={args.timeout}s")

    async def _runner() -> list[RunResult]:
        out: list[RunResult] = []
        for q in qps_list:
            r = await _run_qps(
                url=url,
                target_qps=q,
                duration_s=args.duration,
                warmup_s=args.warmup,
                timeout_s=args.timeout,
                max_in_flight=args.max_in_flight,
                jitter_ms=args.jitter_ms,
                conn_limit=args.conn_limit,
                seed=args.seed,
            )
            out.append(r)
        return out

    results = asyncio.run(_runner())
    _print_table(results)


if __name__ == "__main__":
    main()

