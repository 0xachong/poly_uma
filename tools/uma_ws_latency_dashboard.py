#!/usr/bin/env python3
"""Read-only HTTP dashboard for UMA WebSocket latency JSONL files."""

import argparse
import json
import math
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import urlparse


HTML = r"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>UMA WS 实时延迟</title>
<style>
:root{color-scheme:dark;--bg:#090d16;--panel:#111827;--line:#273247;--text:#e8edf7;--muted:#8b98ad;--green:#37d67a;--yellow:#f5b942;--red:#ff647c;--blue:#62a8ff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0,#14213a 0,var(--bg) 38%);color:var(--text);font:14px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace}
main{max-width:1450px;margin:auto;padding:24px}.head{display:flex;justify-content:space-between;gap:16px;align-items:end;margin-bottom:18px}h1{font:700 25px/1.1 system-ui;margin:0}.sub{color:var(--muted);margin-top:7px}.live{display:flex;align-items:center;gap:8px;color:var(--green)}.dot{width:9px;height:9px;border-radius:50%;background:currentColor;box-shadow:0 0 15px currentColor}.grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px}.card,.panel{background:linear-gradient(145deg,#151d2d,#0f1623);border:1px solid var(--line);border-radius:12px}.card{padding:14px}.label{color:var(--muted);font-size:12px}.value{font:700 23px/1.2 system-ui;margin-top:7px}.good{color:var(--green)}.warn{color:var(--yellow)}.bad{color:var(--red)}.panels{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px}.panel{padding:16px;overflow:hidden}h2{font:650 16px system-ui;margin:0 0 14px}.barrow{display:grid;grid-template-columns:65px 1fr 72px;gap:10px;align-items:center;margin:9px 0}.track{height:8px;background:#202a3d;border-radius:8px;overflow:hidden}.bar{height:100%;background:linear-gradient(90deg,var(--blue),#9f7aea);border-radius:8px}.events{margin-top:14px}.tablewrap{overflow:auto;max-height:470px}table{width:100%;border-collapse:collapse;white-space:nowrap}th,td{text-align:left;padding:9px 11px;border-bottom:1px solid #202a3d}th{position:sticky;top:0;background:#111827;color:var(--muted);font-weight:500}td.hash{max-width:230px;overflow:hidden;text-overflow:ellipsis}.empty{color:var(--muted);padding:30px;text-align:center}.foot{color:var(--muted);margin-top:12px;font-size:12px}
.charthead{display:flex;align-items:center;justify-content:space-between;gap:12px}.charthead h2{margin:0}.charthead select{background:#0d1421;color:var(--text);border:1px solid var(--line);border-radius:7px;padding:7px 10px}.chartlegend{display:flex;gap:18px;color:var(--muted);font-size:12px;margin:12px 0}.chartlegend span:before{content:'';display:inline-block;width:18px;height:3px;margin-right:6px;vertical-align:middle;border-radius:2px;background:var(--c)}.chartwrap{height:340px;position:relative;overflow-x:auto;overflow-y:hidden}.chartwrap svg{display:block;width:100%;min-width:900px;height:100%}.gridline{stroke:#344158;stroke-width:1}.gridline.vertical{stroke:#202a3d;stroke-dasharray:4 5}.axisline{stroke:#65738b;stroke-width:1.3}.axistext{fill:#b3bfd2;font-size:12px}.axistitle{fill:#d5ddec;font-size:12px;font-weight:700}.chartline{fill:none;stroke-width:2.5;stroke-linejoin:round;stroke-linecap:round}.chartpoint{stroke:#101827;stroke-width:1.5}.chartnote{color:var(--muted);font-size:12px;margin-top:8px}
.subscription-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.subscription{border:1px solid #273247;background:#0d1421;border-radius:10px;padding:14px}.subscription-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:12px}.subscription-name{font:700 17px system-ui}.badge{border:1px solid currentColor;border-radius:999px;padding:3px 9px;font-size:12px}.subscription-data{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.datum .label{margin-bottom:3px}.datum .value{font-size:15px;margin:0;word-break:break-word}.subscription-error{color:var(--muted);margin-top:12px;padding-top:10px;border-top:1px solid #202a3d;overflow-wrap:anywhere}
.metricbar{display:flex;justify-content:space-between;align-items:center;gap:12px}.metricbar select{background:#0d1421;color:var(--text);border:1px solid var(--line);border-radius:7px;padding:7px 10px}.rate{font-weight:700}.miss-table td:nth-child(n+3),.miss-table th:nth-child(n+3){text-align:right}
@media(max-width:1000px){.grid{grid-template-columns:repeat(3,1fr)}.panels,.subscription-grid{grid-template-columns:1fr}}@media(max-width:600px){main{padding:14px}.grid{grid-template-columns:repeat(2,1fr)}.head{align-items:start;flex-direction:column}.subscription-data{grid-template-columns:repeat(2,1fr)}}
</style></head><body><main>
<div class="head"><div><h1>UMA WebSocket 延迟看板</h1><div class="sub">采集端 43.135.87.223 → 数据服务 43.154.60.204:8011</div></div><div class="live"><span class="dot"></span><span id="state">读取中</span></div></div>
<div class="grid" id="cards"></div>
<section class="panel events"><h2>下游 WSS 订阅状态</h2><div class="subscription-grid" id="subscriptions"></div><div class="chartnote">展示本看板采集客户端到 Master 的两条独立 WSS 连接；不是 Master 上全部外部订阅者数量。</div></section>
<section class="panel events"><div class="metricbar"><h2>市场富化 Miss 统计</h2><select id="missHours"><option value="1">最近 1 小时</option><option value="5" selected>最近 5 小时</option><option value="24">最近 24 小时</option><option value="168">最近 7 天</option></select></div><div class="grid" id="missCards"></div><div class="panels"><div><h2>按 Tag</h2><div class="tablewrap"><table class="miss-table"><thead><tr><th>Tag</th><th>Tag ID</th><th>总事件</th><th>Miss</th><th>映射 Miss</th><th>快照 Miss</th><th>Miss 率</th></tr></thead><tbody id="missTags"></tbody></table></div></div><div><h2>按事件类型</h2><table class="miss-table"><thead><tr><th>事件</th><th>总事件</th><th>Miss</th><th>Miss 率</th></tr></thead><tbody id="missEvents"></tbody></table></div></div><div class="chartnote">分母为窗口内链上唯一 propose/dispute；分子为首次富化未命中的唯一 tx_hash + log_index。多 Tag 市场会分别进入各 Tag 统计。</div></section>
<section class="panel events"><h2>最近富化 Miss</h2><div class="tablewrap"><table><thead><tr><th>时间 CST</th><th>事件</th><th>类型</th><th>Tag</th><th>Market</th><th>区块</th><th>Log</th><th>Transaction</th></tr></thead><tbody id="recentMisses"></tbody></table></div></section>
<div class="panels"><section class="panel"><h2>Proposed 延迟分布</h2><div id="proposedBars"></div></section><section class="panel"><h2>Disputed 延迟分布</h2><div id="disputedBars"></div></section></div>
<section class="panel events"><div class="charthead"><h2>端到端延迟趋势（最近 2 小时，5 分钟分桶）</h2><select id="metric"><option value="freshness" selected>端到端（链上→前端）</option><option value="service">Master 服务处理</option><option value="network">Master→前端网络</option></select></div><div class="chartlegend"><span style="--c:#37d67a">P50</span><span style="--c:#f5b942">P95</span><span style="--c:#ff647c">P99</span></div><div class="chartwrap" id="chart"></div><div class="chartnote">线性坐标；仅统计 proposed realtime，backfill 不进入曲线。悬停数据点可查看时间和精确延迟。</div></section>
<section class="panel events"><h2>最近收到的事件</h2><div class="tablewrap"><table><thead><tr><th>客户端接收时间 CST</th><th>链上时间 CST</th><th>类型</th><th>来源</th><th>区块新鲜度</th><th>服务处理</th><th>映射等待</th><th>持久化</th><th>补发排队</th><th>网络传输</th><th>区块时钟领先</th><th>区块</th><th>Log</th><th>Market</th><th>Transaction</th></tr></thead><tbody id="rows"></tbody></table></div></section>
<div class="foot" id="foot"></div></main>
<script>
const fmt=v=>v==null?'—':v<0?v.toFixed(0)+' ms':v>=1000?(v/1000).toFixed(3)+' s':v.toFixed(0)+' ms';
const cls=v=>v==null?'':v>10000?'bad':v>3000?'warn':'good';
const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function card(label,value,c=''){return `<div class="card"><div class="label">${label}</div><div class="value ${c}">${value}</div></div>`}
function bars(id,d){let max=Math.max(1,...d.buckets.map(x=>x.count));document.getElementById(id).innerHTML=d.buckets.map(x=>`<div class="barrow"><span>${x.label}</span><div class="track"><div class="bar" style="width:${100*x.count/max}%"></div></div><span>${x.count}</span></div>`).join('')||'<div class="empty">暂无数据</div>'}
function ago(seconds){if(seconds==null)return '—';if(seconds<60)return Math.round(seconds)+' 秒前';if(seconds<3600)return Math.round(seconds/60)+' 分钟前';return (seconds/3600).toFixed(1)+' 小时前'}
function subscription(s){let state=s.connected?'在线':'断线',color=s.connected?'good':'bad',sources=s.messages_by_source||{};return `<article class="subscription"><div class="subscription-head"><div><div class="subscription-name">${esc(s.endpoint)}</div><div class="label">${esc(s.url)}</div></div><span class="badge ${color}">${state}</span></div><div class="subscription-data"><div class="datum"><div class="label">连接建立</div><div class="value">${esc(s.connected_since_cst)}</div></div><div class="datum"><div class="label">最后收包</div><div class="value">${esc(s.last_event_cst)}<br><span class="label">${ago(s.last_event_age_s)}</span></div></div><div class="datum"><div class="label">累计消息</div><div class="value">${s.messages_total}</div></div><div class="datum"><div class="label">连接握手</div><div class="value">${fmt(s.handshake_ms)}</div></div><div class="datum"><div class="label">Realtime</div><div class="value">${sources.realtime||0}</div></div><div class="datum"><div class="label">Delayed replay</div><div class="value">${sources.delayed_replay||0}</div></div><div class="datum"><div class="label">Backfill</div><div class="value">${sources.backfill||0}</div></div><div class="datum"><div class="label">断线记录</div><div class="value ${s.reconnects?'warn':'good'}">${s.reconnects}</div></div></div><div class="subscription-error">最近断线：${esc(s.last_disconnect_cst)} · 最近错误：${esc(s.last_error)}</div></article>`}
const rate=v=>(Number(v)||0).toFixed(2)+'%';
function renderMiss(m){if(!m||m.error){document.getElementById('missCards').innerHTML=card('统计状态',esc(m?.error||'暂无数据'),'bad');return}let o=m.overall||{};document.getElementById('missCards').innerHTML=card('总事件',o.total||0)+card('Miss',o.misses||0,o.misses?'bad':'good')+card('Miss 率',rate(o.miss_rate),o.miss_rate>5?'bad':o.miss_rate>1?'warn':'good')+card('映射 Miss',o.mapping_misses||0)+card('快照 Miss',o.snapshot_misses||0)+card('统计窗口',m.hours+' 小时');document.getElementById('missTags').innerHTML=(m.by_tag||[]).map(x=>`<tr><td>${esc(x.label)}</td><td>${esc(x.key)}</td><td>${x.total}</td><td>${x.misses}</td><td>${x.mapping_misses}</td><td>${x.snapshot_misses}</td><td class="rate ${x.miss_rate>5?'bad':x.miss_rate>1?'warn':'good'}">${rate(x.miss_rate)}</td></tr>`).join('')||'<tr><td colspan="7" class="empty">暂无数据</td></tr>';document.getElementById('missEvents').innerHTML=(m.by_event||[]).map(x=>`<tr><td>${esc(x.label)}</td><td>${x.total}</td><td>${x.misses}</td><td class="rate ${x.miss_rate>5?'bad':x.miss_rate>1?'warn':'good'}">${rate(x.miss_rate)}</td></tr>`).join('');document.getElementById('recentMisses').innerHTML=(m.recent_misses||[]).map(x=>`<tr><td>${new Date(x.observed_at_ms).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false})}</td><td>${esc(x.event_type)}</td><td>${esc(x.kind)}</td><td>${esc((x.tags||[]).map(t=>t.label||t.slug||t.id).join(', ')||'unknown')}</td><td>${esc(x.market_id)}</td><td>${x.block_number}</td><td>${x.log_index}</td><td class="hash" title="${esc(x.transaction_hash)}">${esc(x.transaction_hash)}</td></tr>`).join('')}
let latestSeries=[];
function drawChart(){let metric=document.getElementById('metric').value,metricLabel=document.getElementById('metric').selectedOptions[0].text,rows=latestSeries.map(x=>({...x,...x[metric]})),vals=rows.flatMap(x=>[x.p50,x.p95,x.p99]).filter(Number.isFinite),el=document.getElementById('chart');if(!vals.length){el.innerHTML='<div class="empty">当前时间窗口暂无 realtime 数据</div>';return}let W=1200,H=340,L=82,R=24,T=26,B=48,max=Math.max(...vals,1),power=Math.pow(10,Math.floor(Math.log10(max))),ratio=max/power,nice=(ratio<=1?1:ratio<=2?2:ratio<=5?5:10)*power,x=i=>L+i*(W-L-R)/Math.max(1,rows.length-1),y=v=>T+(nice-v)*(H-T-B)/nice,path=k=>{let parts=[],open=false;rows.forEach((r,i)=>{let v=r[k];if(Number.isFinite(v)){parts.push(`${open?'L':'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`);open=true}else open=false});return parts.join(' ')},colors={p50:'#37d67a',p95:'#f5b942',p99:'#ff647c'},yTicks=5,xStep=Math.max(1,Math.ceil(rows.length/7)),horizontal=Array.from({length:yTicks+1},(_,i)=>{let v=nice*(yTicks-i)/yTicks,yy=y(v);return `<line class="gridline" x1="${L}" y1="${yy}" x2="${W-R}" y2="${yy}"/><text class="axistext" x="${L-12}" y="${yy+4}" text-anchor="end">${fmt(v)}</text>`}).join(''),vertical=rows.map((r,i)=>(i%xStep==0||i==rows.length-1)?`<line class="gridline vertical" x1="${x(i)}" y1="${T}" x2="${x(i)}" y2="${H-B}"/><text class="axistext" x="${x(i)}" y="${H-19}" text-anchor="middle">${r.time}</text>`:'').join(''),lines=Object.keys(colors).map(k=>`<path class="chartline" stroke="${colors[k]}" d="${path(k)}"/>`).join(''),points=Object.keys(colors).map(k=>rows.map((r,i)=>Number.isFinite(r[k])?`<circle class="chartpoint" cx="${x(i)}" cy="${y(r[k])}" r="3.5" fill="${colors[k]}"><title>${r.time} · ${k.toUpperCase()} · ${fmt(r[k])}</title></circle>`:'').join('')).join('');el.innerHTML=`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(metricLabel)}线性延迟趋势图">${horizontal}${vertical}<line class="axisline" x1="${L}" y1="${T}" x2="${L}" y2="${H-B}"/><line class="axisline" x1="${L}" y1="${H-B}" x2="${W-R}" y2="${H-B}"/><text class="axistitle" x="${L}" y="15">延迟（毫秒 / 秒）</text><text class="axistitle" x="${W-R}" y="${H-5}" text-anchor="end">时间（CST）</text>${lines}${points}</svg>`}
document.addEventListener('change',e=>{if(e.target.id==='metric')drawChart();if(e.target.id==='missHours')refresh()});
async function refresh(){try{let hours=document.getElementById('missHours').value,r=await fetch('/api/status?hours='+hours,{cache:'no-store'}),d=await r.json(),p=d.endpoints.proposed,q=d.endpoints.disputed;
document.getElementById('state').textContent=(p.connected&&q.connected)?'两个订阅均在线':'存在订阅断线';document.querySelector('.live').className='live '+((p.connected&&q.connected)?'good':'bad');
document.getElementById('cards').innerHTML=card('Proposed 样本',p.count)+card('P50',fmt(p.p50_ms),cls(p.p50_ms))+card('P95',fmt(p.p95_ms),cls(p.p95_ms))+card('P99',fmt(p.p99_ms),cls(p.p99_ms))+card('最大延迟',fmt(p.max_ms),cls(p.max_ms))+card('>10秒',p.over_10s,p.over_10s?'bad':'good')+card('Disputed 样本',q.count)+card('Disputed P95',fmt(q.p95_ms),cls(q.p95_ms))+card('重连次数',p.reconnects+q.reconnects,(p.reconnects+q.reconnects)?'warn':'good')+card('>3秒',p.over_3s,p.over_3s?'warn':'good')+card('>30秒',p.over_30s,p.over_30s?'bad':'good')+card('数据文件',d.files);
document.getElementById('subscriptions').innerHTML=(d.subscriptions||[]).map(subscription).join('');renderMiss(d.enrichment);
bars('proposedBars',p);bars('disputedBars',q);latestSeries=d.latency_series||[];drawChart();document.getElementById('rows').innerHTML=d.recent.map(x=>`<tr><td>${x.received_cst}</td><td>${x.chain_cst}</td><td>${x.endpoint}</td><td>${x.source??'unknown'}</td><td class="${cls(x.lag_ms)}">${fmt(x.lag_ms)}</td><td>${fmt(x.service_processing_ms)}</td><td>${fmt(x.mapping_wait_ms)}</td><td>${fmt(x.mapping_persist_ms)}</td><td>${fmt(x.replay_queue_ms)}</td><td>${fmt(x.network_ms)}</td><td>${fmt(x.block_clock_lead_ms)}</td><td>${x.block_number??''}</td><td>${x.log_index??''}</td><td>${x.market_id??''}</td><td class="hash" title="${x.transaction_hash??''}">${x.transaction_hash??''}</td></tr>`).join('');document.getElementById('foot').textContent=`浏览器每 3 秒刷新 · 区块新鲜度负值按 0 计，原始负值显示为区块时钟领先 · 服务端统计窗口：全部保留数据 · API 生成耗时 ${d.generated_in_ms} ms · 更新时间 ${d.generated_cst}`;
}catch(e){document.getElementById('state').textContent='读取失败';document.querySelector('.live').className='live bad'}}refresh();setInterval(refresh,3000);
</script></body></html>"""


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * fraction
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return round(ordered[lo], 3)
    return round(ordered[lo] * (hi - pos) + ordered[hi] * (pos - lo), 3)


def endpoint_stats(records, endpoint, connected, reconnects):
    values = [r["lag_ms"] for r in records if r.get("endpoint") == endpoint and isinstance(r.get("lag_ms"), (int, float))]
    ranges = [(-float("inf"), 1000, "<1s"), (1000, 3000, "1–3s"), (3000, 10000, "3–10s"), (10000, 30000, "10–30s"), (30000, float("inf"), ">30s")]
    return {
        "connected": connected.get(endpoint, False), "reconnects": reconnects.get(endpoint, 0), "count": len(values),
        "min_ms": round(min(values), 3) if values else None, "p50_ms": percentile(values, .5),
        "p95_ms": percentile(values, .95), "p99_ms": percentile(values, .99), "max_ms": round(max(values), 3) if values else None,
        "over_3s": sum(v > 3000 for v in values), "over_10s": sum(v > 10000 for v in values), "over_30s": sum(v > 30000 for v in values),
        "buckets": [{"label": label, "count": sum(low <= v < high for v in values)} for low, high, label in ranges],
    }


def latency_series(records, now, window_seconds=2 * 3600, bucket_seconds=5 * 60):
    end = int(now // bucket_seconds) * bucket_seconds + bucket_seconds
    start = end - window_seconds
    buckets = {stamp: [] for stamp in range(start, end, bucket_seconds)}
    for record in records:
        if record.get("endpoint") != "proposed" or record.get("source") != "realtime":
            continue
        received = record.get("received_at_ns")
        if not isinstance(received, (int, float)):
            continue
        stamp = int((received / 1e9) // bucket_seconds) * bucket_seconds
        if stamp in buckets:
            buckets[stamp].append(record)

    def metric(items, key):
        values = [item[key] for item in items if isinstance(item.get(key), (int, float))]
        return {"p50": percentile(values, .5), "p95": percentile(values, .95), "p99": percentile(values, .99)}

    return [{
        "timestamp": stamp,
        "time": time.strftime("%H:%M", time.localtime(stamp)),
        "count": len(items),
        "freshness": metric(items, "lag_ms"),
        "service": metric(items, "service_processing_ms"),
        "network": metric(items, "network_ms"),
    } for stamp, items in buckets.items()]


class DataStore:
    def __init__(self, data_dir, master_api):
        self.data_dir = Path(data_dir)
        self.master_api = master_api.rstrip("/")
        self.lock = threading.Lock()
        self.cached_signature = None
        self.cached_result = None

    def status(self, hours=5):
        started = time.perf_counter()
        files = sorted(self.data_dir.glob("events-*.jsonl"))
        signature = (hours, int(time.time() // 15), tuple((str(p), p.stat().st_size, p.stat().st_mtime_ns) for p in files))
        with self.lock:
            if signature == self.cached_signature and self.cached_result:
                result = dict(self.cached_result)
                result["generated_in_ms"] = round((time.perf_counter() - started) * 1000, 2)
                return result
        events, connected, reconnects = [], {}, {}
        connections = {name: {
            "endpoint": name, "connected": False, "connected_since_ns": None,
            "last_disconnect_ns": None, "last_error": None, "handshake_ms": None,
            "last_event_ns": None, "messages_total": 0, "messages_by_source": {},
        } for name in ("proposed", "disputed")}
        for path in files:
            with path.open(encoding="utf-8") as stream:
                for line in stream:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if record.get("record_type") == "event":
                        raw_lag = record.get("raw_lag_ms", record.get("lag_ms"))
                        if isinstance(raw_lag, (int, float)):
                            record["raw_lag_ms"] = raw_lag
                            record["lag_ms"] = max(0, raw_lag)
                            record["block_clock_lead_ms"] = max(0, -raw_lag)
                        events.append(record)
                        endpoint = record.get("endpoint")
                        if endpoint in connections:
                            state = connections[endpoint]
                            state["last_event_ns"] = record.get("received_at_ns")
                            state["messages_total"] += 1
                            source = record.get("source") or "unknown"
                            state["messages_by_source"][source] = state["messages_by_source"].get(source, 0) + 1
                    elif record.get("record_type") == "connection":
                        endpoint = record.get("endpoint")
                        is_connected = record.get("state") == "connected"
                        connected[endpoint] = is_connected
                        if endpoint in connections:
                            state = connections[endpoint]
                            state["connected"] = is_connected
                            if is_connected:
                                state["connected_since_ns"] = record.get("received_at_ns")
                                state["handshake_ms"] = record.get("handshake_ms")
                            else:
                                state["last_disconnect_ns"] = record.get("received_at_ns")
                                state["last_error"] = record.get("error")
                        if not is_connected:
                            reconnects[endpoint] = reconnects.get(endpoint, 0) + 1
        recent = []
        for record in events[-1000:][::-1]:
            item = dict(record)
            ns = item.get("received_at_ns", 0)
            item["received_cst"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ns / 1e9))
            chain_ts = item.get("event_timestamp", 0)
            item["chain_cst"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(chain_ts)) if chain_ts else "—"
            recent.append(item)
        now = time.time()
        subscriptions = []
        for endpoint in ("proposed", "disputed"):
            state = dict(connections[endpoint])
            state["reconnects"] = reconnects.get(endpoint, 0)
            state["url"] = f"ws://43.154.60.204:8011/uma/v1/ws/{endpoint}"
            for source_key, output_key in (("connected_since_ns", "connected_since_cst"), ("last_disconnect_ns", "last_disconnect_cst"), ("last_event_ns", "last_event_cst")):
                ns = state.get(source_key)
                state[output_key] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ns / 1e9)) if ns else "—"
            last_event_ns = state.get("last_event_ns")
            state["last_event_age_s"] = round(max(0, now - last_event_ns / 1e9), 1) if last_event_ns else None
            subscriptions.append(state)
        result = {
            "generated_cst": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)), "files": len(files),
            "endpoints": {name: endpoint_stats(events, name, connected, reconnects) for name in ("proposed", "disputed")},
            "subscriptions": subscriptions,
            "latency_series": latency_series(events, now),
            "recent": recent, "generated_in_ms": round((time.perf_counter() - started) * 1000, 2),
        }
        try:
            with urlopen(f"{self.master_api}/uma/v1/metrics/enrichment?hours={hours}", timeout=5) as response:
                result["enrichment"] = json.load(response)
        except Exception as exc:
            result["enrichment"] = {"error": f"{type(exc).__name__}: {exc}"}
        with self.lock:
            self.cached_signature, self.cached_result = signature, result
        return result


def handler_factory(store):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlparse(self.path).path
            if path in ("/", "/index.html"):
                body, content_type = HTML.encode(), "text/html; charset=utf-8"
            elif path == "/api/status":
                try:
                    hours = int(dict(part.split("=", 1) for part in urlparse(self.path).query.split("&") if "=" in part).get("hours", "5"))
                except ValueError:
                    hours = 5
                body, content_type = json.dumps(store.status(hours), ensure_ascii=False).encode(), "application/json; charset=utf-8"
            elif path == "/healthz":
                body, content_type = b'{"status":"ok"}', "application/json"
            else:
                self.send_error(404); return
            self.send_response(200); self.send_header("Content-Type", content_type); self.send_header("Cache-Control", "no-store"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
        def log_message(self, fmt, *args):
            return
    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--listen", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--master-api", default="http://43.154.60.204:8011")
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.listen, args.port), handler_factory(DataStore(args.data_dir, args.master_api)))
    server.serve_forever()


if __name__ == "__main__":
    main()
