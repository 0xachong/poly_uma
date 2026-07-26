package main

const dashboardHTML = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>UMA Slave Control</title>
<style>
:root{color-scheme:dark;--bg:#071019;--panel:#0d1925;--line:#203548;--text:#e8f1f7;--muted:#8ba1b1;--cyan:#39d8d0;--green:#43dc8b;--amber:#ffbf5a;--red:#ff657a}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 10% 0,#14334a 0,var(--bg) 34%);color:var(--text);font:14px/1.45 Inter,system-ui,sans-serif}
main{max-width:1700px;margin:auto;padding:24px}.head{display:flex;justify-content:space-between;align-items:end;gap:20px;margin-bottom:18px}h1{margin:0;font-size:27px;letter-spacing:-.5px}.sub,.muted{color:var(--muted)}.live{display:flex;gap:8px;align-items:center}.dot{width:9px;height:9px;border-radius:50%;background:var(--green);box-shadow:0 0 16px var(--green)}
.summary{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}.card,.tablewrap{background:linear-gradient(145deg,#102131,#0b1621);border:1px solid var(--line);border-radius:13px}.card{padding:15px}.label{font-size:12px;color:var(--muted)}.value{font-size:24px;font-weight:750;margin-top:6px}
.tablewrap{margin-top:15px;overflow:auto}table{width:100%;border-collapse:collapse;white-space:nowrap}th,td{padding:12px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:middle}th{position:sticky;top:0;background:#102131;color:var(--muted);font-size:12px;font-weight:600;z-index:1}tbody tr:hover{background:#122638}tbody tr:last-child td{border-bottom:0}.name{font-weight:750}.address{font-size:12px;color:var(--muted)}.badge{display:inline-block;border:1px solid currentColor;border-radius:999px;padding:3px 9px;font-size:12px}.ok{color:var(--green)}.warn{color:var(--amber)}.bad{color:var(--red)}.streamcell{line-height:1.55}.streammeta{font-size:12px;color:var(--muted)}.actions{display:flex;gap:6px;align-items:center}
button{background:#122738;color:var(--text);border:1px solid #31506a;border-radius:8px;padding:7px 9px;cursor:pointer}button:hover{border-color:var(--cyan)}button.danger{border-color:#7f3442;color:#ff9bab}button.primary{border-color:#277d79;color:#7cf4ed}.weight{display:flex;align-items:center;gap:6px}input{width:58px;background:#08131d;color:var(--text);border:1px solid #31506a;border-radius:7px;padding:7px}
.notice{margin-top:15px;padding:12px 14px;background:#201c0c;border:1px solid #66521f;border-radius:10px;color:#ffd981}.foot{margin-top:15px;color:var(--muted);font-size:12px}
@media(max-width:950px){.summary{grid-template-columns:repeat(2,1fr)}}@media(max-width:600px){main{padding:14px}.head{align-items:start;flex-direction:column}}
</style>
</head>
<body><main>
<div class="head"><div><h1>UMA Slave Cluster</h1><div class="sub">预发布控制面 · 43.135.4.241 · 生产流量尚未接入</div></div><div class="live"><span class="dot"></span><span id="refresh">加载中</span></div></div>
<div class="summary" id="summary"></div>
<div class="notice">当前入口仅用于测试。DRAIN只停止新连接；MAINT会立即从负载池摘除节点。节点恢复后可执行渐进回流，每2秒小批量释放超载节点连接。 <button class="primary" id="rebalance" onclick="rebalance()">渐进回流</button></div>
<div class="tablewrap"><table>
<thead><tr><th>节点</th><th>状态</th><th>连接</th><th>权重</th><th>运行时间</th><th>HTTP缓存</th><th>Proposed</th><th>Disputed</th><th>流量操作</th><th>权重设置</th></tr></thead>
<tbody id="nodes"></tbody>
</table></div>
<div class="foot" id="foot"></div>
</main>
<script>
const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const card=(l,v,c='')=>'<div class="card"><div class="label">'+l+'</div><div class="value '+c+'">'+v+'</div></div>';
const stream=s=>'<div class="streamcell"><span class="'+(s?.upstream_connected?'ok':'bad')+'">'+(s?.upstream_connected?'上游在线':'上游断线')+'</span><div class="streammeta">下游 '+(s?.subscribers??0)+' · 收包 '+(s?.messages_received??0)+'<br>慢客户端 '+(s?.slow_clients_disconnected??0)+'</div></div>';
function nodeHTML(n){let h=n.health||{},ss=h.streams||{},status=n.healthy&&n.haproxy_status.startsWith('UP')?'ok':n.haproxy_status==='DRAIN'?'warn':'bad';return '<tr><td><div class="name">'+esc(n.id)+'</div><div class="address">'+esc(n.address)+'</div></td><td><span class="badge '+status+'">'+esc(n.haproxy_status)+'</span></td><td>'+n.connections+'</td><td>'+n.weight+'</td><td>'+Math.floor((h.uptime_seconds||0)/60)+'m</td><td>'+(h.http_cache?.entries??0)+'/'+(h.http_cache?.capacity??0)+'</td><td>'+stream(ss['/uma/v1/ws/proposed'])+'</td><td>'+stream(ss['/uma/v1/ws/disputed'])+'</td><td><div class="actions"><button class="primary" onclick="act(\''+n.id+'\',\'ready\')">恢复</button><button onclick="act(\''+n.id+'\',\'drain\')">摘流</button><button onclick="act(\''+n.id+'\',\'maintenance\')">维护</button><button class="danger" onclick="act(\''+n.id+'\',\'force\')">强制</button></div></td><td><span class="weight"><input id="w-'+n.id+'" type="number" min="0" max="100" value="'+n.weight+'"><button onclick="weight(\''+n.id+'\')">设置</button></span></td></tr>'}
async function act(id,action,value=0){if((action==='force')&&!confirm('确认断开 '+id+' 的全部现有连接并摘除节点？'))return;let r=await fetch('/api/nodes/'+encodeURIComponent(id)+'/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,value})});if(!r.ok)alert(await r.text());await refresh()}
function weight(id){let v=Number(document.getElementById('w-'+id).value);act(id,'weight',v)}
async function rebalance(){if(!confirm('确认渐进释放超载节点连接并重新均衡？客户端需要支持自动重连。'))return;let r=await fetch('/api/rebalance',{method:'POST'});if(!r.ok)alert(await r.text());await refresh()}
async function refresh(){try{let r=await fetch('/api/status',{cache:'no-store'}),d=await r.json(),nodes=d.nodes||[],healthy=nodes.filter(x=>x.healthy).length,connections=nodes.reduce((a,x)=>a+x.connections,0),upstreams=nodes.reduce((a,x)=>a+Object.values(x.health?.streams||{}).filter(s=>s.upstream_connected).length,0),rb=document.getElementById('rebalance');rb.disabled=!!d.rebalancing;rb.textContent=d.rebalancing?'回流中…':'渐进回流';document.getElementById('summary').innerHTML=card('节点在线',healthy+'/'+nodes.length,healthy===nodes.length?'ok':'bad')+card('测试入口连接',connections)+card('Master上游WSS',upstreams,upstreams===nodes.length*2?'ok':'warn')+card('接流节点',nodes.filter(x=>x.haproxy_status.startsWith('UP')).length)+card('控制器运行',Math.floor(d.uptime_seconds/60)+'m');document.getElementById('nodes').innerHTML=nodes.map(nodeHTML).join('');document.getElementById('refresh').textContent=d.rebalancing?'正在渐进回流':'控制面在线';document.getElementById('foot').textContent='每3秒刷新 · '+new Date(d.generated_at_ms).toLocaleString()+(d.haproxy_error?' · HAProxy: '+d.haproxy_error:'')}catch(e){document.getElementById('refresh').textContent='读取失败'}}refresh();setInterval(refresh,3000);
</script></body></html>`
