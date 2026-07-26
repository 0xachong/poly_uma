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
main{max-width:1500px;margin:auto;padding:24px}.head{display:flex;justify-content:space-between;align-items:end;gap:20px;margin-bottom:18px}h1{margin:0;font-size:27px;letter-spacing:-.5px}.sub,.muted{color:var(--muted)}.live{display:flex;gap:8px;align-items:center}.dot{width:9px;height:9px;border-radius:50%;background:var(--green);box-shadow:0 0 16px var(--green)}
.summary{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}.card,.node{background:linear-gradient(145deg,#102131,#0b1621);border:1px solid var(--line);border-radius:13px}.card{padding:15px}.label{font-size:12px;color:var(--muted)}.value{font-size:24px;font-weight:750;margin-top:6px}.nodes{display:grid;grid-template-columns:repeat(2,1fr);gap:13px;margin-top:15px}.node{padding:17px}.nodehead{display:flex;justify-content:space-between;gap:12px;align-items:start}.name{font-size:18px;font-weight:750}.badge{border:1px solid currentColor;border-radius:999px;padding:3px 9px;font-size:12px}.ok{color:var(--green)}.warn{color:var(--amber)}.bad{color:var(--red)}.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:17px 0}.stat b{display:block;font-size:17px;margin-top:3px}.streams{display:grid;grid-template-columns:1fr 1fr;gap:8px}.stream{background:#08131d;border:1px solid #1b3041;border-radius:9px;padding:10px}.streamtop{display:flex;justify-content:space-between}.actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px;padding-top:14px;border-top:1px solid var(--line)}
button{background:#122738;color:var(--text);border:1px solid #31506a;border-radius:8px;padding:8px 11px;cursor:pointer}button:hover{border-color:var(--cyan)}button.danger{border-color:#7f3442;color:#ff9bab}button.primary{border-color:#277d79;color:#7cf4ed}.weight{display:flex;align-items:center;gap:7px;margin-left:auto}input{width:63px;background:#08131d;color:var(--text);border:1px solid #31506a;border-radius:7px;padding:8px}
.notice{margin-top:15px;padding:12px 14px;background:#201c0c;border:1px solid #66521f;border-radius:10px;color:#ffd981}.foot{margin-top:15px;color:var(--muted);font-size:12px}
@media(max-width:950px){.summary{grid-template-columns:repeat(2,1fr)}.nodes{grid-template-columns:1fr}}@media(max-width:600px){main{padding:14px}.stats{grid-template-columns:repeat(2,1fr)}.head{align-items:start;flex-direction:column}.weight{margin-left:0}}
</style>
</head>
<body><main>
<div class="head"><div><h1>UMA Slave Cluster</h1><div class="sub">预发布控制面 · 43.135.4.241 · 生产流量尚未接入</div></div><div class="live"><span class="dot"></span><span id="refresh">加载中</span></div></div>
<div class="summary" id="summary"></div>
<div class="notice">当前入口仅用于测试。DRAIN只停止新连接；MAINT会立即从负载池摘除节点。生产切流需要单独确认。</div>
<section class="nodes" id="nodes"></section>
<div class="foot" id="foot"></div>
</main>
<script>
const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const card=(l,v,c='')=>'<div class="card"><div class="label">'+l+'</div><div class="value '+c+'">'+v+'</div></div>';
const stream=(name,s)=>'<div class="stream"><div class="streamtop"><b>'+name+'</b><span class="'+(s?.upstream_connected?'ok':'bad')+'">'+(s?.upstream_connected?'上游在线':'上游断线')+'</span></div><div class="muted">下游 '+(s?.subscribers??0)+' · 收包 '+(s?.messages_received??0)+' · 慢客户端 '+(s?.slow_clients_disconnected??0)+'</div></div>';
function nodeHTML(n){let h=n.health||{},ss=h.streams||{},status=n.healthy&&n.haproxy_status.startsWith('UP')?'ok':n.haproxy_status==='DRAIN'?'warn':'bad';return '<article class="node"><div class="nodehead"><div><div class="name">'+esc(n.id)+'</div><div class="muted">'+esc(n.address)+'</div></div><span class="badge '+status+'">'+esc(n.haproxy_status)+'</span></div><div class="stats"><div class="stat"><span class="label">HAProxy连接</span><b>'+n.connections+'</b></div><div class="stat"><span class="label">权重</span><b>'+n.weight+'</b></div><div class="stat"><span class="label">运行时间</span><b>'+Math.floor((h.uptime_seconds||0)/60)+'m</b></div><div class="stat"><span class="label">HTTP缓存</span><b>'+(h.http_cache?.entries??0)+'/'+(h.http_cache?.capacity??0)+'</b></div></div><div class="streams">'+stream('Proposed',ss['/uma/v1/ws/proposed'])+stream('Disputed',ss['/uma/v1/ws/disputed'])+'</div><div class="actions"><button class="primary" onclick="act(\''+n.id+'\',\'ready\')">恢复接流</button><button onclick="act(\''+n.id+'\',\'drain\')">优雅摘流</button><button onclick="act(\''+n.id+'\',\'maintenance\')">维护摘除</button><button class="danger" onclick="act(\''+n.id+'\',\'force\')">断开并摘除</button><span class="weight"><input id="w-'+n.id+'" type="number" min="0" max="100" value="100"><button onclick="weight(\''+n.id+'\')">设权重</button></span></div></article>'}
async function act(id,action,value=0){if((action==='force')&&!confirm('确认断开 '+id+' 的全部现有连接并摘除节点？'))return;let r=await fetch('/api/nodes/'+encodeURIComponent(id)+'/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,value})});if(!r.ok)alert(await r.text());await refresh()}
function weight(id){let v=Number(document.getElementById('w-'+id).value);act(id,'weight',v)}
async function refresh(){try{let r=await fetch('/api/status',{cache:'no-store'}),d=await r.json(),nodes=d.nodes||[],healthy=nodes.filter(x=>x.healthy).length,connections=nodes.reduce((a,x)=>a+x.connections,0),upstreams=nodes.reduce((a,x)=>a+Object.values(x.health?.streams||{}).filter(s=>s.upstream_connected).length,0);document.getElementById('summary').innerHTML=card('节点在线',healthy+'/'+nodes.length,healthy===nodes.length?'ok':'bad')+card('测试入口连接',connections)+card('Master上游WSS',upstreams,upstreams===nodes.length*2?'ok':'warn')+card('接流节点',nodes.filter(x=>x.haproxy_status.startsWith('UP')).length)+card('控制器运行',Math.floor(d.uptime_seconds/60)+'m');document.getElementById('nodes').innerHTML=nodes.map(nodeHTML).join('');document.getElementById('refresh').textContent='控制面在线';document.getElementById('foot').textContent='每3秒刷新 · '+new Date(d.generated_at_ms).toLocaleString()+(d.haproxy_error?' · HAProxy: '+d.haproxy_error:'')}catch(e){document.getElementById('refresh').textContent='读取失败'}}refresh();setInterval(refresh,3000);
</script></body></html>`
