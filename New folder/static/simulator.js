let chain=[];let snapshot=null;let exposureChart=null;let activeView='gex';let playbackTimer=null;let playbackRunning=false;let chainLoading=false;const PLAYBACK_DELAY_MS=1200;
const $=id=>document.getElementById(id);
const num=v=>Number.isFinite(Number(v))?Number(v):0;
const fmt=v=>v===null||v===undefined||!Number.isFinite(Number(v))?'-':Number(v).toLocaleString('en-IN',{maximumFractionDigits:2});
const oiL=v=>v===null||v===undefined?'-':(Number(v)/100000).toFixed(2);
const signed=v=>{if(v===null||v===undefined)return'-';const n=Number(v);return `${n>0?'+':''}${(n/100000).toFixed(2)}`};
function metricValue(row,side){const m=$('greekMetric').value;const key=`${side}_${m}`;const v=row[key];if(v===null||v===undefined)return'-';return m==='gamma'?Number(v).toFixed(6):Number(v).toFixed(4)}
async function fetchJson(url){const r=await fetch(url);const d=await r.json();if(!r.ok||d.ok===false)throw new Error(d.error||'Request failed');return d}
async function loadDefaults(){const d=await fetchJson(`/api/defaults?dataset=${$('symbol').value}`);if(!$('queryDate').value)$('queryDate').value=d.query_date;if(!$('queryTime').value)$('queryTime').value=d.query_time||'09:30'}
async function loadChain(){if(chainLoading)return;chainLoading=true;try{$('refreshBtn').disabled=true;setPlaybackButtonsDisabled(true);const q=new URLSearchParams({dataset:$('symbol').value,date:$('queryDate').value,time:$('queryTime').value,interval:$('interval').value,strike_count:$('strikeCount').value,expiry:$('expirySelect').value||'',_:Date.now()});snapshot=await fetchJson(`/api/chain?${q}`);chain=snapshot.rows||[];renderHeader();renderExpiry();renderChain();renderLevels();renderExposure()}catch(e){stopPlayback();alert(e.message)}finally{$('refreshBtn').disabled=false;chainLoading=false;setPlaybackButtonsDisabled(false)}}
function setPlaybackButtonsDisabled(disabled){
  ['prevIntervalBtn','nextIntervalBtn'].forEach(id=>{const el=$(id);if(el)el.disabled=disabled});
}
function timeToMinutes(value){const [h,m]=String(value||'09:15').split(':').map(Number);return (Number.isFinite(h)?h:9)*60+(Number.isFinite(m)?m:15)}
function minutesToTime(total){const safe=Math.max(0,Math.min(23*60+59,total));return `${String(Math.floor(safe/60)).padStart(2,'0')}:${String(safe%60).padStart(2,'0')}`}
async function moveInterval(direction){
  if(chainLoading)return false;
  const input=$('queryTime');
  const step=Math.max(1,num($('interval').value));
  const current=timeToMinutes(input.value);
  const marketOpen=9*60+15;
  const marketClose=15*60+30;
  const target=current+(direction*step);
  if(target<marketOpen){input.value=minutesToTime(marketOpen);return false}
  if(target>marketClose){input.value=minutesToTime(marketClose);stopPlayback();return false}
  input.value=minutesToTime(target);
  await loadChain();
  return target<marketClose;
}
function updatePlayButton(){
  const btn=$('playPauseBtn');if(!btn)return;
  btn.textContent=playbackRunning?'❚❚':'▶';
  btn.classList.toggle('playing',playbackRunning);
  btn.title=playbackRunning?'Pause simulation':'Play simulation';
  btn.setAttribute('aria-label',btn.title);
}
function stopPlayback(){
  playbackRunning=false;
  if(playbackTimer){clearTimeout(playbackTimer);playbackTimer=null}
  updatePlayButton();
}
async function playbackTick(){
  if(!playbackRunning)return;
  const canContinue=await moveInterval(1);
  if(playbackRunning&&canContinue)playbackTimer=setTimeout(playbackTick,PLAYBACK_DELAY_MS);
  else stopPlayback();
}
function togglePlayback(){
  if(playbackRunning){stopPlayback();return}
  playbackRunning=true;updatePlayButton();
  playbackTimer=setTimeout(playbackTick,150);
}
function renderHeader(){$('spotValue').textContent=fmt(snapshot.spot);$('vixValue').textContent=fmt(snapshot.india_vix);$('dteValue').textContent=fmt(snapshot.dte)}
function renderExpiry(){const s=$('expirySelect');const old=s.value;s.innerHTML='';(snapshot.available_expiries||[]).forEach(x=>{const o=document.createElement('option');o.value=x.value;o.textContent=x.label;if(x.value===(old||snapshot.expiry))o.selected=true;s.appendChild(o)})}
function renderChain(){const body=$('chainBody');body.innerHTML='';const metric=$('greekMetric').value;$('ceGreekHead').textContent=metric.toUpperCase();$('peGreekHead').textContent=metric.toUpperCase();let call=0,put=0;chain.forEach(r=>{call+=num(r.ce_oi);put+=num(r.pe_oi);const tr=document.createElement('tr');if(r.atm)tr.className='atm';tr.innerHTML=`<td>${oiL(r.ce_oi)}</td><td class="${num(r.ce_change_oi)>=0?'positive':'negative'}">${signed(r.ce_change_oi)}</td><td>${fmt(r.ce_ltp)}</td><td>${fmt(num(r.ce_iv)*100)}</td><td class="positive">${metricValue(r,'ce')}</td><td class="strike-cell">${fmt(r.strike)}${r.atm?'<span class="atm-badge">ATM</span>':''}</td><td class="negative">${metricValue(r,'pe')}</td><td>${fmt(num(r.pe_iv)*100)}</td><td>${fmt(r.pe_ltp)}</td><td class="${num(r.pe_change_oi)>=0?'positive':'negative'}">${signed(r.pe_change_oi)}</td><td>${oiL(r.pe_oi)}</td>`;body.appendChild(tr)});$('callTotals').textContent=`Total Call OI: ${oiL(call)} L`;$('putTotals').textContent=`Total Put OI: ${oiL(put)} L`}
function renderLevels(){const l=snapshot.levels||{};const strikes=chain.map(r=>num(r.strike));const min=Math.min(...strikes),max=Math.max(...strikes);const y=v=>max===min?50:((num(v)-min)/(max-min))*100;[['r2Marker','R2',l.r2],['r1Marker','R1',l.r1],['spotMarker','Spot',snapshot.spot],['s1Marker','S1',l.s1],['s2Marker','S2',l.s2]].forEach(([id,label,v])=>{const el=$(id);el.style.top=`${100-y(v)}%`;el.innerHTML=`${label}<span>${fmt(v)}</span>`});[['r2Value',l.r2],['r1Value',l.r1],['spotLevel',snapshot.spot],['s1Value',l.s1],['s2Value',l.s2],['gammaFlip',l.gamma_flip],['zeroGamma',l.zero_gamma],['maxPositive',l.max_positive_gex],['maxNegative',l.max_negative_gex],['callWall',l.call_wall],['putWall',l.put_wall]].forEach(([id,v])=>$(id).textContent=fmt(v));$('totalGex').textContent=fmt(l.total_gex);$('marketBias').textContent=num(l.total_gex)>=0?'Bullish / stabilising':'Bearish / unstable'}
function exposures(){return chain.map(r=>{let value=0;if(activeView==='gex')value=num(r.net_gex);if(activeView==='dex')value=(num(r.ce_delta)*num(r.ce_oi)+num(r.pe_delta)*num(r.pe_oi))*num(snapshot.lot_size)*num(snapshot.spot);if(activeView==='vex')value=(num(r.ce_vega)*num(r.ce_oi)+num(r.pe_vega)*num(r.pe_oi))*num(snapshot.lot_size);if(activeView==='tex')value=(num(r.ce_theta)*num(r.ce_oi)+num(r.pe_theta)*num(r.pe_oi))*num(snapshot.lot_size);return{strike:r.strike,value}})}
function renderExposure(){const titles={gex:'Gamma Exposure (GEX)',dex:'Delta Exposure (DEX)',vex:'Vega Exposure (VEX)',tex:'Theta Exposure (TEX)'};$('chartTitle').textContent=titles[activeView];const rows=exposures();if(exposureChart)exposureChart.destroy();exposureChart=new Chart($('exposureChart'),{type:'bar',data:{labels:rows.map(x=>x.strike),datasets:[{label:titles[activeView],data:rows.map(x=>x.value),backgroundColor:rows.map(x=>x.value>=0?'rgba(13,160,77,.9)':'rgba(240,40,60,.9)'),borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>`${titles[activeView]}: ${fmt(c.raw)}`}}},scales:{x:{title:{display:true,text:'Strike Price'},grid:{display:false}},y:{title:{display:true,text:'Exposure'},grid:{color:'#e8edf3'}}}}})}
$('refreshBtn').addEventListener('click',()=>{stopPlayback();loadChain()});$('prevIntervalBtn').addEventListener('click',()=>{stopPlayback();moveInterval(-1)});$('nextIntervalBtn').addEventListener('click',()=>{stopPlayback();moveInterval(1)});$('playPauseBtn').addEventListener('click',togglePlayback);$('queryTime').addEventListener('change',stopPlayback);$('interval').addEventListener('change',stopPlayback);$('greekMetric').addEventListener('change',renderChain);$('expirySelect').addEventListener('change',loadChain);$('symbol').addEventListener('change',async()=>{await loadDefaults();await loadChain()});document.querySelectorAll('.exposure-tabs button').forEach(b=>b.addEventListener('click',()=>{document.querySelectorAll('.exposure-tabs button').forEach(x=>x.classList.remove('active'));b.classList.add('active');activeView=b.dataset.view;renderExposure()}));
(async()=>{await loadDefaults();await loadChain()})();
