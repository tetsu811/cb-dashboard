let DATA,zone='上漲結構',selectedIndex=-1,industryChartObj,breadthChartObj;
const fmt=(v,d=0)=>v==null?'—':Number(v).toLocaleString('zh-TW',{maximumFractionDigits:d,minimumFractionDigits:d});
const pct=v=>v==null?'—':`${v>=0?'+':''}${fmt(v*100,1)}%`;
const volumeLabel=v=>v>=1e8?`${fmt(v/1e8,2)} 億股`:`${fmt(v/1e6,1)} 百萬股`;
const moneyLabel=v=>`${fmt(v/1e8,2)} 億元`;
const zoneRows=()=>DATA.series[selectedIndex]?.zones?.[zone]||[];

function renderTabs(){
  document.querySelectorAll('[data-zone]').forEach(b=>{
    const count=zoneRowsFor(b.dataset.zone).reduce((n,x)=>n+x.count,0);
    b.textContent=`${b.dataset.zone} ${fmt(count)} 檔`;
    b.classList.toggle('active',b.dataset.zone===zone);
  });
}
function zoneRowsFor(name){return DATA.series[selectedIndex]?.zones?.[name]||[]}
function topText(name){
  const rows=zoneRowsFor(name).slice(0,3);
  const count=rows.reduce((n,x)=>n+x.count,0);
  return rows.length?rows.map(x=>`${x.industry} ${x.count}檔／${moneyLabel(x.turnover)}`).join('、'):'目前無明顯產業群聚';
}
function renderSummary(){
  const x=DATA.series[selectedIndex],spread=x.long_bull_cap_pct-x.long_bull_count_pct;
  const breadth=x.long_bull_count_pct-x.long_bear_count_pct;
  const state=breadth>10?'多方家數較廣':breadth<-10?'空方家數較廣':'多空家數接近';
  const concentration=spread>10?'多方主要集中在大型權值股':spread<-10?'多方以中小型股擴散較明顯':'多方家數與市值權重接近';
  cards.innerHTML=`<div class=card><div class=label>${x.date} 多方家數比</div><div class=value>${fmt(x.long_bull_count_pct,1)}%</div><div class=note>符合收盤 ＞ 20日線 ＞ 60日線</div></div><div class=card><div class=label>空方家數比</div><div class=value>${fmt(x.long_bear_count_pct,1)}%</div><div class=note>符合收盤 ＜ 20日線 ＜ 60日線</div></div><div class=card><div class=label>多方市值權重</div><div class=value>${fmt(x.long_bull_cap_pct,1)}%</div><div class=note>${concentration}</div></div><div class=card><div class=label>加權指數</div><div class=value>${fmt(x.taiex,0)}</div><div class=note>${fmt(x.coverage)} 檔有效樣本</div></div>`;
  insight.innerHTML=`<strong>${x.date}｜${state}</strong><div>${concentration}。多方與空方家數差為 ${breadth>=0?'+':''}${fmt(breadth,1)} 個百分點。</div><div class="drivers"><b>上漲結構主要貢獻：</b>${topText('上漲結構')}<br><b>套牢壓力主要貢獻：</b>${topText('套牢壓力')}</div>`;
}
function renderIndustry(){
  const rows=zoneRows(),date=DATA.series[selectedIndex].date;
  zoneTitle.textContent=`${date}｜${zone}｜產業貢獻`;
  stockTitle.textContent=selectedIndex===DATA.series.length-1?`${zone}｜最新個股明細`:`最新交易日個股明細（產業圖目前停在 ${date}）`;
  industryBody.innerHTML=rows.map((x,i)=>`<tr><td>${i+1}</td><td>${x.industry}</td><td>${fmt(x.count)} 檔</td><td>${fmt(x.zone_cap_pct,1)}%</td><td>${volumeLabel(x.volume)}</td><td>${moneyLabel(x.turnover)}</td></tr>`).join('')||'<tr><td colspan="6">目前沒有可彙總的產業資料</td></tr>';
  industryChartObj?.destroy();const top=rows.slice(0,12).reverse();
  industryChartObj=new Chart(industryChart,{type:'bar',data:{labels:top.map(x=>x.industry),datasets:[{label:'區域內市值占比',data:top.map(x=>x.zone_cap_pct),backgroundColor:'#2457d6'},{label:'約當成交金額占比',data:top.map(x=>x.zone_turnover_pct),backgroundColor:'#e09f3e'}]},options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},plugins:{tooltip:{callbacks:{label:c=>`${c.dataset.label}：${fmt(c.raw,1)}%`}}},scales:{x:{beginAtZero:true,title:{display:true,text:'當日區域內占比 %'}}}}});
}
function renderTable(){
  const term=q.value.trim(),m=market.value,s=state.value;
  const rows=DATA.stocks.filter(x=>x.zone===zone&&(!term||(x.id+x.name+x.industry).includes(term))&&(!m||x.market===m)&&(!s||x.long_state===s));
  body.innerHTML=rows.map(x=>`<tr><td>${x.id}</td><td>${x.name}</td><td>${x.market}</td><td>${x.industry}</td><td><span class="pill ${x.zone==='上漲結構'?'rise':x.zone==='套牢壓力'?'trap':'other'}">${x.zone}</span></td><td>${fmt(x.close,2)}</td><td>${x.short_state}</td><td>${x.long_state}</td><td>${pct(x.drawdown120)}</td><td>${fmt(x.overhead_volume_pct,1)}%</td><td>${fmt(x.vwap60,2)}</td><td>${volumeLabel(x.volume)}</td><td>${moneyLabel(x.turnover)}</td><td>${fmt(x.market_cap/1e8,1)} 億</td></tr>`).join('')||'<tr><td colspan="14">目前篩選條件沒有資料</td></tr>';
}
function selectDate(index,autoZone=false){
  selectedIndex=Math.max(0,Math.min(index,DATA.series.length-1));
  if(autoZone){const up=zoneRowsFor('上漲結構').reduce((n,x)=>n+x.count,0),trap=zoneRowsFor('套牢壓力').reduce((n,x)=>n+x.count,0);zone=trap>up?'套牢壓力':'上漲結構'}
  renderTabs();renderSummary();renderIndustry();renderTable();breadthChartObj?.draw();
}
const structureOverlay={id:'structureOverlay',beforeDatasetsDraw(chart){
  if(!DATA)return;const {ctx,chartArea:{top,bottom},scales:{x}}=chart;ctx.save();
  DATA.series.forEach((r,i)=>{const left=i?((x.getPixelForValue(i-1)+x.getPixelForValue(i))/2):x.left,right=i<DATA.series.length-1?((x.getPixelForValue(i)+x.getPixelForValue(i+1))/2):x.right,d=r.long_bull_count_pct-r.long_bear_count_pct;ctx.fillStyle=d>10?'rgba(20,128,74,.055)':d<-10?'rgba(191,53,53,.055)':'rgba(100,116,139,.025)';ctx.fillRect(left,top,right-left,bottom-top)});ctx.restore();
},afterDatasetsDraw(chart){if(selectedIndex<0)return;const {ctx,chartArea:{top,bottom},scales:{x}}=chart,p=x.getPixelForValue(selectedIndex);ctx.save();ctx.strokeStyle='#e09f3e';ctx.lineWidth=2;ctx.setLineDash([5,4]);ctx.beginPath();ctx.moveTo(p,top);ctx.lineTo(p,bottom);ctx.stroke();ctx.restore()}};

fetch('market_breadth_latest.json?v='+Date.now()).then(r=>r.json()).then(d=>{
  DATA=d;selectedIndex=d.series.length-1;asof.textContent=`更新：${d.as_of}｜點線圖可查看歷史產業貢獻｜不構成投資建議`;
  const S=d.series,base=S[0].taiex,breadth=[{label:'多方家數比',data:S.map(x=>x.long_bull_count_pct),borderColor:'#14804a'},{label:'多方市值權重',data:S.map(x=>x.long_bull_cap_pct),borderColor:'#2457d6'},{label:'空方家數比',data:S.map(x=>x.long_bear_count_pct),borderColor:'#bf3535'},{label:'空方市值權重',data:S.map(x=>x.long_bear_cap_pct),borderColor:'#7754d8'}].map(x=>({...x,pointRadius:0,pointHitRadius:12,borderWidth:2,tension:.2,yAxisID:'y'}));
  breadth.unshift({label:'加權指數（起點=100）',data:S.map(x=>x.taiex/base*100),borderColor:'#111827',pointRadius:0,pointHitRadius:12,borderWidth:2.5,tension:.15,yAxisID:'index'});
  breadthChartObj=new Chart(breadthChart,{
    type:'line',data:{labels:S.map(x=>x.date),datasets:breadth},plugins:[structureOverlay],
    options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
      onClick:(event)=>{const p=breadthChartObj.getElementsAtEventForMode(event,'index',{intersect:false},true);if(p.length)selectDate(p[0].index,true)},
      plugins:{tooltip:{callbacks:{afterBody:items=>{const r=S[items[0].dataIndex];return [`加權指數：${fmt(r.taiex,0)} 點`,`點擊查看當日產業貢獻`]}}}},
      scales:{y:{min:0,max:100,title:{display:true,text:'股票家數／市值占比 %'}},index:{position:'right',grid:{drawOnChartArea:false},title:{display:true,text:'加權指數相對位置'}},x:{ticks:{maxTicksLimit:10}}}
    }
  });
  selectDate(selectedIndex);
}).catch(()=>asof.textContent='資料載入失敗，請稍後重試');

document.querySelectorAll('[data-zone]').forEach(b=>b.onclick=()=>{zone=b.dataset.zone;renderTabs();renderIndustry();renderTable()});
[q,market,state].forEach(el=>el.addEventListener('input',renderTable));
