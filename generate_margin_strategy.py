#!/usr/bin/env python3
import json, re, time, requests
from pathlib import Path
from datetime import datetime

BASE=Path(__file__).resolve().parent
OUT=BASE/'margin_strategy_latest.json'
HEAD={'User-Agent':'Mozilla/5.0'}
def n(x):
    try:return float(str(x).replace(',','').replace('+','').strip())
    except:return None
def get(u):
    last=None
    for attempt in range(3):
        try:
            r=requests.get(u,headers=HEAD,timeout=45);r.raise_for_status();return r.json()
        except requests.RequestException as exc:
            last=exc
            if attempt<2:time.sleep(2**attempt)
    raise last
def band(x):
    return '低於130%' if x<130 else '130–140%' if x<140 else '140–150%' if x<150 else '150–166%' if x<166 else '166%以上'
def strategy(x):
    if x['maintenance']<140:return '高風險'
    if x.get('margin_change_20d') is not None and x['margin_change_20d']>0.10 and x.get('drawdown',0)<-0.10:return '擁擠下跌'
    if x.get('margin_change_20d') is not None and x['margin_change_20d']<-0.10 and x.get('drawdown',0)<-0.10:return '融資清洗'
    if x['maintenance']<166:return '防守觀察'
    return '正常觀察'

old=json.loads(OUT.read_text()) if OUT.exists() else {'stocks':[]}
state={x['id']:x for x in old.get('stocks',[])}
trend=get('https://wwwc.twse.com.tw/rwd/marginTrading/BFIJ3U_TREND?response=json&days=6')['data']
credit=get('https://wwwc.twse.com.tw/rwd/marginTrading/MI_MARGN_TREND?response=json&days=6')['data'][-1]
otc=get('https://www.tpex.org.tw/www/zh-tw/dashboardOtc/margin?lang=zh-tw')
asof=datetime.strptime(trend[-1]['date'],'%Y%m%d').strftime('%Y-%m-%d')
if old.get('as_of') == asof:
    print(json.dumps({'as_of':asof,'status':'unchanged','reason':'official data has not advanced'},ensure_ascii=False))
    raise SystemExit(0)
twse_p=get(f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={trend[-1]["date"]}&type=ALLBUT0999&response=json')
twse_m=get(f'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={trend[-1]["date"]}&selectType=STOCK&response=json')
tpex_p=get(f'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date={asof.replace("-","/")}&id=&response=json')
tpex_m=get(f'https://www.tpex.org.tw/www/zh-tw/margin/balance?date={asof.replace("-","/")}&id=&response=json')
pt=next(t for t in twse_p['tables'] if '每日收盤行情' in (t.get('title') or ''));mt=next(t for t in twse_m['tables'] if '融資融券彙總' in (t.get('title') or ''))
prices={r[0]:r for r in pt['data']};updates=[]
taiex_table=twse_p['tables'][0]
taiex=n(next(r for r in taiex_table['data'] if '發行量加權股價指數' in str(r[0]))[1])
for market,mrows,pmap,isotc in [('上市',mt['data'],prices,False),('上櫃',tpex_m['tables'][0]['data'],{r[0]:r for r in tpex_p['tables'][0]['data']},True)]:
  for r in mrows:
    sid=str(r[0]).strip()
    if not re.fullmatch(r'[1-9]\d{3}',sid) or sid not in pmap:continue
    p=pmap[sid]; close=n(p[2] if isotc else p[8]); high=n(p[5] if isotc else p[6]); bal=n(r[6]); buy=n(r[3] if isotc else r[2]); short=n(r[14] if isotc else r[12])
    if not close or not bal or bal<=0:continue
    prev=state.get(sid,{}); pc=prev.get('cost') or close; oldbal=max(0,bal-(buy or 0))
    hist=list(prev.get('balance_history') or []); same_day=prev.get('as_of')==asof or (hist and hist[-1][0]==asof)
    cost=pc if same_day else (pc*oldbal+close*(buy or 0))/bal
    if same_day and hist:hist[-1]=[asof,bal]
    else:hist.append([asof,bal])
    hist=hist[-21:]; b20=hist[0][1] if len(hist)>=21 else None
    ph=list(prev.get('price_history') or [])
    if same_day and ph:ph[-1]=[asof,high or close]
    else:ph.append([asof,high or close])
    ph=ph[-20:]; h20=max(x[1] for x in ph)
    change20=bal/b20-1 if b20 else prev.get('margin_change_20d')
    x={'id':sid,'name':str(r[1]).strip(),'market':market,'close':close,'high20':h20,'drawdown':close/h20-1,'margin_balance':bal,'short_balance':short or 0,'margin_change_20d':change20,'cost':cost,'maintenance':close/(cost*.6)*100,'quality':prev.get('quality','低'),'balance_history':hist,'price_history':ph,'as_of':asof,'volume':n(p[8] if isotc else p[2]) or 0}
    x['band']=band(x['maintenance']);x['strategy']=strategy(x);updates.append(x)
updates.sort(key=lambda x:x['maintenance'])
payload={'as_of':asof,'generated_at':datetime.now().isoformat(timespec='seconds'),'market':{'keep_rate':trend[-1]['keepRate'],'prev_keep_rate':trend[-2]['keepRate'],'twse_margin_amount_100m':credit['marginAmt']/10000,'twse_margin_shares':credit['marginShr'],'twse_short_shares':credit['shortShr'],'tpex_margin_amount_100m':otc['marginBalance'],'tpex_short_shares':otc['shortBalance']},'stocks':updates}
OUT.write_text(json.dumps(payload,ensure_ascii=False,separators=(',',':')))
cutoff=(datetime.strptime(asof,'%Y-%m-%d').date().replace(day=1))
for digit in sorted(set(x['id'][0] for x in updates)):
    hp=BASE/f'margin_history_{digit}.json'
    shard=json.loads(hp.read_text()) if hp.exists() else {'as_of':asof,'index_name':'發行量加權股價指數','stocks':{}}
    shard['as_of']=asof
    for x in (z for z in updates if z['id'][0]==digit):
        item=shard['stocks'].setdefault(x['id'],{'name':x['name'],'market':x['market'],'h':[]})
        row=[asof,x['close'],x['volume'],round(x['maintenance'],2),taiex,x['margin_balance']]
        if item['h'] and item['h'][-1][0]==asof:item['h'][-1]=row
        else:item['h'].append(row)
        item['h']=item['h'][-135:]
    hp.write_text(json.dumps(shard,ensure_ascii=False,separators=(',',':')))
print(json.dumps({'as_of':asof,'stocks':len(updates),'keep_rate':trend[-1]['keepRate']},ensure_ascii=False))
