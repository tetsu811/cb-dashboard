#!/usr/bin/env python3
import json, math, statistics, requests
from pathlib import Path
from datetime import datetime

BASE=Path(__file__).resolve().parent
OUT=BASE/'market_breadth_latest.json'
HEAD={'User-Agent':'Mozilla/5.0'}

def number(value):
    try:return float(str(value).replace(',','').strip())
    except:return None

def avg(values, length):
    return sum(values[-length:])/length if len(values)>=length else None

def company_data():
    sources=[
        ('上市','https://openapi.twse.com.tw/v1/opendata/t187ap03_L','公司代號','已發行普通股數或TDR原股發行股數','產業別'),
        ('上櫃','https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O','SecuritiesCompanyCode','IssueShares','SecuritiesIndustryCode')]
    result={}
    for market,url,id_key,shares_key,industry_key in sources:
        rows=requests.get(url,headers=HEAD,timeout=45).json()
        for row in rows:
            sid=str(row.get(id_key,'')).strip();shares=number(row.get(shares_key))
            if len(sid)==4 and sid.isdigit() and shares and shares>0:
                result[sid]={'shares':shares,'industry':str(row.get(industry_key,'')).strip(),'market':market}
    return result

def classify(rows):
    closes=[r[1] for r in rows];volumes=[max(0,r[2] or 0) for r in rows]
    m5,m10,m20,m60=(avg(closes,n) for n in (5,10,20,60))
    if m60 is None:return None
    prev20=sum(closes[-21:-1])/20 if len(closes)>=21 else m20
    short_bull=closes[-1]>m5>m10>m20 and m20>prev20
    short_bear=closes[-1]<m5<m10<m20 and m20<prev20
    long_bull=closes[-1]>m20>m60
    long_bear=closes[-1]<m20<m60
    r60=rows[-60:];den=sum(max(0,r[2] or 0) for r in r60)
    vwap60=sum(r[1]*max(0,r[2] or 0) for r in r60)/den if den else m60
    r120=rows[-120:];den120=sum(max(0,r[2] or 0) for r in r120)
    overhead=sum(max(0,r[2] or 0) for r in r120 if r[1]>closes[-1])/den120 if den120 else None
    drawdown=closes[-1]/max(closes[-120:])-1
    rising=long_bull and closes[-1]>vwap60 and drawdown>=-.10
    trapped=closes[-1]<vwap60 and overhead is not None and overhead>=.60 and drawdown<=-.10 and m20<m60
    return {'short_bull':short_bull,'short_bear':short_bear,'long_bull':long_bull,'long_bear':long_bear,'rising':rising,'trapped':trapped,'vwap60':vwap60,'overhead':overhead,'drawdown':drawdown}

companies=company_data();stocks={}
for digit in '123456789':
    path=BASE/f'margin_history_{digit}.json'
    if not path.exists():continue
    shard=json.loads(path.read_text())
    for sid,item in shard.get('stocks',{}).items():
        if sid not in companies:continue
        rows=[r for r in item.get('h',[]) if r[1] and r[2] is not None]
        if len(rows)>=120:stocks[sid]={'name':item['name'],'market':item['market'],'rows':rows,'company':companies[sid]}

dates=sorted(set(r[0] for s in stocks.values() for r in s['rows']))
series=[]
for date in dates:
    metrics={k:0 for k in ('short_bull','short_bear','long_bull','long_bear')};weights={k:0.0 for k in metrics};total=0;cap_total=0;taiex=[]
    for s in stocks.values():
        upto=[r for r in s['rows'] if r[0]<=date]
        if len(upto)<60 or upto[-1][0]!=date:continue
        c=classify(upto);cap=upto[-1][1]*s['company']['shares']
        total+=1;cap_total+=cap;taiex.append(upto[-1][4])
        for key in metrics:
            if c[key]:metrics[key]+=1;weights[key]+=cap
    if total>=100 and cap_total:
        series.append({'date':date,'taiex':round(statistics.median(taiex),2),'coverage':total,
            **{f'{k}_count_pct':round(metrics[k]/total*100,2) for k in metrics},
            **{f'{k}_cap_pct':round(weights[k]/cap_total*100,2) for k in metrics}})

latest=[]
for sid,s in stocks.items():
    c=classify(s['rows']);last=s['rows'][-1];cap=last[1]*s['company']['shares']
    zone='上漲結構' if c['rising'] else '套牢壓力' if c['trapped'] else '其他整理'
    latest.append({'id':sid,'name':s['name'],'market':s['market'],'industry':s['company']['industry'],'close':last[1],
        'market_cap':round(cap),'zone':zone,'short_state':'多頭排列' if c['short_bull'] else '空頭排列' if c['short_bear'] else '整理',
        'long_state':'多頭排列' if c['long_bull'] else '空頭排列' if c['long_bear'] else '整理','drawdown120':round(c['drawdown'],4),
        'overhead_volume_pct':round(c['overhead']*100,2) if c['overhead'] is not None else None,'vwap60':round(c['vwap60'],2)})
latest.sort(key=lambda x:x['market_cap'],reverse=True)
payload={'as_of':series[-1]['date'],'generated_at':datetime.now().isoformat(timespec='seconds'),'universe':{
    'stocks':len(stocks),'description':'具至少120個交易日行情的上市櫃普通股樣本','weight_method':'當日收盤價 × 最新官方已發行普通股數（市值近似權重）'},
    'series':series,'stocks':latest,'disclaimer':'本工具為市場結構統計與歷史分類，不構成投資建議；分類與權重皆為估算。'}
if OUT.exists():
    previous=json.loads(OUT.read_text())
    comparable_previous={k:v for k,v in previous.items() if k!='generated_at'}
    comparable_current={k:v for k,v in payload.items() if k!='generated_at'}
    if comparable_previous==comparable_current:
        payload['generated_at']=previous.get('generated_at',payload['generated_at'])
OUT.write_text(json.dumps(payload,ensure_ascii=False,separators=(',',':')))
print(json.dumps({'as_of':payload['as_of'],'stocks':len(stocks),'series':len(series)},ensure_ascii=False))
