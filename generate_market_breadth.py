#!/usr/bin/env python3
import json, math, statistics, requests
from pathlib import Path
from datetime import datetime

BASE=Path(__file__).resolve().parent
OUT=BASE/'market_breadth_latest.json'
HEAD={'User-Agent':'Mozilla/5.0'}
INDUSTRIES={'01':'水泥工業','02':'食品工業','03':'塑膠工業','04':'紡織纖維','05':'電機機械','06':'電器電纜','08':'玻璃陶瓷','09':'造紙工業','10':'鋼鐵工業','11':'橡膠工業','12':'汽車工業','14':'建材營造','15':'航運業','16':'觀光餐旅','17':'金融保險','18':'貿易百貨','20':'其他業','21':'化學工業','22':'生技醫療','23':'油電燃氣','24':'半導體','25':'電腦及週邊','26':'光電業','27':'通信網路','28':'電子零組件','29':'電子通路','30':'資訊服務','31':'其他電子','32':'文化創意','33':'農業科技','34':'電子商務','35':'綠能環保','36':'數位雲端','37':'運動休閒','38':'居家生活'}

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
                code=str(row.get(industry_key,'')).strip()
                result[sid]={'shares':shares,'industry_code':code,'industry':INDUSTRIES.get(code,f'其他（{code or "未分類"}）'),'market':market}
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

def aggregate_industries(rows,total_market_cap):
    result={}
    for zone in ('上漲結構','套牢壓力','其他整理'):
        zone_rows=[x for x in rows if x['zone']==zone];zone_cap=sum(x['market_cap'] for x in zone_rows);zone_volume=sum(x['volume'] for x in zone_rows);zone_turnover=sum(x['turnover'] for x in zone_rows);groups={}
        for x in zone_rows:
            g=groups.setdefault(x['industry'],{'industry':x['industry'],'count':0,'market_cap':0,'volume':0,'turnover':0})
            g['count']+=1;g['market_cap']+=x['market_cap'];g['volume']+=x['volume'];g['turnover']+=x['turnover']
        result[zone]=sorted([{**g,'count_pct':round(g['count']/len(zone_rows)*100,2) if zone_rows else 0,
            'zone_cap_pct':round(g['market_cap']/zone_cap*100,2) if zone_cap else 0,'market_cap_pct':round(g['market_cap']/total_market_cap*100,2) if total_market_cap else 0,
            'zone_volume_pct':round(g['volume']/zone_volume*100,2) if zone_volume else 0,'zone_turnover_pct':round(g['turnover']/zone_turnover*100,2) if zone_turnover else 0} for g in groups.values()],key=lambda x:x['turnover'],reverse=True)
    return result

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
    metrics={k:0 for k in ('short_bull','short_bear','long_bull','long_bear')};weights={k:0.0 for k in metrics};total=0;cap_total=0;taiex=[];date_rows=[]
    for s in stocks.values():
        upto=[r for r in s['rows'] if r[0]<=date]
        if len(upto)<60 or upto[-1][0]!=date:continue
        c=classify(upto);cap=upto[-1][1]*s['company']['shares']
        total+=1;cap_total+=cap;taiex.append(upto[-1][4])
        zone='上漲結構' if c['rising'] else '套牢壓力' if c['trapped'] else '其他整理'
        date_rows.append({'industry':s['company']['industry'],'zone':zone,'market_cap':round(cap),'volume':round(upto[-1][2]),'turnover':round(upto[-1][1]*upto[-1][2])})
        for key in metrics:
            if c[key]:metrics[key]+=1;weights[key]+=cap
    if total>=100 and cap_total:
        series.append({'date':date,'taiex':round(statistics.median(taiex),2),'coverage':total,'zones':aggregate_industries(date_rows,cap_total),
            **{f'{k}_count_pct':round(metrics[k]/total*100,2) for k in metrics},
            **{f'{k}_cap_pct':round(weights[k]/cap_total*100,2) for k in metrics}})

latest=[]
for sid,s in stocks.items():
    if s['rows'][-1][0]!=series[-1]['date']:continue
    c=classify(s['rows']);last=s['rows'][-1];cap=last[1]*s['company']['shares']
    zone='上漲結構' if c['rising'] else '套牢壓力' if c['trapped'] else '其他整理'
    latest.append({'id':sid,'name':s['name'],'market':s['market'],'industry':s['company']['industry'],'industry_code':s['company']['industry_code'],'close':last[1],
        'market_cap':round(cap),'zone':zone,'short_state':'多頭排列' if c['short_bull'] else '空頭排列' if c['short_bear'] else '整理',
        'long_state':'多頭排列' if c['long_bull'] else '空頭排列' if c['long_bear'] else '整理','drawdown120':round(c['drawdown'],4),
        'overhead_volume_pct':round(c['overhead']*100,2) if c['overhead'] is not None else None,'vwap60':round(c['vwap60'],2),
        'volume':round(last[2]),'turnover':round(last[1]*last[2])})
latest.sort(key=lambda x:x['market_cap'],reverse=True)
total_market_cap=sum(x['market_cap'] for x in latest)
industries=aggregate_industries(latest,total_market_cap)
payload={'as_of':series[-1]['date'],'generated_at':datetime.now().isoformat(timespec='seconds'),'universe':{
    'stocks':len(stocks),'description':'具至少120個交易日行情的上市櫃普通股樣本','weight_method':'當日收盤價 × 最新官方已發行普通股數（市值近似權重）'},
    'series':series,'industries':industries,'stocks':latest,'disclaimer':'本工具為市場結構統計與歷史分類，不構成投資建議；分類與權重皆為估算。'}
if OUT.exists():
    previous=json.loads(OUT.read_text())
    comparable_previous={k:v for k,v in previous.items() if k!='generated_at'}
    comparable_current={k:v for k,v in payload.items() if k!='generated_at'}
    if comparable_previous==comparable_current:
        payload['generated_at']=previous.get('generated_at',payload['generated_at'])
OUT.write_text(json.dumps(payload,ensure_ascii=False,separators=(',',':')))
print(json.dumps({'as_of':payload['as_of'],'stocks':len(stocks),'series':len(series)},ensure_ascii=False))
