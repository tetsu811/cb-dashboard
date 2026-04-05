#!/usr/bin/env python3
"""
可轉債策略儀表板 v4
- CB 資料：thefew.tw/cb（全部 400+ 筆）+ /cb/recent（含掛牌日，策略一用）
- 融券+借券：TWSE TWT93U（每日盤後自動更新，無需登入）
- 策略一：CBAS新上市（需掛牌日，來自 /cb/recent）
- 策略二：轉換套利（全部 CB）
"""

import requests
import json
import os
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta

# ── 路徑設定（GitHub Actions 用）─────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_HTML = os.path.join(BASE_DIR, 'index.html')

TODAY = date.today()
TODAY_STR = TODAY.strftime('%Y%m%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36',
    'Accept-Language': 'zh-TW,zh;q=0.9',
    'Referer': 'https://thefew.tw/',
}

# ─────────────────────────────────────────────────────────────
# 1. 抓全部 CB（thefew.tw/cb → 400+ 筆，含到期日、溢價、已轉換）
# ─────────────────────────────────────────────────────────────
def fetch_all_cbs():
    print("[1/3] 抓取全部CB (thefew.tw/cb)...")
    try:
        r = requests.get('https://thefew.tw/cb', headers=HEADERS, timeout=20)
        if r.status_code != 200 or 'login' in r.url.lower():
            raise ValueError(f"需要登入或失敗 status={r.status_code}")
        soup = BeautifulSoup(r.text, 'html.parser')
        rows = soup.select('table tbody tr')
        data = []
        for tr in rows:
            cells = tr.select('td')
            if len(cells) < 8:
                continue
            code_div = cells[0].select_one('div[class*="w-1/3"]')
            name_div = cells[0].select_one('div[class*="w-2/3"]')
            cb_code  = code_div.get_text(strip=True) if code_div else ''
            cb_name  = name_div.get_text(strip=True) if name_div else ''
            if not cb_code or len(cb_code) < 4:
                continue
            def parse_num(txt):
                m = re.match(r'^([\d.]+)', txt.strip())
                return float(m.group(1)) if m else None
            data.append({
                'cb_code':          cb_code,
                'cb_name':          cb_name,
                'stock_code':       cb_code[:4],
                'cb_price':         parse_num(cells[1].get_text()),
                'conv_val':         parse_num(cells[2].get_text()),
                'premium_rate':     parse_num(cells[3].get_text().replace('%','')),
                'stock_price':      parse_num(cells[4].get_text()),
                'conversion_price': parse_num(cells[5].get_text()),
                'converted_pct':    parse_num(cells[6].get_text().replace('%','')) or 0.0,
                'maturity_date':    cells[7].get_text(strip=True),
                'listing_date':     None,  # /cb 沒有掛牌日
            })
        print(f"  → 全部CB: {len(data)} 筆")
        return data
    except Exception as e:
        print(f"  ⚠ 無法抓取 thefew.tw/cb: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# 2. 抓近期CB（thefew.tw/cb/recent → 含掛牌日，策略一必需）
# ─────────────────────────────────────────────────────────────
def fetch_recent_cbs():
    print("[2/3] 抓取近期CB (thefew.tw/cb/recent)...")
    try:
        r = requests.get('https://thefew.tw/cb/recent', headers=HEADERS, timeout=20)
        if r.status_code != 200:
            raise ValueError(f"status={r.status_code}")
        soup = BeautifulSoup(r.text, 'html.parser')

        # 找含掛牌日的 JSON 資料（嵌在頁面 script 或 table 裡）
        data = []
        rows = soup.select('table tbody tr')
        for tr in rows:
            cells = tr.select('td')
            if len(cells) < 7:
                continue
            # 結構可能不同，依實際頁面調整
            texts = [c.get_text(strip=True) for c in cells]
            # 嘗試抓 cb_code（通常在第一欄）
            code_match = re.match(r'(\d{4,6})', texts[0])
            if not code_match:
                continue
            cb_code = code_match.group(1)

            # 找掛牌日（格式 YYYY-MM-DD）
            listing_date = None
            for t in texts:
                m = re.search(r'(\d{4}-\d{2}-\d{2})', t)
                if m:
                    listing_date = m.group(1)
                    break

            data.append({
                'cb_code':       cb_code,
                'stock_code':    cb_code[:4],
                'listing_date':  listing_date,
                'raw':           texts,
            })

        if data:
            print(f"  → 近期CB: {len(data)} 筆")
            return {d['cb_code']: d for d in data}
        else:
            raise ValueError("解析到 0 筆")

    except Exception as e:
        print(f"  ⚠ 無法抓取 /cb/recent: {e}")
        return {}


# ─────────────────────────────────────────────────────────────
# 3. 抓融券+借券賣出餘額
#    TWSE TWT93U：上市股票 1,262 支（date 參數）
#    TPEX SBL   ：上櫃股票  903 支（自動最新日）
#    兩者欄位相同（單位：股，/1000 = 張）：
#    [0]代號 [1]名稱
#    融資: [2-7]
#    融券+借券: [8]前日餘額 [9]當日賣出 [10]當日還券 [11]調整 [12]今日餘額 [13]限額
# ─────────────────────────────────────────────────────────────
def _parse_short_rows(rows, data_date):
    """共用解析邏輯：把 TWSE/TPEX 的 row 陣列轉成 short_map"""
    def ti(s):
        try:
            return round(int(str(s).replace(',', '').strip() or '0') / 1000)
        except:
            return 0
    short_map = {}
    for row in rows:
        code = row[0]
        if not code or code == '合計':
            continue
        prev    = ti(row[8])
        today_v = ti(row[12])
        short_map[code] = {
            'name':         row[1],
            'short_prev':   prev,
            'short_today':  today_v,
            'short_change': today_v - prev,
            'short_sell':   ti(row[9]),
            'short_cover':  ti(row[10]),
            'increasing':   (today_v - prev) > 0,
            'data_date':    data_date,
        }
    return short_map


def fetch_short_data():
    print("[3/3] 抓取融券+借券資料 (TWSE + TPEX)...")
    short_map = {}
    data_date = 'N/A'

    # ── TWSE TWT93U（上市，需帶 date 參數）──────────────────
    for delta in range(0, 5):
        try_date = (TODAY - timedelta(days=delta)).strftime('%Y%m%d')
        try:
            url = 'https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U'
            r = requests.get(url, params={'date': try_date, 'response': 'json'},
                             headers=HEADERS, timeout=20)
            d = r.json()
            if d.get('stat') == 'OK' and d.get('data'):
                twse_map = _parse_short_rows(d['data'], try_date)
                short_map.update(twse_map)
                data_date = try_date
                print(f"  → TWSE: {len(twse_map)} 支（{try_date}）")
                break
        except Exception as e:
            print(f"  ⚠ TWSE {try_date}: {e}")

    # ── TPEX SBL（上櫃，自動返回最新日）────────────────────
    try:
        url = 'https://www.tpex.org.tw/www/zh-tw/margin/sbl'
        r = requests.get(url, headers=HEADERS, timeout=20)
        d = r.json()
        if d.get('stat') == 'ok' and d.get('tables'):
            rows = d['tables'][0]['data']
            tpex_date = d.get('date', data_date)
            tpex_map = _parse_short_rows(rows, tpex_date)
            # TPEX 補上市場沒有的上櫃股（不覆蓋 TWSE 已有資料）
            added = 0
            for code, v in tpex_map.items():
                if code not in short_map:
                    short_map[code] = v
                    added += 1
            print(f"  → TPEX: {len(tpex_map)} 支，新增 {added} 支上櫃（{tpex_date}）")
            if data_date == 'N/A':
                data_date = tpex_date
    except Exception as e:
        print(f"  ⚠ TPEX: {e}")

    if short_map:
        print(f"  → 合計: {len(short_map)} 支股票")
        return short_map, data_date

    print("  ⚠ 無融券資料")
    return {}, 'N/A'


# ─────────────────────────────────────────────────────────────
# 4. 計算交易日數（只數週一～五，不含週末）
# ─────────────────────────────────────────────────────────────
def trading_days_between(start_str, end_date=None):
    if not start_str:
        return None
    if end_date is None:
        end_date = TODAY
    try:
        start = datetime.strptime(start_str, '%Y-%m-%d').date()
        if start > end_date:
            return -1
        count = 0
        cur = start
        while cur <= end_date:
            if cur.weekday() < 5:  # 週一=0 週五=4
                count += 1
            cur += timedelta(days=1)
        return count - 1  # 掛牌當天算 Day 0
    except:
        return None

def calendar_days_to(maturity_str, end_date=None):
    if end_date is None:
        end_date = TODAY
    try:
        mat = datetime.strptime(maturity_str, '%Y-%m-%d').date()
        return (mat - end_date).days
    except:
        return 0


# ─────────────────────────────────────────────────────────────
# 5. 策略訊號邏輯
# ─────────────────────────────────────────────────────────────
def evaluate_s1(cb, short_map, recent_map):
    """策略一：CBAS 新上市（3 條件）"""
    rec = recent_map.get(cb['cb_code'], {})
    listing_date = rec.get('listing_date') or cb.get('listing_date')
    if not listing_date:
        return None  # 沒有掛牌日，無法判斷

    td = trading_days_between(listing_date)
    if td is None or td < 0:
        return {'signal': '即將上市', 'cls': 'info', 'td': td,
                'c1': False, 'c2': False, 'c3': None}

    cbp  = cb.get('cb_price') or 0
    sc   = cb['stock_code']
    sh   = short_map.get(sc)

    c1 = 4 <= td <= 8         # 條件一：掛牌日 D4-D8
    c2 = cbp >= 98             # 條件二：CB ≥ 98
    if sh is None:
        c3 = None              # 無融券資料（真的不可放空）
    else:
        c3 = sh['increasing']  # 條件三：融券+借券增加

    n_ok = sum(x for x in [c1, c2] if x) + (1 if c3 else 0)

    if not c1:
        if td <= 3:
            sig, cls = f'觀察 D{td}', 'watch'
        elif 8 < td <= 20:
            sig, cls = f'出場 D{td}', 'sell'
        else:
            sig, cls = '─', 'neutral'
    elif c1 and c2 and c3:
        sig, cls = f'★ 買入 D{td} (3/3)', 'buy'
    elif c1 and c2 and c3 is None:
        sig, cls = f'◑ 不可放空 D{td}', 'watch'
    elif c1 and c2:
        sig, cls = f'✗ 融券未增 D{td}', 'sell'
    else:
        sig, cls = f'─ D{td}', 'neutral'

    return {'signal': sig, 'cls': cls, 'td': td,
            'c1': c1, 'c2': c2, 'c3': c3,
            'short_today':  sh['short_today']  if sh else None,
            'short_change': sh['short_change'] if sh else None,
            'listing_date': listing_date}


def evaluate_s2(cb, short_map):
    """策略二：轉換套利（4 條件）"""
    prem  = cb.get('premium_rate') or 0
    conv  = cb.get('converted_pct') or 0
    dtm   = calendar_days_to(cb.get('maturity_date', ''))
    sc    = cb['stock_code']
    sh    = short_map.get(sc)

    d1 = prem <= 2            # 溢價 ≤ 2%
    d2 = conv < 60            # 已轉換 < 60%
    d3 = dtm >= 90            # 距到期 ≥ 90 天
    d4 = sh['increasing'] if sh else None  # 融券+借券增加

    if sh is None:
        short_today = None; short_change = None
    else:
        short_today = sh['short_today']; short_change = sh['short_change']

    if d1 and d2 and d3 and d4:
        sig, cls = '★ 套利 (4/4)', 'buy'
    elif d1 and d2 and d3 and d4 is None:
        sig, cls = '◑ 不可放空 (3+?/4)', 'watch'
    elif d1 and d2 and d3:
        sig, cls = '✗ 融券未增 (3/4)', 'sell'
    elif prem <= 5 and d2 and d3:
        sig, cls = '接近套利區', 'watch'
    else:
        sig, cls = '─', 'neutral'

    return {'signal': sig, 'cls': cls,
            'c1': d1, 'c2': d2, 'c3': d3, 'c4': d4,
            'days_to_mat': dtm,
            'short_today': short_today, 'short_change': short_change}


# ─────────────────────────────────────────────────────────────
# 6. 生成 HTML
# ─────────────────────────────────────────────────────────────
def chk(ok, na=False):
    if na:  return '<span class="chk chk-na">?</span>'
    return '<span class="chk chk-y">✓</span>' if ok else '<span class="chk chk-n">✗</span>'

def fmt(v, d=1):
    if v is None: return '─'
    try: return f'{float(v):.{d}f}'
    except: return '─'

def sc_fmt(v):
    if v is None: return '─'
    return f'+{v}' if v > 0 else str(v)

def sc_cls(v):
    if v is None or v == 0: return ''
    return 'short-up' if v > 0 else 'short-dn'

def generate_html(all_cbs, recent_map, short_map, short_date):
    # 計算所有訊號
    results = []
    for cb in all_cbs:
        s1 = evaluate_s1(cb, short_map, recent_map)
        s2 = evaluate_s2(cb, short_map)
        results.append({**cb, 's1': s1, 's2': s2})

    # 分類
    s1_items = [r for r in results if r['s1'] and r['s1']['td'] is not None and r['s1']['td'] >= 0 and r['s1']['td'] <= 20]
    s1_items.sort(key=lambda x: x['s1']['td'])
    s2_items = sorted(results, key=lambda x: (
        0 if '★' in x['s2']['signal'] else 1 if '◑' in x['s2']['signal'] else 2 if '✗' not in x['s2']['signal'] and x['s2']['signal'] != '─' else 3,
        x.get('premium_rate') or 99
    ))

    s1_buy   = sum(1 for r in results if r['s1'] and '★' in r['s1']['signal'])
    s1_pend  = sum(1 for r in results if r['s1'] and '◑' in r['s1']['signal'])
    s2_buy   = sum(1 for r in results if '★' in r['s2']['signal'])
    s2_pend  = sum(1 for r in results if '◑' in r['s2']['signal'])

    # ── S1 rows ──
    s1_rows_html = ''
    for r in s1_items:
        s1 = r['s1']
        cbas = '✓ 可拆' if s1['td'] >= 6 else f'D6可拆'
        s1_rows_html += f"""<tr class="{'row-buy' if '★' in s1['signal'] else 'row-watch' if '◑' in s1['signal'] else 'row-sell' if '✗' in s1['signal'] else ''}">
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="center">D{s1['td']}</td>
  <td class="center">{cbas}</td>
  <td class="center cond">{chk(s1['c1'])} D4-D8<br>{chk(s1['c2'])} CB≥98<br>{chk(s1['c3'], s1['c3'] is None)} 融+借↑</td>
  <td class="num">{fmt(s1.get('short_today'),0)}張</td>
  <td class="num {sc_cls(s1.get('short_change'))}">{sc_fmt(s1.get('short_change'))}</td>
  <td class="center"><span class="badge {s1['cls']}">{s1['signal']}</span></td>
</tr>"""

    # ── S2 rows ──
    s2_rows_html = ''
    for r in s2_items:
        s2 = r['s2']
        pc = 'prem-neg' if (r.get('premium_rate') or 0) < 0 else ''
        s2_rows_html += f"""<tr class="{'row-buy' if '★' in s2['signal'] else 'row-watch' if '◑' in s2['signal'] or '接近' in s2['signal'] else ''}">
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="num {pc}">{fmt(r.get('premium_rate'))}%</td>
  <td class="num">{fmt(r.get('stock_price'))}</td>
  <td class="num">{fmt(r.get('conversion_price'))}</td>
  <td class="center cond">{chk(s2['c1'])} 溢價≤2%<br>{chk(s2['c2'])} 已轉&lt;60%<br>{chk(s2['c3'])} 距到期≥90天<br>{chk(s2['c4'], s2['c4'] is None)} 融+借↑</td>
  <td class="num">{s2['days_to_mat']}天</td>
  <td class="num">{fmt(s2.get('short_today'),0)}張</td>
  <td class="num {sc_cls(s2.get('short_change'))}">{sc_fmt(s2.get('short_change'))}</td>
  <td class="center"><span class="badge {s2['cls']}">{s2['signal']}</span></td>
</tr>"""

    # ── All rows ──
    all_rows_html = ''
    for r in results:
        s1 = r['s1']
        s2 = r['s2']
        s1sig = s1['signal'] if s1 else '─'
        s1cls = s1['cls'] if s1 else 'neutral'
        pc = 'prem-neg' if (r.get('premium_rate') or 0) < 0 else ''
        sh = short_map.get(r['stock_code'])
        all_rows_html += f"""<tr>
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="num {pc}">{fmt(r.get('premium_rate'))}%</td>
  <td class="num">{fmt(r.get('stock_price'))}</td>
  <td class="num">{fmt(r.get('conversion_price'))}</td>
  <td>{r.get('maturity_date','─')}</td>
  <td class="num">{fmt(sh['short_today'],0) if sh else '─'}張</td>
  <td class="num {sc_cls(sh['short_change'] if sh else None)}">{sc_fmt(sh['short_change'] if sh else None)}</td>
  <td class="center"><span class="badge {s1cls}">{s1sig}</span></td>
  <td class="center"><span class="badge {s2['cls']}">{s2['signal']}</span></td>
</tr>"""

    CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:13px}
.hdr{background:linear-gradient(135deg,#1e40af,#3b82f6);color:#fff;padding:18px 24px 14px}
.hdr h1{font-size:18px;font-weight:700;margin-bottom:3px}
.hdr .sub{font-size:11px;opacity:.8}
.stats{display:flex;gap:10px;padding:14px 24px;background:#fff;border-bottom:1px solid var(--brd);flex-wrap:wrap}
.sc{background:var(--bg);border:1px solid var(--brd);border-radius:8px;padding:10px 16px;min-width:110px}
.sc .n{font-size:24px;font-weight:700;color:var(--bl)}.sc .l{font-size:11px;color:var(--mu);margin-top:2px}
.sc.gr .n{color:var(--gr)}.sc.am .n{color:var(--am)}
.tabs{display:flex;padding:0 24px;border-bottom:2px solid var(--brd);background:#fff}
.tab{padding:11px 20px;cursor:pointer;border-bottom:3px solid transparent;font-size:12px;font-weight:600;color:var(--mu);margin-bottom:-2px}
.tab.active{border-bottom-color:var(--bl);color:var(--bl)}
.pane{display:none;padding:18px 24px}.pane.active{display:block}
.ttl{font-size:14px;font-weight:700;margin-bottom:4px}
.desc{font-size:12px;color:var(--mu);margin-bottom:12px;line-height:1.6}
.tag{display:inline-block;background:#dbeafe;color:#1d4ed8;border-radius:4px;padding:1px 6px;font-size:11px;font-weight:600;margin-right:3px}
.box{background:#eff6ff;border:1px solid #bfdbfe;border-radius:7px;padding:9px 13px;font-size:11.5px;color:#1e40af;margin-bottom:12px;line-height:1.7}
.box.warn{background:#fffbeb;border-color:#fde68a;color:#92400e}
table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.06)}
th{background:#f1f5f9;font-weight:700;color:var(--mu);font-size:10.5px;text-transform:uppercase;padding:9px 10px;text-align:left;border-bottom:2px solid var(--brd);white-space:nowrap}
td{padding:7px 10px;border-bottom:1px solid var(--brd);vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#f8fafc}
.num{text-align:right;font-variant-numeric:tabular-nums}
.center{text-align:center}
tr.row-buy td{background:#f0fdf4}
tr.row-buy:hover td{background:#dcfce7}
tr.row-watch td{background:#fefce8}
tr.row-sell td{background:#fff7ed}
.badge{display:inline-block;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:700;white-space:nowrap}
.badge.buy{background:#dcfce7;color:#15803d}
.badge.watch{background:#fef9c3;color:#854d0e}
.badge.sell{background:#ffedd5;color:#c2410c}
.badge.info{background:#dbeafe;color:#1d4ed8}
.badge.neutral{background:#f1f5f9;color:#94a3b8}
.cond{font-size:11px;line-height:2;white-space:nowrap}
.chk{display:inline-block;width:15px;height:15px;border-radius:50%;font-size:9px;text-align:center;line-height:15px;font-weight:700;margin-right:2px}
.chk-y{background:#dcfce7;color:#15803d}
.chk-n{background:#fee2e2;color:#dc2626}
.chk-na{background:#fef9c3;color:#854d0e}
.short-up{color:#16a34a;font-weight:700}
.short-dn{color:#dc2626;font-weight:700}
.prem-neg{color:#16a34a;font-weight:700}
.ft{text-align:center;color:var(--mu);font-size:11px;padding:16px;border-top:1px solid var(--brd)}
@media(max-width:768px){.hdr,.stats,.pane{padding-left:14px;padding-right:14px}.tabs{padding:0 14px;overflow-x:auto}th,td{padding:7px 7px}}
"""

    html = f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>可轉債策略儀表板 v4</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>📊 可轉債策略儀表板 v4</h1>
  <div class="sub">CB資料：thefew.tw（{len(all_cbs)}筆）｜融券+借券：TWSE TWT93U（{short_date}）｜更新：{TODAY}</div>
</div>
<div class="stats">
  <div class="sc"><div class="n">{len(all_cbs)}</div><div class="l">全部CB數</div></div>
  <div class="sc gr"><div class="n">{s1_buy}</div><div class="l">S1全條件買入</div></div>
  <div class="sc am"><div class="n">{s1_pend}</div><div class="l">S1不可放空</div></div>
  <div class="sc gr"><div class="n">{s2_buy}</div><div class="l">S2套利(4/4)</div></div>
  <div class="sc am"><div class="n">{s2_pend}</div><div class="l">S2不可放空</div></div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('s1',this)">策略一：CBAS新上市</div>
  <div class="tab" onclick="showTab('s2',this)">策略二：轉換套利（{len(all_cbs)}筆）</div>
  <div class="tab" onclick="showTab('all',this)">全部可轉債</div>
</div>
<div id="pane-s1" class="pane active">
  <div class="ttl">策略一：CBAS 新上市短壓</div>
  <div class="desc">法人買CB → 放空股票 (D1–5) → D6 CBAS拆解 → 融券+借券回補 → 股價反彈<br>
    <span class="tag">條件1</span>掛牌後第4–8交易日
    <span class="tag">條件2</span>CB現價≥98元
    <span class="tag">條件3</span>融券+借券餘額增加</div>
  <div class="box"><b>融券+借券 說明：</b>
    <span class="chk chk-y">✓</span>達標 &nbsp;
    <span class="chk chk-n">✗</span>未達標 &nbsp;
    <span class="chk chk-na">?</span>該股目前不可放空（TWSE TWT93U 無此股記錄）<br>
    資料來源：TWSE「融券借券賣出餘額」每日盤後自動更新，同時包含融券和借券。</div>
  <table><thead><tr>
    <th>CB代號</th><th>CB名稱</th><th>股票</th><th class="num">CB價</th>
    <th class="center">天數</th><th class="center">CBAS</th>
    <th class="center">條件1/2/3</th>
    <th class="num">融+借餘額</th><th class="num">日變化</th><th class="center">訊號</th>
  </tr></thead><tbody>{s1_rows_html}</tbody></table>
</div>
<div id="pane-s2" class="pane">
  <div class="ttl">策略二：轉換套利（全部 {len(all_cbs)} 支 CB）</div>
  <div class="desc">買CB + 放空股票 → 等待轉換 → 轉成股票回補 → 套利<br>
    <span class="tag">條件1</span>轉換溢價率≤2% <span class="tag">條件2</span>已轉換&lt;60%
    <span class="tag">條件3</span>距到期≥90天 <span class="tag">條件4</span>融券+借券增加</div>
  <div class="box warn"><b>注意：</b>溢價率顯示<span style="color:#16a34a;font-weight:700">綠色</span>（負值）代表CB低於轉換價值，套利空間最大。
    需確認：融+借是否充足、有無提前轉換限制。</div>
  <table><thead><tr>
    <th>CB代號</th><th>CB名稱</th><th>股票</th><th class="num">CB價</th>
    <th class="num">溢價率</th><th class="num">股價</th><th class="num">轉換價</th>
    <th class="center">條件1/2/3/4</th>
    <th class="num">距到期</th><th class="num">融+借餘額</th><th class="num">日變化</th>
    <th class="center">訊號</th>
  </tr></thead><tbody>{s2_rows_html}</tbody></table>
</div>
<div id="pane-all" class="pane">
  <div class="ttl">全部 {len(all_cbs)} 支可轉債</div>
  <div class="desc">資料來源：thefew.tw｜融券+借券：TWSE TWT93U {short_date}</div>
  <table><thead><tr>
    <th>CB代號</th><th>CB名稱</th><th>股票</th><th class="num">CB價</th>
    <th class="num">溢價率</th><th class="num">股價</th><th class="num">轉換價</th>
    <th>到期日</th><th class="num">融+借餘額</th><th class="num">日變化</th>
    <th class="center">S1</th><th class="center">S2</th>
  </tr></thead><tbody>{all_rows_html}</tbody></table>
</div>
<div class="ft">本工具僅供學習研究，不構成投資建議。<br>
融券+借券資料來源：TWSE「融券借券賣出餘額(TWT93U)」，每日盤後約17:30更新。不在名單內代表該股目前不可放空。</div>
<script>
function showTab(id,el){{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('pane-'+id).classList.add('active');
}}
</script></body></html>"""
    return html


# ─────────────────────────────────────────────────────────────
# 7. 主程式
# ─────────────────────────────────────────────────────────────
def main():
    print(f"\n=== 可轉債策略掃描 {TODAY} ===")
    all_cbs    = fetch_all_cbs()
    recent_map = fetch_recent_cbs()
    short_map, short_date = fetch_short_data()

    # 補上掛牌日（從 recent_map 合併到 all_cbs）
    rec_ld = {cb_code: d.get('listing_date') for cb_code, d in recent_map.items()}
    for cb in all_cbs:
        if cb['cb_code'] in rec_ld:
            cb['listing_date'] = rec_ld[cb['cb_code']]

    html = generate_html(all_cbs, recent_map, short_map, short_date)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n✅ 儀表板已產生：{OUTPUT_HTML}")
    print(f"   全部CB: {len(all_cbs)} 筆")
    print(f"   融券+借券資料: {len(short_map)} 支股票（{short_date}）")

    # 統計
    s1_buy = s2_buy = 0
    for cb in all_cbs:
        s1 = evaluate_s1(cb, short_map, recent_map)
        s2 = evaluate_s2(cb, short_map)
        if s1 and '★' in s1['signal']: s1_buy += 1
        if '★' in s2['signal']: s2_buy += 1
    print(f"   S1買入: {s1_buy} 筆 | S2套利: {s2_buy} 筆")

if __name__ == '__main__':
    main()
