#!/usr/bin/env python3
"""
å¯è½åµç­ç¥åè¡¨æ¿
- CB è³æï¼thefew.tw/cbï¼å¨é¨ 400+ ç­ï¼+ /cb/recentï¼å«æçæ¥ï¼ç­ç¥ä¸ç¨ï¼
- èå¸+åå¸ï¼TWSE TWT93Uï¼æ¯æ¥ç¤å¾èªåæ´æ°ï¼ç¡éç»å¥ï¼
- ç­ç¥ä¸ï¼CBASæ°ä¸å¸ï¼éæçæ¥ï¼ä¾èª /cb/recentï¼
- ç­ç¥äºï¼è½æå¥å©ï¼å¨é¨ CBï¼
"""

import requests
import json
import os
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from playwright.sync_api import sync_playwright

# ââ è·¯å¾è¨­å®ï¼GitHub Actions ç¨ï¼âââââââââââââââââââââââââ
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

# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# # å±ç¨ï¼è§£ææ¸å­ï¼å«è² æ¸ï¼
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def parse_num(txt):
    m = re.match(r'^(-?[\d.]+)', txt.strip())
    return float(m.group(1)) if m else None


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# 1. æå¨é¨ CBï¼Playwright è¼å¥ thefew.tw/cbï¼åå¾ 400+ ç­å®æ´è³æï¼
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def fetch_all_cbs():
    print("[1/3] æåå¨é¨CB (thefew.tw/cb) â Playwright...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto('https://thefew.tw/cb', wait_until='networkidle', timeout=90000)
            # Wait until JS renders 100+ data rows (8 cells each)
            page.wait_for_function(
                """() => {
                    const rows = document.querySelectorAll('#cb-table tbody tr');
                    let n = 0;
                    for (const r of rows) { if (r.querySelectorAll('td').length === 8) n++; }
                    return n > 100;
                }""",
                timeout=60000
            )
            page.wait_for_timeout(2000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, 'html.parser')
        rows = soup.select('#cb-table tbody tr')
        data = []
        for tr in rows:
            cells = tr.select('td')
            if len(cells) != 8:
                continue
            code_div = cells[0].select_one('div[class*="w-1/3"]')
            name_div = cells[0].select_one('div[class*="w-2/3"]')
            cb_code  = code_div.get_text(strip=True) if code_div else ''
            cb_name  = name_div.get_text(strip=True) if name_div else ''
            if not cb_code or len(cb_code) < 4:
                continue
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
                'listing_date':     None,
            })
        print(f"  â å¨é¨CB: {len(data)} ç­")
        if len(data) < 50:
            raise ValueError(f"è³æä¸è¶³ï¼å {len(data)} ç­ï¼é æ 400+ï¼")
        return data
    except Exception as e:
        print(f"  â  ç¡æ³æå thefew.tw/cb: {e}")
        raise


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# 2. æè¿æCBï¼thefew.tw/cb/recent â å«æçæ¥ï¼ç­ç¥ä¸å¿éï¼
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def fetch_recent_cbs():
    print("[2/3] æåè¿æCB (thefew.tw/cb/recent) â Playwright...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto('https://thefew.tw/cb/recent', wait_until='networkidle', timeout=90000)
            page.wait_for_function(
                "() => document.querySelectorAll('table tbody tr').length > 5",
                timeout=60000
            )
            page.wait_for_timeout(2000)
            html = page.content()
            browser.close()
        soup = BeautifulSoup(html, 'html.parser')

        # æ¾å«æçæ¥ç JSON è³æï¼åµå¨é é¢ script æ table è£¡ï¼
        data = []
        rows = soup.select('table tbody tr')
        for tr in rows:
            cells = tr.select('td')
            if len(cells) < 7:
                continue
            # çµæ§å¯è½ä¸åï¼ä¾å¯¦éé é¢èª¿æ´
            texts = [c.get_text(strip=True) for c in cells]
            # åè©¦æ cb_codeï¼éå¸¸å¨ç¬¬ä¸æ¬ï¼
            code_match = re.match(r'(\d{4,6})', texts[0])
            if not code_match:
                continue
            cb_code = code_match.group(1)

            # æ¾æçæ¥ï¼æ ¼å¼ YYYY-MM-DDï¼
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
            print(f"  â è¿æCB: {len(data)} ç­")
            return {d['cb_code']: d for d in data}
        else:
            raise ValueError("è§£æå° 0 ç­")

    except Exception as e:
        print(f"  â  ç¡æ³æå /cb/recent: {e}")
        return {}


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# 3. æèå¸+åå¸è³£åºé¤é¡
#    TWSE TWT93Uï¼ä¸å¸è¡ç¥¨ 1,262 æ¯ï¼date åæ¸ï¼
#    TPEX SBL   ï¼ä¸æ«è¡ç¥¨  903 æ¯ï¼èªåææ°æ¥ï¼
#    å©èæ¬ä½ç¸åï¼å®ä½ï¼è¡ï¼/1000 = å¼µï¼ï¼
#    [0]ä»£è [1]åç¨±
#    èè³: [2-7]
#    èå¸+åå¸: [8]åæ¥é¤é¡ [9]ç¶æ¥è³£åº [10]ç¶æ¥éå¸ [11]èª¿æ´ [12]ä»æ¥é¤é¡ [13]éé¡
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def _parse_short_rows(rows, data_date):
    """å±ç¨è§£æéè¼¯ï¼æ TWSE/TPEX ç row é£åè½æ short_map"""
    def ti(s):
        try:
            return round(int(str(s).replace(',', '').strip() or '0') / 1000)
        except:
            return 0
    short_map = {}
    for row in rows:
        code = row[0]
        if not code or code == 'åè¨':
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
    print("[3/3] æåèå¸+åå¸è³æ (TWSE + TPEX)...")
    short_map = {}
    data_date = 'N/A'

    # ââ TWSE TWT93Uï¼ä¸å¸ï¼éå¸¶ date åæ¸ï¼ââââââââââââââââââ
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
                print(f"  â TWSE: {len(twse_map)} æ¯ï¼{try_date}ï¼")
                break
        except Exception as e:
            print(f"  â  TWSE {try_date}: {e}")

    # ââ TPEX SBLï¼ä¸æ«ï¼èªåè¿åææ°æ¥ï¼ââââââââââââââââââââ
    try:
        url = 'https://www.tpex.org.tw/www/zh-tw/margin/sbl'
        r = requests.get(url, headers=HEADERS, timeout=20)
        d = r.json()
        if d.get('stat') == 'ok' and d.get('tables'):
            rows = d['tables'][0]['data']
            tpex_date = d.get('date', data_date)
            tpex_map = _parse_short_rows(rows, tpex_date)
            # TPEX è£ä¸å¸å ´æ²æçä¸æ«è¡ï¼ä¸è¦è TWSE å·²æè³æï¼
            added = 0
            for code, v in tpex_map.items():
                if code not in short_map:
                    short_map[code] = v
                    added += 1
            print(f"  â TPEX: {len(tpex_map)} æ¯ï¼æ°å¢ {added} æ¯ä¸æ«ï¼{tpex_date}ï¼")
            if data_date == 'N/A':
                data_date = tpex_date
    except Exception as e:
        print(f"  â  TPEX: {e}")

    if short_map:
        print(f"  â åè¨: {len(short_map)} æ¯è¡ç¥¨")
        return short_map, data_date

    print("  â  ç¡èå¸è³æ")
    return {}, 'N/A'


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# 4. è¨ç®äº¤ææ¥æ¸ï¼åªæ¸é±ä¸ï½äºï¼ä¸å«é±æ«ï¼
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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
            if cur.weekday() < 5:  # é±ä¸=0 é±äº=4
                count += 1
            cur += timedelta(days=1)
        return count - 1  # æçç¶å¤©ç® Day 0
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


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# 5. ç­ç¥è¨èéè¼¯
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def evaluate_s1(cb, short_map, recent_map):
    """ç­ç¥ä¸ï¼CBAS æ°ä¸å¸ï¼3 æ¢ä»¶ï¼"""
    rec = recent_map.get(cb['cb_code'], {})
    listing_date = rec.get('listing_date') or cb.get('listing_date')
    if not listing_date:
        return None  # æ²ææçæ¥ï¼ç¡æ³å¤æ·

    td = trading_days_between(listing_date)
    if td is None or td < 0:
        return {'signal': 'å³å°ä¸å¸', 'cls': 'info', 'td': td,
                'c1': False, 'c2': False, 'c3': None}

    cbp  = cb.get('cb_price') or 0
    sc   = cb['stock_code']
    sh   = short_map.get(sc)

    c1 = 4 <= td <= 8         # æ¢ä»¶ä¸ï¼æçæ¥ D4-D8
    c2 = cbp >= 98             # æ¢ä»¶äºï¼CB â¥ 98
    if sh is None:
        c3 = None              # ç¡èå¸è³æï¼ççä¸å¯æ¾ç©ºï¼
    else:
        c3 = sh['increasing']  # æ¢ä»¶ä¸ï¼èå¸+åå¸å¢å 

    n_ok = sum(x for x in [c1, c2] if x) + (1 if c3 else 0)

    if not c1:
        if td <= 3:
            sig, cls = f'è§å¯ D{td}', 'watch'
        elif 8 < td <= 20:
            sig, cls = f'åºå ´ D{td}', 'sell'
        else:
            sig, cls = 'â', 'neutral'
    elif c1 and c2 and c3:
        sig, cls = f'â è²·å¥ D{td} (3/3)', 'buy'
    elif c1 and c2 and c3 is None:
        sig, cls = f'â ä¸å¯æ¾ç©º D{td}', 'watch'
    elif c1 and c2:
        sig, cls = f'â èå¸æªå¢ D{td}', 'sell'
    else:
        sig, cls = f'â D{td}', 'neutral'

    return {'signal': sig, 'cls': cls, 'td': td,
            'c1': c1, 'c2': c2, 'c3': c3,
            'short_today':  sh['short_today']  if sh else None,
            'short_change': sh['short_change'] if sh else None,
            'listing_date': listing_date}


def evaluate_s2(cb, short_map):
    """ç­ç¥äºï¼è½æå¥å©ï¼4 æ¢ä»¶ï¼"""
    prem  = cb.get('premium_rate') or 0
    conv  = cb.get('converted_pct') or 0
    dtm   = calendar_days_to(cb.get('maturity_date', ''))
    sc    = cb['stock_code']
    sh    = short_map.get(sc)

    d1 = prem <= 2            # æº¢å¹ â¤ 2%
    d2 = conv < 60            # å·²è½æ < 60%
    d3 = dtm >= 90            # è·å°æ â¥ 90 å¤©
    d4 = sh['increasing'] if sh else None  # èå¸+åå¸å¢å 

    if sh is None:
        short_today = None; short_change = None
    else:
        short_today = sh['short_today']; short_change = sh['short_change']

    if d1 and d2 and d3 and d4:
        sig, cls = 'â å¥å© (4/4)', 'buy'
    elif d1 and d2 and d3 and d4 is None:
        sig, cls = 'â ä¸å¯æ¾ç©º (3+?/4)', 'watch'
    elif d1 and d2 and d3:
        sig, cls = 'â èå¸æªå¢ (3/4)', 'sell'
    elif prem <= 5 and d2 and d3:
        sig, cls = 'æ¥è¿å¥å©å', 'watch'
    else:
        sig, cls = 'â', 'neutral'

    return {'signal': sig, 'cls': cls,
            'c1': d1, 'c2': d2, 'c3': d3, 'c4': d4,
            'days_to_mat': dtm,
            'short_today': short_today, 'short_change': short_change}


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# 6. çæ HTML
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def chk(ok, na=False):
    if na:  return '<span class="chk chk-na">?</span>'
    return '<span class="chk chk-y">â</span>' if ok else '<span class="chk chk-n">â</span>'

def fmt(v, d=1):
    if v is None: return 'â'
    try: return f'{float(v):.{d}f}'
    except: return 'â'

def sc_fmt(v):
    if v is None: return 'â'
    return f'+{v}' if v > 0 else str(v)

def sc_cls(v):
    if v is None or v == 0: return ''
    return 'short-up' if v > 0 else 'short-dn'

def generate_html(all_cbs, recent_map, short_map, short_date):
    # è¨ç®ææè¨è
    results = []
    for cb in all_cbs:
        s1 = evaluate_s1(cb, short_map, recent_map)
        s2 = evaluate_s2(cb, short_map)
        results.append({**cb, 's1': s1, 's2': s2})

    # åé¡
    s1_items = [r for r in results if r['s1'] and r['s1']['td'] is not None and r['s1']['td'] >= 0 and r['s1']['td'] <= 20]
    s1_items.sort(key=lambda x: x['s1']['td'])
    s2_items = sorted(results, key=lambda x: (
        0 if 'â' in x['s2']['signal'] else 1 if 'â' in x['s2']['signal'] else 2 if 'â' not in x['s2']['signal'] and x['s2']['signal'] != 'â' else 3,
        x.get('premium_rate') or 99
    ))

    s1_buy   = sum(1 for r in results if r['s1'] and 'â' in r['s1']['signal'])
    s1_pend  = sum(1 for r in results if r['s1'] and 'â' in r['s1']['signal'])
    s2_buy   = sum(1 for r in results if 'â' in r['s2']['signal'])
    s2_pend  = sum(1 for r in results if 'â' in r['s2']['signal'])

    # ââ S1 rows ââ
    s1_rows_html = ''
    for r in s1_items:
        s1 = r['s1']
        cbas = 'â å¯æ' if s1['td'] >= 6 else f'D6å¯æ'
        s1_rows_html += f"""<tr class="{'row-buy' if 'â' in s1['signal'] else 'row-watch' if 'â' in s1['signal'] else 'row-sell' if 'â' in s1['signal'] else ''}">
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="center">D{s1['td']}</td>
  <td class="center">{cbas}</td>
  <td class="center cond">{chk(s1['c1'])} æçåæ<br>{chk(s1['c2'])} CBå¹éæ¨<br>{chk(s1['c3'], s1['c3'] is None)} è+åâ</td>
  <td class="num">{fmt(s1.get('short_today'),0)}å¼µ</td>
  <td class="num {sc_cls(s1.get('short_change'))}">{sc_fmt(s1.get('short_change'))}</td>
  <td class="center"><span class="badge {s1['cls']}">{s1['signal']}</span></td>
</tr>"""

    # ââ S2 rows ââ
    s2_rows_html = ''
    for r in s2_items:
        s2 = r['s2']
        pc = 'prem-neg' if (r.get('premium_rate') or 0) < 0 else ''
        s2_rows_html += f"""<tr class="{'row-buy' if 'â' in s2['signal'] else 'row-watch' if 'â' in s2['signal'] or 'æ¥è¿' in s2['signal'] else ''}">
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="num {pc}">{fmt(r.get('premium_rate'))}%</td>
  <td class="num">{fmt(r.get('stock_price'))}</td>
  <td class="num">{fmt(r.get('conversion_price'))}</td>
  <td class="center cond">{chk(s2['c1'])} ä½æº¢å¹<br>{chk(s2['c2'])} è½ææ¯ä¾ä½<br>{chk(s2['c3'])} è·å°æåè£<br>{chk(s2['c4'], s2['c4'] is None)} è+åâ</td>
  <td class="num">{s2['days_to_mat']}å¤©</td>
  <td class="num">{fmt(s2.get('short_today'),0)}å¼µ</td>
  <td class="num {sc_cls(s2.get('short_change'))}">{sc_fmt(s2.get('short_change'))}</td>
  <td class="center"><span class="badge {s2['cls']}">{s2['signal']}</span></td>
</tr>"""

    # ââ All rows ââ
    all_rows_html = ''
    for r in results:
        s1 = r['s1']
        s2 = r['s2']
        s1sig = s1['signal'] if s1 else 'â'
        s1cls = s1['cls'] if s1 else 'neutral'
        pc = 'prem-neg' if (r.get('premium_rate') or 0) < 0 else ''
        sh = short_map.get(r['stock_code'])
        all_rows_html += f"""<tr>
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="num {pc}">{fmt(r.get('premium_rate'))}%</td>
  <td class="num">{fmt(r.get('stock_price'))}</td>
  <td class="num">{fmt(r.get('conversion_price'))}</td>
  <td>{r.get('maturity_date','â')}</td>
  <td class="num">{fmt(sh['short_today'],0) if sh else 'â'}å¼µ</td>
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
<title>å¯è½åµç­ç¥åè¡¨æ¿</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>ð å¯è½åµç­ç¥åè¡¨æ¿</h1>
  <div class="sub">æ´æ°ï¼{TODAY}</div>
</div>
<div class="stats">
  <div class="sc"><div class="n">{len(all_cbs)}</div><div class="l">å¨é¨CBæ¸</div></div>
  <div class="sc gr"><div class="n">{s1_buy}</div><div class="l">S1å¨æ¢ä»¶è²·å¥</div></div>
  <div class="sc am"><div class="n">{s1_pend}</div><div class="l">S1ä¸å¯æ¾ç©º</div></div>
  <div class="sc gr"><div class="n">{s2_buy}</div><div class="l">S2å¥å©(4/4)</div></div>
  <div class="sc am"><div class="n">{s2_pend}</div><div class="l">S2ä¸å¯æ¾ç©º</div></div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('s1',this)">ç­ç¥ä¸ï¼CBASæ°ä¸å¸</div>
  <div class="tab" onclick="showTab('s2',this)">ç­ç¥äºï¼è½æå¥å©ï¼{len(all_cbs)}ç­ï¼</div>
  <div class="tab" onclick="showTab('all',this)">å¨é¨å¯è½åµ</div>
</div>
<div id="pane-s1" class="pane active">
  <div class="ttl">ç­ç¥ä¸ï¼CBAS æ°ä¸å¸ç­å£</div>
  <div class="desc">æ³äººè²·CB â æ¾ç©ºè¡ç¥¨ (D1â5) â D6 CBASæè§£ â èå¸+åå¸åè£ â è¡å¹åå½<br>
    <span class="tag">æ¢ä»¶1</span>æçåæäº¤ææ¥
    <span class="tag">æ¢ä»¶2</span>CBç¾å¹éä¸å®æ°´æº
    <span class="tag">æ¢ä»¶3</span>èå¸+åå¸é¤é¡å¢å </div>
  <div class="box"><b>èå¸+åå¸ èªªæï¼</b>
    <span class="chk chk-y">â</span>éæ¨ &nbsp;
    <span class="chk chk-n">â</span>æªéæ¨ &nbsp;
    <span class="chk chk-na">?</span>è©²è¡ç®åä¸å¯æ¾ç©ºï¼TWSE TWT93U ç¡æ­¤è¡è¨éï¼<br>
    è³æä¾æºï¼TWSEãèå¸åå¸è³£åºé¤é¡ãæ¯æ¥ç¤å¾èªåæ´æ°ï¼åæåå«èå¸ååå¸ã</div>
  <table><thead><tr>
    <th>CBä»£è</th><th>CBåç¨±</th><th>è¡ç¥¨</th><th class="num">CBå¹</th>
    <th class="center">å¤©æ¸</th><th class="center">CBAS</th>
    <th class="center">æ¢ä»¶1/2/3</th>
    <th class="num">è+åé¤é¡</th><th class="num">æ¥è®å</th><th class="center">è¨è</th>
  </tr></thead><tbody>{s1_rows_html}</tbody></table>
</div>
<div id="pane-s2" class="pane">
  <div class="ttl">ç­ç¥äºï¼è½æå¥å©ï¼å¨é¨ {len(all_cbs)} æ¯ CBï¼</div>
  <div class="desc">è²·CB + æ¾ç©ºè¡ç¥¨ â ç­å¾è½æ â è½æè¡ç¥¨åè£ â å¥å©<br>
    <span class="tag">æ¢ä»¶1</span>è½ææº¢å¹çä½ <span class="tag">æ¢ä»¶2</span>å·²è½ææ¯ä¾ä½
    <span class="tag">æ¢ä»¶3</span>è·å°ææ¥åè£ <span class="tag">æ¢ä»¶4</span>èå¸+åå¸å¢å </div>
  <div class="box warn"><b>æ³¨æï¼</b>æº¢å¹çé¡¯ç¤º<span style="color:#16a34a;font-weight:700">ç¶ è²</span>ï¼è² å¼ï¼ä»£è¡¨CBä½æ¼è½æå¹å¼ï¼å¥å©ç©ºéæå¤§ã
    éç¢ºèªï¼è+åæ¯å¦åè¶³ãæç¡æåè½æéå¶ã</div>
  <table><thead><tr>
    <th>CBä»£è</th><th>CBåç¨±</th><th>è¡ç¥¨</th><th class="num">CBå¹</th>
    <th class="num">æº¢å¹ç</th><th class="num">è¡å¹</th><th class="num">è½æå¹</th>
    <th class="center">æ¢ä»¶1/2/3/4</th>
    <th class="num">è·å°æ</th><th class="num">è+åé¤é¡</th><th class="num">æ¥è®å</th>
    <th class="center">è¨è</th>
  </tr></thead><tbody>{s2_rows_html}</tbody></table>
</div>
<div id="pane-all" class="pane">
  <div class="ttl">å¨é¨ {len(all_cbs)} æ¯å¯è½åµ</div>
  <div class="desc">è³æä¾æºï¼thefew.twï½èå¸+åå¸ï¼TWSE TWT93U {short_date}</div>
  <table><thead><tr>
    <th>CBä»£è</th><th>CBåç¨±</th><th>è¡ç¥¨</th><th class="num">CBå¹</th>
    <th class="num">æº¢å¹ç</th><th class="num">è¡å¹</th><th class="num">è½æå¹</th>
    <th>å°ææ¥</th><th class="num">è+åé¤é¡</th><th class="num">æ¥è®å</th>
    <th class="center">S1</th><th class="center">S2</th>
  </tr></thead><tbody>{all_rows_html}</tbody></table>
</div>
<div class="ft">æ¬å·¥å·åä¾å­¸ç¿ç ç©¶ï¼ä¸æ§ææè³å»ºè­°ã<br>
èå¸+åå¸è³æä¾æºï¼TWSEãèå¸åå¸è³£åºé¤é¡(TWT93U)ãï¼æ¯æ¥ç¤å¾ç´17:30æ´æ°ãä¸å¨åå®å§ä»£è¡¨è©²è¡ç®åä¸å¯æ¾ç©ºã</div>
<script>
function showTab(id,el){{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('pane-'+id).classList.add('active');
}}
</script></body></html>"""
    return html


# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# 7. ä¸»ç¨å¼
# âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def main():
    print(f"\n=== å¯è½åµç­ç¥ææ {TODAY} ===")
    all_cbs    = fetch_all_cbs()
    recent_map = fetch_recent_cbs()
    short_map, short_date = fetch_short_data()

    # è£ä¸æçæ¥ï¼å¾ recent_map åä½µå° all_cbsï¼
    rec_ld = {cb_code: d.get('listing_date') for cb_code, d in recent_map.items()}
    for cb in all_cbs:
        if cb['cb_code'] in rec_ld:
            cb['listing_date'] = rec_ld[cb['cb_code']]

    html = generate_html(all_cbs, recent_map, short_map, short_date)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\nâ åè¡¨æ¿å·²ç¢çï¼{OUTPUT_HTML}")
    print(f"   å¨é¨CB: {len(all_cbs)} ç­")
    print(f"   èå¸+åå¸è³æ: {len(short_map)} æ¯è¡ç¥¨ï¼{short_date}ï¼")

    # çµ±è¨
    s1_buy = s2_buy = 0
    for cb in all_cbs:
        s1 = evaluate_s1(cb, short_map, recent_map)
        s2 = evaluate_s2(cb, short_map)
        if s1 and 'â' in s1['signal']: s1_buy += 1
        if 'â' in s2['signal']: s2_buy += 1
    print(f"   S1è²·å¥: {s1_buy} ç­ | S2å¥å©: {s2_buy} ç­")

if __name__ == '__main__':
    main()
