#!/usr/bin/env python3
"""
可轉債策略儀表板資料產生器 - TPEX data source
"""
import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta
from io import StringIO

import requests
from bs4 import BeautifulSoup

# Constants
TODAY = datetime.now().date()
OUTPUT_HTML = 'index.html'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# TPEX data source URLs
TPEX_CB_INFO_PATTERN = "https://www.tpex.org.tw/storage/bond_zone/tradeinfo/cb/{year}/{yearmonth}/RSdrs001.{yearmonthday}-C.csv"
TPEX_CB_TRADING_PATTERN = "https://www.tpex.org.tw/storage/bond_zone/tradeinfo/cb/{year}/{yearmonth}/RSta0113.{yearmonthday}-C.csv"
TPEX_ISSUANCE_API = "https://www.tpex.org.tw/openapi/v1/bond_ISSBD5_data"

# Field indices for CB info CSV (after BODY prefix)
CB_INFO = {
    "cb_code": 1, "cb_name": 2,
    "conversion_start": 3, "conversion_end": 4, "conversion_price": 5,
    "buyback_start": 7, "buyback_end": 8, "buyback_price": 9,
    "original_issue_amount": 14, "outstanding_amount": 15,
    "cb_reference_price": 16, "stock_price": 17,
    "trading_stop_end": 19, "coupon_rate": 20,
}

# Field indices for CB trading CSV
CB_TRADE = {
    "cb_code": 1, "cb_name": 2,
    "close_price": 3, "open_price": 5, "high_price": 6, "low_price": 7,
    "volume": 9, "amount": 10,
}

def parse_num(txt):
    """Parse number from string, handling commas and whitespace."""
    if not txt or not txt.strip():
        return None
    cleaned = txt.strip().replace(",", "").replace("，", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_date_tw(s):
    """Parse YYYY/MM/DD to YYYY-MM-DD."""
    if not s or not s.strip():
        return None
    try:
        return datetime.strptime(s.strip(), "%Y/%m/%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _fetch_tpex_csv(url):
    """Fetch Big5-encoded CSV from TPEX, return BODY rows."""
    resp = requests.get(url, timeout=15)
    resp.encoding = "big5"
    rows = []
    for row in csv.reader(StringIO(resp.text)):
        if row and row[0].strip() == "BODY":
            rows.append(row)
    return rows


def _get_latest_tpex_csv(pattern, max_days=7):
    """Try fetching TPEX CSV going back up to max_days."""
    base = datetime.now()
    for d in range(max_days):
        dt = base - timedelta(days=d)
        url = pattern.format(
            year=dt.strftime("%Y"),
            yearmonth=dt.strftime("%Y%m"),
            yearmonthday=dt.strftime("%Y%m%d"),
        )
        try:
            rows = _fetch_tpex_csv(url)
            if rows:
                print(f"✓ TPEX CSV fetched ({dt.strftime('%Y-%m-%d')})")
                return rows
        except Exception:
            continue
    return []


def _parse_any_date(s):
    """Parse date in many formats: 2026/01/15, 2026-01-15, 20260115, 115/01/15 (ROC), 1150115 (ROC)"""
    s = str(s).strip()
    if not s:
        return None
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 3:
            try:
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                if y < 1911:
                    y += 1911
                return datetime(y, m, d)
            except (ValueError, TypeError):
                pass
    if "-" in s and len(s) >= 8:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except ValueError:
            pass
    if s.isdigit():
        if len(s) == 8:
            try:
                return datetime.strptime(s, "%Y%m%d")
            except ValueError:
                pass
        if len(s) == 7:
            try:
                return datetime(int(s[:3]) + 1911, int(s[3:5]), int(s[5:7]))
            except ValueError:
                pass
    return None


def fetch_issuance_dates():
    """從 TPEX API 取得 CB 上市/到期日期 (defensive: tries multiple field names and date formats)"""
    listing_map, maturity_map = {}, {}
    try:
        r = requests.get(TPEX_ISSUANCE_API, timeout=20, headers=HEADERS)
        print(f"  issuance API status={r.status_code} len={len(r.text)}")
        try:
            data = r.json()
        except Exception as e:
            print(f"  issuance JSON decode failed: {e}; head={r.text[:200]}")
            return listing_map, maturity_map

        if isinstance(data, dict):
            print(f"  issuance dict keys={list(data.keys())[:10]}")
            for wk in ("data", "Data", "result", "Result", "aaData", "rows", "items"):
                if wk in data and isinstance(data[wk], list):
                    data = data[wk]
                    break

        if not isinstance(data, list):
            print(f"  issuance unexpected type: {type(data).__name__}")
            return listing_map, maturity_map

        if data:
            sample = data[0]
            if isinstance(sample, dict):
                print(f"  issuance items={len(data)} sample_keys={list(sample.keys())}")
                print(f"  issuance sample={ {k: sample.get(k) for k in list(sample.keys())[:10]} }")

        code_keys = ["BondCode", "bond_code", "BondID", "BondId", "code", "Code", "bondCode", "證券代號", "債券代碼", "Bond_Code", "BondNo"]
        list_keys = ["ListingDate", "listing_date", "IssueDate", "issue_date", "上市日期", "發行日期", "listingDate", "issueDate", "Listing_Date", "Issue_Date", "ListedDate", "IssDate"]
        mat_keys = ["MaturityDate", "maturity_date", "到期日", "到期日期", "MaturityDay", "maturityDate", "Maturity_Date", "ExpireDate", "ExpiryDate"]

        for item in data:
            if not isinstance(item, dict):
                continue
            code = ""
            for k in code_keys:
                v = item.get(k)
                if v:
                    code = str(v).strip()
                    break
            if not code:
                continue
            ld_raw = ""
            for k in list_keys:
                v = item.get(k)
                if v:
                    ld_raw = str(v).strip()
                    break
            md_raw = ""
            for k in mat_keys:
                v = item.get(k)
                if v:
                    md_raw = str(v).strip()
                    break
            if ld_raw:
                d = _parse_any_date(ld_raw)
                if d:
                    listing_map[code] = d.strftime("%Y-%m-%d")
            if md_raw:
                d = _parse_any_date(md_raw)
                if d:
                    maturity_map[code] = d.strftime("%Y-%m-%d")

        print(f"  issuance parsed: listing={len(listing_map)} maturity={len(maturity_map)}")
    except Exception as e:
        print(f"  issuance fetch error: {type(e).__name__}: {e}")
    return listing_map, maturity_map


def derive_stock_code(cb_code):
    """Derive stock code from CB code."""
    if not cb_code:
        return None
    code = cb_code.strip()
    if len(code) == 5:
        return code[:4]
    if len(code) == 6 and code[0] in ("1", "2"):
        return code[:4]
    return None


def fetch_all_cbs():
    """從TPEX取得所有可轉債資料"""
    listing_map, maturity_map = fetch_issuance_dates()
    info_rows = _get_latest_tpex_csv(TPEX_CB_INFO_PATTERN)
    if not info_rows:
        raise ValueError("Failed to fetch CB info from TPEX")
    trade_rows = _get_latest_tpex_csv(TPEX_CB_TRADING_PATTERN)
    trade_map = {}
    if trade_rows:
        for row in trade_rows:
            try:
                trade_map[row[CB_TRADE["cb_code"]].strip()] = row
            except (IndexError, AttributeError):
                pass
    cbs = []
    for row in info_rows:
        try:
            cb_code = row[CB_INFO["cb_code"]].strip()
            cb_name = row[CB_INFO["cb_name"]].strip()
            cb_price = parse_num(row[CB_INFO["cb_reference_price"]])
            stock_price = parse_num(row[CB_INFO["stock_price"]])
            conversion_price = parse_num(row[CB_INFO["conversion_price"]])
            conversion_end = parse_date_tw(row[CB_INFO["conversion_end"]])
            original_issue = parse_num(row[CB_INFO["original_issue_amount"]])
            outstanding = parse_num(row[CB_INFO["outstanding_amount"]])
            trading_stop_end = parse_date_tw(row[CB_INFO["trading_stop_end"]])
            maturity_date = maturity_map.get(cb_code) or trading_stop_end or conversion_end
            volume = None
            if cb_code in trade_map:
                volume = parse_num(trade_map[cb_code][CB_TRADE["volume"]])
            premium = None
            if cb_price and stock_price and conversion_price and conversion_price > 0:
                parity = (stock_price / conversion_price) * 100
                if parity > 0:
                    premium = (cb_price / parity - 1) * 100
            converted_pct = None
            if original_issue and outstanding and original_issue > 0:
                converted_pct = (1 - outstanding / original_issue) * 100
            listing_date = listing_map.get(cb_code)
            stock_code = derive_stock_code(cb_code)
            cbs.append({
                "cb_code": cb_code,
                "cb_name": cb_name,
                "cb_price": cb_price,
                "stock_price": stock_price,
                "premium": premium,
                "volume": volume,
                "conversion_price": conversion_price,
                "converted_pct": converted_pct,
                "maturity_date": maturity_date,
                "listing_date": listing_date,
                "stock_code": stock_code,
            })
        except (IndexError, ValueError, AttributeError):
            continue
    if len(cbs) < 100:
        raise ValueError(f"Only found {len(cbs)} CBs, expected >= 100")
    print(f"→ 全部CB: {len(cbs)} 筆")
    return cbs


def fetch_recent_cbs(days=90):
    """取得近N天上市的CB"""
    listing_map, _ = fetch_issuance_dates()
    cutoff = datetime.now() - timedelta(days=days)
    recent = {}
    for code, date_str in listing_map.items():
        try:
            if datetime.strptime(date_str, "%Y-%m-%d") >= cutoff:
                recent[code] = {"listing_date": date_str}
        except ValueError:
            continue
    print(f"→ 近{days}天CB: {len(recent)} 筆")
    return recent


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
    s2_items = sorted([r for r in results if ("★" in r["s2"]["signal"] or "◑" in r["s2"]["signal"])], key=lambda x: (
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
  <div class="tab" onclick="showTab('s2',this)">策略二：轉換套利（{len(s2_items)}筆）</div>
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
  <div class="ttl">策略二：轉換套利（符合條件 {len(s2_items)} 支 CB）</div>
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
