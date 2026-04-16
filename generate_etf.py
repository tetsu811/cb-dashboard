#!/usr/bin/env python3
"""
主動式 ETF 持股追蹤儀表板 — 資料產生器
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ── Section 1: Constants ─────────────────────────────────────────────────────

TODAY = datetime.now().date()
OUTPUT_HTML = 'etf_index.html'
DATA_DIR = 'etf_data'
LATEST_CACHE = 'etf_latest.json'
CONFIG_FILE = 'etf_config.json'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

MONEYDJ_FULL_URL = 'https://www.moneydj.com/etf/x/basic/Basic0007B.xdjhtm?etfid={etf_id}'
MONEYDJ_TOP10_URL = 'https://www.moneydj.com/etf/x/basic/basic0007.xdjhtm?etfid={etf_id}'

WEIGHT_CHANGE_THRESHOLD = 0.5
MAX_HISTORY_DAYS = 30
FETCH_DELAY = 1.5


# ── Section 2: Data fetching ─────────────────────────────────────────────────

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _parse_stock_cell(text):
    """Parse '台積電(2330.TW)' or 'Tesla(TSLA.US)' → (code, name)"""
    m = re.match(r'^(.+?)\((\w+)\.(TW|US)\)$', text.strip())
    if m:
        return m.group(2), m.group(1)
    return None, None


def _parse_num(txt):
    if not txt or not txt.strip():
        return None
    cleaned = txt.strip().replace(",", "").replace("，", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _scrape_holdings_from_html(html_text, etf_code, allow_na=False):
    """Parse holdings table from MoneyDJ HTML response."""
    soup = BeautifulSoup(html_text, 'lxml')
    tables = soup.find_all('table')
    holdings = []
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) < 3:
            continue
        header_cells = [c.get_text(strip=True) for c in rows[0].find_all(['th', 'td'])]
        header_text = ''.join(header_cells)
        if '個股名稱' not in header_text:
            continue
        for row in rows[1:]:
            cells = [c.get_text(strip=True) for c in row.find_all('td')]
            if len(cells) < 3:
                continue
            stock_code, stock_name = _parse_stock_cell(cells[0])
            if not stock_code:
                continue
            weight_pct = _parse_num(cells[1])
            shares = _parse_num(cells[2])
            if weight_pct is None and not allow_na:
                continue
            holdings.append({
                "stock_code": stock_code,
                "stock_name": stock_name,
                "weight_pct": weight_pct if weight_pct is not None else 0,
                "shares": int(shares) if shares else 0,
                "weight_na": weight_pct is None,
            })
        if holdings:
            break
    return holdings


def fetch_holdings_http(etf_code):
    """Fetch holdings from MoneyDJ via HTTP. Try full page first, fallback to top-10, then N/A-tolerant."""
    etf_id = f"{etf_code}.tw"
    try:
        url = MONEYDJ_FULL_URL.format(etf_id=etf_id)
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = 'utf-8'
        if resp.status_code == 200:
            holdings = _scrape_holdings_from_html(resp.text, etf_code)
            if len(holdings) >= 3:
                return holdings

        url = MONEYDJ_TOP10_URL.format(etf_id=etf_id)
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = 'utf-8'
        if resp.status_code == 200:
            holdings = _scrape_holdings_from_html(resp.text, etf_code)
            if holdings:
                print(f"  [HTTP] {etf_code}: got {len(holdings)} holdings from top-10 page")
                return holdings
            holdings = _scrape_holdings_from_html(resp.text, etf_code, allow_na=True)
            if holdings:
                print(f"  [HTTP] {etf_code}: got {len(holdings)} holdings (weights N/A)")
                return holdings

        print(f"  [HTTP] {etf_code}: no holdings found on either page")
        return None
    except Exception as e:
        print(f"  [HTTP] {etf_code}: {e}")
        return None


def fetch_holdings_playwright(etf_code):
    """Fallback: use Playwright headless browser to fetch holdings."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print(f"  [Playwright] not installed, skipping {etf_code}")
        return None
    etf_id = f"{etf_code}.tw"
    url = MONEYDJ_FULL_URL.format(etf_id=etf_id)
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=30000)
            page.wait_for_timeout(3000)
            tables = page.query_selector_all('table')
            holdings = []
            for table in tables:
                rows = table.query_selector_all('tr')
                if len(rows) < 3:
                    continue
                header = rows[0].inner_text()
                if '個股名稱' not in header:
                    continue
                for row in rows[1:]:
                    cells = row.query_selector_all('td')
                    if len(cells) < 3:
                        continue
                    cell_texts = [c.inner_text().strip() for c in cells]
                    stock_code, stock_name = _parse_stock_cell(cell_texts[0])
                    if not stock_code:
                        continue
                    weight_pct = _parse_num(cell_texts[1])
                    shares = _parse_num(cell_texts[2])
                    if weight_pct is None:
                        continue
                    holdings.append({
                        "stock_code": stock_code,
                        "stock_name": stock_name,
                        "weight_pct": weight_pct,
                        "shares": int(shares) if shares else 0,
                    })
                if holdings:
                    break
            browser.close()
            if len(holdings) < 3:
                print(f"  [Playwright] {etf_code}: only {len(holdings)} holdings")
                return None
            return holdings
    except Exception as e:
        print(f"  [Playwright] {etf_code}: {e}")
        return None


def fetch_holdings(etf_code):
    """Try HTTP first, fallback to Playwright."""
    result = fetch_holdings_http(etf_code)
    if result:
        return result, "http"
    print(f"  HTTP failed for {etf_code}, trying Playwright...")
    result = fetch_holdings_playwright(etf_code)
    if result:
        return result, "playwright"
    return None, "failed"


def fetch_all_etf_holdings(etf_list):
    """Fetch holdings for all ETFs, return unified data dict."""
    data = {}
    for etf in etf_list:
        code = etf['code']
        name = etf['name']
        print(f"Fetching {code} {name}...")
        holdings, method = fetch_holdings(code)
        if holdings:
            has_na = any(h.get('weight_na') for h in holdings)
            data[code] = {
                "name": name,
                "issuer": etf.get('issuer', ''),
                "status": "partial" if has_na else "ok",
                "method": method,
                "holdings_count": len(holdings),
                "holdings": holdings,
            }
            label = f"(partial, weights N/A)" if has_na else ""
            print(f"  ✓ {code}: {len(holdings)} holdings via {method} {label}")
        else:
            data[code] = {
                "name": name,
                "issuer": etf.get('issuer', ''),
                "status": "error",
                "method": "failed",
                "error_msg": "All fetch methods failed",
                "holdings_count": 0,
                "holdings": [],
            }
            print(f"  ✗ {code}: fetch failed")
        time.sleep(FETCH_DELAY)
    return data


# ── Section 3: Persistence ───────────────────────────────────────────────────

def save_daily_snapshot(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{TODAY.isoformat()}.json")
    payload = {
        "fetched_at": datetime.now().isoformat(timespec='seconds'),
        "etfs": data,
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Saved snapshot to {path}")


def save_latest_cache(data):
    payload = {
        "fetched_at": datetime.now().isoformat(timespec='seconds'),
        "date": TODAY.isoformat(),
        "etfs": data,
    }
    with open(LATEST_CACHE, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_snapshot(date_str):
    path = os.path.join(DATA_DIR, f"{date_str}.json")
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_latest_cache():
    if not os.path.exists(LATEST_CACHE):
        return None
    with open(LATEST_CACHE, 'r', encoding='utf-8') as f:
        return json.load(f)


def cleanup_old_snapshots(keep_days=MAX_HISTORY_DAYS):
    if not os.path.exists(DATA_DIR):
        return
    cutoff = TODAY - timedelta(days=keep_days)
    for fname in os.listdir(DATA_DIR):
        if not fname.endswith('.json'):
            continue
        try:
            fdate = datetime.strptime(fname.replace('.json', ''), '%Y-%m-%d').date()
            if fdate < cutoff:
                os.remove(os.path.join(DATA_DIR, fname))
                print(f"Cleaned up old snapshot: {fname}")
        except ValueError:
            pass


# ── Section 4: Change detection engine ───────────────────────────────────────

def _holdings_by_code(holdings_list):
    """Convert holdings list to dict keyed by stock_code."""
    return {h['stock_code']: h for h in holdings_list}


def compute_holdings_diff(today_data, prev_data):
    """Compare today vs previous holdings for each ETF."""
    if not prev_data or 'etfs' not in prev_data:
        return {}
    prev_etfs = prev_data['etfs']
    diffs = {}
    for code, today_etf in today_data.items():
        if today_etf['status'] not in ('ok', 'partial'):
            continue
        prev_etf = prev_etfs.get(code)
        if not prev_etf or prev_etf.get('status') not in ('ok', 'partial'):
            continue
        today_map = _holdings_by_code(today_etf['holdings'])
        prev_map = _holdings_by_code(prev_etf['holdings'])
        today_codes = set(today_map.keys())
        prev_codes = set(prev_map.keys())
        new_buys = []
        for sc in sorted(today_codes - prev_codes):
            h = today_map[sc]
            new_buys.append({**h, "signal": "新買入"})
        new_sells = []
        for sc in sorted(prev_codes - today_codes):
            h = prev_map[sc]
            new_sells.append({**h, "signal": "賣出"})
        weight_up = []
        weight_down = []
        for sc in sorted(today_codes & prev_codes):
            tw = today_map[sc]['weight_pct']
            pw = prev_map[sc]['weight_pct']
            delta = tw - pw
            if delta > WEIGHT_CHANGE_THRESHOLD:
                weight_up.append({**today_map[sc], "prev_weight": pw, "delta": round(delta, 2), "signal": "加碼"})
            elif delta < -WEIGHT_CHANGE_THRESHOLD:
                weight_down.append({**today_map[sc], "prev_weight": pw, "delta": round(delta, 2), "signal": "減碼"})
        if new_buys or new_sells or weight_up or weight_down:
            diffs[code] = {
                "name": today_etf['name'],
                "new_buys": new_buys,
                "new_sells": new_sells,
                "weight_increased": sorted(weight_up, key=lambda x: -x['delta']),
                "weight_decreased": sorted(weight_down, key=lambda x: x['delta']),
            }
    return diffs


def _build_stock_etf_map(data):
    """Build {stock_code: set(etf_codes)} from snapshot data."""
    if not data:
        return {}
    etfs = data.get('etfs', data) if isinstance(data, dict) else data
    if 'etfs' in etfs:
        etfs = etfs['etfs']
    result = {}
    for code, etf in etfs.items():
        if etf.get('status') not in ('ok', 'partial'):
            continue
        for h in etf.get('holdings', []):
            sc = h['stock_code']
            if sc not in result:
                result[sc] = set()
            result[sc].add(code)
    return result


def compute_cross_etf_consensus(today_data, prev_data=None):
    """Find stocks held by multiple active ETFs, with change tracking."""
    stock_map = {}
    for code, etf in today_data.items():
        if etf['status'] not in ('ok', 'partial'):
            continue
        for h in etf['holdings']:
            sc = h['stock_code']
            if sc not in stock_map:
                stock_map[sc] = {
                    "stock_code": sc,
                    "stock_name": h['stock_name'],
                    "holders": [],  # list of {etf, weight, weight_na}
                }
            stock_map[sc]['holders'].append({
                "etf": code,
                "weight": h['weight_pct'],
                "weight_na": h.get('weight_na', False),
            })

    prev_map = _build_stock_etf_map(prev_data) if prev_data else {}

    result = []
    for sc, info in stock_map.items():
        if len(info['holders']) >= 2:
            # Sort holders by weight desc (N/A treated as 0)
            sorted_holders = sorted(info['holders'], key=lambda h: -h['weight'])
            known_weights = [h['weight'] for h in info['holders'] if not h['weight_na']]
            avg_w = round(sum(known_weights) / len(known_weights), 2) if known_weights else 0
            prev_etfs = prev_map.get(sc, set())
            today_etfs = set(h['etf'] for h in info['holders'])
            newly_added = sorted(today_etfs - prev_etfs)
            recently_removed = sorted(prev_etfs - today_etfs)
            prev_count = len(prev_etfs)
            delta = len(today_etfs) - prev_count
            result.append({
                "stock_code": sc,
                "stock_name": info['stock_name'],
                "etf_count": len(info['holders']),
                "holders": sorted_holders,
                "etfs": [h['etf'] for h in sorted_holders],
                "avg_weight": avg_w,
                "prev_count": prev_count if prev_data else None,
                "delta": delta if prev_data else None,
                "newly_added_by": newly_added,
                "recently_removed_by": recently_removed,
            })
    result.sort(key=lambda x: (-x['etf_count'], -x['avg_weight']))
    return result


def compute_weekly_diff(today_data, week_ago_data):
    """Same as daily diff but comparing to 7 days ago."""
    return compute_holdings_diff(today_data, week_ago_data)


# ── Section 5: Health check ──────────────────────────────────────────────────

def build_health_report(today_data, prev_data):
    """Build per-ETF health status report."""
    report = {}
    for code, etf in today_data.items():
        entry = {
            "name": etf['name'],
            "status": etf['status'],
            "method": etf.get('method', 'unknown'),
            "holdings_count": etf.get('holdings_count', 0),
            "last_success": datetime.now().isoformat(timespec='seconds') if etf['status'] in ('ok', 'partial') else None,
        }
        if etf['status'] == 'error' and prev_data and 'etfs' in prev_data:
            prev_etf = prev_data['etfs'].get(code)
            if prev_etf and prev_etf.get('status') == 'ok':
                entry['status'] = 'stale'
                entry['holdings_count'] = prev_etf.get('holdings_count', 0)
                entry['last_success'] = prev_data.get('fetched_at', 'unknown')
                today_data[code]['holdings'] = prev_etf['holdings']
                today_data[code]['holdings_count'] = prev_etf['holdings_count']
        report[code] = entry
    return report


# ── Section 6: HTML generation ───────────────────────────────────────────────

CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b;--card:#fff;--hover:#eef2ff}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:14px;line-height:1.5}
.hdr{background:linear-gradient(135deg,#0f172a 0%,#1e3a8a 50%,#2563eb 100%);color:#fff;padding:22px 28px 16px}
.hdr h1{font-size:20px;font-weight:800;margin-bottom:4px;letter-spacing:-0.3px}
.hdr .sub{font-size:11.5px;opacity:.85}
.nav-link{color:#93c5fd;font-size:11.5px;text-decoration:none;margin-left:14px;border-bottom:1px dashed #93c5fd;padding-bottom:1px;transition:opacity .15s}
.nav-link:hover{opacity:.7}
.warn-box{background:#fffbeb;border:1px solid #fde68a;color:#92400e;border-radius:8px;padding:10px 14px;font-size:12px;margin:14px 28px 0;line-height:1.7}
.stats{display:flex;gap:12px;padding:16px 28px;background:var(--card);border-bottom:1px solid var(--brd);flex-wrap:wrap}
.sc{background:var(--bg);border:1px solid var(--brd);border-radius:10px;padding:14px 20px;min-width:120px;transition:transform .15s,box-shadow .15s;border-left:3px solid var(--bl)}
.sc:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,.08)}
.sc .n{font-size:28px;font-weight:800;color:var(--bl);letter-spacing:-0.5px}.sc .l{font-size:11px;color:var(--mu);margin-top:3px;font-weight:500}
.sc.gr{border-left-color:var(--gr)}.sc.gr .n{color:var(--gr)}
.sc.rd{border-left-color:var(--rd)}.sc.rd .n{color:var(--rd)}
.sc.am{border-left-color:var(--am)}.sc.am .n{color:var(--am)}
.tabs{display:flex;padding:0 28px;border-bottom:2px solid var(--brd);background:var(--card);gap:4px}
.tab{padding:12px 22px;cursor:pointer;border-bottom:3px solid transparent;font-size:12.5px;font-weight:600;color:var(--mu);margin-bottom:-2px;transition:all .15s;border-radius:6px 6px 0 0}
.tab:hover{background:#f1f5f9;color:var(--txt)}
.tab.active{border-bottom-color:var(--bl);color:var(--bl);background:transparent}
.pane{display:none;padding:20px 28px}.pane.active{display:block}
.ttl{font-size:15px;font-weight:700;margin-bottom:6px}
.desc{font-size:12.5px;color:var(--mu);margin-bottom:14px;line-height:1.7}
.etf-section{margin-bottom:24px}
.etf-section h3{font-size:13.5px;font-weight:700;color:var(--bl);margin-bottom:10px;padding:8px 0;border-bottom:2px solid #dbeafe}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06);margin-bottom:16px}
th{background:#edf2f7;font-weight:700;color:var(--mu);font-size:11px;text-transform:uppercase;padding:10px 12px;text-align:left;border-bottom:2px solid var(--brd);white-space:nowrap;letter-spacing:0.3px}
td{padding:9px 12px;border-bottom:1px solid var(--brd);vertical-align:middle;transition:background .1s}
tr:last-child td{border-bottom:none}
tr:hover td{background:var(--hover)}
.num{text-align:right;font-variant-numeric:tabular-nums}
.center{text-align:center}
tr.row-buy td{background:#f0fdf4;border-left:3px solid var(--gr)}
tr.row-buy:hover td{background:#dcfce7}
tr.row-sell td{background:#fef2f2;border-left:3px solid var(--rd)}
tr.row-sell:hover td{background:#fee2e2}
tr.row-up td{background:#f0fdf4;border-left:3px solid var(--gr)}
tr.row-down td{background:#fff7ed;border-left:3px solid #f97316}
.badge{display:inline-block;padding:3px 10px;border-radius:6px;font-size:11px;font-weight:600;white-space:nowrap;letter-spacing:0.2px}
.badge.buy{background:#dcfce7;color:#15803d;border:1px solid #bbf7d0}
.badge.sell{background:#fee2e2;color:#dc2626;border:1px solid #fecaca}
.badge.up{background:#dcfce7;color:#15803d;border:1px solid #bbf7d0}
.badge.down{background:#ffedd5;color:#c2410c;border:1px solid #fed7aa}
.badge.ok{background:#dcfce7;color:#15803d;border:1px solid #bbf7d0}
.badge.error{background:#fee2e2;color:#dc2626;border:1px solid #fecaca}
.badge.stale{background:#fef9c3;color:#854d0e;border:1px solid #fde68a}
.badge.partial{background:#e0e7ff;color:#3730a3;border:1px solid #c7d2fe}
.badge.info{background:#dbeafe;color:#1d4ed8;border:1px solid #bfdbfe}
.badge.etf-tag{background:#ede9fe;color:#6d28d9;font-size:10px;padding:2px 7px;margin:1px 2px;border:1px solid #ddd6fe}
.empty-msg{color:var(--mu);font-size:13px;padding:16px 0;text-align:center}
.delta-up{color:#16a34a;font-weight:700}
.delta-down{color:#dc2626;font-weight:700}
select.etf-select{padding:8px 14px;border:1px solid var(--brd);border-radius:8px;font-size:13px;margin-bottom:14px;background:var(--card);transition:border-color .15s}
select.etf-select:focus{outline:none;border-color:var(--bl)}
.ft{text-align:center;color:var(--mu);font-size:11.5px;padding:20px 28px;border-top:1px solid var(--brd);line-height:1.8;background:var(--card)}
@media(max-width:768px){.hdr,.stats,.pane{padding-left:16px;padding-right:16px}.tabs{padding:0 16px;overflow-x:auto}th,td{padding:8px 8px}.sc{min-width:100px;padding:10px 14px}.sc .n{font-size:22px}.warn-box{margin-left:16px;margin-right:16px}}
"""

JS = """
function showTab(id,el){
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  el.classList.add('active');
}
function filterETF(sel){
  var v=sel.value;
  document.querySelectorAll('.etf-detail').forEach(function(d){
    d.style.display=(v==='all'||d.dataset.code===v)?'block':'none';
  });
}
"""


def _fmt_num(v, decimals=0):
    if v is None:
        return '─'
    if decimals == 0:
        return f"{int(v):,}"
    return f"{v:,.{decimals}f}"


def _fmt_delta(v):
    if v > 0:
        return f'<span class="delta-up">+{v:.2f}%</span>'
    return f'<span class="delta-down">{v:.2f}%</span>'


def _signal_badge(signal):
    cls_map = {"新買入": "buy", "賣出": "sell", "加碼": "up", "減碼": "down"}
    cls = cls_map.get(signal, "info")
    return f'<span class="badge {cls}">{signal}</span>'


def _gen_warn_box(health):
    # No public-facing warnings; monitoring is done via script output only.
    return ''


def _gen_stats(today_data, diffs, consensus, health):
    total = len(today_data)
    total_buys = sum(len(d.get('new_buys', [])) for d in diffs.values())
    total_sells = sum(len(d.get('new_sells', [])) for d in diffs.values())
    consensus_count = sum(1 for c in consensus if c['etf_count'] >= 3)
    return f"""<div class="stats">
  <div class="sc"><div class="n">{total}</div><div class="l">追蹤ETF數</div></div>
  <div class="sc gr"><div class="n">{total_buys}</div><div class="l">今日新買入</div></div>
  <div class="sc rd"><div class="n">{total_sells}</div><div class="l">今日賣出</div></div>
  <div class="sc am"><div class="n">{consensus_count}</div><div class="l">共識持股(≥3檔)</div></div>
</div>"""


def _gen_daily_changes(diffs, today_data):
    if not diffs:
        return '<div class="empty-msg">今日無異動資料（可能是首次執行或無前日資料可比較）</div>'
    html = ''
    for code, diff in sorted(diffs.items()):
        changes = diff['new_buys'] + diff['new_sells'] + diff['weight_increased'] + diff['weight_decreased']
        if not changes:
            continue
        html += f'<div class="etf-section"><h3>{code} {diff["name"]} — {len(changes)} 筆異動</h3>'
        html += '<table><tr><th>股票代號</th><th>股票名稱</th><th>訊號</th><th class="num">今日權重%</th><th class="num">前日權重%</th><th class="num">變化</th></tr>'
        for item in diff['new_buys']:
            html += f'<tr class="row-buy"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td>{_signal_badge("新買入")}</td><td class="num">{item["weight_pct"]:.2f}</td><td class="num">─</td><td class="num">─</td></tr>'
        for item in diff['new_sells']:
            html += f'<tr class="row-sell"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td>{_signal_badge("賣出")}</td><td class="num">─</td><td class="num">{item["weight_pct"]:.2f}</td><td class="num">─</td></tr>'
        for item in diff['weight_increased']:
            html += f'<tr class="row-up"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td>{_signal_badge("加碼")}</td><td class="num">{item["weight_pct"]:.2f}</td><td class="num">{item["prev_weight"]:.2f}</td><td class="num">{_fmt_delta(item["delta"])}</td></tr>'
        for item in diff['weight_decreased']:
            html += f'<tr class="row-down"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td>{_signal_badge("減碼")}</td><td class="num">{item["weight_pct"]:.2f}</td><td class="num">{item["prev_weight"]:.2f}</td><td class="num">{_fmt_delta(item["delta"])}</td></tr>'
        html += '</table></div>'
    if not html:
        html = '<div class="empty-msg">今日所有 ETF 持股均無顯著異動</div>'
    return html


def _gen_consensus(consensus):
    if not consensus:
        return '<div class="empty-msg">無共識持股資料</div>'
    has_prev = any(item.get('prev_count') is not None for item in consensus)
    html = '<table><tr><th>股票代號</th><th>股票名稱</th><th class="center">持有ETF數</th>'
    if has_prev:
        html += '<th class="center">變化</th>'
    html += '<th class="num">平均權重%</th><th>持有ETF（依權重排序）</th></tr>'

    for item in consensus:
        row_cls = ''
        if item.get('newly_added_by'):
            row_cls = ' class="row-buy"'
        elif item['etf_count'] >= 4:
            row_cls = ' class="row-buy"'

        holders = item.get('holders', [])
        # Find max weight for this stock to scale color intensity
        max_w = max((h['weight'] for h in holders if not h['weight_na']), default=0)

        etf_tags = ''
        newly_added = set(item.get('newly_added_by', []))
        for h in holders:
            etf = h['etf']
            weight = h['weight']
            na = h['weight_na']
            is_new = etf in newly_added

            if na:
                # Partial data: gray with dash
                label = f'{etf} <span style="opacity:0.6">─</span>'
                style = 'background:#f1f5f9;color:#64748b;border-color:#e2e8f0'
            else:
                # Weight-proportional color intensity (0.15 to 0.9 alpha)
                ratio = (weight / max_w) if max_w > 0 else 0
                # Interpolate purple: light to dark
                alpha = 0.15 + ratio * 0.75
                bg = f'rgba(109,40,217,{alpha:.2f})'
                color = '#fff' if alpha > 0.5 else '#4c1d95'
                label = f'{etf} <b>{weight:.2f}%</b>'
                style = f'background:{bg};color:{color};border-color:rgba(109,40,217,0.3)'

            if is_new:
                label += ' ✦新'
                if not na:
                    style = 'background:#16a34a;color:#fff;border-color:#15803d'
            etf_tags += f'<span class="badge etf-tag" style="{style}">{label}</span>'

        delta_cell = ''
        if has_prev:
            d = item.get('delta')
            if d is not None and d > 0:
                delta_cell = f'<td class="center"><span class="delta-up">+{d}</span></td>'
            elif d is not None and d < 0:
                delta_cell = f'<td class="center"><span class="delta-down">{d}</span></td>'
            else:
                delta_cell = '<td class="center">─</td>'
        html += f'<tr{row_cls}><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td class="center"><b>{item["etf_count"]}</b></td>{delta_cell}<td class="num">{item["avg_weight"]:.2f}</td><td>{etf_tags}</td></tr>'
    html += '</table>'
    return html


def _gen_individual_holdings(today_data):
    ok_etfs = {c: e for c, e in today_data.items() if e['status'] in ('ok', 'partial') and e['holdings']}
    if not ok_etfs:
        return '<div class="empty-msg">無持股資料</div>'
    html = '<select class="etf-select" onchange="filterETF(this)"><option value="all">全部 ETF</option>'
    for code in sorted(ok_etfs.keys()):
        html += f'<option value="{code}">{code} {ok_etfs[code]["name"]}</option>'
    html += '</select>'
    for code in sorted(ok_etfs.keys()):
        etf = ok_etfs[code]
        html += f'<div class="etf-detail" data-code="{code}">'
        html += f'<div class="etf-section"><h3>{code} {etf["name"]}（{etf["holdings_count"]} 檔持股）</h3>'
        html += '<table><tr><th>#</th><th>股票代號</th><th>股票名稱</th><th class="num">權重%</th><th class="num">持股數</th></tr>'
        for i, h in enumerate(etf['holdings'], 1):
            w = '─' if h.get('weight_na') else f'{h["weight_pct"]:.2f}'
            html += f'<tr><td>{i}</td><td><b>{h["stock_code"]}</b></td><td>{h["stock_name"]}</td><td class="num">{w}</td><td class="num">{_fmt_num(h["shares"])}</td></tr>'
        html += '</table></div></div>'
    return html


def _gen_weekly_changes(weekly_diffs, compare_date):
    if not compare_date:
        return '<div class="empty-msg">尚未累積一週的歷史資料，系統運行滿 7 天後將自動產生週度比較</div>'
    if not weekly_diffs:
        return f'<div class="empty-msg">無週度比較資料（比較日期：{compare_date or "無"}）</div>'
    html = f'<div class="desc">與 {compare_date} 相比的持股異動</div>'
    for code, diff in sorted(weekly_diffs.items()):
        changes = diff['new_buys'] + diff['new_sells'] + diff['weight_increased'] + diff['weight_decreased']
        if not changes:
            continue
        html += f'<div class="etf-section"><h3>{code} {diff["name"]} — {len(changes)} 筆異動</h3>'
        html += '<table><tr><th>股票代號</th><th>股票名稱</th><th>訊號</th><th class="num">今日權重%</th><th class="num">上週權重%</th><th class="num">變化</th></tr>'
        for item in diff['new_buys']:
            html += f'<tr class="row-buy"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td>{_signal_badge("新買入")}</td><td class="num">{item["weight_pct"]:.2f}</td><td class="num">─</td><td class="num">─</td></tr>'
        for item in diff['new_sells']:
            html += f'<tr class="row-sell"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td>{_signal_badge("賣出")}</td><td class="num">─</td><td class="num">{item["weight_pct"]:.2f}</td><td class="num">─</td></tr>'
        for item in diff['weight_increased']:
            html += f'<tr class="row-up"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td>{_signal_badge("加碼")}</td><td class="num">{item["weight_pct"]:.2f}</td><td class="num">{item["prev_weight"]:.2f}</td><td class="num">{_fmt_delta(item["delta"])}</td></tr>'
        for item in diff['weight_decreased']:
            html += f'<tr class="row-down"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td>{_signal_badge("減碼")}</td><td class="num">{item["weight_pct"]:.2f}</td><td class="num">{item["prev_weight"]:.2f}</td><td class="num">{_fmt_delta(item["delta"])}</td></tr>'
        html += '</table></div>'
    if '<table>' not in html:
        html += '<div class="empty-msg">本週所有 ETF 持股均無顯著異動</div>'
    return html


def _gen_health(health):
    html = '<table><tr><th>ETF代號</th><th>ETF名稱</th><th class="center">狀態</th><th class="num">持股數</th><th>爬取方式</th><th>上次成功</th></tr>'
    for code in sorted(health.keys()):
        h = health[code]
        status_cls = {"ok": "ok", "partial": "partial", "error": "error", "stale": "stale"}.get(h['status'], 'info')
        status_text = {"ok": "正常", "partial": "部分", "error": "錯誤", "stale": "過時"}.get(h['status'], h['status'])
        last = h.get('last_success') or '─'
        html += f'<tr><td><b>{code}</b></td><td>{h["name"]}</td><td class="center"><span class="badge {status_cls}">{status_text}</span></td><td class="num">{h["holdings_count"]}</td><td>{h["method"]}</td><td>{last}</td></tr>'
    html += '</table>'
    return html


def generate_etf_html(today_data, diff_daily, diff_weekly, consensus, health, compare_date):
    warn = _gen_warn_box(health)
    stats = _gen_stats(today_data, diff_daily, consensus, health)
    tab_daily = _gen_daily_changes(diff_daily, today_data)
    tab_consensus = _gen_consensus(consensus)
    tab_holdings = _gen_individual_holdings(today_data)
    tab_weekly = _gen_weekly_changes(diff_weekly, compare_date)

    return f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>主動式 ETF 持股追蹤儀表板</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>主動式 ETF 持股追蹤儀表板</h1>
  <div class="sub">更新：{TODAY.isoformat()}（每個交易日收盤後自動更新）<a class="nav-link" href="index.html">→ 可轉債儀表板</a></div>
</div>
{warn}
{stats}
<div class="tabs">
  <div class="tab active" onclick="showTab('t1',this)">每日異動</div>
  <div class="tab" onclick="showTab('t2',this)">共識持股</div>
  <div class="tab" onclick="showTab('t3',this)">各ETF持股</div>
  <div class="tab" onclick="showTab('t4',this)">週報比較</div>
</div>
<div id="t1" class="pane active">
  <div class="ttl">每日持股異動</div>
  <div class="desc">偵測每檔主動式 ETF 的新買入、賣出、加碼、減碼動作（權重變化門檻：±{WEIGHT_CHANGE_THRESHOLD}%）</div>
  {tab_daily}
</div>
<div id="t2" class="pane">
  <div class="ttl">跨 ETF 共識持股</div>
  <div class="desc">被多檔主動式 ETF 同時持有的個股。徽章依權重由大到小排序，顏色越深代表該 ETF 持倉比重越高。標示「✦新」代表該 ETF 近期新買入此標的</div>
  {tab_consensus}
</div>
<div id="t3" class="pane">
  <div class="ttl">各 ETF 完整持股明細</div>
  <div class="desc">選擇 ETF 查看完整持股清單</div>
  {tab_holdings}
</div>
<div id="t4" class="pane">
  <div class="ttl">週度異動比較</div>
  <div class="desc">與一週前的持股比較，觀察中期持倉策略變化</div>
  {tab_weekly}
</div>
<div class="ft">資料來源：MoneyDJ ｜ 僅供研究參考，不構成投資建議 ｜ 主動式 ETF 持股追蹤系統</div>
<script>{JS}</script>
</body></html>"""


# ── Section 7: Main ──────────────────────────────────────────────────────────

def main():
    config = load_config()
    etf_list = config['etfs']
    print(f"=== 主動式 ETF 持股追蹤 ({TODAY.isoformat()}) ===")
    print(f"追蹤 {len(etf_list)} 檔 ETF\n")

    prev_data = load_latest_cache()

    today_data = fetch_all_etf_holdings(etf_list)

    save_daily_snapshot(today_data)
    save_latest_cache(today_data)
    cleanup_old_snapshots()

    diff_daily = compute_holdings_diff(today_data, prev_data) if prev_data else {}

    week_ago_date = (TODAY - timedelta(days=7)).isoformat()
    week_ago_data = load_snapshot(week_ago_date)
    if not week_ago_data:
        for d in range(6, 10):
            alt_date = (TODAY - timedelta(days=d)).isoformat()
            week_ago_data = load_snapshot(alt_date)
            if week_ago_data:
                week_ago_date = alt_date
                break
    diff_weekly = compute_weekly_diff(today_data, week_ago_data) if week_ago_data else {}

    consensus = compute_cross_etf_consensus(today_data, prev_data)
    health = build_health_report(today_data, prev_data)

    html = generate_etf_html(today_data, diff_daily, diff_weekly, consensus, health, week_ago_date if week_ago_data else None)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\nDashboard written to {OUTPUT_HTML}")

    ok_count = sum(1 for h in health.values() if h['status'] in ('ok', 'partial'))
    err_count = sum(1 for h in health.values() if h['status'] == 'error')
    stale_count = sum(1 for h in health.values() if h['status'] == 'stale')
    print(f"Status: {ok_count} ok, {stale_count} stale, {err_count} error")

    if err_count == len(etf_list):
        print("::error::All ETF fetches failed!", file=sys.stderr)
        sys.exit(1)
    elif err_count + stale_count > len(etf_list) // 2:
        print(f"::warning::ETF fetch issues: {err_count} errors, {stale_count} stale", file=sys.stderr)


if __name__ == '__main__':
    main()
