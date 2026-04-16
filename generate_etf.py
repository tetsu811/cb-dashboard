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
FIRST_SEEN_FILE = 'etf_first_seen.json'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

MONEYDJ_FULL_URL = 'https://www.moneydj.com/etf/x/basic/Basic0007B.xdjhtm?etfid={etf_id}'
MONEYDJ_TOP10_URL = 'https://www.moneydj.com/etf/x/basic/basic0007.xdjhtm?etfid={etf_id}'
YAHOO_PROFILE_URL = 'https://tw.stock.yahoo.com/quote/{etf_code}.TW/profile'

WEIGHT_CHANGE_THRESHOLD = 0.5       # 百分點（用於舊邏輯）
MAX_HISTORY_DAYS = 30
FETCH_DELAY = 1.5

# ── Flow monitoring thresholds ──
BIG_ETF_AUM_THRESHOLD = 100         # 億 TWD — 大資金 ETF 的門檻
CAPITAL_FLOW_THRESHOLD = 1.0        # 億 TWD — 重大資金流入/出的絕對門檻
WEIGHT_RATIO_THRESHOLD = 0.20       # 20% — 權重相對變化門檻（兩者任一達標）
NEW_BUY_WINDOW_DAYS = 7             # 首見後多少天仍顯示「✦新」


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


def fetch_aum_yahoo(etf_code):
    """Fetch AUM (total assets in TWD) from Yahoo Finance TW. Returns value in 億 (100M TWD)."""
    try:
        url = YAHOO_PROFILE_URL.format(etf_code=etf_code)
        resp = requests.get(url, headers=HEADERS, timeout=10)
        m = re.search(r'"totalAssets":"([0-9.]+)"', resp.text)
        if m:
            return round(float(m.group(1)) / 100_000_000, 2)  # TWD → 億
    except Exception as e:
        print(f"  [AUM] {etf_code}: {e}")
    return None


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
            aum = fetch_aum_yahoo(code)
            data[code] = {
                "name": name,
                "issuer": etf.get('issuer', ''),
                "status": "partial" if has_na else "ok",
                "method": method,
                "holdings_count": len(holdings),
                "aum_billion": aum,  # In 億 TWD (100M)
                "holdings": holdings,
            }
            label = f"(partial, weights N/A)" if has_na else ""
            aum_label = f"AUM {aum:.1f}億" if aum else "AUM n/a"
            print(f"  ✓ {code}: {len(holdings)} holdings via {method} {label} | {aum_label}")
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


def load_first_seen():
    """Load historical first-seen record: {stock_code: {etf_code: date}}.
    Returns (data, existed) tuple — `existed` is False on first-ever run."""
    if not os.path.exists(FIRST_SEEN_FILE):
        return {}, False
    with open(FIRST_SEEN_FILE, 'r', encoding='utf-8') as f:
        return json.load(f), True


def save_first_seen(first_seen):
    with open(FIRST_SEEN_FILE, 'w', encoding='utf-8') as f:
        json.dump(first_seen, f, ensure_ascii=False, indent=2, sort_keys=True)


def update_first_seen(today_data, first_seen):
    """Update first_seen record: mark (stock, etf) combos that didn't exist before."""
    today_str = TODAY.isoformat()
    for etf_code, etf in today_data.items():
        if etf.get('status') not in ('ok', 'partial'):
            continue
        for h in etf.get('holdings', []):
            sc = h['stock_code']
            if sc not in first_seen:
                first_seen[sc] = {}
            if etf_code not in first_seen[sc]:
                first_seen[sc][etf_code] = today_str
    return first_seen


def is_recent_first_buy(first_seen, stock_code, etf_code, window_days=NEW_BUY_WINDOW_DAYS, bootstrap=False):
    """Check if (stock, etf) was first seen within the last N days.
    If bootstrap=True, always returns False (first-ever run where registry is being populated today)."""
    if bootstrap:
        return False
    record = first_seen.get(stock_code, {}).get(etf_code)
    if not record:
        return False
    try:
        first_date = datetime.strptime(record, '%Y-%m-%d').date()
        return (TODAY - first_date).days <= window_days
    except ValueError:
        return False


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


def _build_stock_capital_map(data):
    """Build {stock_code: {etf_code: {capital, weight, aum}}} from snapshot data."""
    if not data:
        return {}
    etfs = data.get('etfs', data) if isinstance(data, dict) else data
    if 'etfs' in etfs:
        etfs = etfs['etfs']
    result = {}
    for code, etf in etfs.items():
        if etf.get('status') not in ('ok', 'partial'):
            continue
        aum = etf.get('aum_billion')
        for h in etf.get('holdings', []):
            sc = h['stock_code']
            w = h.get('weight_pct') or 0
            na = h.get('weight_na', False)
            cap = round(aum * w / 100, 2) if (aum and not na and w) else None
            result.setdefault(sc, {})[code] = {
                "capital": cap, "weight": w, "aum": aum, "weight_na": na
            }
    return result


def compute_cross_etf_consensus(today_data, prev_data=None, first_seen=None, bootstrap=False):
    """Find stocks held by multiple active ETFs, with change tracking and capital exposure."""
    stock_map = {}
    for code, etf in today_data.items():
        if etf['status'] not in ('ok', 'partial'):
            continue
        aum = etf.get('aum_billion')
        for h in etf['holdings']:
            sc = h['stock_code']
            if sc not in stock_map:
                stock_map[sc] = {
                    "stock_code": sc,
                    "stock_name": h['stock_name'],
                    "holders": [],
                }
            # Capital exposure = AUM × weight%
            na = h.get('weight_na', False)
            if aum and not na and h['weight_pct']:
                capital = round(aum * h['weight_pct'] / 100, 2)  # in 億
            else:
                capital = None
            stock_map[sc]['holders'].append({
                "etf": code,
                "weight": h['weight_pct'],
                "weight_na": na,
                "aum": aum,
                "capital": capital,
            })

    prev_map = _build_stock_etf_map(prev_data) if prev_data else {}
    prev_cap_map = _build_stock_capital_map(prev_data) if prev_data else {}

    result = []
    for sc, info in stock_map.items():
        if len(info['holders']) >= 2:
            # Annotate each holder with its capital delta vs previous snapshot
            prev_caps = prev_cap_map.get(sc, {})
            for h in info['holders']:
                prev_holder = prev_caps.get(h['etf'])
                if prev_holder and prev_holder['capital'] is not None and h['capital'] is not None:
                    h['prev_capital'] = prev_holder['capital']
                    h['capital_delta'] = round(h['capital'] - prev_holder['capital'], 2)
                else:
                    h['prev_capital'] = None
                    h['capital_delta'] = None

            # Sort holders by capital desc (falls back to weight if capital N/A)
            sorted_holders = sorted(
                info['holders'],
                key=lambda h: -(h['capital'] if h['capital'] is not None else h['weight'] * -0.0001)
            )
            known_weights = [h['weight'] for h in info['holders'] if not h['weight_na']]
            avg_w = round(sum(known_weights) / len(known_weights), 2) if known_weights else 0
            total_capital = round(sum(h['capital'] for h in info['holders'] if h['capital']), 2)

            # Previous total capital (only include ETFs present in both snapshots)
            prev_total = 0
            comparable = False
            for h in info['holders']:
                if h.get('prev_capital') is not None:
                    prev_total += h['prev_capital']
                    comparable = True
            prev_total = round(prev_total, 2)
            capital_delta = round(total_capital - prev_total, 2) if comparable else None

            prev_etfs = prev_map.get(sc, set())
            today_etfs = set(h['etf'] for h in info['holders'])
            # Use first_seen registry for "newly_added" to avoid false positives on first run
            if first_seen and not bootstrap:
                newly_added = sorted(
                    etf for etf in today_etfs
                    if is_recent_first_buy(first_seen, sc, etf, bootstrap=False)
                )
            else:
                newly_added = []  # first run: nothing is truly "new"
            recently_removed = sorted(prev_etfs - today_etfs) if prev_data else []
            prev_count = len(prev_etfs)
            delta = len(today_etfs) - prev_count
            result.append({
                "stock_code": sc,
                "stock_name": info['stock_name'],
                "etf_count": len(info['holders']),
                "holders": sorted_holders,
                "etfs": [h['etf'] for h in sorted_holders],
                "avg_weight": avg_w,
                "total_capital": total_capital,  # 億 TWD
                "capital_delta": capital_delta,  # 今日 vs 前日 資金變化 億
                "prev_count": prev_count if prev_data else None,
                "delta": delta if prev_data else None,
                "newly_added_by": newly_added,
                "recently_removed_by": recently_removed,
            })
    result.sort(key=lambda x: (-x['total_capital'], -x['etf_count']))
    return result


def compute_weekly_diff(today_data, week_ago_data):
    """Same as daily diff but comparing to 7 days ago."""
    return compute_holdings_diff(today_data, week_ago_data)


# ── Section 4b: Capital flow engine ──────────────────────────────────────────

def _capital_of(etf_entry, holding):
    """Compute capital (億) for a holding given the ETF's AUM, or None if unavailable."""
    aum = etf_entry.get('aum_billion')
    if not aum:
        return None
    if holding.get('weight_na'):
        return None
    w = holding.get('weight_pct') or 0
    if w <= 0:
        return None
    return round(aum * w / 100, 2)


def _index_prev_etf(prev_data):
    """Index previous snapshot: {etf_code: {stock_code: holding_dict}}."""
    if not prev_data:
        return {}
    etfs = prev_data.get('etfs', prev_data)
    if 'etfs' in etfs:
        etfs = etfs['etfs']
    out = {}
    for code, etf in etfs.items():
        if etf.get('status') not in ('ok', 'partial'):
            continue
        out[code] = {
            "aum_billion": etf.get('aum_billion'),
            "holdings": {h['stock_code']: h for h in etf.get('holdings', [])},
        }
    return out


def compute_capital_flows(today_data, prev_data, first_seen, bootstrap=False):
    """
    Core flow engine. For every stock × ETF pair, compute:
      - today's capital, previous capital, delta
      - is_new_buy (first-seen within window, via first_seen registry)
      - is_exit (held yesterday, not held today)
      - is_material (meets capital or weight ratio threshold)

    Returns: {
        stock_code: {
            "stock_name": ...,
            "net_flow": sum of all deltas (億),
            "inflows": [{etf, delta, today_capital, prev_capital, weight, is_new_buy, is_material}],
            "outflows": [{etf, delta, today_capital, prev_capital, weight, is_exit, is_material}],
            "material_count": number of material actions,
        }
    }
    """
    # First-run guard: no comparison possible without previous data
    if not prev_data:
        return {}
    prev_idx = _index_prev_etf(prev_data)
    flows = {}

    # 1) Walk today's holdings → compute deltas (inflow or pass-through)
    for etf_code, etf in today_data.items():
        if etf.get('status') not in ('ok', 'partial'):
            continue
        prev_etf = prev_idx.get(etf_code, {})
        prev_holdings = prev_etf.get('holdings', {})
        for h in etf.get('holdings', []):
            sc = h['stock_code']
            today_cap = _capital_of(etf, h)
            if today_cap is None:
                continue

            prev_h = prev_holdings.get(sc)
            if prev_h:
                prev_etf_for_cap = {
                    'aum_billion': prev_etf.get('aum_billion'),
                }
                prev_cap = _capital_of(prev_etf_for_cap, prev_h)
            else:
                prev_cap = None

            # Delta: today - yesterday. If not held yesterday → full delta = today_cap.
            if prev_cap is None:
                delta = today_cap
            else:
                delta = round(today_cap - prev_cap, 2)

            if abs(delta) < 0.005:
                continue  # skip negligible drift

            # Weight change (for relative threshold)
            prev_w = prev_h.get('weight_pct') if prev_h else None
            today_w = h.get('weight_pct') or 0
            weight_ratio = 0.0
            if prev_w and prev_w > 0:
                weight_ratio = (today_w - prev_w) / prev_w

            is_new = is_recent_first_buy(first_seen, sc, etf_code, bootstrap=bootstrap)
            is_material = (abs(delta) >= CAPITAL_FLOW_THRESHOLD) or (abs(weight_ratio) >= WEIGHT_RATIO_THRESHOLD)

            record = {
                "etf": etf_code,
                "etf_name": etf.get('name'),
                "etf_aum": etf.get('aum_billion'),
                "today_capital": today_cap,
                "prev_capital": prev_cap,
                "delta": delta,
                "weight": today_w,
                "prev_weight": prev_w,
                "weight_ratio": round(weight_ratio, 3),
                "is_new_buy": is_new,
                "is_material": is_material,
            }

            entry = flows.setdefault(sc, {
                "stock_code": sc,
                "stock_name": h['stock_name'],
                "inflows": [],
                "outflows": [],
                "net_flow": 0.0,
                "material_count": 0,
            })
            if delta > 0:
                entry['inflows'].append(record)
            else:
                record['is_exit'] = False
                entry['outflows'].append(record)
            entry['net_flow'] = round(entry['net_flow'] + delta, 2)
            if is_material:
                entry['material_count'] += 1

    # 2) Walk previous holdings to find complete exits (held yesterday, not today)
    for etf_code, prev_etf in prev_idx.items():
        today_etf = today_data.get(etf_code)
        if not today_etf or today_etf.get('status') not in ('ok', 'partial'):
            continue
        today_stocks = {h['stock_code'] for h in today_etf.get('holdings', [])}
        prev_etf_for_cap = {'aum_billion': prev_etf.get('aum_billion')}
        for sc, prev_h in prev_etf['holdings'].items():
            if sc in today_stocks:
                continue  # still held; already processed
            prev_cap = _capital_of(prev_etf_for_cap, prev_h)
            if prev_cap is None:
                continue
            delta = round(-prev_cap, 2)
            if abs(delta) < 0.005:
                continue
            is_material = abs(delta) >= CAPITAL_FLOW_THRESHOLD
            record = {
                "etf": etf_code,
                "etf_name": today_etf.get('name'),
                "etf_aum": today_etf.get('aum_billion'),
                "today_capital": 0,
                "prev_capital": prev_cap,
                "delta": delta,
                "weight": 0,
                "prev_weight": prev_h.get('weight_pct'),
                "weight_ratio": -1.0,
                "is_new_buy": False,
                "is_exit": True,
                "is_material": is_material,
            }
            entry = flows.setdefault(sc, {
                "stock_code": sc,
                "stock_name": prev_h.get('stock_name', sc),
                "inflows": [],
                "outflows": [],
                "net_flow": 0.0,
                "material_count": 0,
            })
            entry['outflows'].append(record)
            entry['net_flow'] = round(entry['net_flow'] + delta, 2)
            if is_material:
                entry['material_count'] += 1

    # 3) Sort each stock's actions by |delta| desc
    for entry in flows.values():
        entry['inflows'].sort(key=lambda r: -r['delta'])
        entry['outflows'].sort(key=lambda r: r['delta'])  # most negative first

    return flows


def filter_big_etfs(today_data, threshold=BIG_ETF_AUM_THRESHOLD):
    """Return list of ETF codes with AUM >= threshold, sorted by AUM desc."""
    big = []
    for code, etf in today_data.items():
        aum = etf.get('aum_billion') or 0
        if aum >= threshold:
            big.append((code, aum, etf.get('name')))
    big.sort(key=lambda x: -x[1])
    return big


def compute_big_etf_actions(today_data, prev_data, big_etf_codes, first_seen, bootstrap=False):
    """
    For each big ETF, list its buy/sell/increase/decrease actions.
    Returns: {etf_code: {name, aum, aum_delta, buys, sells, increases, decreases}}
    """
    if not prev_data:
        return {}
    prev_idx = _index_prev_etf(prev_data)
    result = {}
    for code in big_etf_codes:
        etf = today_data.get(code)
        if not etf or etf.get('status') not in ('ok', 'partial'):
            continue
        prev_etf = prev_idx.get(code, {})
        prev_aum = prev_etf.get('aum_billion')
        aum = etf.get('aum_billion')
        aum_delta = round(aum - prev_aum, 2) if (aum and prev_aum) else None

        today_holdings = {h['stock_code']: h for h in etf.get('holdings', [])}
        prev_holdings = prev_etf.get('holdings', {})

        buys, sells, increases, decreases = [], [], [], []

        for sc, h in today_holdings.items():
            today_cap = _capital_of(etf, h)
            if today_cap is None:
                continue
            prev_h = prev_holdings.get(sc)
            if not prev_h:
                # Not held yesterday → buy
                buys.append({
                    "stock_code": sc,
                    "stock_name": h['stock_name'],
                    "today_capital": today_cap,
                    "delta": today_cap,
                    "weight": h.get('weight_pct') or 0,
                    "is_new_buy": is_recent_first_buy(first_seen, sc, code, bootstrap=bootstrap),
                })
            else:
                prev_etf_for_cap = {'aum_billion': prev_aum}
                prev_cap = _capital_of(prev_etf_for_cap, prev_h)
                if prev_cap is None:
                    continue
                delta = round(today_cap - prev_cap, 2)
                if abs(delta) < CAPITAL_FLOW_THRESHOLD * 0.1:
                    continue  # skip tiny drift
                record = {
                    "stock_code": sc,
                    "stock_name": h['stock_name'],
                    "today_capital": today_cap,
                    "prev_capital": prev_cap,
                    "delta": delta,
                    "weight": h.get('weight_pct') or 0,
                    "prev_weight": prev_h.get('weight_pct'),
                }
                if delta > 0:
                    increases.append(record)
                else:
                    decreases.append(record)

        # Sells: held yesterday, not today
        for sc, prev_h in prev_holdings.items():
            if sc in today_holdings:
                continue
            prev_etf_for_cap = {'aum_billion': prev_aum}
            prev_cap = _capital_of(prev_etf_for_cap, prev_h)
            if prev_cap is None:
                continue
            sells.append({
                "stock_code": sc,
                "stock_name": prev_h.get('stock_name', sc),
                "prev_capital": prev_cap,
                "delta": -prev_cap,
                "prev_weight": prev_h.get('weight_pct'),
            })

        buys.sort(key=lambda r: -r['delta'])
        sells.sort(key=lambda r: r['delta'])
        increases.sort(key=lambda r: -r['delta'])
        decreases.sort(key=lambda r: r['delta'])

        result[code] = {
            "name": etf.get('name'),
            "aum": aum,
            "aum_delta": aum_delta,
            "buys": buys,
            "sells": sells,
            "increases": increases,
            "decreases": decreases,
        }
    return result


def compute_collective_moves(big_etf_actions, min_count=2):
    """
    Detect stocks where ≥ min_count big ETFs simultaneously buy or sell today.
    Returns: {
        "buys": [{stock_code, stock_name, etf_count, total_capital, actions}],
        "sells": [...]
    }
    where actions = [{etf, etf_name, action_type, delta}]
    """
    stock_buy_actions = {}
    stock_sell_actions = {}

    for etf_code, actions in big_etf_actions.items():
        etf_name = actions['name']
        # Buys + increases = positive actions
        for action in actions['buys'] + actions['increases']:
            sc = action['stock_code']
            entry = stock_buy_actions.setdefault(sc, {
                "stock_code": sc,
                "stock_name": action['stock_name'],
                "actions": [],
                "total_capital": 0.0,
            })
            action_type = "新買入" if action in actions['buys'] else "加碼"
            entry['actions'].append({
                "etf": etf_code,
                "etf_name": etf_name,
                "action_type": action_type,
                "delta": action['delta'],
                "is_new_buy": action.get('is_new_buy', False),
            })
            entry['total_capital'] = round(entry['total_capital'] + action['delta'], 2)

        # Sells + decreases = negative actions
        for action in actions['sells'] + actions['decreases']:
            sc = action['stock_code']
            entry = stock_sell_actions.setdefault(sc, {
                "stock_code": sc,
                "stock_name": action['stock_name'],
                "actions": [],
                "total_capital": 0.0,
            })
            action_type = "完全賣出" if action in actions['sells'] else "減碼"
            entry['actions'].append({
                "etf": etf_code,
                "etf_name": etf_name,
                "action_type": action_type,
                "delta": action['delta'],
            })
            entry['total_capital'] = round(entry['total_capital'] + action['delta'], 2)

    collective_buys = [
        {**v, "etf_count": len(v['actions'])}
        for v in stock_buy_actions.values()
        if len(v['actions']) >= min_count
    ]
    collective_sells = [
        {**v, "etf_count": len(v['actions'])}
        for v in stock_sell_actions.values()
        if len(v['actions']) >= min_count
    ]

    collective_buys.sort(key=lambda x: -x['total_capital'])
    collective_sells.sort(key=lambda x: x['total_capital'])

    return {"buys": collective_buys, "sells": collective_sells}


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


def _fmt_capital(v):
    """Format capital in 億/百萬 for display."""
    if v is None:
        return '─'
    if abs(v) >= 1:
        return f'{v:.1f}億'
    return f'{v*100:.0f}百萬'


def _fmt_cap_delta(v):
    """Format capital delta with sign and color class."""
    if v is None or v == 0:
        return '─'
    if v > 0:
        return f'<span class="delta-up">+{_fmt_capital(v)}</span>'
    return f'<span class="delta-down">{_fmt_capital(v)}</span>'


def _gen_consensus(consensus):
    if not consensus:
        return '<div class="empty-msg">無共識持股資料</div>'
    has_prev = any(item.get('capital_delta') is not None for item in consensus)
    html = '<table><tr><th>股票代號</th><th>股票名稱</th><th class="center">持有ETF數</th>'
    html += '<th class="num">市場影響力（億）</th>'
    if has_prev:
        html += '<th class="num">資金變化</th>'
    html += '<th>持有ETF（依資金量排序）</th></tr>'

    for item in consensus:
        row_cls = ''
        cap_delta = item.get('capital_delta')
        if item.get('newly_added_by'):
            row_cls = ' class="row-buy"'
        elif cap_delta is not None and cap_delta >= 5:
            row_cls = ' class="row-buy"'
        elif item['etf_count'] >= 4:
            row_cls = ' class="row-buy"'

        holders = item.get('holders', [])
        max_cap = max((h['capital'] for h in holders if h['capital']), default=0)

        etf_tags = ''
        newly_added = set(item.get('newly_added_by', []))
        for h in holders:
            etf = h['etf']
            weight = h['weight']
            na = h['weight_na']
            capital = h['capital']
            cap_d = h.get('capital_delta')
            is_new = etf in newly_added

            if na or capital is None:
                label = f'{etf} <span style="opacity:0.6">─</span>'
                style = 'background:#f1f5f9;color:#64748b;border-color:#e2e8f0'
                title = f'{etf}：權重資料未揭露'
            else:
                ratio = (capital / max_cap) if max_cap > 0 else 0
                alpha = 0.15 + ratio * 0.75
                bg = f'rgba(109,40,217,{alpha:.2f})'
                color = '#fff' if alpha > 0.5 else '#4c1d95'
                cap_str = _fmt_capital(capital)
                label = f'{etf} <b>{cap_str}</b>'
                style = f'background:{bg};color:{color};border-color:rgba(109,40,217,0.3)'
                title = f'{etf}：權重 {weight:.2f}% × 基金規模 {h["aum"]:.1f}億 = {cap_str}'
                if cap_d is not None and abs(cap_d) >= 0.1:
                    arrow = '▲' if cap_d > 0 else '▼'
                    label += f' <span style="opacity:0.85;font-size:9.5px">{arrow}{_fmt_capital(abs(cap_d))}</span>'
                    title += f'（較前日 {"+" if cap_d > 0 else ""}{_fmt_capital(cap_d)}）'

            if is_new:
                label += ' ✦新'
                if not na and capital is not None:
                    style = 'background:#16a34a;color:#fff;border-color:#15803d'
            etf_tags += f'<span class="badge etf-tag" style="{style}" title="{title}">{label}</span>'

        total_cap = item.get('total_capital', 0)
        cap_display = f'<b>{total_cap:.1f}</b>' if total_cap >= 1 else f'{total_cap*100:.0f}百萬'
        delta_cell = ''
        if has_prev:
            delta_cell = f'<td class="num">{_fmt_cap_delta(cap_delta)}</td>'
        html += f'<tr{row_cls}><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td class="center"><b>{item["etf_count"]}</b></td><td class="num">{cap_display}</td>{delta_cell}<td>{etf_tags}</td></tr>'
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
        aum = etf.get('aum_billion')
        aum_str = f'，基金規模 {aum:.1f} 億' if aum else ''
        html += f'<div class="etf-section"><h3>{code} {etf["name"]}（{etf["holdings_count"]} 檔持股{aum_str}）</h3>'
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


def _gen_flow_stats(flows, big_etfs, collective):
    """Top-level stats: big ETF count, material inflows/outflows, collective signals, total flow."""
    big_count = len(big_etfs)
    material_in = sum(1 for f in flows.values() if f['net_flow'] > 0 and any(r['is_material'] for r in f['inflows']))
    material_out = sum(1 for f in flows.values() if f['net_flow'] < 0 and any(r['is_material'] for r in f['outflows']))
    collective_buys = len(collective.get('buys', []))
    collective_sells = len(collective.get('sells', []))
    total_flow = round(sum(abs(f['net_flow']) for f in flows.values()), 1)
    return f"""<div class="stats">
  <div class="sc"><div class="n">{big_count}</div><div class="l">大資金 ETF（≥100億）</div></div>
  <div class="sc gr"><div class="n">{material_in}</div><div class="l">重大資金流入個股</div></div>
  <div class="sc rd"><div class="n">{material_out}</div><div class="l">重大資金流出個股</div></div>
  <div class="sc am"><div class="n">{collective_buys}/{collective_sells}</div><div class="l">集體買/賣訊號</div></div>
  <div class="sc"><div class="n">{total_flow:.0f}億</div><div class="l">今日總資金流動</div></div>
</div>"""


def _gen_flow_overview(flows, big_etf_codes):
    """Tab 1: Unified capital flow overview per stock, sorted by |net_flow|."""
    if not flows:
        return '<div class="empty-msg">系統正在建立基準資料。明日起將開始顯示當日相對於前日的資金流向動作</div>'

    # Filter: only show stocks with at least one material action OR net_flow >= threshold
    filtered = [
        f for f in flows.values()
        if f['material_count'] > 0 or abs(f['net_flow']) >= CAPITAL_FLOW_THRESHOLD
    ]
    if not filtered:
        return '<div class="empty-msg">今日無重大資金流向動作（門檻：單筆 ≥ 1 億 或 權重變化 ≥ 20%）</div>'

    # Sort by absolute net_flow desc
    filtered.sort(key=lambda f: -abs(f['net_flow']))

    big_set = set(big_etf_codes)

    html = '<table><tr><th>股票代號</th><th>股票名稱</th><th class="num">今日淨流入</th><th>資金流入（來源 ETF）</th><th>資金流出（來源 ETF）</th></tr>'
    for f in filtered:
        sc = f['stock_code']
        name = f['stock_name']
        net = f['net_flow']

        # Row color based on net flow direction and magnitude
        row_cls = ''
        if net >= CAPITAL_FLOW_THRESHOLD:
            row_cls = ' class="row-buy"'
        elif net <= -CAPITAL_FLOW_THRESHOLD:
            row_cls = ' class="row-sell"'

        net_str = _fmt_cap_delta(net) if net != 0 else '─'

        def render_badges(records, is_positive):
            badges = ''
            for r in records:
                etf = r['etf']
                delta = r['delta']
                is_big = etf in big_set
                is_new = r.get('is_new_buy', False)
                is_exit = r.get('is_exit', False)
                is_mat = r.get('is_material', False)

                # Capital label
                label_parts = [etf]
                sign = '+' if delta > 0 else ''
                label_parts.append(f'<b>{sign}{_fmt_capital(delta)}</b>')
                if is_new:
                    label_parts.append('<span style="font-size:9.5px">✦首次</span>')
                if is_exit:
                    label_parts.append('<span style="font-size:9.5px">✕撤出</span>')

                # Badge style
                if is_positive:
                    # Green gradient; big ETFs are saturated
                    if is_big and is_mat:
                        style = 'background:#16a34a;color:#fff;border-color:#15803d'
                    elif is_big:
                        style = 'background:#86efac;color:#14532d;border-color:#4ade80'
                    elif is_mat:
                        style = 'background:#bbf7d0;color:#15803d;border-color:#86efac'
                    else:
                        style = 'background:#dcfce7;color:#15803d;border-color:#bbf7d0'
                else:
                    if is_big and is_mat:
                        style = 'background:#dc2626;color:#fff;border-color:#b91c1c'
                    elif is_big:
                        style = 'background:#fca5a5;color:#7f1d1d;border-color:#f87171'
                    elif is_mat:
                        style = 'background:#fecaca;color:#b91c1c;border-color:#fca5a5'
                    else:
                        style = 'background:#fee2e2;color:#dc2626;border-color:#fecaca'

                title = f'{etf}：{sign}{_fmt_capital(delta)}'
                if r.get('prev_weight') is not None:
                    title += f' | 權重 {r.get("prev_weight", 0):.2f}% → {r.get("weight", 0):.2f}%'
                if is_big:
                    title += ' | 大資金 ETF'

                badges += f'<span class="badge etf-tag" style="{style}" title="{title}">{" ".join(label_parts)}</span>'
            return badges or '<span style="color:#94a3b8">─</span>'

        inflow_badges = render_badges(f['inflows'], True)
        outflow_badges = render_badges(f['outflows'], False)

        html += f'<tr{row_cls}><td><b>{sc}</b></td><td>{name}</td><td class="num">{net_str}</td><td>{inflow_badges}</td><td>{outflow_badges}</td></tr>'
    html += '</table>'
    return html


def _gen_big_etf_actions(big_actions, big_etf_list):
    """Tab 2: Per-big-ETF action report."""
    if not big_actions:
        return '<div class="empty-msg">目前無符合大資金 ETF 門檻（AUM ≥ 100 億）的基金</div>'

    html = ''
    for code, aum, name in big_etf_list:
        actions = big_actions.get(code)
        if not actions:
            continue
        aum_delta = actions.get('aum_delta')
        aum_delta_str = ''
        if aum_delta is not None and abs(aum_delta) >= 0.1:
            if aum_delta > 0:
                aum_delta_str = f' <span class="delta-up">▲{_fmt_capital(aum_delta)}</span>'
            else:
                aum_delta_str = f' <span class="delta-down">▼{_fmt_capital(abs(aum_delta))}</span>'

        total_actions = len(actions['buys']) + len(actions['sells']) + len(actions['increases']) + len(actions['decreases'])
        html += f'<div class="etf-section"><h3>{code} {actions["name"]}（AUM {aum:.1f} 億{aum_delta_str}）</h3>'

        if total_actions == 0:
            html += '<div class="empty-msg">今日無異動</div></div>'
            continue

        html += '<table><tr><th>股票代號</th><th>股票名稱</th><th>動作</th><th class="num">資金變化</th><th class="num">權重</th></tr>'

        def fmt_delta_cell(v):
            sign = '+' if v > 0 else ''
            cls = 'delta-up' if v > 0 else 'delta-down'
            return f'<span class="{cls}">{sign}{_fmt_capital(v)}</span>'

        for a in actions['buys']:
            new_mark = ' ✦首次' if a.get('is_new_buy') else ''
            html += f'<tr class="row-buy"><td><b>{a["stock_code"]}</b></td><td>{a["stock_name"]}</td><td><span class="badge buy">新買入{new_mark}</span></td><td class="num">{fmt_delta_cell(a["delta"])}</td><td class="num">{a["weight"]:.2f}%</td></tr>'

        for a in actions['increases']:
            html += f'<tr class="row-up"><td><b>{a["stock_code"]}</b></td><td>{a["stock_name"]}</td><td><span class="badge up">加碼</span></td><td class="num">{fmt_delta_cell(a["delta"])}</td><td class="num">{a["prev_weight"]:.2f}% → {a["weight"]:.2f}%</td></tr>'

        for a in actions['decreases']:
            html += f'<tr class="row-down"><td><b>{a["stock_code"]}</b></td><td>{a["stock_name"]}</td><td><span class="badge down">減碼</span></td><td class="num">{fmt_delta_cell(a["delta"])}</td><td class="num">{a["prev_weight"]:.2f}% → {a["weight"]:.2f}%</td></tr>'

        for a in actions['sells']:
            html += f'<tr class="row-sell"><td><b>{a["stock_code"]}</b></td><td>{a["stock_name"]}</td><td><span class="badge sell">完全賣出</span></td><td class="num">{fmt_delta_cell(a["delta"])}</td><td class="num">{a.get("prev_weight", 0):.2f}% → ─</td></tr>'

        html += '</table></div>'
    return html or '<div class="empty-msg">今日大資金 ETF 均無異動</div>'


def _gen_collective_moves(collective):
    """Tab 3: Collective buy/sell signals by ≥2 big ETFs."""
    html = ''

    html += '<div class="etf-section"><h3 style="color:#15803d">🟢 集體買入訊號（≥2 檔大 ETF 同時買入/加碼）</h3>'
    if not collective['buys']:
        html += '<div class="empty-msg">今日無集體買入訊號</div>'
    else:
        html += '<table><tr><th>股票代號</th><th>股票名稱</th><th class="center">ETF數</th><th class="num">總流入</th><th>ETF 動作</th></tr>'
        for item in collective['buys']:
            actions_html = ''
            for a in item['actions']:
                act_color = '#16a34a' if a['action_type'] == '新買入' else '#059669'
                new_mark = ' ✦' if a.get('is_new_buy') else ''
                actions_html += f'<span class="badge etf-tag" style="background:{act_color};color:#fff;border-color:{act_color}" title="{a["etf_name"]}">{a["etf"]} {a["action_type"]}{new_mark} +{_fmt_capital(a["delta"])}</span>'
            html += f'<tr class="row-buy"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td class="center"><b>{item["etf_count"]}</b></td><td class="num"><span class="delta-up">+{_fmt_capital(item["total_capital"])}</span></td><td>{actions_html}</td></tr>'
        html += '</table>'
    html += '</div>'

    html += '<div class="etf-section"><h3 style="color:#b91c1c">🔴 集體賣出訊號（≥2 檔大 ETF 同時賣出/減碼）</h3>'
    if not collective['sells']:
        html += '<div class="empty-msg">今日無集體賣出訊號</div>'
    else:
        html += '<table><tr><th>股票代號</th><th>股票名稱</th><th class="center">ETF數</th><th class="num">總流出</th><th>ETF 動作</th></tr>'
        for item in collective['sells']:
            actions_html = ''
            for a in item['actions']:
                act_color = '#dc2626' if a['action_type'] == '完全賣出' else '#ea580c'
                actions_html += f'<span class="badge etf-tag" style="background:{act_color};color:#fff;border-color:{act_color}" title="{a["etf_name"]}">{a["etf"]} {a["action_type"]} {_fmt_capital(a["delta"])}</span>'
            html += f'<tr class="row-sell"><td><b>{item["stock_code"]}</b></td><td>{item["stock_name"]}</td><td class="center"><b>{item["etf_count"]}</b></td><td class="num"><span class="delta-down">{_fmt_capital(item["total_capital"])}</span></td><td>{actions_html}</td></tr>'
        html += '</table>'
    html += '</div>'

    return html


def generate_etf_html(today_data, flows, big_actions, collective, consensus, big_etf_list, first_seen):
    big_etf_codes = [code for code, _, _ in big_etf_list]
    stats = _gen_flow_stats(flows, big_etf_list, collective)
    tab_flow = _gen_flow_overview(flows, big_etf_codes)
    tab_big = _gen_big_etf_actions(big_actions, big_etf_list)
    tab_collective = _gen_collective_moves(collective)
    tab_consensus = _gen_consensus(consensus)
    tab_holdings = _gen_individual_holdings(today_data)

    return f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>主動式 ETF 資金流向監測</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>主動式 ETF 資金流向監測</h1>
  <div class="sub">更新：{TODAY.isoformat()}（每個交易日收盤後自動更新）<a class="nav-link" href="index.html">→ 可轉債儀表板</a></div>
</div>
{stats}
<div class="tabs">
  <div class="tab active" onclick="showTab('t1',this)">資金流向</div>
  <div class="tab" onclick="showTab('t2',this)">大 ETF 動向</div>
  <div class="tab" onclick="showTab('t3',this)">集體行為</div>
  <div class="tab" onclick="showTab('t4',this)">持倉總覽</div>
  <div class="tab" onclick="showTab('t5',this)">各 ETF 持股</div>
</div>
<div id="t1" class="pane active">
  <div class="ttl">今日資金流向</div>
  <div class="desc">按股票聚合當日所有 ETF 的資金動作。門檻：單筆流入/流出 ≥ {CAPITAL_FLOW_THRESHOLD:.0f} 億 或 權重變化 ≥ {int(WEIGHT_RATIO_THRESHOLD*100)}%。<b>深色徽章 = 大資金 ETF（AUM ≥ {BIG_ETF_AUM_THRESHOLD} 億）+ 重大異動</b>。「✦首次」= 該 ETF 首次持有此股票（近 {NEW_BUY_WINDOW_DAYS} 天內），「✕撤出」= 昨日持有今日完全賣出</div>
  {tab_flow}
</div>
<div id="t2" class="pane">
  <div class="ttl">大資金 ETF 當日動向（AUM ≥ {BIG_ETF_AUM_THRESHOLD} 億）</div>
  <div class="desc">每檔大資金 ETF 的今日買入、賣出、加碼、減碼明細。AUM 變化反映該基金的淨申贖流量</div>
  {tab_big}
</div>
<div id="t3" class="pane">
  <div class="ttl">機構集體行為</div>
  <div class="desc">≥ 2 檔大資金 ETF 今日同時做出相同動作的股票。多家機構同步動作是最強烈的共識訊號</div>
  {tab_collective}
</div>
<div id="t4" class="pane">
  <div class="ttl">跨 ETF 持倉總覽</div>
  <div class="desc">當下靜態快照：哪些個股被多檔 ETF 持有、市場影響力多少。徽章依資金量排序</div>
  {tab_consensus}
</div>
<div id="t5" class="pane">
  <div class="ttl">各 ETF 完整持股明細</div>
  <div class="desc">選擇 ETF 查看完整持股清單</div>
  {tab_holdings}
</div>
<div class="ft">資料來源：MoneyDJ（持股）、Yahoo Finance（基金規模）｜ 僅供研究參考，不構成投資建議 ｜ 主動式 ETF 資金流向監測系統</div>
<script>{JS}</script>
</body></html>"""


# ── Section 7: Main ──────────────────────────────────────────────────────────

def main():
    config = load_config()
    etf_list = config['etfs']
    print(f"=== 主動式 ETF 資金流向監測 ({TODAY.isoformat()}) ===")
    print(f"追蹤 {len(etf_list)} 檔 ETF\n")

    # Load previous state
    prev_data = load_latest_cache()
    first_seen, first_seen_existed = load_first_seen()
    bootstrap = not first_seen_existed  # first-ever run: don't flag anything as "new"
    if bootstrap:
        print("⚠ 首次執行：正在建立基準資料（first_seen registry）")

    # Fetch today
    today_data = fetch_all_etf_holdings(etf_list)

    # Persist
    save_daily_snapshot(today_data)
    save_latest_cache(today_data)
    cleanup_old_snapshots()

    # Update historical first-seen registry
    first_seen = update_first_seen(today_data, first_seen)
    save_first_seen(first_seen)

    # Health
    health = build_health_report(today_data, prev_data)

    # Core flow engine
    flows = compute_capital_flows(today_data, prev_data, first_seen, bootstrap=bootstrap)

    # Big ETFs and their actions
    big_etf_list = filter_big_etfs(today_data, BIG_ETF_AUM_THRESHOLD)
    big_etf_codes = [code for code, _, _ in big_etf_list]
    print(f"大資金 ETF（AUM ≥ {BIG_ETF_AUM_THRESHOLD} 億）: {len(big_etf_list)} 檔")
    for code, aum, name in big_etf_list:
        print(f"  {code} {name}: {aum:.1f} 億")

    big_actions = compute_big_etf_actions(today_data, prev_data, big_etf_codes, first_seen, bootstrap=bootstrap)
    collective = compute_collective_moves(big_actions)

    # Static consensus view (tab 4)
    consensus = compute_cross_etf_consensus(today_data, prev_data, first_seen, bootstrap=bootstrap)

    # Render
    html = generate_etf_html(today_data, flows, big_actions, collective, consensus, big_etf_list, first_seen)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\nDashboard written to {OUTPUT_HTML}")

    # Summary
    material_flows = sum(1 for f in flows.values() if f['material_count'] > 0)
    print(f"\n資金流向統計:")
    print(f"  有動作的股票: {len(flows)}")
    print(f"  重大異動股票: {material_flows}")
    print(f"  集體買入訊號: {len(collective['buys'])}")
    print(f"  集體賣出訊號: {len(collective['sells'])}")

    ok_count = sum(1 for h in health.values() if h['status'] in ('ok', 'partial'))
    err_count = sum(1 for h in health.values() if h['status'] == 'error')
    stale_count = sum(1 for h in health.values() if h['status'] == 'stale')
    print(f"\nStatus: {ok_count} ok, {stale_count} stale, {err_count} error")

    if err_count == len(etf_list):
        print("::error::All ETF fetches failed!", file=sys.stderr)
        sys.exit(1)
    elif err_count + stale_count > len(etf_list) // 2:
        print(f"::warning::ETF fetch issues: {err_count} errors, {stale_count} stale", file=sys.stderr)


if __name__ == '__main__':
    main()
