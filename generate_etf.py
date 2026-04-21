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
CAPITAL_FLOW_THRESHOLD = 1.0        # 億 TWD — 重大資金流入/出的絕對門檻（舊邏輯）
WEIGHT_RATIO_THRESHOLD = 0.20       # 20% — 權重相對變化門檻
NEW_BUY_WINDOW_DAYS = 7             # 首見後多少天仍顯示「✦新」
MATERIAL_RATIO_OF_AUM = 0.03        # 3% — 動作量體 ≥ 該基金 AUM × 3% 視為重大
TRADING_DAYS_LOOKBACK = 5           # 「最近一週」= 5 個交易日
MINOR_WEIGHT_THRESHOLD = 0.8        # 權重 < 0.8% 視為小持倉（預設收起）


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


def fetch_one_etf(etf):
    """Fetch a single ETF's holdings + AUM. Returns a data-dict entry."""
    code = etf['code']
    name = etf['name']
    print(f"Fetching {code} {name}...")
    holdings, method = fetch_holdings(code)
    if holdings:
        has_na = any(h.get('weight_na') for h in holdings)
        aum = fetch_aum_yahoo(code)
        entry = {
            "name": name,
            "issuer": etf.get('issuer', ''),
            "status": "partial" if has_na else "ok",
            "method": method,
            "holdings_count": len(holdings),
            "aum_billion": aum,
            "holdings": holdings,
        }
        label = f"(partial, weights N/A)" if has_na else ""
        aum_label = f"AUM {aum:.1f}億" if aum else "AUM n/a"
        print(f"  ✓ {code}: {len(holdings)} holdings via {method} {label} | {aum_label}")
        return entry
    print(f"  ✗ {code}: fetch failed")
    return {
        "name": name,
        "issuer": etf.get('issuer', ''),
        "status": "error",
        "method": "failed",
        "error_msg": "All fetch methods failed",
        "holdings_count": 0,
        "holdings": [],
    }


def fetch_all_etf_holdings(etf_list):
    """Fetch holdings for all ETFs, return unified data dict."""
    data = {}
    for etf in etf_list:
        data[etf['code']] = fetch_one_etf(etf)
        time.sleep(FETCH_DELAY)
    return data


def retry_failed_etfs(data, etf_list, max_rounds=2, backoff_seconds=10):
    """Retry any ETFs with status='error'. Mutates and returns data.
    Runs up to `max_rounds` extra passes with exponential-ish backoff."""
    etfs_by_code = {e['code']: e for e in etf_list}
    for round_idx in range(1, max_rounds + 1):
        failed = [c for c, e in data.items() if e.get('status') == 'error']
        if not failed:
            break
        wait = backoff_seconds * round_idx
        print(f"\n🔄 Retry round {round_idx}: {len(failed)} failed ETFs — waiting {wait}s")
        time.sleep(wait)
        for code in failed:
            etf = etfs_by_code.get(code)
            if not etf:
                continue
            data[code] = fetch_one_etf(etf)
            time.sleep(FETCH_DELAY)
    final_failed = [c for c, e in data.items() if e.get('status') == 'error']
    if final_failed:
        print(f"\n⚠️  After retries, still failed: {final_failed}")
    return data


def snapshot_is_complete(snap, etf_list, required_ok_ratio=0.9):
    """Return True if an existing snapshot already has good coverage — we can
    skip re-fetching to avoid hammering upstream when a later cron window fires."""
    if not snap or not snap.get('etfs'):
        return False
    etfs = snap['etfs']
    total = len(etf_list)
    if total == 0:
        return False
    ok_or_partial = sum(
        1 for e in etf_list
        if etfs.get(e['code'], {}).get('status') in ('ok', 'partial')
    )
    return ok_or_partial >= total * required_ok_ratio


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


def trading_days_back(d, n):
    """Return date that is N trading days (Mon–Fri) before d."""
    out = d
    count = 0
    while count < n:
        out = out - timedelta(days=1)
        if out.weekday() < 5:
            count += 1
    return out


def find_snapshot_n_trading_days_back(n):
    """Find the closest snapshot at or before T-n trading days. Returns (data, date_str) or (None, None).
    First tries exact T-n; if missing, walks backwards up to 5 extra calendar days."""
    target = trading_days_back(TODAY, n)
    # Try exact target then walk back a few days for resilience
    for offset in range(0, 8):
        d = target - timedelta(days=offset)
        snap = load_snapshot(d.isoformat())
        if snap:
            return snap, d.isoformat()
    return None, None


def find_oldest_available_snapshot():
    """Return the oldest daily snapshot in etf_data/. Returns (data, date_str) or (None, None)."""
    try:
        files = sorted(f for f in os.listdir(DATA_DIR) if re.match(r'^\d{4}-\d{2}-\d{2}\.json$', f))
    except FileNotFoundError:
        return None, None
    for fname in files:
        date_str = fname[:-5]  # strip .json
        snap = load_snapshot(date_str)
        if snap:
            return snap, date_str
    return None, None


def count_trading_days_between(d1_str, d2):
    """Count trading weekdays strictly after d1_str up to and including d2.
    d1_str: ISO date string, d2: date object."""
    d1 = datetime.strptime(d1_str, '%Y-%m-%d').date()
    count = 0
    cur = d1 + timedelta(days=1)
    while cur <= d2:
        if cur.weekday() < 5:
            count += 1
        cur += timedelta(days=1)
    return count


def is_tw_only_ok(etf_entry):
    """ETF qualifies for the universe if status=ok and ALL holdings are TW stocks (numeric codes)."""
    if etf_entry.get('status') != 'ok':
        return False
    holdings = etf_entry.get('holdings', [])
    if not holdings:
        return False
    return all(h.get('stock_code', '').isdigit() for h in holdings)


def filter_universe(today_data):
    """Return subset of today_data containing only TW-only ok ETFs."""
    return {code: e for code, e in today_data.items() if is_tw_only_ok(e)}


def filter_universe_snapshot(snapshot, universe_codes):
    """Subset a historical snapshot to a given set of ETF codes (preserves shape)."""
    if not snapshot:
        return None
    etfs = snapshot.get('etfs', {})
    return {
        'fetched_at': snapshot.get('fetched_at'),
        'etfs': {c: etfs[c] for c in universe_codes if c in etfs}
    }


def load_first_seen():
    """Load first-seen record. Returns (data_dict, baseline_date_str).
    baseline_date is the date the registry was first built — anything recorded
    on or before that date is treated as 'pre-existing' (NOT new).
    On first-ever run, baseline_date = TODAY so nothing today is flagged as new."""
    if not os.path.exists(FIRST_SEEN_FILE):
        return {}, TODAY.isoformat()
    with open(FIRST_SEEN_FILE, 'r', encoding='utf-8') as f:
        raw = json.load(f)
    baseline = raw.pop('_baseline_date', None)
    if not baseline:
        # Migration: old format without baseline. Treat all existing records as
        # pre-existing by setting baseline to today — only future additions count as new.
        baseline = TODAY.isoformat()
    return raw, baseline


def save_first_seen(first_seen, baseline_date):
    payload = {'_baseline_date': baseline_date, **first_seen}
    with open(FIRST_SEEN_FILE, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)


def update_first_seen(today_data, first_seen):
    """Record today's date for any (stock, etf) pair we haven't seen before."""
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


def is_recent_first_buy(first_seen, stock_code, etf_code, baseline_date,
                        window_days=NEW_BUY_WINDOW_DAYS):
    """Truly-new = first_seen date is strictly AFTER the baseline date AND within window.
    Anything recorded on or before baseline is treated as pre-existing."""
    record = first_seen.get(stock_code, {}).get(etf_code)
    if not record or not baseline_date:
        return False
    try:
        first_date = datetime.strptime(record, '%Y-%m-%d').date()
        baseline = datetime.strptime(baseline_date, '%Y-%m-%d').date()
    except ValueError:
        return False
    if first_date <= baseline:
        return False  # was already known when registry was created
    return (TODAY - first_date).days <= window_days


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


def compute_cross_etf_consensus(today_data, prev_data=None, first_seen=None, baseline_date=None):
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
            if first_seen and baseline_date:
                newly_added = sorted(
                    etf for etf in today_etfs
                    if is_recent_first_buy(first_seen, sc, etf, baseline_date)
                )
            else:
                newly_added = []
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


def compute_capital_flows(today_data, prev_data, first_seen, baseline_date):
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
            prev_was_na = prev_h is not None and prev_h.get('weight_na', False)
            if prev_h and not prev_was_na:
                prev_etf_for_cap = {
                    'aum_billion': prev_etf.get('aum_billion'),
                }
                prev_cap = _capital_of(prev_etf_for_cap, prev_h)
            else:
                prev_cap = None

            # Delta: today - yesterday. If not held yesterday → full delta = today_cap.
            # BUT: if prev had this stock with weight_na (data quality upgrade), skip — not a real action.
            if prev_was_na:
                continue  # data quality changed, not a real flow event
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

            is_new = is_recent_first_buy(first_seen, sc, etf_code, baseline_date)
            etf_aum = etf.get('aum_billion') or 0
            material_capital_threshold = etf_aum * MATERIAL_RATIO_OF_AUM
            is_material = (
                (etf_aum > 0 and abs(delta) >= material_capital_threshold)
                or (abs(weight_ratio) >= WEIGHT_RATIO_THRESHOLD)
            )

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
            if prev_h.get('weight_na'):
                continue  # prev was partial-data, can't reliably say it exited
            prev_cap = _capital_of(prev_etf_for_cap, prev_h)
            if prev_cap is None:
                continue
            delta = round(-prev_cap, 2)
            if abs(delta) < 0.005:
                continue
            today_aum = today_etf.get('aum_billion') or 0
            is_material = today_aum > 0 and abs(delta) >= today_aum * MATERIAL_RATIO_OF_AUM
            record = {
                "etf": etf_code,
                "etf_name": today_etf.get('name'),
                "etf_aum": today_aum,
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


def compute_big_etf_actions(today_data, prev_data, big_etf_codes, first_seen, baseline_date):
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
            prev_was_na = prev_h is not None and prev_h.get('weight_na', False)
            if prev_was_na:
                continue  # data quality changed (partial → ok), not a real action
            if not prev_h:
                # Not held yesterday → buy
                buys.append({
                    "stock_code": sc,
                    "stock_name": h['stock_name'],
                    "today_capital": today_cap,
                    "delta": today_cap,
                    "weight": h.get('weight_pct') or 0,
                    "is_new_buy": is_recent_first_buy(first_seen, sc, code, baseline_date),
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
            if prev_h.get('weight_na'):
                continue  # prev was partial-data, can't reliably say it exited
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


# ── Section 4c: Stock & Fund views ───────────────────────────────────────────

def build_stock_view(today_data, flows_1d, flows_5d):
    """Per-stock aggregated view combining 1-day and 5-day flow data.

    Returns a list of stock entries sorted by combined absolute flow:
    [{
      stock_code, stock_name,
      holders: [{etf, etf_aum, capital, weight, flow_1d, flow_5d, is_first_buy}],
      holder_count, total_capital,
      net_flow_1d, net_flow_5d,
      has_first_buy_5d,
      material_count_1d, material_count_5d
    }]
    """
    stock_map = {}

    # Build today's holdings index
    for etf_code, etf in today_data.items():
        if etf.get('status') != 'ok':
            continue
        aum = etf.get('aum_billion')
        for h in etf.get('holdings', []):
            sc = h['stock_code']
            cap = _capital_of(etf, h)
            if cap is None:
                continue
            stock_map.setdefault(sc, {
                'stock_code': sc,
                'stock_name': h['stock_name'],
                'holders': [],
                'flows_1d_by_etf': {},
                'flows_5d_by_etf': {},
            })
            stock_map[sc]['holders'].append({
                'etf': etf_code,
                'etf_aum': aum,
                'capital': cap,
                'weight': h.get('weight_pct') or 0,
            })

    # Layer in 1-day flows
    for sc, flow in (flows_1d or {}).items():
        entry = stock_map.setdefault(sc, {
            'stock_code': sc,
            'stock_name': flow['stock_name'],
            'holders': [],
            'flows_1d_by_etf': {},
            'flows_5d_by_etf': {},
        })
        for r in flow['inflows'] + flow['outflows']:
            entry['flows_1d_by_etf'][r['etf']] = r

    # Layer in 5-day flows
    for sc, flow in (flows_5d or {}).items():
        entry = stock_map.setdefault(sc, {
            'stock_code': sc,
            'stock_name': flow['stock_name'],
            'holders': [],
            'flows_1d_by_etf': {},
            'flows_5d_by_etf': {},
        })
        for r in flow['inflows'] + flow['outflows']:
            entry['flows_5d_by_etf'][r['etf']] = r

    # Compose output
    result = []
    for sc, info in stock_map.items():
        holders = info['holders']
        # Annotate each holder with its 1d/5d flow and first-buy flags
        annotated = []
        any_first_5d = False
        material_1d = 0
        material_5d = 0
        for h in holders:
            etf = h['etf']
            f1 = info['flows_1d_by_etf'].get(etf)
            f5 = info['flows_5d_by_etf'].get(etf)
            holder = {
                'etf': etf,
                'etf_aum': h['etf_aum'],
                'capital': h['capital'],
                'weight': h['weight'],
                'flow_1d': f1['delta'] if f1 else 0,
                'flow_5d': f5['delta'] if f5 else 0,
                'is_material_1d': bool(f1 and f1.get('is_material')),
                'is_material_5d': bool(f5 and f5.get('is_material')),
                'is_first_buy': bool(f5 and f5.get('is_new_buy')) or bool(f1 and f1.get('is_new_buy')),
            }
            annotated.append(holder)
            if holder['is_first_buy']:
                any_first_5d = True
            if holder['is_material_1d']:
                material_1d += 1
            if holder['is_material_5d']:
                material_5d += 1
        # Also pick up exit-only ETFs (in flow data but not in current holders)
        held_etfs = {h['etf'] for h in holders}
        for etf, r in info['flows_1d_by_etf'].items():
            if etf in held_etfs:
                continue
            if not r.get('is_exit'):
                continue
            annotated.append({
                'etf': etf,
                'etf_aum': r.get('etf_aum'),
                'capital': 0,
                'weight': 0,
                'flow_1d': r['delta'],
                'flow_5d': info['flows_5d_by_etf'].get(etf, {}).get('delta', r['delta']),
                'is_material_1d': r.get('is_material', False),
                'is_material_5d': info['flows_5d_by_etf'].get(etf, {}).get('is_material', False),
                'is_first_buy': False,
                'is_exit': True,
            })
            if r.get('is_material'):
                material_1d += 1
        annotated.sort(key=lambda x: -x['capital'])

        net_1d = round(sum(h['flow_1d'] for h in annotated), 2)
        net_5d = round(sum(h['flow_5d'] for h in annotated), 2)
        total_cap = round(sum(h['capital'] for h in annotated if h['capital']), 2)

        result.append({
            'stock_code': sc,
            'stock_name': info['stock_name'],
            'holders': annotated,
            'holder_count': sum(1 for h in annotated if h['capital'] > 0),
            'total_capital': total_cap,
            'net_flow_1d': net_1d,
            'net_flow_5d': net_5d,
            'has_first_buy_5d': any_first_5d,
            'material_count_1d': material_1d,
            'material_count_5d': material_5d,
        })

    # Sort by combined absolute flow + total capital tiebreaker
    result.sort(key=lambda r: (-(abs(r['net_flow_1d']) + abs(r['net_flow_5d'])), -r['total_capital']))
    return result


def _compute_fund_actions(etf, etf_code, prev_snap_etf, threshold, first_seen, baseline_date):
    """Compute material buys/sells/first-buys for one ETF against one baseline snapshot.
    Returns (material_buys, material_sells, first_buys) — sorted."""
    material_buys, material_sells, first_buys = [], [], []
    if not prev_snap_etf:
        return material_buys, material_sells, first_buys

    prev_holdings = prev_snap_etf.get('holdings', {})
    today_holdings = {h['stock_code']: h for h in etf.get('holdings', [])}

    for sc, h in today_holdings.items():
        today_cap = _capital_of(etf, h)
        if today_cap is None:
            continue
        ph = prev_holdings.get(sc)
        prev_was_na = ph is not None and ph.get('weight_na', False)
        if prev_was_na:
            continue
        prev_cap = None
        if ph:
            prev_cap = _capital_of({'aum_billion': prev_snap_etf.get('aum_billion')}, ph)
        if prev_cap is None:
            delta = today_cap
            is_new = is_recent_first_buy(first_seen, sc, etf_code, baseline_date)
        else:
            delta = round(today_cap - prev_cap, 2)
            is_new = False
        if abs(delta) < 0.005:
            continue
        row = {
            'stock_code': sc,
            'stock_name': h['stock_name'],
            'delta': delta,
            'weight': h.get('weight_pct') or 0,
            'prev_weight': ph.get('weight_pct') if ph else None,
            'is_new_buy': is_new,
        }
        if is_new:
            first_buys.append(row)
        if abs(delta) >= threshold and threshold > 0:
            if delta > 0:
                material_buys.append(row)
            else:
                material_sells.append(row)

    # Exits
    for sc, ph in prev_holdings.items():
        if sc in today_holdings:
            continue
        if ph.get('weight_na'):
            continue
        prev_cap = _capital_of({'aum_billion': prev_snap_etf.get('aum_billion')}, ph)
        if prev_cap is None:
            continue
        delta = round(-prev_cap, 2)
        if abs(delta) >= threshold and threshold > 0:
            material_sells.append({
                'stock_code': sc,
                'stock_name': ph.get('stock_name', sc),
                'delta': delta,
                'weight': 0,
                'prev_weight': ph.get('weight_pct'),
                'is_exit': True,
            })

    material_buys.sort(key=lambda r: -r['delta'])
    material_sells.sort(key=lambda r: r['delta'])
    first_buys.sort(key=lambda r: -r['delta'])
    return material_buys, material_sells, first_buys


def build_fund_view(today_data, snap_1d, snap_5d, first_seen, baseline_date):
    """Per-fund summary: AUM, day/week deltas, recent material moves and first-time buys.

    Computes actions against BOTH 1d and 5d baselines so the UI can fall back to
    1d data while the 5d window is still accumulating.
    """
    snap_1d_idx = _index_prev_etf(snap_1d) if snap_1d else {}
    snap_5d_idx = _index_prev_etf(snap_5d) if snap_5d else {}

    result = {}
    for etf_code, etf in today_data.items():
        if etf.get('status') != 'ok':
            continue
        aum = etf.get('aum_billion')
        threshold = (aum or 0) * MATERIAL_RATIO_OF_AUM

        prev_1d = snap_1d_idx.get(etf_code)
        prev_5d = snap_5d_idx.get(etf_code)

        aum_d1 = round(aum - prev_1d['aum_billion'], 2) if (aum and prev_1d and prev_1d.get('aum_billion')) else None
        aum_d5 = round(aum - prev_5d['aum_billion'], 2) if (aum and prev_5d and prev_5d.get('aum_billion')) else None

        buys_1d, sells_1d, firsts_1d = _compute_fund_actions(etf, etf_code, prev_1d, threshold, first_seen, baseline_date)
        buys_5d, sells_5d, firsts_5d = _compute_fund_actions(etf, etf_code, prev_5d, threshold, first_seen, baseline_date)

        result[etf_code] = {
            'name': etf.get('name'),
            'aum': aum,
            'aum_delta_1d': aum_d1,
            'aum_delta_5d': aum_d5,
            'threshold_billion': round(threshold, 2),
            'material_buys_1d': buys_1d,
            'material_sells_1d': sells_1d,
            'first_buys_1d': firsts_1d,
            'action_count_1d': len(buys_1d) + len(sells_1d) + len(firsts_1d),
            'material_buys_5d': buys_5d,
            'material_sells_5d': sells_5d,
            'first_buys_5d': firsts_5d,
            'action_count_5d': len(buys_5d) + len(sells_5d) + len(firsts_5d),
        }
    return result


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
.legend{background:#f8fafc;border:1px solid var(--brd);border-radius:8px;padding:12px 16px;margin-bottom:16px;display:flex;flex-wrap:wrap;align-items:center;gap:6px 12px;font-size:12px;color:var(--mu);line-height:1.8}
.legend .lg-title{font-weight:700;color:var(--txt);margin-right:4px}
.legend .lg-text{font-size:11.5px;color:var(--mu);margin-right:8px}
.toggle-minor{cursor:pointer;font-size:10.5px;font-weight:600;padding:3px 8px;background:#f1f5f9;color:#64748b;border:1px solid var(--brd);border-radius:6px;margin:1px 2px;transition:all .15s}
.toggle-minor:hover{background:#e2e8f0;color:var(--txt)}
.minor-holders.expanded{display:inline !important}
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
th.sortable-th{cursor:pointer;user-select:none;transition:background .12s}
th.sortable-th:hover{background:#dbeafe;color:var(--bl)}
th.sortable-th .sort-hint{display:inline-block;margin-left:4px;font-size:10px;opacity:0.4}
th.sortable-th.sort-asc,th.sortable-th.sort-desc{background:#dbeafe;color:var(--bl)}
th.sortable-th.sort-asc .sort-hint,th.sortable-th.sort-desc .sort-hint{opacity:1;color:var(--bl)}
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
function toggleMinor(btn){
  var row = btn.closest('tr');
  var minor = row.querySelector('.minor-holders');
  var count = btn.dataset.count;
  if(minor.classList.contains('expanded')){
    minor.classList.remove('expanded');
    btn.innerHTML = '+' + count + ' 檔小持倉（權重&lt;0.8%）';
  } else {
    minor.classList.add('expanded');
    btn.innerHTML = '收合小持倉';
  }
}
function sortTable(th,colIdx,type){
  var table = th.closest('table');
  var rows = Array.from(table.querySelectorAll('tr')).slice(1);
  var isDesc = !th.classList.contains('sort-desc');
  table.querySelectorAll('th.sortable-th').forEach(function(h){
    h.classList.remove('sort-asc','sort-desc');
    var hint = h.querySelector('.sort-hint');
    if(hint) hint.textContent = '⇅';
  });
  th.classList.add(isDesc ? 'sort-desc' : 'sort-asc');
  var hint = th.querySelector('.sort-hint');
  if(hint) hint.textContent = isDesc ? '▼' : '▲';
  rows.sort(function(a,b){
    var av = a.cells[colIdx] ? a.cells[colIdx].dataset.sort : '';
    var bv = b.cells[colIdx] ? b.cells[colIdx].dataset.sort : '';
    if(type === 'num'){
      av = parseFloat(av) || 0;
      bv = parseFloat(bv) || 0;
      return isDesc ? bv - av : av - bv;
    }
    return isDesc ? String(bv).localeCompare(String(av)) : String(av).localeCompare(String(bv));
  });
  var parent = rows[0] ? rows[0].parentNode : null;
  if(parent) rows.forEach(function(r){ parent.appendChild(r); });
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


def _gen_v3_stats(today_data, stock_view, collective, snap_1d_date, snap_5d_date, snap_5d_label='5 日'):
    """Stats for the v3 layout: universe size, AUM, 1d/5d total flows, signals."""
    total_etf = len(today_data)
    total_aum = round(sum(e.get('aum_billion', 0) or 0 for e in today_data.values()), 0)
    flow_1d = round(sum(abs(s['net_flow_1d']) for s in stock_view), 1)
    flow_5d = round(sum(abs(s['net_flow_5d']) for s in stock_view), 1)
    collective_n = len(collective.get('buys', [])) + len(collective.get('sells', []))
    flow_5d_val = f'{flow_5d:.0f}億' if snap_5d_date else '累積中'
    return f"""<div class="stats">
  <div class="sc"><div class="n">{int(total_aum):,}億</div><div class="l">追蹤主動式 ETF 總規模</div></div>
  <div class="sc"><div class="n">{total_etf}</div><div class="l">追蹤 ETF 檔數（純台股）</div></div>
  <div class="sc gr"><div class="n">{flow_1d:.0f}億</div><div class="l">1 日總資金流動</div></div>
  <div class="sc am"><div class="n">{flow_5d_val}</div><div class="l">{snap_5d_label}總資金流動</div></div>
  <div class="sc rd"><div class="n">{collective_n}</div><div class="l">集體訊號數</div></div>
</div>"""


def _gen_stock_view(stock_view, today_data, snap_5d_date, baseline_date, snap_5d_label='5 日'):
    """Render stock-perspective table."""
    if not stock_view:
        return '<div class="empty-msg">無資料</div>'

    # Filter: only show stocks that actually have movement OR are widely held
    filtered = [s for s in stock_view if abs(s['net_flow_1d']) >= 0.005 or abs(s['net_flow_5d']) >= 0.005 or s['holder_count'] >= 3]
    if not filtered:
        return '<div class="empty-msg">尚無資金流向資料（首次執行或無歷史快照可比較）</div>'

    html = '<table class="sortable"><tr>'
    html += '<th>股票代號</th>'
    html += '<th>股票名稱</th>'
    html += '<th class="center sortable-th" onclick="sortTable(this,2,\'num\')">持有<br/>ETF 數 <span class="sort-hint">⇅</span></th>'
    html += '<th class="num sortable-th" onclick="sortTable(this,3,\'num\')">總投入<br/>(億) <span class="sort-hint">⇅</span></th>'
    html += '<th class="num sortable-th" onclick="sortTable(this,4,\'num\')">1 日<br/>淨流入 <span class="sort-hint">⇅</span></th>'
    if snap_5d_date:
        html += f'<th class="num sortable-th" onclick="sortTable(this,5,\'num\')">{snap_5d_label}<br/>淨流入 <span class="sort-hint">⇅</span></th>'
    else:
        html += f'<th class="num" style="color:#94a3b8">{snap_5d_label}<br/>淨流入</th>'
    html += '<th>持有 ETF（徽章顯示：ETF 代號｜持倉金額｜佔該 ETF 權重｜今日進出）</th></tr>'

    for s in filtered:
        sc = s['stock_code']
        name = s['stock_name']
        net_1d = s['net_flow_1d']
        net_5d = s['net_flow_5d']

        row_cls = ''
        # Highlight if material 1d action OR significant 1d net flow
        if s['material_count_1d'] > 0 and net_1d > 0:
            row_cls = ' class="row-buy"'
        elif s['material_count_1d'] > 0 and net_1d < 0:
            row_cls = ' class="row-sell"'

        first_mark = ' <span class="badge" style="background:#fef3c7;color:#b45309;border:1px solid #fde68a;font-size:9.5px;padding:1px 6px" title="近期新增持股（系統起算日 ' + (baseline_date or 'n/a') + ' 之後才出現在該 ETF 的持股中）">✦ 近期新增</span>' if s['has_first_buy_5d'] else ''

        net_1d_str = _fmt_cap_delta(net_1d) if abs(net_1d) >= 0.005 else '─'
        net_5d_str = _fmt_cap_delta(net_5d) if abs(net_5d) >= 0.005 else '─'

        # Build holder badges, sorted by capital desc, color-coded by 1-day flow direction
        # Split into main vs minor (weight < threshold AND no material/first-buy/exit action)
        def render_badge(h):
            etf = h['etf']
            cap = h['capital']
            f1 = h['flow_1d']
            f5 = h['flow_5d']
            is_first = h['is_first_buy']
            is_exit = h.get('is_exit', False)
            weight = h['weight']
            weight_str = f'<span style="font-size:9.5px;opacity:0.75;font-weight:500">{weight:.1f}%</span>'

            if is_exit:
                style = 'background:#dc2626;color:#fff;border-color:#b91c1c'
                label = f'{etf} ✕撤出 {_fmt_capital(f1)}'
            elif is_first:
                # First-buy: amber palette so it pops against the green/red flow colors
                if h['is_material_1d']:
                    style = 'background:#f59e0b;color:#fff;border-color:#d97706'
                else:
                    style = 'background:#fef3c7;color:#b45309;border-color:#fde68a'
                label = f'{etf} <b>{_fmt_capital(cap)}</b> {weight_str} <span style="font-size:9.5px">✦新增</span>'
            elif h['is_material_1d'] and f1 > 0:
                style = 'background:#16a34a;color:#fff;border-color:#15803d'
                label = f'{etf} <b>{_fmt_capital(cap)}</b> {weight_str} <span style="font-size:9.5px">▲{_fmt_capital(abs(f1))}</span>'
            elif h['is_material_1d'] and f1 < 0:
                style = 'background:#dc2626;color:#fff;border-color:#b91c1c'
                label = f'{etf} <b>{_fmt_capital(cap)}</b> {weight_str} <span style="font-size:9.5px">▼{_fmt_capital(abs(f1))}</span>'
            elif f1 > 0:
                style = 'background:#dcfce7;color:#15803d;border-color:#bbf7d0'
                label = f'{etf} <b>{_fmt_capital(cap)}</b> {weight_str} <span style="font-size:9.5px">▲{_fmt_capital(abs(f1))}</span>'
            elif f1 < 0:
                style = 'background:#fee2e2;color:#dc2626;border-color:#fecaca'
                label = f'{etf} <b>{_fmt_capital(cap)}</b> {weight_str} <span style="font-size:9.5px">▼{_fmt_capital(abs(f1))}</span>'
            else:
                style = 'background:#ede9fe;color:#6d28d9;border-color:#ddd6fe'
                label = f'{etf} <b>{_fmt_capital(cap)}</b> {weight_str}'

            title = f'{etf}: 持倉 {_fmt_capital(cap)}（佔該 ETF {weight:.2f}%）'
            if abs(f1) >= 0.005:
                title += f' | 1 日 {("+" if f1>0 else "")}{_fmt_capital(f1)}'
            if snap_5d_date and abs(f5) >= 0.005:
                title += f' | {snap_5d_label} {("+" if f5>0 else "")}{_fmt_capital(f5)}'
            if is_first:
                title += f' | 近期新增（系統起算日 {baseline_date or "n/a"} 後首見）'

            return f'<span class="badge etf-tag" style="{style}" title="{title}">{label}</span>'

        def is_minor(h):
            # Always show: material actions, first buys, exits
            if h.get('is_material_1d') or h.get('is_material_5d'):
                return False
            if h.get('is_first_buy'):
                return False
            if h.get('is_exit'):
                return False
            return h['weight'] < MINOR_WEIGHT_THRESHOLD

        main_badges = ''
        minor_badges = ''
        minor_count = 0
        for h in s['holders']:
            if is_minor(h):
                minor_badges += render_badge(h)
                minor_count += 1
            else:
                main_badges += render_badge(h)

        if minor_count > 0:
            toggle_btn = f'<button class="toggle-minor" onclick="toggleMinor(this)" data-count="{minor_count}">+{minor_count} 檔小持倉（權重&lt;{MINOR_WEIGHT_THRESHOLD}%）</button>'
            minor_wrapper = f'<span class="minor-holders" style="display:none">{minor_badges}</span>'
            badges = main_badges + toggle_btn + minor_wrapper
        else:
            badges = main_badges

        cap_total = f'<b>{s["total_capital"]:.1f}</b>' if s['total_capital'] >= 1 else f'{s["total_capital"]*100:.0f}百萬'

        html += f'<tr{row_cls}><td><b>{sc}</b>{first_mark}</td><td>{name}</td>'
        html += f'<td class="center" data-sort="{s["holder_count"]}"><b>{s["holder_count"]}</b></td>'
        html += f'<td class="num" data-sort="{s["total_capital"]}">{cap_total}</td>'
        html += f'<td class="num" data-sort="{net_1d}">{net_1d_str}</td>'
        if snap_5d_date:
            html += f'<td class="num" data-sort="{net_5d}">{net_5d_str}</td>'
        else:
            html += '<td class="num" style="color:#94a3b8;font-size:11px">累積資料中</td>'
        html += f'<td>{badges}</td></tr>'

    html += '</table>'
    return html


def _gen_fund_view(fund_view, today_data, snap_1d_date, snap_5d_date, baseline_date, snap_5d_label='5 日'):
    """Render fund-perspective view: dropdown selector + per-fund cards.
    Falls back to 1-day window (holdings + AUM) when 5-day data is still accumulating."""
    if not fund_view:
        return '<div class="empty-msg">無基金資料</div>'

    use_5d = bool(snap_5d_date)
    window_label = snap_5d_label if use_5d else '1 日'
    suffix = '_5d' if use_5d else '_1d'

    sorted_funds = sorted(fund_view.items(), key=lambda x: -(x[1]['aum'] or 0))

    if use_5d:
        banner = ''
    else:
        banner = f'<div class="warn-box" style="margin:0 0 14px;">{snap_5d_label}比較資料尚在累積（基準：{snap_1d_date or "無"}），目前以 <b>1 日</b> 變化顯示。累積滿 5 個交易日後自動切換。</div>'

    html = banner
    html += '<select class="etf-select" onchange="filterETF(this)"><option value="all">全部 ETF</option>'
    for code, info in sorted_funds:
        html += f'<option value="{code}">{code} {info["name"]} (AUM {info["aum"]:.1f} 億)</option>'
    html += '</select>'

    for code, info in sorted_funds:
        aum = info['aum']
        threshold = info['threshold_billion']

        def fmt_aum_delta(d, label):
            if d is None:
                return f'<span style="color:#94a3b8">{label} 無資料</span>'
            if abs(d) < 0.05:
                return f'<span style="color:#64748b">{label} ─</span>'
            cls = 'delta-up' if d > 0 else 'delta-down'
            arrow = '▲' if d > 0 else '▼'
            return f'<span class="{cls}">{label} {arrow}{_fmt_capital(abs(d))}</span>'

        aum_d1_str = fmt_aum_delta(info['aum_delta_1d'], '1 日')
        aum_d5_str = fmt_aum_delta(info['aum_delta_5d'], snap_5d_label) if use_5d else ''

        html += f'<div class="etf-detail" data-code="{code}"><div class="etf-section">'
        html += f'<h3>{code} {info["name"]}</h3>'
        html += f'<div style="margin-bottom:14px;display:flex;flex-wrap:wrap;gap:12px;align-items:center;font-size:13px">'
        html += f'<span><b>AUM</b>: {aum:.1f} 億</span>'
        html += f'<span style="color:#cbd5e1">|</span>'
        html += f'<span>{aum_d1_str}</span>'
        if use_5d:
            html += f'<span style="color:#cbd5e1">|</span><span>{aum_d5_str}</span>'
        html += f'<span style="color:#cbd5e1">|</span>'
        html += f'<span style="color:#64748b">重大門檻：{_fmt_capital(threshold)}（AUM × 3%）</span>'
        html += '</div>'

        first_buys = info[f'first_buys{suffix}']
        material_buys = info[f'material_buys{suffix}']
        material_sells = info[f'material_sells{suffix}']
        action_count = info[f'action_count{suffix}']

        if first_buys:
            bd = baseline_date or 'n/a'
            html += f'<div style="margin-bottom:12px"><div style="font-weight:700;font-size:12.5px;color:#b45309;margin-bottom:6px">✦ 近期新增持股（系統起算日 {bd} 後才出現）</div>'
            html += '<table><tr><th>股票代號</th><th>股票名稱</th><th class="num">投入金額</th><th class="num">當前權重</th></tr>'
            for r in first_buys:
                html += f'<tr class="row-buy"><td><b>{r["stock_code"]}</b></td><td>{r["stock_name"]}</td><td class="num"><span class="delta-up">+{_fmt_capital(r["delta"])}</span></td><td class="num">{r["weight"]:.2f}%</td></tr>'
            html += '</table></div>'

        if material_buys:
            html += f'<div style="margin-bottom:12px"><div style="font-weight:700;font-size:12.5px;color:#15803d;margin-bottom:6px">▲ {window_label}重大加碼（≥ {_fmt_capital(threshold)}）</div>'
            html += '<table><tr><th>股票代號</th><th>股票名稱</th><th class="num">資金變化</th><th class="num">權重變化</th></tr>'
            for r in material_buys:
                pw = r.get('prev_weight')
                w_str = f'{pw:.2f}% → {r["weight"]:.2f}%' if pw is not None else f'{r["weight"]:.2f}%'
                html += f'<tr class="row-buy"><td><b>{r["stock_code"]}</b></td><td>{r["stock_name"]}</td><td class="num"><span class="delta-up">+{_fmt_capital(r["delta"])}</span></td><td class="num">{w_str}</td></tr>'
            html += '</table></div>'

        if material_sells:
            html += f'<div style="margin-bottom:12px"><div style="font-weight:700;font-size:12.5px;color:#b91c1c;margin-bottom:6px">▼ {window_label}重大減碼（≥ {_fmt_capital(threshold)}）</div>'
            html += '<table><tr><th>股票代號</th><th>股票名稱</th><th class="num">資金變化</th><th class="num">權重變化</th></tr>'
            for r in material_sells:
                pw = r.get('prev_weight')
                if r.get('is_exit'):
                    w_str = f'{pw:.2f}% → ─' if pw is not None else '─'
                else:
                    w_str = f'{pw:.2f}% → {r["weight"]:.2f}%' if pw is not None else f'{r["weight"]:.2f}%'
                exit_mark = ' ✕撤出' if r.get('is_exit') else ''
                html += f'<tr class="row-sell"><td><b>{r["stock_code"]}</b>{exit_mark}</td><td>{r["stock_name"]}</td><td class="num"><span class="delta-down">{_fmt_capital(r["delta"])}</span></td><td class="num">{w_str}</td></tr>'
            html += '</table></div>'

        if action_count == 0:
            html += f'<div class="empty-msg" style="text-align:left;padding:8px 0;color:#64748b">{window_label}內無重大動作（門檻：AUM × 3%）</div>'

        html += '</div></div>'

    return html


def _gen_flow_stats(flows, big_etfs, collective, today_data):
    """[Legacy] Top-level stats: total AUM tracked, big ETF count, material flows, collective signals."""
    big_count = len(big_etfs)
    material_in = sum(1 for f in flows.values() if f['net_flow'] > 0 and any(r['is_material'] for r in f['inflows']))
    material_out = sum(1 for f in flows.values() if f['net_flow'] < 0 and any(r['is_material'] for r in f['outflows']))
    collective_buys = len(collective.get('buys', []))
    collective_sells = len(collective.get('sells', []))
    total_flow = round(sum(abs(f['net_flow']) for f in flows.values()), 1)
    total_aum = round(sum(e.get('aum_billion', 0) or 0 for e in today_data.values()), 0)
    total_etf = len(today_data)
    return f"""<div class="stats">
  <div class="sc"><div class="n">{int(total_aum):,}億</div><div class="l">追蹤主動式 ETF 總規模</div></div>
  <div class="sc"><div class="n">{total_etf}</div><div class="l">追蹤 ETF 檔數</div></div>
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


def _gen_big_etf_actions(big_actions, big_etf_list, today_data):
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

        is_partial = today_data.get(code, {}).get('status') == 'partial'
        partial_note = ' <span style="font-size:11px;color:#3730a3;background:#e0e7ff;padding:2px 7px;border-radius:5px;border:1px solid #c7d2fe;font-weight:600;margin-left:4px">權重未揭露</span>' if is_partial else ''

        total_actions = len(actions['buys']) + len(actions['sells']) + len(actions['increases']) + len(actions['decreases'])
        html += f'<div class="etf-section"><h3>{code} {actions["name"]}（AUM {aum:.1f} 億{aum_delta_str}）{partial_note}</h3>'

        if total_actions == 0:
            if is_partial:
                html += '<div class="empty-msg">此基金僅揭露持股名稱、未揭露權重，無法計算個股資金流動。基金規模變化請參考上方 AUM 數值。</div></div>'
            else:
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
                new_mark = ' ✦新增' if a.get('is_new_buy') else ''
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


def generate_etf_html(today_data_tw, stock_view, fund_view, collective, snap_1d_date, snap_5d_date, baseline_date, snap_5d_label='5 日'):
    stats = _gen_v3_stats(today_data_tw, stock_view, collective, snap_1d_date, snap_5d_date, snap_5d_label)
    tab_stock = _gen_stock_view(stock_view, today_data_tw, snap_5d_date, baseline_date, snap_5d_label)
    tab_fund = _gen_fund_view(fund_view, today_data_tw, snap_1d_date, snap_5d_date, baseline_date, snap_5d_label)
    tab_collective = _gen_collective_moves(collective)
    tab_holdings = _gen_individual_holdings(today_data_tw)

    snap_1d_str = snap_1d_date or '尚無'
    snap_5d_str = snap_5d_date or '累積中'

    return f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>主動式 ETF 資金流向監測</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>主動式 ETF 資金流向監測</h1>
  <div class="sub">更新：{TODAY.isoformat()}｜1 日比較基準：{snap_1d_str}｜{snap_5d_label}比較基準：{snap_5d_str}<a class="nav-link" href="index.html">→ 可轉債儀表板</a></div>
</div>
{stats}
<div class="tabs">
  <div class="tab active" onclick="showTab('t1',this)">股票視角</div>
  <div class="tab" onclick="showTab('t2',this)">基金視角</div>
  <div class="tab" onclick="showTab('t3',this)">集體行為</div>
  <div class="tab" onclick="showTab('t4',this)">各 ETF 持股</div>
</div>
<div id="t1" class="pane active">
  <div class="ttl">股票視角：今天主動式 ETF 買了誰、賣了誰</div>
  <div class="desc">
    <b>一列 = 一檔股票</b>；列出所有持有它的主動式 ETF。<br/>
    徽章格式：<code style="background:#f1f5f9;padding:1px 4px;border-radius:3px">ETF代號　持倉金額　佔 ETF 權重%　▲/▼ 今日進出</code>（<b>權重% = 該持股佔這檔 ETF 規模的比例</b>）。<b>顏色</b>代表今日動作方向與強度。<b>表頭可點擊排序</b>。
  </div>
  <div class="legend">
    <span class="lg-title">今日動作：</span>
    <span class="badge etf-tag" style="background:#16a34a;color:#fff;border-color:#15803d">00XXXA <b>13.0億</b> <span style="font-size:9.5px;opacity:0.75">7.5%</span> <span style="font-size:9.5px">▲5億</span></span><span class="lg-text">大買（今日進 ≥ 該 ETF 規模 × {int(MATERIAL_RATIO_OF_AUM*100)}%）</span>
    <span class="badge etf-tag" style="background:#dcfce7;color:#15803d;border-color:#bbf7d0">00XXXA <b>13.0億</b> <span style="font-size:9.5px;opacity:0.75">7.5%</span> <span style="font-size:9.5px">▲2.5億</span></span><span class="lg-text">小買</span>
    <span class="badge etf-tag" style="background:#ede9fe;color:#6d28d9;border-color:#ddd6fe">00XXXA <b>13.0億</b> <span style="font-size:9.5px;opacity:0.75">7.5%</span></span><span class="lg-text">只是持有，今日沒動</span>
    <span class="badge etf-tag" style="background:#fee2e2;color:#dc2626;border-color:#fecaca">00XXXA <b>13.0億</b> <span style="font-size:9.5px;opacity:0.75">7.5%</span> <span style="font-size:9.5px">▼1.8億</span></span><span class="lg-text">小賣</span>
    <span class="badge etf-tag" style="background:#dc2626;color:#fff;border-color:#b91c1c">00XXXA <b>13.0億</b> <span style="font-size:9.5px;opacity:0.75">7.5%</span> <span style="font-size:9.5px">▼5億</span></span><span class="lg-text">大賣</span>
    <br/>
    <span class="lg-title" style="margin-top:4px">特殊標記：</span>
    <span class="badge etf-tag" style="background:#fef3c7;color:#b45309;border-color:#fde68a">00XXXA <b>2.0億</b> <span style="font-size:9.5px;opacity:0.75">1.2%</span> <span style="font-size:9.5px">✦新增</span></span><span class="lg-text">✦ = 該 ETF 近期新增這支股（系統起算日 {baseline_date} 之後才出現；不代表歷史首次）</span>
    <span class="badge etf-tag" style="background:#dc2626;color:#fff;border-color:#b91c1c">00XXXA ✕撤出 -13.0億</span><span class="lg-text">✕撤出 = 今日完全賣光</span>
  </div>
  {tab_stock}
</div>
<div id="t2" class="pane">
  <div class="ttl">基金視角：每檔 ETF 最近在做什麼</div>
  <div class="desc">
    <b>一張卡片 = 一檔 ETF</b>，顯示它近期的規模變化與重要持股動作。<br/>
    <b>重大動作</b>的定義：單日或 5 日內，買進/賣出金額 ≥ 該 ETF 規模 × {int(MATERIAL_RATIO_OF_AUM*100)}%（大到足以影響基金的倉位）。下拉選單可鎖定單一 ETF。
  </div>
  {tab_fund}
</div>
<div id="t3" class="pane">
  <div class="ttl">機構集體行為</div>
  <div class="desc">≥ 2 檔大資金 ETF（AUM ≥ {BIG_ETF_AUM_THRESHOLD} 億）今日同時做出相同動作的股票。多家機構同步動作是最強烈的共識訊號</div>
  {tab_collective}
</div>
<div id="t4" class="pane">
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
    print(f"追蹤所有 {len(etf_list)} 檔 ETF（資料抓取）\n")

    # Load previous state (full snapshot, all 20 ETFs)
    first_seen, baseline_date = load_first_seen()
    print(f"first_seen baseline: {baseline_date}（早於此日期的記錄視為既有持股）")

    # Skip-if-complete: if a prior cron window today already produced a good
    # snapshot, reuse it instead of re-hammering upstream. Retries still kick
    # in if coverage is partial.
    cached_today = load_snapshot(TODAY.isoformat())
    if snapshot_is_complete(cached_today, etf_list):
        print(f"✓ Today's snapshot already complete ({len(cached_today['etfs'])} ETFs) — reusing, skipping fetch")
        today_data = cached_today['etfs']
    else:
        if cached_today:
            print(f"⚠ Today's snapshot exists but coverage is partial — refetching")
        today_data = fetch_all_etf_holdings(etf_list)
        retry_failed_etfs(today_data, etf_list)

    # Persist all 20 ETFs (data collection unaffected by analysis universe)
    save_daily_snapshot(today_data)
    save_latest_cache(today_data)
    cleanup_old_snapshots()

    # Update historical first-seen registry (also from full data)
    first_seen = update_first_seen(today_data, first_seen)
    save_first_seen(first_seen, baseline_date)

    # Health (from full data)
    health = build_health_report(today_data, None)

    # ── Filter to analysis universe: TW-only, ok status (12 ETFs)
    today_data_tw = filter_universe(today_data)
    universe_codes = list(today_data_tw.keys())
    universe_aum = sum(e.get('aum_billion', 0) or 0 for e in today_data_tw.values())
    print(f"\n分析 universe（純台股 ok ETF）: {len(today_data_tw)} 檔，總 AUM {universe_aum:.0f} 億")
    for c in sorted(today_data_tw.keys(), key=lambda x: -(today_data_tw[x].get('aum_billion') or 0)):
        e = today_data_tw[c]
        print(f"  {c} {e['name']}: AUM {e.get('aum_billion'):.1f} 億, {e.get('holdings_count')} 檔持股")

    # ── Load comparison snapshots for 1d / 5d periods (filtered to universe)
    snap_1d_full, snap_1d_date = find_snapshot_n_trading_days_back(1)
    snap_5d_full, snap_5d_date = find_snapshot_n_trading_days_back(TRADING_DAYS_LOOKBACK)

    # Fallback: if T-5 snapshot not yet accumulated, use the oldest available snapshot
    snap_5d_label = '5 日'
    if snap_5d_full is None:
        snap_5d_full, snap_5d_date = find_oldest_available_snapshot()
        if snap_5d_date:
            actual_days = count_trading_days_between(snap_5d_date, TODAY)
            snap_5d_label = f'{actual_days} 日'
            print(f"  ↳ 未找到 T-5 快照，改用最舊快照 {snap_5d_date}（實際 {actual_days} 個交易日）")

    snap_1d = filter_universe_snapshot(snap_1d_full, universe_codes)
    snap_5d = filter_universe_snapshot(snap_5d_full, universe_codes)
    print(f"\n比較基準: 1日 = {snap_1d_date or '無'} | {snap_5d_label} = {snap_5d_date or '累積中'}")

    # ── Compute flows for both periods
    flows_1d = compute_capital_flows(today_data_tw, snap_1d, first_seen, baseline_date) if snap_1d else {}
    flows_5d = compute_capital_flows(today_data_tw, snap_5d, first_seen, baseline_date) if snap_5d else {}

    # ── Build views
    stock_view = build_stock_view(today_data_tw, flows_1d, flows_5d)
    fund_view = build_fund_view(today_data_tw, snap_1d, snap_5d, first_seen, baseline_date)

    # ── Collective moves (from big ETFs within the universe)
    big_etf_list = [(c, e.get('aum_billion'), e.get('name')) for c, e in today_data_tw.items() if (e.get('aum_billion') or 0) >= BIG_ETF_AUM_THRESHOLD]
    big_etf_list.sort(key=lambda x: -(x[1] or 0))
    big_etf_codes = [c for c, _, _ in big_etf_list]
    big_actions = compute_big_etf_actions(today_data_tw, snap_1d, big_etf_codes, first_seen, baseline_date) if snap_1d else {}
    collective = compute_collective_moves(big_actions)

    # ── Render
    html = generate_etf_html(today_data_tw, stock_view, fund_view, collective, snap_1d_date, snap_5d_date, baseline_date, snap_5d_label)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\nDashboard written to {OUTPUT_HTML}")

    # Summary
    print(f"\n統計：")
    print(f"  股票視角列數: {len(stock_view)}")
    print(f"  基金視角項目: {len(fund_view)}")
    print(f"  集體買入訊號: {len(collective['buys'])}")
    print(f"  集體賣出訊號: {len(collective['sells'])}")

    err_count = sum(1 for h in health.values() if h['status'] == 'error')
    if err_count == len(etf_list):
        print("::error::All ETF fetches failed!", file=sys.stderr)
        sys.exit(1)
    elif err_count > len(etf_list) // 2:
        print(f"::warning::ETF fetch issues: {err_count} errors", file=sys.stderr)


if __name__ == '__main__':
    main()
