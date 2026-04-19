#!/usr/bin/env python3
"""
台股大戶持股追蹤儀表板 — 資料產生器

資料來源：FinMind TaiwanStockHoldingSharesPer（贊助會員等級）。
原始資料源自 TDCC 集保戶股權分散表，每週五更新。

用法：
  python generate_big_holders.py                 # 抓最新一週（若快照已存在則跳過寫入）
  python generate_big_holders.py --backfill N    # 同時回補過去 N 週的歷史快照

環境變數：
  FINMIND_TOKEN    FinMind API 存取 token（必要）。本地可放 .env；CI 用 GitHub Secret。

輸出：
  - big_holders_data/<YYYYMMDD>.json   每週快照（stock-level aggregation）
  - big_holders_latest.json            最新週 + 上週比較結果
  - big_holders_index.html             獨立 dashboard
  - stock_names.json                   證券代號→名稱對照（自 TWSE ISIN 快取）
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / 'big_holders_data'
LATEST_JSON = ROOT / 'big_holders_latest.json'
OUTPUT_HTML = ROOT / 'big_holders_index.html'
NAMES_CACHE = ROOT / 'stock_names.json'
ENV_FILE = ROOT / '.env'

FINMIND_URL = 'https://api.finmindtrade.com/api/v4/data'
FINMIND_DATASET = 'TaiwanStockHoldingSharesPer'
ISIN_URLS = [
    ('TWSE', 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'),
    ('OTC', 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'),
    ('Emerging', 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=5'),
]

HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; BigHoldersBot/1.0)'}

# FinMind HoldingSharesLevel → 是否列入大戶
# 400 張大戶 = 持有 >400,000 股 = 400,001 以上的四個級距加總
LEVELS_400 = {
    '400,001-600,000',
    '600,001-800,000',
    '800,001-1,000,000',
    'more than 1,000,001',
}
LEVELS_1000 = {'more than 1,000,001'}
LEVEL_TOTAL = 'total'

TOP_N = 50                  # 榜單數量
MIN_CHANGE_PP = 0.05        # 最小變化門檻（百分點），低於此值視為雜訊
NAMES_CACHE_TTL_DAYS = 30   # 股名快取 TTL
FINMIND_SLEEP = 0.4         # 每次 FinMind 呼叫之間的禮貌 sleep（秒）


def load_token():
    """讀取 FinMind token：優先環境變數，其次 .env 檔。"""
    token = os.environ.get('FINMIND_TOKEN')
    if not token and ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if line.startswith('FINMIND_TOKEN='):
                token = line.split('=', 1)[1].strip()
                break
    if not token:
        print('ERROR: FINMIND_TOKEN not set (env var or .env file)', file=sys.stderr)
        sys.exit(2)
    return token


# ── Data fetching ────────────────────────────────────────────────────────────

def fetch_finmind_date(token, date_str):
    """抓 FinMind 指定單日的全市場持股分級表。date_str 格式 YYYY-MM-DD。
    若當日非 TDCC 資料日（週五），會回傳空 list。"""
    r = requests.get(FINMIND_URL, params={
        'dataset': FINMIND_DATASET,
        'start_date': date_str,
        'end_date': date_str,
    }, headers={'Authorization': f'Bearer {token}'}, timeout=120)
    r.raise_for_status()
    d = r.json()
    if d.get('status') != 200 and d.get('status', 200) != 200:
        raise RuntimeError(f'FinMind error: {d}')
    return d.get('data', [])


def fetch_latest(token):
    """往前探最近一個有資料的週五。"""
    today = datetime.now().date()
    # 從今天往前找至多 10 天
    for back in range(10):
        d = (today - timedelta(days=back)).strftime('%Y-%m-%d')
        print(f'→ probing FinMind for {d} …', flush=True)
        rows = fetch_finmind_date(token, d)
        if rows:
            print(f'  got {len(rows):,} rows', flush=True)
            return d, rows
        time.sleep(FINMIND_SLEEP)
    raise RuntimeError('No data in last 10 days')


def fetch_range(token, start_date, end_date):
    """抓一段日期內所有可用快照。回傳 {YYYYMMDD: rows}."""
    # 找一支股票當探針，取得區間內實際存在的日期
    print(f'→ probing available dates between {start_date} and {end_date} …', flush=True)
    r = requests.get(FINMIND_URL, params={
        'dataset': FINMIND_DATASET,
        'data_id': '2330',
        'start_date': start_date,
        'end_date': end_date,
    }, headers={'Authorization': f'Bearer {token}'}, timeout=60)
    r.raise_for_status()
    probe = r.json().get('data', [])
    dates = sorted(set(row['date'] for row in probe))
    if not dates:
        print('  no dates found', flush=True)
        return {}
    print(f'  found {len(dates)} dates: {dates}', flush=True)

    out = {}
    for d in dates:
        print(f'→ fetching {d} …', flush=True)
        rows = fetch_finmind_date(token, d)
        out[d.replace('-', '')] = rows
        print(f'  {len(rows):,} rows', flush=True)
        time.sleep(FINMIND_SLEEP)
    return out


def fetch_stock_names():
    """從 TWSE ISIN 抓取股票代號→名稱對照表（上市 + 上櫃 + 興櫃）。"""
    names = {}
    for label, url in ISIN_URLS:
        print(f'→ fetching {label} ISIN …', flush=True)
        r = requests.get(url, headers=HEADERS, timeout=120)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'lxml')
        for tr in soup.find_all('tr'):
            tds = [t.get_text(strip=True) for t in tr.find_all('td')]
            if not tds or len(tds) < 2:
                continue
            cell = tds[0]
            if '\u3000' not in cell:
                continue
            code, name = cell.split('\u3000', 1)
            code = code.strip()
            name = name.strip()
            if code and name and code.isdigit():
                names[code] = name
    print(f'  mapped {len(names):,} tickers', flush=True)
    return names


def load_or_refresh_names():
    """股名快取：存在且未過期則用快取，否則重新抓。"""
    if NAMES_CACHE.exists():
        try:
            cached = json.loads(NAMES_CACHE.read_text('utf-8'))
            age_days = (time.time() - cached.get('ts', 0)) / 86400
            if age_days < NAMES_CACHE_TTL_DAYS and cached.get('names'):
                print(f'→ using cached stock names (age {age_days:.1f}d)', flush=True)
                return cached['names']
        except Exception:
            pass
    names = fetch_stock_names()
    NAMES_CACHE.write_text(
        json.dumps({'ts': time.time(), 'names': names}, ensure_ascii=False),
        'utf-8',
    )
    return names


# ── Aggregation ──────────────────────────────────────────────────────────────

def aggregate(rows):
    """把 FinMind 原始 row 依股票彙總出大戶持股比例。

    回傳：{stock_id: {'pct_400': float, 'pct_1000': float,
                     'holders_400': int, 'holders_1000': int,
                     'total_shares': int}}
    FinMind schema: date / stock_id / HoldingSharesLevel / people / percent / unit
    """
    result = {}
    for r in rows:
        code = r['stock_id']
        level = r['HoldingSharesLevel']
        try:
            pct = float(r.get('percent') or 0)
            people = int(r.get('people') or 0)
            unit = int(r.get('unit') or 0)
        except (ValueError, TypeError):
            continue

        bucket = result.setdefault(
            code,
            {'pct_400': 0.0, 'pct_1000': 0.0,
             'holders_400': 0, 'holders_1000': 0,
             'total_shares': 0},
        )
        if level in LEVELS_400:
            bucket['pct_400'] += pct
            bucket['holders_400'] += people
        if level in LEVELS_1000:
            bucket['pct_1000'] += pct
            bucket['holders_1000'] += people
        if level == LEVEL_TOTAL:
            bucket['total_shares'] = unit
    # 四捨五入到小數 2 位（避免浮點累加誤差）
    for v in result.values():
        v['pct_400'] = round(v['pct_400'], 2)
        v['pct_1000'] = round(v['pct_1000'], 2)
    return result


def filter_stocks(agg_by_code, names):
    """只保留 4 碼股票代號（上市/上櫃普通股），過濾 ETF/權證/債券/興櫃等。"""
    out = {}
    for code, v in agg_by_code.items():
        if not (code.isdigit() and len(code) == 4):
            continue
        # 排除 0xxx（多為 ETF/指數成分）
        if code.startswith('0'):
            continue
        v = dict(v)
        v['name'] = names.get(code, '')
        out[code] = v
    return out


# ── Snapshot management ─────────────────────────────────────────────────────

def save_snapshot(date, stocks):
    DATA_DIR.mkdir(exist_ok=True)
    path = DATA_DIR / f'{date}.json'
    path.write_text(
        json.dumps({'date': date, 'stocks': stocks}, ensure_ascii=False, separators=(',', ':')),
        'utf-8',
    )
    print(f'  saved snapshot {path.name}', flush=True)


def list_snapshots():
    if not DATA_DIR.exists():
        return []
    return sorted(p.stem for p in DATA_DIR.glob('*.json') if p.stem.isdigit())


def load_snapshot(date):
    path = DATA_DIR / f'{date}.json'
    if not path.exists():
        return None
    return json.loads(path.read_text('utf-8'))


# ── Diff & ranking ──────────────────────────────────────────────────────────

def compute_changes(curr, prev):
    """比對本週與上週，回傳每支股票的變化。"""
    rows = []
    prev_stocks = prev['stocks'] if prev else {}
    for code, c in curr['stocks'].items():
        p = prev_stocks.get(code)
        if not p:
            # 新增的股票，無上週資料
            rows.append({
                'code': code,
                'name': c['name'],
                'pct_400': c['pct_400'],
                'pct_1000': c['pct_1000'],
                'prev_pct_400': None,
                'prev_pct_1000': None,
                'delta_400': None,
                'delta_1000': None,
                'holders_400': c['holders_400'],
                'holders_1000': c['holders_1000'],
            })
            continue
        rows.append({
            'code': code,
            'name': c['name'],
            'pct_400': c['pct_400'],
            'pct_1000': c['pct_1000'],
            'prev_pct_400': p['pct_400'],
            'prev_pct_1000': p['pct_1000'],
            'delta_400': round(c['pct_400'] - p['pct_400'], 4),
            'delta_1000': round(c['pct_1000'] - p['pct_1000'], 4),
            'holders_400': c['holders_400'],
            'holders_1000': c['holders_1000'],
        })
    return rows


def rank(rows, key, top_n=TOP_N, ascending=False, min_abs=MIN_CHANGE_PP):
    filt = [r for r in rows if r[key] is not None and abs(r[key]) >= min_abs]
    filt.sort(key=lambda r: r[key], reverse=not ascending)
    return filt[:top_n]


# ── HTML rendering ──────────────────────────────────────────────────────────

CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b;--card:#fff;--hover:#eef2ff}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:14px;line-height:1.5}
.hdr{background:linear-gradient(135deg,#0f172a 0%,#1e3a8a 50%,#2563eb 100%);color:#fff;padding:22px 28px 16px}
.hdr h1{font-size:20px;font-weight:800;margin-bottom:4px;letter-spacing:-0.3px}
.hdr .sub{font-size:11.5px;opacity:.85}
.nav-link{color:#93c5fd;font-size:11.5px;text-decoration:none;margin-left:14px;border-bottom:1px dashed #93c5fd;padding-bottom:1px;transition:opacity .15s}
.nav-link:hover{opacity:.7}
.stats{display:flex;gap:12px;padding:16px 28px;background:var(--card);border-bottom:1px solid var(--brd);flex-wrap:wrap}
.sc{background:var(--bg);border:1px solid var(--brd);border-radius:10px;padding:14px 20px;min-width:120px;border-left:3px solid var(--bl)}
.sc .n{font-size:26px;font-weight:800;color:var(--bl);letter-spacing:-0.5px}.sc .l{font-size:11px;color:var(--mu);margin-top:3px;font-weight:500}
.sc.gr{border-left-color:var(--gr)}.sc.gr .n{color:var(--gr)}
.sc.rd{border-left-color:var(--rd)}.sc.rd .n{color:var(--rd)}
.sc.am{border-left-color:var(--am)}.sc.am .n{color:var(--am)}
.tabs{display:flex;padding:0 28px;border-bottom:2px solid var(--brd);background:var(--card);gap:4px;flex-wrap:wrap}
.tab{padding:12px 20px;cursor:pointer;border-bottom:3px solid transparent;font-size:12.5px;font-weight:600;color:var(--mu);margin-bottom:-2px;transition:all .15s;border-radius:6px 6px 0 0}
.tab:hover{background:#f1f5f9;color:var(--txt)}
.tab.active{border-bottom-color:var(--bl);color:var(--bl);background:transparent}
.pane{display:none;padding:20px 28px}.pane.active{display:block}
.ttl{font-size:15px;font-weight:700;margin-bottom:6px}
.desc{font-size:12.5px;color:var(--mu);margin-bottom:14px;line-height:1.7}
.section{margin-bottom:28px}
.section h3{font-size:13.5px;font-weight:700;color:var(--bl);margin-bottom:10px;padding:8px 0;border-bottom:2px solid #dbeafe}
.section h3.sell{color:var(--rd);border-bottom-color:#fecaca}
.dir-toggle{display:inline-flex;background:#eef2ff;border-radius:10px;padding:4px;margin-bottom:14px;gap:2px}
.dir-btn{padding:8px 18px;border:none;background:transparent;cursor:pointer;font-size:13px;font-weight:600;color:var(--mu);border-radius:7px;transition:all .15s;font-family:inherit}
.dir-btn:hover{color:var(--txt)}
.dir-btn.active{background:var(--card);color:var(--txt);box-shadow:0 1px 3px rgba(0,0,0,.08)}
.dir-btn[data-dir="up"].active{color:var(--gr)}
.dir-btn[data-dir="dn"].active{color:var(--rd)}
.dir-pane{display:none}
.dir-pane.active{display:block}
th.sortable-th{cursor:pointer;user-select:none;transition:background .12s}
th.sortable-th:hover{background:#dbeafe;color:var(--bl)}
th.sortable-th .sort-hint{display:inline-block;margin-left:4px;font-size:10px;opacity:0.4}
th.sortable-th.sort-asc,th.sortable-th.sort-desc{background:#dbeafe;color:var(--bl)}
th.sortable-th.sort-asc .sort-hint,th.sortable-th.sort-desc .sort-hint{opacity:1;color:var(--bl)}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06);margin-bottom:16px}
th{background:#edf2f7;font-weight:700;color:var(--mu);font-size:11px;text-transform:uppercase;padding:10px 12px;text-align:left;border-bottom:2px solid var(--brd);white-space:nowrap;letter-spacing:0.3px}
td{padding:9px 12px;border-bottom:1px solid var(--brd);vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:var(--hover)}
.num{text-align:right;font-variant-numeric:tabular-nums}
.rank{text-align:center;color:var(--mu);font-size:11px;width:44px}
.code{font-weight:700;color:var(--bl);font-variant-numeric:tabular-nums}
.delta-up{color:#16a34a;font-weight:700}
.delta-down{color:#dc2626;font-weight:700}
tr.row-up td{background:#f0fdf4;border-left:3px solid var(--gr)}
tr.row-down td{background:#fef2f2;border-left:3px solid var(--rd)}
tr.row-up:hover td{background:#dcfce7}
tr.row-down:hover td{background:#fee2e2}
.ft{text-align:center;color:var(--mu);font-size:11.5px;padding:20px 28px;border-top:1px solid var(--brd);line-height:1.8;background:var(--card)}
.ft a{color:var(--bl);text-decoration:none}.ft a:hover{text-decoration:underline}
.empty-msg{color:var(--mu);font-size:13px;padding:16px;text-align:center;background:var(--card);border-radius:10px}
@media(max-width:768px){.hdr,.stats,.pane{padding-left:16px;padding-right:16px}.tabs{padding:0 16px;overflow-x:auto}th,td{padding:8px 8px}.sc{min-width:100px;padding:10px 14px}.sc .n{font-size:20px}}
"""


def _fmt_pct(v, digits=2):
    if v is None:
        return '—'
    return f'{v:.{digits}f}%'


def _fmt_delta(v):
    if v is None:
        return '<span style="color:var(--mu)">新</span>'
    sign = '+' if v > 0 else ''
    cls = 'delta-up' if v > 0 else ('delta-down' if v < 0 else '')
    return f'<span class="{cls}">{sign}{v:.2f}</span>'


def _render_table(rows, delta_key, pct_key, prev_pct_key, label, row_cls, threshold_label):
    if not rows:
        return f'<div class="empty-msg">目前沒有{label}超過 {MIN_CHANGE_PP}pp 的股票。</div>'
    # 預設依 delta 欄排序（由後端已排好），所以預設標示在第 5 欄
    default_dir = 'desc' if label == '增加' else 'asc'
    parts = [
        '<table class="sortable"><thead><tr>',
        '<th class="rank">#</th>',
        '<th class="sortable-th" data-sort-type="str" data-col="1">代號 <span class="sort-hint">⇅</span></th>',
        '<th class="sortable-th" data-sort-type="str" data-col="2">名稱 <span class="sort-hint">⇅</span></th>',
        f'<th class="num sortable-th" data-sort-type="num" data-col="3" title="本週大戶持股佔全部股份的比例">本週 {threshold_label}佔比 <span class="sort-hint">⇅</span></th>',
        f'<th class="num sortable-th" data-sort-type="num" data-col="4" title="上週大戶持股佔全部股份的比例">上週 {threshold_label}佔比 <span class="sort-hint">⇅</span></th>',
        f'<th class="num sortable-th sort-{default_dir}" data-sort-type="num" data-col="5" title="本週 − 上週，單位為百分點">週變化 (pp) <span class="sort-hint">{"▼" if default_dir == "desc" else "▲"}</span></th>',
        f'<th class="num sortable-th" data-sort-type="num" data-col="6" title="本週{threshold_label}的股東人數">{threshold_label}人數 <span class="sort-hint">⇅</span></th>',
        '</tr></thead><tbody>',
    ]
    for i, r in enumerate(rows, 1):
        holders = r['holders_1000'] if '1000' in pct_key else r['holders_400']
        pct_now = r[pct_key]
        pct_prev = r[prev_pct_key]
        delta = r[delta_key]
        # data-sort attrs 提供排序時的原始數值
        parts.append(
            f'<tr class="{row_cls}">'
            f'<td class="rank">{i}</td>'
            f'<td data-sort="{r["code"]}"><span class="code">{r["code"]}</span></td>'
            f'<td data-sort="{r["name"] or ""}">{r["name"] or "—"}</td>'
            f'<td class="num" data-sort="{pct_now if pct_now is not None else -999}">{_fmt_pct(pct_now)}</td>'
            f'<td class="num" data-sort="{pct_prev if pct_prev is not None else -999}">{_fmt_pct(pct_prev)}</td>'
            f'<td class="num" data-sort="{delta if delta is not None else -999}">{_fmt_delta(delta)}</td>'
            f'<td class="num" data-sort="{holders}">{holders:,}</td>'
            f'</tr>'
        )
    parts.append('</tbody></table>')
    return ''.join(parts)


def render_html(curr_date, prev_date, rows, stats):
    up_400 = rank(rows, 'delta_400', ascending=False)
    dn_400 = rank(rows, 'delta_400', ascending=True)
    up_1000 = rank(rows, 'delta_1000', ascending=False)
    dn_1000 = rank(rows, 'delta_1000', ascending=True)

    curr_fmt = f'{curr_date[:4]}-{curr_date[4:6]}-{curr_date[6:]}'
    prev_fmt = f'{prev_date[:4]}-{prev_date[4:6]}-{prev_date[6:]}' if prev_date else '—'

    html = f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>台股大戶持股追蹤</title>
<style>{CSS}</style></head>
<body>
<div class="hdr">
  <h1>台股大戶持股變化監測</h1>
  <div class="sub">本週資料日 {curr_fmt}｜比較基準 {prev_fmt}｜資料來源 TDCC 集保戶股權分散表（每週五更新）
    <a class="nav-link" href="etf_index.html">→ 主動式 ETF</a>
    <a class="nav-link" href="index.html">→ 可轉債</a>
  </div>
</div>
<div class="stats">
  <div class="sc"><div class="n">{stats['tracked']:,}</div><div class="l">追蹤個股檔數</div></div>
  <div class="sc gr"><div class="n">{stats['up_400']:,}</div><div class="l">400 張大戶增加</div></div>
  <div class="sc rd"><div class="n">{stats['dn_400']:,}</div><div class="l">400 張大戶減少</div></div>
  <div class="sc gr"><div class="n">{stats['up_1000']:,}</div><div class="l">1000 張大戶增加</div></div>
  <div class="sc rd"><div class="n">{stats['dn_1000']:,}</div><div class="l">1000 張大戶減少</div></div>
</div>
<div class="tabs">
  <div class="tab active" data-tab="t400">400 張大戶</div>
  <div class="tab" data-tab="t1000">1000 張大戶</div>
</div>

<div class="pane active" id="t400">
  <div class="ttl">400 張以上大戶持股比例變化</div>
  <div class="desc">
    <b>大戶定義</b>：單一股東持有該股 <b>400,001 股以上</b>（約 400 張以上，含四個級距：400,001–600,000 / 600,001–800,000 / 800,001–1,000,000 / 1,000,001 以上）。<br>
    <b>本週 400張大戶佔比</b>：這些大戶持有的股數 ÷ 該股集保總股數。例如「台積電 88.42%」表示台積電有 88.42% 的股份由 400 張以上大戶持有。<br>
    <b>週變化 (pp)</b>：本週佔比 − 上週佔比，單位百分點（percentage points）。正值＝大戶加碼；負值＝大戶減碼。<br>
    榜單取變化幅度前 {TOP_N} 名，變化門檻 ±{MIN_CHANGE_PP}pp 以下視為雜訊不列入。
  </div>
  <div class="dir-toggle" data-scope="t400">
    <button class="dir-btn active" data-dir="up">🟢 增加 Top {TOP_N}</button>
    <button class="dir-btn" data-dir="dn">🔴 減少 Top {TOP_N}</button>
  </div>
  <div class="dir-pane active" data-scope="t400" data-dir="up">
    {_render_table(up_400, 'delta_400', 'pct_400', 'prev_pct_400', '增加', 'row-up', '400張大戶')}
  </div>
  <div class="dir-pane" data-scope="t400" data-dir="dn">
    {_render_table(dn_400, 'delta_400', 'pct_400', 'prev_pct_400', '減少', 'row-down', '400張大戶')}
  </div>
</div>

<div class="pane" id="t1000">
  <div class="ttl">1000 張以上大戶持股比例變化</div>
  <div class="desc">
    <b>大戶定義</b>：單一股東持有該股 <b>1,000,001 股以上</b>（約 1,000 張以上）。這個級距多為政府基金、法人、信託、董監與主要股東。<br>
    <b>本週 1000張大戶佔比</b>：這些大戶持有的股數 ÷ 該股集保總股數。例如「台積電 85.69%」表示台積電有 85.69% 的股份由 1,000 張以上大戶持有。<br>
    <b>週變化 (pp)</b>：本週佔比 − 上週佔比，單位百分點。正值＝大戶加碼；負值＝大戶減碼。<br>
    榜單取變化幅度前 {TOP_N} 名，變化門檻 ±{MIN_CHANGE_PP}pp 以下視為雜訊不列入。
  </div>
  <div class="dir-toggle" data-scope="t1000">
    <button class="dir-btn active" data-dir="up">🟢 增加 Top {TOP_N}</button>
    <button class="dir-btn" data-dir="dn">🔴 減少 Top {TOP_N}</button>
  </div>
  <div class="dir-pane active" data-scope="t1000" data-dir="up">
    {_render_table(up_1000, 'delta_1000', 'pct_1000', 'prev_pct_1000', '增加', 'row-up', '1000張大戶')}
  </div>
  <div class="dir-pane" data-scope="t1000" data-dir="dn">
    {_render_table(dn_1000, 'delta_1000', 'pct_1000', 'prev_pct_1000', '減少', 'row-down', '1000張大戶')}
  </div>
</div>

<div class="ft">
  資料來源：TDCC 集保戶股權分散表｜股名對照：TWSE ISIN<br>
  每日排程跑一次，資料為週頻（每週五更新上週五資料）。產生於 {datetime.now().strftime('%Y-%m-%d %H:%M')}
</div>

<script>
document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', () => {{
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(x => x.classList.remove('active'));
  t.classList.add('active');
  document.getElementById(t.dataset.tab).classList.add('active');
}}));

document.querySelectorAll('.dir-btn').forEach(btn => btn.addEventListener('click', () => {{
  const scope = btn.parentElement.dataset.scope;
  const dir = btn.dataset.dir;
  btn.parentElement.querySelectorAll('.dir-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll(`.dir-pane[data-scope="${{scope}}"]`).forEach(p => {{
    p.classList.toggle('active', p.dataset.dir === dir);
  }});
}}));

document.querySelectorAll('table.sortable').forEach(table => {{
  table.querySelectorAll('th.sortable-th').forEach(th => {{
    th.addEventListener('click', () => {{
      const col = parseInt(th.dataset.col, 10);
      const type = th.dataset.sortType;
      const currentlyAsc = th.classList.contains('sort-asc');
      // Clear all siblings
      th.parentElement.querySelectorAll('th').forEach(x => {{
        x.classList.remove('sort-asc', 'sort-desc');
        const hint = x.querySelector('.sort-hint');
        if (hint && x.classList.contains('sortable-th')) hint.textContent = '⇅';
      }});
      const newDir = currentlyAsc ? 'desc' : 'asc';
      th.classList.add('sort-' + newDir);
      const hint = th.querySelector('.sort-hint');
      if (hint) hint.textContent = newDir === 'asc' ? '▲' : '▼';

      const tbody = table.querySelector('tbody');
      const rows = Array.from(tbody.querySelectorAll('tr'));
      rows.sort((a, b) => {{
        const av = a.children[col]?.dataset.sort ?? a.children[col]?.textContent.trim() ?? '';
        const bv = b.children[col]?.dataset.sort ?? b.children[col]?.textContent.trim() ?? '';
        let cmp;
        if (type === 'num') {{
          cmp = parseFloat(av) - parseFloat(bv);
        }} else {{
          cmp = av.localeCompare(bv, 'zh-Hant');
        }}
        return newDir === 'asc' ? cmp : -cmp;
      }});
      // Update rank column and re-append
      rows.forEach((row, i) => {{
        const rankCell = row.querySelector('td.rank');
        if (rankCell) rankCell.textContent = i + 1;
        tbody.appendChild(row);
      }});
    }});
  }});
}});
</script>
</body></html>
"""
    return html


# ── Main ────────────────────────────────────────────────────────────────────

def process_and_save(date_str, rows, names):
    """彙總單日資料並存成快照。回傳 stocks dict。"""
    agg = aggregate(rows)
    stocks = filter_stocks(agg, names)
    date_key = date_str.replace('-', '')
    existing = load_snapshot(date_key)
    if existing and existing.get('stocks') == stocks:
        print(f'  {date_key}: already up-to-date ({len(stocks):,} stocks)', flush=True)
    else:
        save_snapshot(date_key, stocks)
        print(f'  {date_key}: {len(stocks):,} individual stocks', flush=True)
    return date_key, stocks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backfill', type=int, default=0,
                        help='回補過去 N 週的歷史快照（除了最新週之外另外抓）')
    args = parser.parse_args()

    token = load_token()
    names = load_or_refresh_names()

    # 先抓最新
    curr_date_str, latest_rows = fetch_latest(token)
    curr_date, _ = process_and_save(curr_date_str, latest_rows, names)

    # 選擇性回補
    if args.backfill > 0:
        # 往前回推 backfill × 7 天，讓探針涵蓋過去 N 週
        end = datetime.strptime(curr_date_str, '%Y-%m-%d').date() - timedelta(days=1)
        start = end - timedelta(days=args.backfill * 7 + 2)
        historical = fetch_range(
            token,
            start.strftime('%Y-%m-%d'),
            end.strftime('%Y-%m-%d'),
        )
        for date_str, rows in sorted(historical.items()):
            # date_str 已是 YYYYMMDD
            date_key = date_str
            agg = aggregate(rows)
            stocks = filter_stocks(agg, names)
            existing = load_snapshot(date_key)
            if existing and existing.get('stocks') == stocks:
                print(f'  {date_key}: already up-to-date', flush=True)
            else:
                save_snapshot(date_key, stocks)

    # 比較最新 vs 前一筆快照
    snaps = [d for d in list_snapshots() if d != curr_date]
    prev_date = snaps[-1] if snaps else None
    prev = load_snapshot(prev_date) if prev_date else None

    curr = load_snapshot(curr_date)
    changes = compute_changes(curr, prev)

    # Stats
    stats = {
        'tracked': len(changes),
        'up_400': sum(1 for r in changes if r['delta_400'] is not None and r['delta_400'] >= MIN_CHANGE_PP),
        'dn_400': sum(1 for r in changes if r['delta_400'] is not None and r['delta_400'] <= -MIN_CHANGE_PP),
        'up_1000': sum(1 for r in changes if r['delta_1000'] is not None and r['delta_1000'] >= MIN_CHANGE_PP),
        'dn_1000': sum(1 for r in changes if r['delta_1000'] is not None and r['delta_1000'] <= -MIN_CHANGE_PP),
    }
    print(f'  stats: {stats}', flush=True)

    # Persist latest JSON (for external consumers)
    latest = {
        'curr_date': curr_date,
        'prev_date': prev_date,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'stats': stats,
        'changes': changes,
    }
    LATEST_JSON.write_text(
        json.dumps(latest, ensure_ascii=False, separators=(',', ':')),
        'utf-8',
    )
    print(f'  wrote {LATEST_JSON.name}', flush=True)

    # Render HTML
    html = render_html(curr_date, prev_date, changes, stats)
    OUTPUT_HTML.write_text(html, 'utf-8')
    print(f'  wrote {OUTPUT_HTML.name} ({len(html):,} chars)', flush=True)

    print('✓ done', flush=True)


if __name__ == '__main__':
    main()
