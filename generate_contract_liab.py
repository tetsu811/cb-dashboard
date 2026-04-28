#!/usr/bin/env python3
"""
台股「合約負債」策略儀表板 — 資料產生器

策略核心：合約負債 (Contract Liabilities, IFRS 15) 是「客戶已付錢、公司還沒認列的營收」，
本質是未來營收的領先指標。本腳本掃描全市場，找出合約負債暴增、能見度高的個股。

資料源：
  主：FinMind TaiwanStockBalanceSheet / TaiwanStockMonthRevenue / TaiwanStockPrice
  備：MOPS（公開資訊觀測站）— FinMind 個股資料缺漏時補抓單檔
  備：Goodinfo — 雙重驗證（人工核對用，腳本不直接抓）

用法：
  python generate_contract_liab.py                # 跑最新可用季度
  python generate_contract_liab.py --quarter 2025Q4   # 指定季度
  python generate_contract_liab.py --stock 6903   # 單檔深度檢視（巨漢）

環境變數：
  FINMIND_TOKEN    FinMind API 存取 token（必要）。本地可放 .env；CI 用 GitHub Secret。
                   為相容已有設定，也接受 FINMIND_API_KEY。

輸出：
  - contract_liab_data/<YYYYQX>.json   每季快照（全市場合約負債橫斷面）
  - contract_liab_latest.json          最新季度排名 + 8 季趨勢
  - contract_liab_index.html           獨立 dashboard
"""

import argparse
import json
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / 'contract_liab_data'
LATEST_JSON = ROOT / 'contract_liab_latest.json'
OUTPUT_HTML = ROOT / 'contract_liab_index.html'
BACKTEST_JSON = ROOT / 'contract_liab_backtest.json'
NAMES_CACHE = ROOT / 'stock_names.json'  # 共用 generate_big_holders.py 的快取
INFO_CACHE = ROOT / 'stock_info_cache.json'  # TaiwanStockInfo 快取（含產業類別）
BIG_HOLDERS_LATEST = ROOT / 'big_holders_latest.json'
ENV_FILE = ROOT / '.env'

FINMIND_URL = 'https://api.finmindtrade.com/api/v4/data'
MOPS_URL = 'https://mopsov.twse.com.tw/mops/web/ajax_t164sb03'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; ContractLiabBot/1.0)'}

FINMIND_SLEEP = 0.3
TOP_N = 50
INFO_CACHE_TTL_DAYS = 30

# 策略門檻
MIN_CL_TWD = 50_000_000        # 合約負債絕對值下限 (5,000 萬)，過濾雜訊
MIN_CL_YOY = 0.50              # 年增率下限 50%
MIN_CL_TO_ASSETS = 0.02        # 合約負債/總資產 ≥ 2%
MIN_8Q_RANK = 0.75             # 站上 8 季 75 百分位

# 雜訊抑制 cap：YoY/QoQ 在排名前先截上限，避免極小基期股拉爆 z-score
CAP_YOY = 10.0                 # +1000% 以上一律視為 +1000%
CAP_QOQ = 5.0                  # +500% 以上一律視為 +500%

# 評分權重（降低 YoY 比重、加入大戶買超與分數分散風險）
W_YOY = 0.30
W_QOQ = 0.15
W_TO_REV = 0.20
W_TO_ASSETS = 0.10
W_ACCEL = 0.10
W_BIGHOLDER = 0.15             # 大戶持股週變化 (delta_400)

# 產業分組 → 顯示用的中文 group key
INDUSTRY_GROUPS = {
    '半導體鏈': {'半導體業', '其他電子業', '電子零組件業', '光電業', '電腦及週邊設備業', '通信網路業', '電子工業'},
    '建設工程': {'建材營造', '其他'},
    '機電設備': {'電機機械'},
    '生技醫療': {'生技醫療業', '化學生技醫療', '生技醫療'},
    '傳產內需': {'食品工業', '紡織纖維', '塑膠工業', '橡膠工業', '汽車工業', '造紙工業', '鋼鐵工業',
                  '水泥工業', '玻璃陶瓷', '航運業', '觀光餐旅', '貿易百貨', '油電燃氣業'},
    '金融': {'金融保險業', '金融保險'},
}


def industry_to_group(ind):
    if not ind: return '其他'
    for grp, members in INDUSTRY_GROUPS.items():
        if ind in members:
            return grp
    return '其他'

# 季度與季底日期對應
QUARTER_END = {
    'Q1': '03-31', 'Q2': '06-30', 'Q3': '09-30', 'Q4': '12-31',
}


# ── 環境 / Token ─────────────────────────────────────────────────────────────

def load_token():
    """讀取 FinMind token：環境變數優先，其次 .env（接受 FINMIND_TOKEN 或 FINMIND_API_KEY）。"""
    for key in ('FINMIND_TOKEN', 'FINMIND_API_KEY'):
        v = os.environ.get(key)
        if v:
            return v
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            for key in ('FINMIND_TOKEN=', 'FINMIND_API_KEY='):
                if line.startswith(key):
                    return line.split('=', 1)[1].strip()
    # 也試另一個常用位置
    alt_env = Path.home() / '可轉債策略訊號' / '.env'
    if alt_env.exists():
        for line in alt_env.read_text().splitlines():
            for key in ('FINMIND_TOKEN=', 'FINMIND_API_KEY='):
                if line.startswith(key):
                    return line.split('=', 1)[1].strip()
    print('ERROR: FINMIND_TOKEN / FINMIND_API_KEY not set', file=sys.stderr)
    sys.exit(2)


# ── FinMind ──────────────────────────────────────────────────────────────────

def finmind_get(token, dataset, **params):
    p = {'dataset': dataset, **params}
    r = requests.get(FINMIND_URL, params=p,
                     headers={'Authorization': f'Bearer {token}'}, timeout=120)
    r.raise_for_status()
    j = r.json()
    if j.get('status') not in (200, None):
        raise RuntimeError(f'FinMind error {dataset}: {j}')
    return j.get('data', [])


def fetch_balance_sheet_quarter(token, quarter_end_date):
    """抓單季全市場資產負債表。quarter_end_date 為 YYYY-MM-DD（季底）。"""
    print(f'→ FinMind balance sheet {quarter_end_date} …', flush=True)
    rows = finmind_get(token, 'TaiwanStockBalanceSheet',
                      start_date=quarter_end_date, end_date=quarter_end_date)
    print(f'  {len(rows):,} rows', flush=True)
    return rows


def fetch_revenue_window(token, start, end):
    """抓區間月營收（不指定 data_id 為全市場）。FinMind 月營收可能限制單檔；
    若全市場太大，改為個別股票呼叫。"""
    print(f'→ FinMind monthly revenue {start} ~ {end} …', flush=True)
    # 試試全市場
    try:
        rows = finmind_get(token, 'TaiwanStockMonthRevenue',
                          start_date=start, end_date=end)
        print(f'  {len(rows):,} rows (whole market)', flush=True)
        return rows
    except Exception as e:
        print(f'  whole-market fetch failed ({e}); will fall back to per-stock', flush=True)
        return None


def fetch_revenue_single(token, stock_id, start, end):
    return finmind_get(token, 'TaiwanStockMonthRevenue',
                      data_id=stock_id, start_date=start, end_date=end)


def fetch_price_recent(token, stock_id, start, end):
    return finmind_get(token, 'TaiwanStockPrice',
                      data_id=stock_id, start_date=start, end_date=end)


# ── MOPS 備援（單檔合約負債） ────────────────────────────────────────────────

def fetch_mops_balance_sheet(stock_id, year_roc, season):
    """MOPS t164sb03 = 合併資產負債表。year_roc 為民國年（int），season 1~4。
    回傳 {科目名: 數值} dict；找不到則回傳空 dict。
    """
    try:
        r = requests.post(MOPS_URL, data={
            'encodeURIComponent': 1,
            'step': 1,
            'firstin': 1,
            'off': 1,
            'queryName': 'co_id',
            'inpuType': 'co_id',
            'TYPEK': 'all',
            'isnew': 'false',
            'co_id': stock_id,
            'year': str(year_roc),
            'season': f'0{season}',
        }, headers=HEADERS, timeout=60)
        r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, 'lxml')
        out = {}
        for tr in soup.find_all('tr'):
            cells = [td.get_text(strip=True) for td in tr.find_all('td')]
            if len(cells) < 2:
                continue
            name = cells[0]
            if '合約負債' not in name and '總資產' not in name and '資產總計' not in name:
                continue
            for v in cells[1:]:
                s = v.replace(',', '').replace('(', '-').replace(')', '')
                try:
                    out[name] = float(s) * 1000  # MOPS 顯示為千元
                    break
                except ValueError:
                    pass
        return out
    except Exception as e:
        print(f'  MOPS fetch failed for {stock_id}: {e}', flush=True)
        return {}


# ── 股名 ─────────────────────────────────────────────────────────────────────

def load_stock_info(token):
    """抓 TaiwanStockInfo（含產業類別、市場別）。30 天快取。
    回傳 {stock_id: {name, industry, group, market}}.
    """
    if INFO_CACHE.exists():
        try:
            cached = json.loads(INFO_CACHE.read_text('utf-8'))
            if (time.time() - cached.get('ts', 0)) / 86400 < INFO_CACHE_TTL_DAYS and cached.get('info'):
                return cached['info']
        except Exception:
            pass
    print('→ FinMind TaiwanStockInfo …', flush=True)
    rows = finmind_get(token, 'TaiwanStockInfo')
    info = {}
    for r in rows:
        sid = r.get('stock_id')
        if not sid: continue
        info[sid] = {
            'name': r.get('stock_name', ''),
            'industry': r.get('industry_category', '') or '',
            'group': industry_to_group(r.get('industry_category', '')),
            'market': r.get('type', ''),
        }
    INFO_CACHE.write_text(
        json.dumps({'ts': time.time(), 'info': info}, ensure_ascii=False), 'utf-8')
    print(f'  cached {len(info):,} tickers', flush=True)
    return info


def load_big_holders():
    """讀取 big_holders_latest.json，回傳 {code: {delta_400, delta_1000, pct_400, pct_1000}}.
    若檔案不存在或失效則回傳空 dict。
    """
    if not BIG_HOLDERS_LATEST.exists():
        print('  (big_holders_latest.json not found, 略過大戶整合)', flush=True)
        return {}
    try:
        d = json.loads(BIG_HOLDERS_LATEST.read_text('utf-8'))
        out = {}
        for ch in d.get('changes', []):
            out[ch['code']] = {
                'delta_400': ch.get('delta_400'),
                'delta_1000': ch.get('delta_1000'),
                'pct_400': ch.get('pct_400'),
                'pct_1000': ch.get('pct_1000'),
            }
        print(f'  loaded {len(out):,} big-holder records (curr={d.get("curr_date")})', flush=True)
        return out
    except Exception as e:
        print(f'  big_holders load failed: {e}', flush=True)
        return {}


def load_stock_names():
    """共用 generate_big_holders.py 的股名快取。若無則自抓 TWSE ISIN。"""
    if NAMES_CACHE.exists():
        try:
            cached = json.loads(NAMES_CACHE.read_text('utf-8'))
            if cached.get('names'):
                return cached['names']
        except Exception:
            pass
    print('→ fetching stock names from TWSE ISIN …', flush=True)
    names = {}
    for url in (
        'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2',
        'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4',
    ):
        r = requests.get(url, headers=HEADERS, timeout=120)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'lxml')
        for tr in soup.find_all('tr'):
            tds = [t.get_text(strip=True) for t in tr.find_all('td')]
            if not tds or '　' not in tds[0]:
                continue
            code, name = tds[0].split('　', 1)
            if code.strip().isdigit():
                names[code.strip()] = name.strip()
    NAMES_CACHE.write_text(
        json.dumps({'ts': time.time(), 'names': names}, ensure_ascii=False), 'utf-8')
    return names


# ── 季度工具 ─────────────────────────────────────────────────────────────────

def parse_quarter(q):
    """'2025Q4' -> ('2025-12-31', 2025, 4)"""
    year = int(q[:4])
    quarter = int(q[-1])
    end = f'{year}-{QUARTER_END[f"Q{quarter}"]}'
    return end, year, quarter


def quarter_label_from_date(d):
    """'2025-12-31' -> '2025Q4'"""
    y, m, _ = d.split('-')
    return f'{y}Q{(int(m) - 1) // 3 + 1}'


def latest_available_quarter(today=None):
    """根據今天日期推算最近一個應已公布的季度（簡單規則：
    Q1 5/15、Q2 8/14、Q3 11/14、Q4 隔年 3/31 公布）。
    """
    today = today or date.today()
    y, m = today.year, today.month
    if m >= 12 or (m == 11 and today.day >= 14):
        return f'{y}Q3'
    if m >= 9 or (m == 8 and today.day >= 14):
        return f'{y}Q2'
    if m >= 6 or (m == 5 and today.day >= 15):
        return f'{y}Q1'
    if m >= 4:
        return f'{y - 1}Q4'
    return f'{y - 1}Q3'


def prev_quarter(q):
    y, n = int(q[:4]), int(q[-1])
    if n == 1:
        return f'{y - 1}Q4'
    return f'{y}Q{n - 1}'


def shift_quarter(q, n):
    """往前/後 n 個季度"""
    y, qn = int(q[:4]), int(q[-1])
    idx = y * 4 + (qn - 1) + n
    return f'{idx // 4}Q{idx % 4 + 1}'


# ── 快取 ─────────────────────────────────────────────────────────────────────

def snapshot_path(quarter):
    return DATA_DIR / f'{quarter}.json'


def load_snapshot(quarter):
    p = snapshot_path(quarter)
    if not p.exists():
        return None
    return json.loads(p.read_text('utf-8'))


def save_snapshot(quarter, data):
    DATA_DIR.mkdir(exist_ok=True)
    snapshot_path(quarter).write_text(
        json.dumps(data, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'  saved {snapshot_path(quarter).name}', flush=True)


def get_balance_sheet_quarter(token, quarter, force=False):
    """取得單季快照（從快取或 FinMind）。回傳 {stock_id: {CL, TotalAssets}}."""
    if not force:
        snap = load_snapshot(quarter)
        if snap:
            return snap['stocks']
    end_date, _, _ = parse_quarter(quarter)
    rows = fetch_balance_sheet_quarter(token, end_date)
    by_stock = defaultdict(dict)
    for r in rows:
        by_stock[r['stock_id']][r['type']] = r['value']
    out = {}
    for sid, types in by_stock.items():
        if not (sid.isdigit() and len(sid) == 4):
            continue
        if sid.startswith('0'):
            continue
        cl = types.get('CurrentContractLiabilities')
        ta = types.get('TotalAssets')
        if cl is None or ta is None or ta <= 0:
            continue
        out[sid] = {
            'cl': cl,
            'total_assets': ta,
            'cl_to_assets': cl / ta if ta else None,
        }
    save_snapshot(quarter, {'quarter': quarter, 'date': end_date, 'stocks': out})
    time.sleep(FINMIND_SLEEP)
    return out


# ── 計算訊號 ─────────────────────────────────────────────────────────────────

def safe_yoy(curr, prev):
    if prev is None or prev <= 0 or curr is None:
        return None
    return curr / prev - 1


def percentile_rank(value, series):
    """value 在 series 中的百分位（0~1）"""
    s = [x for x in series if x is not None]
    if not s:
        return None
    below = sum(1 for x in s if x < value)
    return below / len(s)


def cap(value, ceiling):
    """限制百分比訊號的上限，避免極小基期拉爆 z-score。"""
    if value is None:
        return None
    return min(value, ceiling)


def build_signals(quarters_data, target_quarter, info=None, big_holders=None):
    """quarters_data: {quarter: {sid: {cl, total_assets, cl_to_assets}}}
    target_quarter: '2025Q4'
    info: {sid: {name, industry, group, market}}
    big_holders: {sid: {delta_400, ...}}
    回傳 [{code, signals...}]
    """
    info = info or {}
    big_holders = big_holders or {}
    target = quarters_data[target_quarter]
    q_yoy = shift_quarter(target_quarter, -4)
    q_prev = shift_quarter(target_quarter, -1)
    q_yoy_prev = shift_quarter(target_quarter, -5)

    results = []
    quarters_sorted = sorted(quarters_data.keys())  # 8 季

    for sid, cur in target.items():
        cl_now = cur['cl']
        if cl_now < MIN_CL_TWD:
            continue
        prev_q = quarters_data.get(q_prev, {}).get(sid, {})
        yoy_q = quarters_data.get(q_yoy, {}).get(sid, {})
        yoy_prev_q = quarters_data.get(q_yoy_prev, {}).get(sid, {})

        cl_yoy_raw = safe_yoy(cl_now, yoy_q.get('cl'))
        cl_qoq_raw = safe_yoy(cl_now, prev_q.get('cl'))
        cl_yoy = cap(cl_yoy_raw, CAP_YOY)
        cl_qoq = cap(cl_qoq_raw, CAP_QOQ)
        cl_yoy_prev_raw = safe_yoy(prev_q.get('cl'), yoy_prev_q.get('cl'))
        cl_yoy_prev = cap(cl_yoy_prev_raw, CAP_YOY)
        cl_2q_accel = (cl_yoy is not None and cl_yoy_prev is not None
                       and cl_yoy > cl_yoy_prev)

        # 8 季百分位
        series = [quarters_data.get(q, {}).get(sid, {}).get('cl')
                  for q in quarters_sorted]
        valid = [x for x in series if x is not None]
        rank8q = percentile_rank(cl_now, valid) if valid else None

        meta = info.get(sid, {})
        bh = big_holders.get(sid, {})

        results.append({
            'code': sid,
            'name': meta.get('name', ''),
            'industry': meta.get('industry', ''),
            'group': meta.get('group', '其他'),
            'market': meta.get('market', ''),
            'cl': cl_now,
            'cl_prev': prev_q.get('cl'),
            'cl_yoy_base': yoy_q.get('cl'),
            'cl_yoy': cl_yoy,
            'cl_yoy_raw': cl_yoy_raw,
            'cl_qoq': cl_qoq,
            'cl_qoq_raw': cl_qoq_raw,
            'cl_yoy_prev': cl_yoy_prev,
            'cl_2q_accel': cl_2q_accel,
            'cl_to_assets': cur['cl_to_assets'],
            'cl_8q_rank': rank8q,
            'cl_history': series,
            'total_assets': cur['total_assets'],
            'bh_delta_400': bh.get('delta_400'),
            'bh_pct_400': bh.get('pct_400'),
        })
    return results


def attach_revenue_and_price(token, items, target_quarter):
    """為前 N 名加掛 TTM 營收、近 3 個月股價漲幅。為節省 API 呼叫，僅對通過 gate 的計算。"""
    end_date, _, _ = parse_quarter(target_quarter)
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    rev_start = (end_dt.replace(year=end_dt.year - 1)).strftime('%Y-%m-%d')

    today_str = date.today().strftime('%Y-%m-%d')
    px_start = (date.today().replace(month=max(date.today().month - 3, 1))
                ).strftime('%Y-%m-%d')

    for it in items:
        sid = it['code']
        # TTM revenue（過去 12 個月）
        try:
            rev_rows = fetch_revenue_single(token, sid, rev_start, end_date)
            ttm_rev = sum(r.get('revenue', 0) or 0 for r in rev_rows[-12:])
            it['ttm_revenue'] = ttm_rev
            it['cl_to_ttm_rev'] = (it['cl'] / ttm_rev) if ttm_rev else None
        except Exception:
            it['ttm_revenue'] = None
            it['cl_to_ttm_rev'] = None
        # 股價（近 3 個月）
        try:
            px_rows = fetch_price_recent(token, sid, px_start, today_str)
            if px_rows:
                first_close = px_rows[0]['close']
                last_close = px_rows[-1]['close']
                it['price'] = last_close
                it['price_3m_chg'] = (last_close / first_close - 1) if first_close else None
            else:
                it['price'] = None
                it['price_3m_chg'] = None
        except Exception:
            it['price'] = None
            it['price_3m_chg'] = None
        time.sleep(FINMIND_SLEEP)
    return items


def zscore(values):
    """非 None 的 z-score；None 給 0。"""
    s = [v for v in values if v is not None]
    if len(s) < 2:
        return [0.0] * len(values)
    mu = statistics.mean(s)
    sd = statistics.pstdev(s)
    if sd == 0:
        return [0.0] * len(values)
    return [(v - mu) / sd if v is not None else 0.0 for v in values]


def score_and_rank(items):
    """套用 gate + 評分。已用 cap 過的 cl_yoy / cl_qoq 進入 z-score。"""
    gated = []
    for it in items:
        if it['cl'] < MIN_CL_TWD: continue
        if it['cl_yoy'] is None or it['cl_yoy'] < MIN_CL_YOY: continue
        if it['cl_to_assets'] is None or it['cl_to_assets'] < MIN_CL_TO_ASSETS: continue
        if it['cl_8q_rank'] is None or it['cl_8q_rank'] < MIN_8Q_RANK: continue
        gated.append(it)

    if not gated:
        return []

    z_yoy = zscore([x['cl_yoy'] for x in gated])
    z_qoq = zscore([x['cl_qoq'] for x in gated])
    z_to_rev = zscore([x.get('cl_to_ttm_rev') for x in gated])
    z_to_assets = zscore([x['cl_to_assets'] for x in gated])
    z_bh = zscore([x.get('bh_delta_400') for x in gated])

    for i, it in enumerate(gated):
        it['score'] = (
            W_YOY * z_yoy[i] + W_QOQ * z_qoq[i]
            + W_TO_REV * z_to_rev[i] + W_TO_ASSETS * z_to_assets[i]
            + W_ACCEL * (1.0 if it['cl_2q_accel'] else 0.0)
            + W_BIGHOLDER * z_bh[i]
        )
        # 也記錄各權重貢獻供 debug
        it['score_breakdown'] = {
            'yoy': W_YOY * z_yoy[i],
            'qoq': W_QOQ * z_qoq[i],
            'to_rev': W_TO_REV * z_to_rev[i],
            'to_assets': W_TO_ASSETS * z_to_assets[i],
            'accel': W_ACCEL * (1.0 if it['cl_2q_accel'] else 0.0),
            'bigholder': W_BIGHOLDER * z_bh[i],
        }

    gated.sort(key=lambda x: x['score'], reverse=True)
    return gated


# ── HTML 渲染 ────────────────────────────────────────────────────────────────

CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--pp:#7c3aed;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b;--card:#fff;--hover:#eef2ff}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:14px;line-height:1.5}
.hdr{background:linear-gradient(135deg,#0f172a 0%,#1e3a8a 50%,#2563eb 100%);color:#fff;padding:22px 28px 16px}
.hdr h1{font-size:20px;font-weight:800;margin-bottom:4px;letter-spacing:-0.3px}
.hdr .sub{font-size:11.5px;opacity:.85}
.nav-link{color:#93c5fd;font-size:11.5px;text-decoration:none;margin-left:14px;border-bottom:1px dashed #93c5fd;padding-bottom:1px}
.stats{display:flex;gap:12px;padding:16px 28px;background:var(--card);border-bottom:1px solid var(--brd);flex-wrap:wrap}
.sc{background:var(--bg);border:1px solid var(--brd);border-radius:10px;padding:14px 20px;min-width:130px;border-left:3px solid var(--bl)}
.sc .n{font-size:24px;font-weight:800;color:var(--bl);letter-spacing:-0.5px}.sc .l{font-size:11px;color:var(--mu);margin-top:3px;font-weight:500}
.sc.gr{border-left-color:var(--gr)}.sc.gr .n{color:var(--gr)}
.sc.am{border-left-color:var(--am)}.sc.am .n{color:var(--am)}
.sc.pp{border-left-color:var(--pp)}.sc.pp .n{color:var(--pp)}
.tabs{display:flex;padding:0 28px;border-bottom:2px solid var(--brd);background:var(--card);gap:4px;flex-wrap:wrap}
.tab{padding:11px 16px;cursor:pointer;border-bottom:3px solid transparent;font-size:12.5px;font-weight:600;color:var(--mu);margin-bottom:-2px;transition:all .15s;border-radius:6px 6px 0 0;background:none;border-left:none;border-right:none;border-top:none;font-family:inherit}
.tab:hover{background:#f1f5f9;color:var(--txt)}
.tab.active{border-bottom-color:var(--bl);color:var(--bl)}
.tab .cnt{display:inline-block;background:#e2e8f0;color:var(--mu);font-size:10.5px;padding:1px 7px;border-radius:8px;margin-left:5px;font-weight:600}
.tab.active .cnt{background:#dbeafe;color:var(--bl)}
.pane{padding:20px 28px}.pane{display:none}.pane.active{display:block}
.box{background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:12px 16px;font-size:12.5px;color:#1e40af;margin-bottom:18px;line-height:1.7}
.box.warn{background:#fffbeb;border-color:#fde68a;color:#92400e}
.box.bt{background:#faf5ff;border-color:#ddd6fe;color:#6b21a8}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06);margin-bottom:16px}
th{background:#edf2f7;font-weight:700;color:var(--mu);font-size:11px;text-transform:uppercase;padding:10px 8px;text-align:left;border-bottom:2px solid var(--brd);white-space:nowrap;letter-spacing:0.3px}
td{padding:8px 8px;border-bottom:1px solid var(--brd);vertical-align:middle;font-size:13px}
tr:last-child td{border-bottom:none}
tr:hover td{background:var(--hover)}
.num{text-align:right;font-variant-numeric:tabular-nums}
.center{text-align:center}
.rank{text-align:center;color:var(--mu);font-size:11px;width:36px}
.code{font-weight:700;color:var(--bl);font-variant-numeric:tabular-nums}
.code a, .nm-link{color:inherit;text-decoration:none;border-bottom:1px dashed transparent;transition:border-color .15s, color .15s}
.code a:hover, .nm-link:hover{border-bottom-color:currentColor;color:#1d4ed8}
.code a::after{content:'↗';font-size:9px;margin-left:3px;opacity:.5;font-weight:400}
.nm-link{font-weight:600;color:var(--txt)}
.nm-link:hover{color:var(--bl)}
.up{color:var(--gr);font-weight:700}.dn{color:var(--rd);font-weight:700}
.spark{display:inline-block;vertical-align:middle}
.score{font-weight:700;color:var(--pp)}
.badge{display:inline-block;padding:2px 7px;border-radius:5px;font-size:10.5px;font-weight:600;letter-spacing:0.2px;white-space:nowrap}
.badge.accel{background:#dcfce7;color:#15803d}
.badge.ind{background:#e0e7ff;color:#4338ca}
.badge.new{background:#fef3c7;color:#b45309;border:1px solid #fde68a}
/* Hero */
.hero{padding:24px 28px;background:linear-gradient(180deg,#fafbff 0%,#fff 100%);border-bottom:1px solid var(--brd)}
.hero-grid{display:grid;grid-template-columns:1fr 280px;gap:20px}
.hero-left .picks{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:10px}
.hero-card-link{text-decoration:none;color:inherit;display:block}
.hero-card{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 16px;border-top:3px solid var(--bl);transition:transform .15s,box-shadow .15s;cursor:pointer;position:relative}
.hero-card:hover{transform:translateY(-2px);box-shadow:0 4px 16px rgba(0,0,0,.08);border-color:var(--bl)}
.hero-card::after{content:'↗';position:absolute;top:10px;right:14px;color:var(--mu);font-size:14px;opacity:.4;transition:opacity .15s}
.hero-card:hover::after{opacity:1;color:var(--bl)}
.hero-card .rk{font-size:11px;color:var(--mu);font-weight:600;letter-spacing:0.4px}
.hero-card .nm{font-size:16px;font-weight:800;margin:3px 0 2px;color:var(--txt)}
.hero-card .nm .c{color:var(--bl);font-variant-numeric:tabular-nums;margin-right:6px}
.hero-card .ind{font-size:10.5px;color:var(--mu);margin-bottom:8px}
.hero-card .sig{display:flex;gap:8px;font-size:11.5px;margin-bottom:6px}
.hero-card .sig b{color:var(--gr);font-weight:700}
.hero-card .why{font-size:12px;color:#475569;line-height:1.55;margin-top:6px;border-top:1px dashed var(--brd);padding-top:6px}
.hero-right{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 16px}
.hero-right h4{font-size:13px;font-weight:700;color:var(--pp);margin-bottom:8px}
.hero-right .next{font-size:24px;font-weight:800;color:var(--pp);margin:6px 0 2px}
.hero-right .due{font-size:11.5px;color:var(--mu)}
.hero-right ul{list-style:none;margin-top:10px;padding-top:10px;border-top:1px solid var(--brd);font-size:11.5px;color:#475569;line-height:1.85}
.hero-right ul li{padding:1px 0}
.hero-title{font-size:14px;font-weight:700;color:var(--txt);display:flex;align-items:center;gap:8px}
.hero-title .pill{background:var(--pp);color:#fff;border-radius:5px;padding:2px 8px;font-size:10.5px;font-weight:600;letter-spacing:0.3px}
/* 巨漢案例 panel */
.case{background:linear-gradient(135deg,#fef3c7 0%,#fde68a 100%);border:1px solid #f59e0b;border-radius:12px;padding:16px 20px;margin:18px 0;position:relative;overflow:hidden}
.case::before{content:'CASE';position:absolute;top:8px;right:14px;background:#92400e;color:#fff;font-size:10px;font-weight:700;padding:3px 9px;border-radius:5px;letter-spacing:0.5px}
.case h3{font-size:14px;font-weight:800;color:#92400e;margin-bottom:6px}
.case .sub{font-size:11.5px;color:#78350f;margin-bottom:12px;line-height:1.65}
.case-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.case-chart{background:rgba(255,255,255,.7);border-radius:8px;padding:10px}
.case-chart h5{font-size:11px;font-weight:700;color:#78350f;margin-bottom:6px;letter-spacing:0.3px;text-transform:uppercase}
.case-stats{font-size:11.5px;line-height:2;color:#78350f}
.case-stats b{color:#15803d;font-weight:700}
/* 持續追蹤 */
.tracking{margin-top:18px;background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:16px 20px}
.tracking h3{font-size:14px;font-weight:700;color:var(--bl);margin-bottom:4px;display:flex;align-items:center;gap:8px}
.tracking .sub{font-size:11.5px;color:var(--mu);margin-bottom:12px}
/* sticky code col on mobile */
.sticky-code{position:sticky;left:0;background:var(--card);z-index:1}
tr:hover .sticky-code{background:var(--hover)}
.scroll-hint{display:none;text-align:center;font-size:11px;color:var(--mu);padding:6px;background:#f1f5f9;border-radius:6px;margin-bottom:6px}
.ft{text-align:center;color:var(--mu);font-size:11.5px;padding:20px 28px;border-top:1px solid var(--brd);background:var(--card);line-height:1.8}
.ft a{color:var(--bl);text-decoration:none}
@media(max-width:900px){
  .hdr,.stats,.pane,.hero{padding-left:14px;padding-right:14px}
  .tabs{padding:0 14px;overflow-x:auto;flex-wrap:nowrap}
  th,td{padding:7px 5px;font-size:12.5px}
  .sc{min-width:110px}
  .hero-grid{grid-template-columns:1fr}
  .hero-left .picks{grid-template-columns:1fr}
  .case-grid{grid-template-columns:1fr}
  .scroll-hint{display:block}
  table{display:block;overflow-x:auto}
}
"""


def fmt_twd(v):
    if v is None: return '—'
    if abs(v) >= 1e8: return f'{v / 1e8:.2f}億'
    if abs(v) >= 1e4: return f'{v / 1e4:.0f}萬'
    return f'{v:,.0f}'


def fmt_pct(v, digits=1):
    if v is None: return '—'
    return f'{v * 100:.{digits}f}%'


def fmt_chg(v):
    if v is None: return '—'
    cls = 'up' if v > 0 else ('dn' if v < 0 else '')
    sign = '+' if v > 0 else ''
    return f'<span class="{cls}">{sign}{v * 100:.1f}%</span>'


def sparkline(values, w=84, h=22):
    """SVG mini spark；忽略 None。"""
    s = [v for v in values if v is not None]
    if len(s) < 2:
        return ''
    lo, hi = min(s), max(s)
    rng = hi - lo or 1
    pts = []
    n = len(values)
    for i, v in enumerate(values):
        if v is None: continue
        x = i * w / (n - 1)
        y = h - (v - lo) / rng * h
        pts.append(f'{x:.1f},{y:.1f}')
    last_color = '#16a34a' if values[-1] and values[-1] >= (s[-2] if len(s) > 1 else 0) else '#dc2626'
    poly = ' '.join(pts)
    last_x = (n - 1) * w / (n - 1)
    last_v = values[-1]
    last_y = h - (last_v - lo) / rng * h if last_v is not None else h / 2
    return (f'<svg class="spark" width="{w}" height="{h}" viewBox="0 0 {w} {h}">'
            f'<polyline points="{poly}" fill="none" stroke="#2563eb" stroke-width="1.5"/>'
            f'<circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="2" fill="{last_color}"/>'
            f'</svg>')


def goodinfo_url(code):
    return f'https://goodinfo.tw/tw/StockDetail.asp?STOCK_ID={code}'


def _render_row(i, it):
    accel = '<span class="badge accel">加速</span>' if it['cl_2q_accel'] else ''
    new_badge = '<span class="badge new">🆕</span>' if it.get('_new') else ''
    ind_badge = (f'<span class="badge ind">{it.get("group", "其他")}</span>'
                 if it.get('group') else '')
    bh = it.get('bh_delta_400')
    bh_html = fmt_chg(bh / 100) if bh is not None else '—'
    code = it['code']
    name = it.get('name', '')
    gi = goodinfo_url(code)
    return f'''<tr>
<td class="rank">{i}</td>
<td class="code sticky-code"><a href="{gi}" target="_blank" rel="noopener" title="在 Goodinfo 開啟 {code} {name}">{code}</a>{new_badge}</td>
<td><a class="nm-link" href="{gi}" target="_blank" rel="noopener" title="在 Goodinfo 開啟 {code} {name}">{name}</a><br>{ind_badge}</td>
<td class="num">{fmt_twd(it['cl'])}</td>
<td class="num">{fmt_twd(it.get('cl_yoy_base'))}</td>
<td class="num">{fmt_chg(it['cl_yoy'])}</td>
<td class="num">{fmt_chg(it['cl_qoq'])} {accel}</td>
<td class="num">{fmt_pct(it['cl_to_assets'])}</td>
<td class="num">{fmt_pct(it.get('cl_to_ttm_rev'))}</td>
<td class="center">{sparkline(it['cl_history'])}</td>
<td class="num">{bh_html}</td>
<td class="num">{it.get('price') or '—'}</td>
<td class="num">{fmt_chg(it.get('price_3m_chg'))}</td>
<td class="num score">{it['score']:+.2f}</td>
</tr>'''


def _render_table(items, empty_msg='本分組無符合條件個股'):
    if not items:
        return f'<div style="text-align:center;color:var(--mu);padding:30px;background:var(--card);border-radius:10px">{empty_msg}</div>'
    rows = [_render_row(i, it) for i, it in enumerate(items, 1)]
    return f'''<table>
<thead><tr>
  <th class="rank">#</th><th>代號</th><th>名稱／產業</th>
  <th class="num">本季CL</th><th class="num">去年同期</th>
  <th class="num">YoY</th><th class="num">QoQ</th>
  <th class="num">CL/總資產</th><th class="num">CL/TTM營收</th>
  <th class="center">8季趨勢</th>
  <th class="num">大戶週變</th>
  <th class="num">收盤</th><th class="num">近3月</th>
  <th class="num">分數</th>
</tr></thead>
<tbody>
{chr(10).join(rows)}
</tbody></table>'''


def _render_backtest_panel():
    """讀取 BACKTEST_JSON 渲染回測 summary panel；若不存在則回傳提示。"""
    if not BACKTEST_JSON.exists():
        return ('<div class="box warn">尚未跑過回測。執行 '
                '<code>python generate_contract_liab.py --backtest 2024Q1:2025Q3</code> '
                '產生回測結果。</div>')
    try:
        bt = json.loads(BACKTEST_JSON.read_text('utf-8'))
    except Exception as e:
        return f'<div class="box warn">回測資料讀取失敗：{e}</div>'

    def fmt_stat(s):
        if not s: return '<td colspan="4" style="text-align:center;color:var(--mu)">無資料</td>'
        return (f'<td class="num">{s["n"]}</td>'
                f'<td class="num {"up" if s["mean"] > 0 else "dn"}">{s["mean"] * 100:+.2f}%</td>'
                f'<td class="num">{s["median"] * 100:+.2f}%</td>'
                f'<td class="num">{s["win_rate"] * 100:.0f}%</td>')

    summary_table = f'''<table>
<thead><tr><th>持有期</th><th class="num">樣本數</th><th class="num">平均報酬</th>
<th class="num">中位數</th><th class="num">勝率</th></tr></thead>
<tbody>
<tr><td><b>1個月 (~20日)</b></td>{fmt_stat(bt.get("stats_1m"))}</tr>
<tr><td><b>3個月 (~60日)</b></td>{fmt_stat(bt.get("stats_3m"))}</tr>
<tr><td><b>6個月 (~120日)</b></td>{fmt_stat(bt.get("stats_6m"))}</tr>
</tbody></table>'''

    # 個股 picks 表
    picks = bt.get('picks', [])
    pick_rows = []
    for p in picks:
        gi = goodinfo_url(p['code'])
        pick_rows.append(f'''<tr>
<td>{p['quarter']}</td><td>{p['entry_date']}</td>
<td class="code"><a href="{gi}" target="_blank" rel="noopener">{p['code']}</a></td>
<td><a class="nm-link" href="{gi}" target="_blank" rel="noopener">{p.get('name', '')}</a></td>
<td class="num">{fmt_chg(p.get('cl_yoy_raw') or p.get('cl_yoy'))}</td>
<td class="num">{fmt_twd(p.get('cl'))}</td>
<td class="num">{fmt_chg(p.get('ret_1m'))}</td>
<td class="num">{fmt_chg(p.get('ret_3m'))}</td>
<td class="num">{fmt_chg(p.get('ret_6m'))}</td>
</tr>''')
    picks_table = f'''<table>
<thead><tr><th>季度</th><th>進場日</th><th>代號</th><th>名稱</th>
<th class="num">當時 YoY</th><th class="num">當時 CL</th>
<th class="num">+1M</th><th class="num">+3M</th><th class="num">+6M</th>
</tr></thead><tbody>
{chr(10).join(pick_rows) if pick_rows else '<tr><td colspan="9" style="text-align:center;color:var(--mu);padding:30px">無回測資料</td></tr>'}
</tbody></table>'''

    return f'''<div class="box bt">
<b>回測區間</b>：{bt['start_quarter']} → {bt['end_quarter']}（每季取前 {bt['top_n']} 名，
共 {bt['total_picks']} 筆建倉樣本）
<br><b>進場規則</b>：每季財報法定截止日 (Q1=5/15, Q2=8/14, Q3=11/14, Q4 次年 3/31) 等權買進
<br><b>持有規則</b>：固定持有 1/3/6 個月後計算報酬，不停損
</div>
<h3 style="font-size:14px;margin:18px 0 10px;color:var(--pp)">📈 績效統計</h3>
{summary_table}
<h3 style="font-size:14px;margin:24px 0 10px;color:var(--pp)">🎯 歷史進場明細（依季度排序）</h3>
{picks_table}'''


# ── 「閱讀模式」helpers ──────────────────────────────────────────────────────

def next_publish_date(today=None):
    """根據今天，回傳下個財報截止日 (Q1=5/15, Q2=8/14, Q3=11/14, Q4 次年 3/31) 與對應季度。"""
    today = today or date.today()
    candidates = [
        (date(today.year, 5, 15), f'{today.year}Q1 截止'),
        (date(today.year, 8, 14), f'{today.year}Q2 截止'),
        (date(today.year, 11, 14), f'{today.year}Q3 截止'),
        (date(today.year + 1, 3, 31), f'{today.year}Q4 截止'),
    ]
    for d, label in candidates:
        if d > today:
            days = (d - today).days
            return d, label, days
    return None, '—', 0


def diff_new_entries(curr_codes, prev_quarter):
    """讀上一季的 latest snapshot 比對。回傳「本季新進榜」的 code set。
    若上一季沒有 ranked 快照（預設只存最新），則用 contract_liab_data/<prev>_ranked.json。"""
    snap = ROOT / 'contract_liab_data' / f'{prev_quarter}_ranked.json'
    if not snap.exists():
        return set()
    try:
        prev_codes = set(json.loads(snap.read_text('utf-8')).get('codes', []))
    except Exception:
        return set()
    return curr_codes - prev_codes


def save_ranked_snapshot(quarter, ranked):
    """把當季 top 50 的 code 列表存起來，下季拿來比對「新進榜」。"""
    DATA_DIR.mkdir(exist_ok=True)
    p = DATA_DIR / f'{quarter}_ranked.json'
    p.write_text(
        json.dumps({'quarter': quarter, 'codes': [it['code'] for it in ranked[:TOP_N]]},
                   ensure_ascii=False), 'utf-8')


# 巨漢 6903 案例：固定資料（從 FinMind 撈過一次後 hardcode）
JUHAN_CASE = {
    'code': '6903', 'name': '巨漢',
    'cl_history': [20_300_000, 49_630_000, 49_440_000, 95_570_000,
                    312_068_000, 381_242_000, 511_005_000, 627_454_000],
    'price_quarterly': [178.5, 192.5, 151.0, 131.5,
                         117.5, 110.0, 136.5, 237.0],
    'quarters': ['2024Q1', '2024Q2', '2024Q3', '2024Q4',
                  '2025Q1', '2025Q2', '2025Q3', '2025Q4'],
    'price_latest': 425.0,  # 2026-04-28
    'first_signal_q': '2025Q1',
    'first_signal_price': 117.5,
    'breakout_low': 99.5,  # 2025-07
    'gain_from_low': 425.0 / 99.5 - 1,  # +327%
    'gain_from_first_signal': 425.0 / 117.5 - 1,  # +262%
}


def _hero_card(rank, item, is_new=False):
    """單張 hero pick 卡。"""
    name = item.get('name', '')
    code = item['code']
    grp = item.get('group', '其他')
    yoy_raw = item.get('cl_yoy_raw') or item.get('cl_yoy', 0)
    cl_b = item['cl'] / 1e8
    bh = item.get('bh_delta_400')
    accel_label = '加速' if item.get('cl_2q_accel') else ''
    new_badge = '<span class="badge new" style="margin-left:6px">🆕 新進榜</span>' if is_new else ''
    bh_label = f'大戶 +{bh:.2f}pp' if bh and bh > 0 else (f'大戶 {bh:.2f}pp' if bh else '')

    # 一句話 takeaway
    why = []
    if yoy_raw and yoy_raw >= 5:
        why.append(f'CL 暴增 {yoy_raw * 100:.0f}%')
    elif yoy_raw and yoy_raw >= 1:
        why.append(f'YoY +{yoy_raw * 100:.0f}%')
    if cl_b >= 50:
        why.append(f'規模 {cl_b:.0f} 億')
    elif cl_b >= 5:
        why.append(f'規模 {cl_b:.1f} 億')
    if accel_label:
        why.append('連 2 季加速')
    if bh and bh > 0.3:
        why.append(f'大戶買超 {bh:.1f}pp')
    why_text = ' · '.join(why[:3])

    gi = goodinfo_url(code)
    return f'''<a href="{gi}" target="_blank" rel="noopener" class="hero-card-link" title="在 Goodinfo 開啟 {code} {name}">
<div class="hero-card">
  <div class="rk">第 {rank} 名 · score {item['score']:+.2f}</div>
  <div class="nm"><span class="c">{code}</span>{name}{new_badge}</div>
  <div class="ind">{grp} · {item.get('industry', '')}</div>
  <div class="sig">
    <span>YoY <b>+{(yoy_raw or 0) * 100:.0f}%</b></span>
    <span>CL <b>{cl_b:.1f}億</b></span>
  </div>
  <div class="why">{why_text or '訊號齊全，建議觀察'}</div>
</div>
</a>'''


def _render_hero(latest, new_codes):
    """頂部 hero：top 3 picks + 下次更新 + 策略一句話。"""
    items = latest['ranked']
    top3 = items[:3]
    quarter = latest['quarter']

    next_date, next_label, days = next_publish_date()
    next_str = next_date.strftime('%Y-%m-%d') if next_date else '—'

    cards_html = ''.join(_hero_card(i + 1, it, is_new=(it['code'] in new_codes))
                          for i, it in enumerate(top3))

    return f'''<div class="hero">
<div class="hero-grid">
  <div class="hero-left">
    <div class="hero-title">🏆 本季前 3 強 <span class="pill">{quarter}</span>
      <span style="font-size:11px;color:var(--mu);font-weight:500;margin-left:auto">合約負債暴增 + 通過全部篩選 · 依綜合分數排序</span>
    </div>
    <div class="picks">{cards_html}</div>
  </div>
  <div class="hero-right">
    <h4>📅 下次資料更新</h4>
    <div class="next">{next_str}</div>
    <div class="due">{next_label} · 約 {days} 天後</div>
    <ul>
      <li>· 公布日當週末本頁面會更新</li>
      <li>· Q1 報 5/15 · Q2 報 8/14</li>
      <li>· Q3 報 11/14 · Q4 次年 3/31</li>
    </ul>
  </div>
</div>
</div>'''


def _render_juhan_case():
    """巨漢 6903 案例 panel — 固定教學區，不隨資料變動。"""
    c = JUHAN_CASE
    cl_arr = c['cl_history']
    px_arr = c['price_quarterly']
    qs = c['quarters']

    # 小條形圖 (CL)
    cl_max = max(cl_arr)
    cl_bars = ''
    for q, v in zip(qs, cl_arr):
        h = (v / cl_max) * 60 + 4
        col = '#dc2626' if q == c['first_signal_q'] else '#92400e'
        cl_bars += (f'<div style="display:flex;flex-direction:column;align-items:center;gap:3px;flex:1">'
                    f'<div style="width:100%;background:{col};height:{h}px;border-radius:3px 3px 0 0"></div>'
                    f'<div style="font-size:9px;color:#78350f">{q[2:]}</div></div>')
    cl_chart = f'<div style="display:flex;align-items:flex-end;gap:4px;height:80px">{cl_bars}</div>'

    # 小折線圖 (價格)
    px_min, px_max = min(px_arr + [c['price_latest']]), max(px_arr + [c['price_latest']])
    rng = px_max - px_min or 1
    pts = []
    n = len(px_arr) + 1
    for i, v in enumerate(px_arr):
        x = i * 100 / (n - 1)
        y = 80 - (v - px_min) / rng * 70
        pts.append(f'{x:.1f},{y:.1f}')
    # 加上最新價
    x_last = (n - 1) * 100 / (n - 1)
    y_last = 80 - (c['price_latest'] - px_min) / rng * 70
    pts.append(f'{x_last:.1f},{y_last:.1f}')
    poly = ' '.join(pts)
    px_chart = (f'<svg width="100%" height="80" viewBox="0 0 100 80" preserveAspectRatio="none">'
                f'<polyline points="{poly}" fill="none" stroke="#dc2626" stroke-width="1.5"/>'
                f'<circle cx="{x_last:.1f}" cy="{y_last:.1f}" r="2.5" fill="#dc2626"/>'
                f'</svg>')

    return f'''<div class="case">
<h3>📚 案例：巨漢 (6903) — 為什麼這個策略有用</h3>
<div class="sub">2025Q1 財報 (5月公布) 合約負債從 9,557 萬暴增到 <b>3.12 億 (+225% QoQ, +1438% YoY)</b>，
策略當期就標記為買進訊號。當時股價 117.5。隨後巨漢主升段啟動，至今 425 元（半年漲 4 倍）。</div>
<div class="case-grid">
  <div class="case-chart">
    <h5>合約負債 8 季走勢（紅柱 = 首次訊號）</h5>
    {cl_chart}
    <div style="font-size:10.5px;color:#78350f;margin-top:6px">2,030 萬 → <b style="color:#dc2626">6.27 億</b> (約 31 倍)</div>
  </div>
  <div class="case-chart">
    <h5>季底股價（紅點 = 最新 425 元）</h5>
    {px_chart}
    <div style="font-size:10.5px;color:#78350f;margin-top:6px">首次訊號日 117.5 → <b style="color:#dc2626">425 (+262%)</b></div>
  </div>
</div>
<div class="case-stats" style="margin-top:10px">
  • 進場日 (2025-05-15)：117.5 → 現價 425 = <b>+{c['gain_from_first_signal'] * 100:.0f}%</b><br>
  • 7月低點 99.5 → 現價 425 = <b>+{c['gain_from_low'] * 100:.0f}%</b>（後段彈性更大）<br>
  • 從第一次合約負債暴增 → 主升段啟動約 4 個月（典型「領先指標 → 反映」時差）
</div>
</div>'''


def _render_tracking_panel():
    """讀取 backtest JSON，顯示最近一季已建倉 picks 的當前報酬。
    這是「固定時間閱讀」最有用的區塊：上一季 picks 表現追蹤。"""
    if not BACKTEST_JSON.exists():
        return ''
    try:
        bt = json.loads(BACKTEST_JSON.read_text('utf-8'))
    except Exception:
        return ''
    picks = bt.get('picks', [])
    if not picks:
        return ''

    # 最近一季的 picks
    last_q = max(p['quarter'] for p in picks)
    last_picks = [p for p in picks if p['quarter'] == last_q]

    # 計算統計
    rs6 = [p['ret_6m'] for p in last_picks if p.get('ret_6m') is not None]
    avg_6m = sum(rs6) / len(rs6) if rs6 else 0
    win = sum(1 for r in rs6 if r > 0) / len(rs6) if rs6 else 0
    entry = last_picks[0]['entry_date'] if last_picks else ''

    # 表格
    rows = []
    for i, p in enumerate(sorted(last_picks, key=lambda x: x.get('ret_6m') or -999, reverse=True), 1):
        r6 = fmt_chg(p.get('ret_6m'))
        r3 = fmt_chg(p.get('ret_3m'))
        r1 = fmt_chg(p.get('ret_1m'))
        gi = goodinfo_url(p['code'])
        rows.append(f'''<tr>
<td class="rank">{i}</td>
<td class="code"><a href="{gi}" target="_blank" rel="noopener">{p['code']}</a></td>
<td><a class="nm-link" href="{gi}" target="_blank" rel="noopener">{p.get('name', '')}</a></td>
<td class="num">{r1}</td>
<td class="num">{r3}</td>
<td class="num"><b>{r6}</b></td>
</tr>''')

    return f'''<div class="tracking">
<h3>📈 上季 ({last_q}) picks 後續追蹤
  <span class="pill" style="background:var(--bl);color:#fff;border-radius:5px;padding:2px 8px;font-size:10.5px">已實現</span>
</h3>
<div class="sub">進場日 {entry}（每季財報截止日等權買進）·
平均 6 個月報酬 <b style="color:{'var(--gr)' if avg_6m > 0 else 'var(--rd)'}">{avg_6m * 100:+.1f}%</b> ·
勝率 <b>{win * 100:.0f}%</b></div>
<table>
<thead><tr>
  <th class="rank">#</th><th>代號</th><th>名稱</th>
  <th class="num">+1個月</th><th class="num">+3個月</th><th class="num">+6個月</th>
</tr></thead><tbody>
{chr(10).join(rows)}
</tbody></table>
</div>'''


def render_html(latest):
    quarter = latest['quarter']
    items = latest['ranked']
    quarter_list = latest.get('quarter_list', [])

    # 統計卡
    stat_total = latest.get('universe_size', 0)
    stat_passed = len(items)
    stat_avg_yoy = (sum(it['cl_yoy'] for it in items) / len(items)) if items else 0
    stat_top_yoy = max((it['cl_yoy'] for it in items), default=0)

    # 「新進榜」比對：與上一季比
    prev_q = shift_quarter(quarter, -1)
    curr_codes = set(it['code'] for it in items[:TOP_N])
    new_codes = diff_new_entries(curr_codes, prev_q)

    # 標記 ranked 中的 _new
    for it in items[:TOP_N]:
        it['_new'] = it['code'] in new_codes

    # 產業分組
    groups = defaultdict(list)
    for it in items:
        groups[it.get('group', '其他')].append(it)
    tab_order = ['全部', '半導體鏈', '機電設備', '建設工程', '生技醫療', '傳產內需', '金融', '其他', '回測']
    quarter_str = ' → '.join(quarter_list) if quarter_list else quarter

    # 渲染 tabs
    tab_html = []
    pane_html = []
    for tk in tab_order:
        if tk == '全部':
            cnt = len(items)
            content = _render_table(items[:TOP_N])
        elif tk == '回測':
            cnt = ''
            content = _render_backtest_panel()
        else:
            grp = groups.get(tk, [])
            cnt = len(grp)
            if cnt == 0:
                continue
            content = _render_table(grp[:TOP_N], empty_msg=f'{tk}分組無符合條件個股')
        cnt_html = f'<span class="cnt">{cnt}</span>' if cnt != '' else ''
        active = ' active' if tk == '全部' else ''
        tab_html.append(f'<button class="tab{active}" data-tab="{tk}">{tk}{cnt_html}</button>')
        pane_html.append(f'<div class="pane{active}" data-pane="{tk}">{content}</div>')

    # 三大新閱讀區塊
    hero_html = _render_hero(latest, new_codes)
    case_html = _render_juhan_case()
    tracking_html = _render_tracking_panel()

    return f'''<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>合約負債策略 — {quarter}</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>📊 台股合約負債策略 — {quarter}</h1>
  <div class="sub">合約負債 (IFRS 15) = 客戶已付錢、公司還沒認列的營收 → 未來營收的領先指標
    <a class="nav-link" href="big_holders_index.html">大戶持股</a>
    <a class="nav-link" href="tw_market.html">台股總覽</a>
    <a class="nav-link" href="index.html">可轉債</a>
  </div>
</div>
{hero_html}
<div class="stats">
  <div class="sc"><div class="n">{stat_total:,}</div><div class="l">掃描股數</div></div>
  <div class="sc gr"><div class="n">{stat_passed}</div><div class="l">通過篩選</div></div>
  <div class="sc am"><div class="n">{stat_avg_yoy * 100:.0f}%</div><div class="l">榜單平均 YoY</div></div>
  <div class="sc pp"><div class="n">{stat_top_yoy * 100:.0f}%</div><div class="l">榜首 YoY</div></div>
</div>
<div class="tabs">
  {chr(10).join(tab_html)}
</div>
<div style="padding:20px 28px">
  {case_html}
  {tracking_html}
  <div class="box" style="margin-top:18px">
    <b>策略邏輯</b>：合約負債暴增代表客戶大量預付，未來 1~3 季營收將被推升。
    篩選條件 → 合約負債 ≥ {MIN_CL_TWD / 1e8:.1f}億、YoY ≥ {MIN_CL_YOY * 100:.0f}%、占總資產 ≥ {MIN_CL_TO_ASSETS * 100:.0f}%、站上 8 季 {MIN_8Q_RANK * 100:.0f} 百分位。
    YoY/QoQ 上限封頂 {CAP_YOY * 100:.0f}% / {CAP_QOQ * 100:.0f}%（避免極小基期拉爆 z-score）。
    評分權重：YoY {W_YOY * 100:.0f}% / QoQ {W_QOQ * 100:.0f}% / CL/TTM營收 {W_TO_REV * 100:.0f}% / CL/總資產 {W_TO_ASSETS * 100:.0f}% / 連續加速 {W_ACCEL * 100:.0f}% / 大戶買超 {W_BIGHOLDER * 100:.0f}%。
    <br><b>資料區間</b>：{quarter_str}（FinMind 為主、MOPS 為備援）
  </div>
  <div style="font-size:11.5px;color:var(--mu);margin:0 0 8px;text-align:right">💡 點代號或名稱可在 Goodinfo 開啟個股財務資料</div>
  <div class="scroll-hint">← 表格可橫向滑動 →</div>
  {chr(10).join(pane_html)}
</div>
<div class="ft">
  資料源：<a href="https://finmindtrade.com/" target="_blank">FinMind</a>（贊助會員）·
  備援 <a href="https://mops.twse.com.tw/" target="_blank">MOPS</a> ·
  <a href="https://goodinfo.tw/" target="_blank">Goodinfo</a><br>
  生成於 {datetime.now().strftime('%Y-%m-%d %H:%M')}　·　僅供研究參考，非投資建議
</div>
<script>
document.querySelectorAll('.tab').forEach(t => {{
  t.addEventListener('click', () => {{
    const k = t.dataset.tab;
    document.querySelectorAll('.tab').forEach(x => x.classList.toggle('active', x.dataset.tab === k));
    document.querySelectorAll('.pane').forEach(x => x.classList.toggle('active', x.dataset.pane === k));
  }});
}});
</script>
</body></html>
'''


# ── 主流程 ───────────────────────────────────────────────────────────────────

def run(target_quarter, force=False, skip_render=False):
    token = load_token()
    names = load_stock_names()
    info = load_stock_info(token)
    big_holders = load_big_holders()

    # 撈 8 季資料 (target 及前 7 季)
    quarters = [shift_quarter(target_quarter, -i) for i in range(7, -1, -1)]
    print(f'\n=== Target: {target_quarter}, fetching {quarters[0]} → {quarters[-1]} ===\n')

    quarters_data = {}
    for q in quarters:
        quarters_data[q] = get_balance_sheet_quarter(token, q, force=force)

    universe_size = len(quarters_data[target_quarter])
    print(f'\n→ universe size at {target_quarter}: {universe_size}')

    # 建立訊號
    items = build_signals(quarters_data, target_quarter, info=info, big_holders=big_holders)
    print(f'→ items with CL ≥ {MIN_CL_TWD / 1e8:.1f}億: {len(items)}')

    # gate
    pre_gated = [it for it in items
                 if it['cl_yoy'] is not None and it['cl_yoy'] >= MIN_CL_YOY
                 and it['cl_to_assets'] >= MIN_CL_TO_ASSETS
                 and it['cl_8q_rank'] is not None and it['cl_8q_rank'] >= MIN_8Q_RANK]
    print(f'→ pre-gated (without TTM revenue check): {len(pre_gated)}')

    # 為通過 gate 的個股加掛營收、股價（截 top 80 預跑，避免太多 API 呼叫）
    pre_gated.sort(key=lambda x: x['cl_yoy'], reverse=True)
    pre_gated = pre_gated[:80]
    print(f'→ enriching top {len(pre_gated)} with revenue/price …')
    attach_revenue_and_price(token, pre_gated, target_quarter)

    # 評分排序
    ranked = score_and_rank(pre_gated)
    print(f'→ ranked: {len(ranked)} (showing top {TOP_N})')

    # 補名稱（fallback 到 stock_names.json，因 TaiwanStockInfo 可能缺）
    for it in ranked:
        if not it.get('name'):
            it['name'] = names.get(it['code'], '')

    # 寫入 latest.json
    latest = {
        'quarter': target_quarter,
        'quarter_list': quarters,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'universe_size': universe_size,
        'ranked': ranked,
        'names': {it['code']: it.get('name', '') for it in ranked},
        'thresholds': {
            'min_cl_twd': MIN_CL_TWD, 'min_cl_yoy': MIN_CL_YOY,
            'min_cl_to_assets': MIN_CL_TO_ASSETS, 'min_8q_rank': MIN_8Q_RANK,
            'cap_yoy': CAP_YOY, 'cap_qoq': CAP_QOQ,
        },
        'weights': {
            'yoy': W_YOY, 'qoq': W_QOQ, 'to_rev': W_TO_REV,
            'to_assets': W_TO_ASSETS, 'accel': W_ACCEL, 'bigholder': W_BIGHOLDER,
        },
    }
    LATEST_JSON.write_text(
        json.dumps(latest, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'→ wrote {LATEST_JSON.name}')

    # 存當季 ranked codes，供下季比對「新進榜」用
    save_ranked_snapshot(target_quarter, ranked)

    if not skip_render:
        html = render_html(latest)
        OUTPUT_HTML.write_text(html, 'utf-8')
        print(f'→ wrote {OUTPUT_HTML.name}')

    return latest


# ── 回測 ─────────────────────────────────────────────────────────────────────

# 各季財報法定公布截止日（用作回測 entry 日期）
QUARTER_PUBLISH = {
    'Q1': '05-15', 'Q2': '08-14', 'Q3': '11-14', 'Q4': '03-31',  # Q4 為次年
}


def quarter_publish_date(quarter):
    y, qn = int(quarter[:4]), int(quarter[-1])
    md = QUARTER_PUBLISH[f'Q{qn}']
    if qn == 4:
        y += 1
    return f'{y}-{md}'


def add_months(d, n):
    """日期加 n 個月（截到月底以避免無效日如 2/30）。"""
    m = d.month - 1 + n
    y = d.year + m // 12
    m = m % 12 + 1
    # 保險起見截到 28 號
    return d.replace(year=y, month=m, day=min(d.day, 28))


def fetch_forward_returns(token, stock_id, entry_date_str, horizons=(20, 60, 120)):
    """從 entry_date 開始的 N 個交易日後報酬。回傳 {N: pct_chg}."""
    entry_dt = datetime.strptime(entry_date_str, '%Y-%m-%d').date()
    # 取 entry_date 起 約 7 個月（120 交易日 ≈ 6 個月，多抓 1 個月當 buffer）
    end_dt = add_months(entry_dt, 7)
    end_dt = min(end_dt, date.today())  # 不能超過今天
    if end_dt <= entry_dt:
        return {h: None for h in horizons}
    try:
        rows = finmind_get(token, 'TaiwanStockPrice',
                          data_id=stock_id, start_date=entry_date_str,
                          end_date=end_dt.strftime('%Y-%m-%d'))
    except Exception:
        return {h: None for h in horizons}
    if not rows:
        return {h: None for h in horizons}
    closes = [r.get('close') for r in rows if r.get('close')]
    if not closes:
        return {h: None for h in horizons}
    base = closes[0]
    out = {}
    for h in horizons:
        if h < len(closes):
            out[h] = closes[h] / base - 1
        elif closes:  # 取最後一筆
            out[h] = closes[-1] / base - 1
        else:
            out[h] = None
    return out


def run_backtest(start_quarter, end_quarter, top_n=10):
    """回測：對每個季度跑策略 → 取 top N → 計算 entry_date 起的 1M/3M/6M 報酬。"""
    token = load_token()
    names = load_stock_names()
    info = load_stock_info(token)

    # 季度區間
    qs = []
    q = start_quarter
    while True:
        qs.append(q)
        if q == end_quarter:
            break
        q = shift_quarter(q, 1)

    print(f'\n=== Backtest {start_quarter} → {end_quarter} ({len(qs)} quarters), top {top_n} per quarter ===\n')

    results = []
    for q in qs:
        # 撈 8 季 (q 及之前 7 季)
        eight = [shift_quarter(q, -i) for i in range(7, -1, -1)]
        qd = {qx: get_balance_sheet_quarter(token, qx) for qx in eight}
        items = build_signals(qd, q, info=info, big_holders={})
        # gate (略過 cl_to_ttm_rev 與大戶條件，加快回測)
        gated = [it for it in items
                 if it['cl_yoy'] is not None and it['cl_yoy'] >= MIN_CL_YOY
                 and it['cl_to_assets'] >= MIN_CL_TO_ASSETS
                 and it['cl_8q_rank'] is not None and it['cl_8q_rank'] >= MIN_8Q_RANK]
        if not gated:
            print(f'  {q}: no gated stocks')
            continue

        # 簡化評分（沒有 TTM 營收與大戶資料時）：YoY z + QoQ z + accel
        z_yoy = zscore([x['cl_yoy'] for x in gated])
        z_qoq = zscore([x['cl_qoq'] for x in gated])
        z_ta = zscore([x['cl_to_assets'] for x in gated])
        for i, it in enumerate(gated):
            it['score'] = (0.45 * z_yoy[i] + 0.25 * z_qoq[i]
                          + 0.15 * z_ta[i]
                          + 0.15 * (1.0 if it['cl_2q_accel'] else 0.0))
        gated.sort(key=lambda x: x['score'], reverse=True)
        picks = gated[:top_n]

        entry = quarter_publish_date(q)
        # 若 entry 在未來，跳過
        if datetime.strptime(entry, '%Y-%m-%d').date() >= date.today():
            print(f'  {q}: entry date {entry} is in the future, skip')
            continue
        print(f'  {q}: entry={entry}, picks={[p["code"] for p in picks]}')

        for p in picks:
            ret = fetch_forward_returns(token, p['code'], entry)
            results.append({
                'quarter': q,
                'entry_date': entry,
                'code': p['code'],
                'name': p.get('name') or names.get(p['code'], ''),
                'group': p.get('group'),
                'cl_yoy': p['cl_yoy'],
                'cl_yoy_raw': p.get('cl_yoy_raw'),
                'cl': p['cl'],
                'score': p['score'],
                'ret_1m': ret.get(20),
                'ret_3m': ret.get(60),
                'ret_6m': ret.get(120),
            })
            time.sleep(FINMIND_SLEEP)

    # 統計
    def stat(key):
        vals = [r[key] for r in results if r[key] is not None]
        if not vals:
            return None
        return {
            'mean': sum(vals) / len(vals),
            'median': sorted(vals)[len(vals) // 2],
            'win_rate': sum(1 for v in vals if v > 0) / len(vals),
            'n': len(vals),
        }

    summary = {
        'start_quarter': start_quarter,
        'end_quarter': end_quarter,
        'top_n': top_n,
        'total_picks': len(results),
        'stats_1m': stat('ret_1m'),
        'stats_3m': stat('ret_3m'),
        'stats_6m': stat('ret_6m'),
        'picks': results,
    }
    BACKTEST_JSON.write_text(
        json.dumps(summary, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    print(f'\n→ wrote {BACKTEST_JSON.name}')
    print('\n=== Summary ===')
    for k in ('stats_1m', 'stats_3m', 'stats_6m'):
        s = summary[k]
        if s:
            print(f'  {k}: n={s["n"]}, mean={s["mean"] * 100:+.1f}%, '
                  f'median={s["median"] * 100:+.1f}%, win_rate={s["win_rate"] * 100:.0f}%')
    return summary


def inspect_single(stock_id):
    """單檔深度檢視（巨漢用 6903）。"""
    token = load_token()
    names = load_stock_names()
    print(f'\n=== {stock_id} {names.get(stock_id, "")} 合約負債 8 季趨勢 ===\n')
    target_quarter = latest_available_quarter()
    quarters = [shift_quarter(target_quarter, -i) for i in range(7, -1, -1)]
    rows = []
    for q in quarters:
        end_date, _, _ = parse_quarter(q)
        data = finmind_get(token, 'TaiwanStockBalanceSheet',
                          data_id=stock_id, start_date=end_date, end_date=end_date)
        types = {r['type']: r['value'] for r in data}
        rows.append({
            'quarter': q,
            'cl': types.get('CurrentContractLiabilities'),
            'ta': types.get('TotalAssets'),
        })
        time.sleep(FINMIND_SLEEP)

    print(f'{"Quarter":<10} {"CL":>16} {"YoY":>10} {"QoQ":>10} {"%TA":>8}')
    for i, r in enumerate(rows):
        cl = r['cl']
        if cl is None:
            print(f'{r["quarter"]:<10} {"—":>16}')
            continue
        prev = rows[i - 1]['cl'] if i >= 1 else None
        yoy = rows[i - 4]['cl'] if i >= 4 else None
        qoq_pct = (cl / prev - 1) * 100 if prev else None
        yoy_pct = (cl / yoy - 1) * 100 if yoy else None
        ta_pct = (cl / r['ta'] * 100) if r['ta'] else None
        print(f'{r["quarter"]:<10} {fmt_twd(cl):>16} '
              f'{(f"{yoy_pct:+.0f}%") if yoy_pct is not None else "—":>10} '
              f'{(f"{qoq_pct:+.0f}%") if qoq_pct is not None else "—":>10} '
              f'{(f"{ta_pct:.1f}%") if ta_pct is not None else "—":>8}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--quarter', help='目標季度 e.g. 2025Q4，省略則自動推算最近可用')
    ap.add_argument('--stock', help='單檔深度檢視')
    ap.add_argument('--force', action='store_true', help='忽略快取，重抓 FinMind')
    ap.add_argument('--backtest', nargs='?', const='2024Q1:2025Q3',
                    help='回測模式 START:END e.g. 2024Q1:2025Q3。不帶值預設 2024Q1:2025Q3')
    ap.add_argument('--top', type=int, default=10, help='回測每季取前 N 名（預設 10）')
    args = ap.parse_args()

    if args.stock:
        inspect_single(args.stock)
        return

    if args.backtest:
        s, e = args.backtest.split(':')
        run_backtest(s, e, top_n=args.top)
        return

    target = args.quarter or latest_available_quarter()
    run(target, force=args.force)


if __name__ == '__main__':
    main()
