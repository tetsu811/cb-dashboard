#!/usr/bin/env python3
"""
處置股策略儀表板 — 資料產生器

兩個策略：
- S2 處置期間反轉：period_start 收盤買入 → period_end 收盤賣出（baseline 無篩選）
- S3 解除回補：period_end+1 開盤買入 → period_end+10 收盤賣出

回測 (2024-04 ~ 2026-04, adj prices):
  S2 baseline: N=994, 勝率 59.3%, 平均 +3.80%, PF 1.94
  S3 10-day:   N=904, 勝率 48.1%, 平均 +3.55%, PF 1.63
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta

import requests

# ── Constants ────────────────────────────────────────────────────────────────

TODAY = datetime.now().date()
OUTPUT_HTML = 'disposition_index.html'
DATA_DIR = 'disposition_data'
LATEST_CACHE = 'disposition_latest.json'
HEADERS = {'Authorization': f'Bearer {os.environ.get("FINMIND_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0wOS0xNSAyMjo1MjoxNiIsInVzZXJfaWQiOiJ0ZXRzdSIsImlwIjoiMTI0LjIxOC4yMTYuMTgzIn0.OzPp41ojdsUDDgVzP9bMkoqfB6FzYMEzcr8TXM1fInI")}'}

API = 'https://api.finmindtrade.com/api/v4/data'

EVENT_LOOKBACK_DAYS = 90        # how far back to fetch disposition events
PRICE_LOOKBACK_DAYS = 60        # K-line history per active stock
S3_TRACK_DAYS = 10              # post-release tracking window
MAX_HISTORY_DAYS = 60           # snapshot retention


# ── Data fetching ────────────────────────────────────────────────────────────

def _get(dataset, **params):
    p = {'dataset': dataset, **params}
    for attempt in range(3):
        try:
            r = requests.get(API, params=p, headers=HEADERS, timeout=30)
            d = r.json()
            if d.get('status') == 200:
                return d.get('data', [])
            print(f'  ! {dataset} {params.get("data_id","")}: {d.get("msg")}')
            return []
        except Exception as e:
            print(f'  ! retry {attempt+1}/3: {e}')
            time.sleep(1)
    return []


def fetch_disposition_events():
    start = (TODAY - timedelta(days=EVENT_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    end = TODAY.strftime('%Y-%m-%d')
    print(f'Fetching dispositions {start} ~ {end} ...')
    events = _get('TaiwanStockDispositionSecuritiesPeriod',
                  start_date=start, end_date=end)
    print(f'  → {len(events)} events')
    return events


def fetch_prices(stock_ids):
    start = (TODAY - timedelta(days=PRICE_LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    end = (TODAY + timedelta(days=1)).strftime('%Y-%m-%d')
    out = {}
    print(f'Fetching adj K-line for {len(stock_ids)} stocks ...')
    for i, sid in enumerate(stock_ids):
        rows = _get('TaiwanStockPriceAdj', data_id=sid, start_date=start, end_date=end)
        out[sid] = [{'date': r['date'], 'o': r['open'], 'c': r['close'],
                     'h': r['max'], 'l': r['min'], 'v': r['Trading_Volume']}
                    for r in rows if r.get('Trading_Volume', 0) > 0]
        if (i + 1) % 25 == 0:
            print(f'  [{i+1}/{len(stock_ids)}]')
        time.sleep(0.05)
    return out


# ── Signal computation ──────────────────────────────────────────────────────

def is_active_today(ev):
    """S2: today is between period_start and period_end (inclusive)."""
    today_str = TODAY.strftime('%Y-%m-%d')
    return ev['period_start'] <= today_str <= ev['period_end']


def is_recently_lifted(ev):
    """S3: period_end is within last S3_TRACK_DAYS calendar days."""
    end = datetime.strptime(ev['period_end'], '%Y-%m-%d').date()
    delta = (TODAY - end).days
    return 0 <= delta <= S3_TRACK_DAYS + 5  # buffer for weekends


def find_idx(rows, date_str):
    """Index of date in rows (sorted asc), or next available."""
    for i, r in enumerate(rows):
        if r['date'] >= date_str:
            return i
    return None


def classify_tier(vol_ratio, intraday, price):
    """Statistical-strength label based on backtest sub-group performance.

    Tier descriptors are pure historical-statistic labels — they describe
    which sub-group the row falls into, not any trading recommendation.

    歷史回測分組（不構成任何建議）：
      A: 量縮 < 30%               → 該分組勝率 68.3%, PF 3.34, N=350
      B: 量縮 < 60% + 紅K + 價≥20 → 該分組勝率 65.0%, PF 2.93, N=226
      C: baseline                  → 全樣本勝率 59.3%, PF 1.94, N=994
      D: 量增 / 雞蛋水餃 / 追漲>5% → 歷史勝率 < 50%
    """
    # D 反指標
    if vol_ratio is not None and 1.0 <= vol_ratio < 2.0:
        return 'D', '量增'
    if price is not None and price < 20:
        return 'D', '低價'
    if intraday is not None and intraday > 5:
        return 'D', '追漲'
    # A 統計最佳
    if vol_ratio is not None and vol_ratio < 0.3:
        return 'A', '量縮<30%'
    # B 強訊號（多條件）
    cond_b = (vol_ratio is not None and vol_ratio < 0.6
              and intraday is not None and intraday > 0
              and price is not None and price >= 20)
    if cond_b:
        return 'B', '量縮+紅K+價≥20'
    # C 一般
    return 'C', '一般'


def compute_s2_row(ev, prices):
    """For each active S2 event, compute live tracking info."""
    sid = ev['stock_id']
    rows = prices.get(sid, [])
    if not rows:
        return None
    s_idx = find_idx(rows, ev['period_start'])
    if s_idx is None or s_idx >= len(rows):
        return None
    entry = rows[s_idx]['c']                    # period_start 收盤
    cur = rows[-1]                              # 最新一根
    ret_pct = (cur['c'] - entry) / entry * 100 if entry > 0 else 0
    # Lowest close since entry
    lows = [r['c'] for r in rows[s_idx:]]
    lowest = min(lows) if lows else entry
    max_dd = (lowest - entry) / entry * 100 if entry > 0 else 0
    # Days remaining
    end_dt = datetime.strptime(ev['period_end'], '%Y-%m-%d').date()
    days_left = (end_dt - TODAY).days
    held_days = len(rows) - s_idx
    # 5-day momentum into period_start
    pre_ret = None
    if s_idx >= 5:
        pre = rows[s_idx - 5]['c']
        pre_ret = (entry - pre) / pre * 100 if pre > 0 else None
    # Statistical-tier classification features
    vol_ratio = None
    if s_idx >= 10:
        avg_vol = sum(rows[i]['v'] for i in range(s_idx - 10, s_idx)) / 10
        if avg_vol > 0:
            vol_ratio = rows[s_idx]['v'] / avg_vol
    s_intraday = None
    if rows[s_idx]['o'] > 0:
        s_intraday = (rows[s_idx]['c'] - rows[s_idx]['o']) / rows[s_idx]['o'] * 100
    tier, tier_reason = classify_tier(vol_ratio, s_intraday, entry)

    return {
        'stock_id': sid,
        'stock_name': ev['stock_name'],
        'disposition_cnt': ev['disposition_cnt'],
        'period_start': ev['period_start'],
        'period_end': ev['period_end'],
        'days_left': days_left,
        'held_days': held_days,
        'entry_price': round(entry, 2),
        'last_price': round(cur['c'], 2),
        'last_date': cur['date'],
        'ret_pct': round(ret_pct, 2),
        'max_dd_pct': round(max_dd, 2),
        'pre_5d_ret': round(pre_ret, 2) if pre_ret is not None else None,
        'is_first': ev['disposition_cnt'] == 1,
        'tier': tier,
        'tier_reason': tier_reason,
        'vol_ratio': round(vol_ratio, 2) if vol_ratio is not None else None,
        's_intraday': round(s_intraday, 2) if s_intraday is not None else None,
    }


def compute_s3_row(ev, prices):
    """For S3 events: track post-release performance."""
    sid = ev['stock_id']
    rows = prices.get(sid, [])
    if not rows:
        return None
    e_idx = find_idx(rows, ev['period_end'])
    if e_idx is None or e_idx >= len(rows):
        return None
    # Buy at next trading day's open
    buy_idx = e_idx + 1
    if buy_idx >= len(rows):
        return {
            'stock_id': sid, 'stock_name': ev['stock_name'],
            'disposition_cnt': ev['disposition_cnt'],
            'period_end': ev['period_end'],
            'entry_price': None, 'last_price': None, 'last_date': None,
            'ret_pct': None, 'days_held': 0, 'in_window': True,
            'status': 'pending_open',
        }
    entry = rows[buy_idx]['o']
    cur = rows[-1]
    ret_pct = (cur['c'] - entry) / entry * 100 if entry > 0 else 0
    days_held = len(rows) - buy_idx
    in_window = days_held <= S3_TRACK_DAYS
    return {
        'stock_id': sid,
        'stock_name': ev['stock_name'],
        'disposition_cnt': ev['disposition_cnt'],
        'period_end': ev['period_end'],
        'entry_price': round(entry, 2),
        'last_price': round(cur['c'], 2),
        'last_date': cur['date'],
        'ret_pct': round(ret_pct, 2),
        'days_held': days_held,
        'in_window': in_window,
        'status': 'tracking' if in_window else 'expired',
    }


def compute_recent_perf(events, prices, window_days=30):
    """Backward-looking: avg return of last 30 days' completed S2/S3."""
    cutoff = TODAY - timedelta(days=window_days)
    s2_returns, s3_returns = [], []
    for ev in events:
        end_dt = datetime.strptime(ev['period_end'], '%Y-%m-%d').date()
        if end_dt < cutoff or end_dt > TODAY:
            continue
        sid = ev['stock_id']
        rows = prices.get(sid, [])
        if not rows:
            continue
        s_idx = find_idx(rows, ev['period_start'])
        e_idx = find_idx(rows, ev['period_end'])
        if s_idx is None or e_idx is None or e_idx >= len(rows) or e_idx <= s_idx:
            continue
        # S2 completed return
        s2_returns.append((rows[e_idx]['c'] - rows[s_idx]['c']) / rows[s_idx]['c'] * 100)
        # S3 5-day return
        if e_idx + 5 < len(rows):
            s3_returns.append((rows[e_idx + 5]['c'] - rows[e_idx + 1]['o']) / rows[e_idx + 1]['o'] * 100)
    avg = lambda xs: round(sum(xs) / len(xs), 2) if xs else None
    win = lambda xs: round(sum(1 for x in xs if x > 0) / len(xs) * 100, 1) if xs else None
    return {
        's2_n': len(s2_returns), 's2_avg': avg(s2_returns), 's2_win': win(s2_returns),
        's3_n': len(s3_returns), 's3_avg': avg(s3_returns), 's3_win': win(s3_returns),
    }


# ── HTML rendering ───────────────────────────────────────────────────────────

CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--pp:#7c3aed;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b;--card:#fff;--hover:#eef2ff}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:14px;line-height:1.5}
.hdr{background:linear-gradient(135deg,#0f172a 0%,#1e3a8a 50%,#7c3aed 100%);color:#fff;padding:22px 28px 16px}
.hdr h1{font-size:20px;font-weight:800;margin-bottom:4px;letter-spacing:-0.3px}
.hdr .sub{font-size:11.5px;opacity:.85}
.nav-link{color:#c4b5fd;font-size:11.5px;text-decoration:none;margin-left:14px;border-bottom:1px dashed #c4b5fd;padding-bottom:1px;transition:opacity .15s}
.nav-link:hover{opacity:.7}
.stats{display:flex;gap:12px;padding:16px 28px;background:var(--card);border-bottom:1px solid var(--brd);flex-wrap:wrap}
.sc{background:var(--bg);border:1px solid var(--brd);border-radius:10px;padding:14px 20px;min-width:130px;transition:transform .15s,box-shadow .15s;border-left:3px solid var(--bl)}
.sc:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,.08)}
.sc .n{font-size:26px;font-weight:800;color:var(--bl);letter-spacing:-0.5px}
.sc .l{font-size:11px;color:var(--mu);margin-top:3px;font-weight:500}
.sc.gr{border-left-color:var(--gr)}.sc.gr .n{color:var(--gr)}
.sc.am{border-left-color:var(--am)}.sc.am .n{color:var(--am)}
.sc.rd{border-left-color:var(--rd)}.sc.rd .n{color:var(--rd)}
.sc.pp{border-left-color:var(--pp)}.sc.pp .n{color:var(--pp)}
.tabs{display:flex;padding:0 28px;border-bottom:2px solid var(--brd);background:var(--card);gap:4px}
.tab{padding:12px 22px;cursor:pointer;border-bottom:3px solid transparent;font-size:12.5px;font-weight:600;color:var(--mu);margin-bottom:-2px;transition:all .15s;border-radius:6px 6px 0 0}
.tab:hover{background:#f1f5f9;color:var(--txt)}
.tab.active{border-bottom-color:var(--bl);color:var(--bl);background:transparent}
.pane{display:none;padding:20px 28px}.pane.active{display:block}
.ttl{font-size:15px;font-weight:700;margin-bottom:6px}
.desc{font-size:12.5px;color:var(--mu);margin-bottom:14px;line-height:1.7}
.tag{display:inline-block;background:#dbeafe;color:#1d4ed8;border-radius:5px;padding:2px 8px;font-size:11px;font-weight:600;margin-right:4px}
.box{background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:10px 14px;font-size:12px;color:#1e40af;margin-bottom:14px;line-height:1.7}
.box.warn{background:#fffbeb;border-color:#fde68a;color:#92400e}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06);margin-bottom:16px}
th{background:#edf2f7;font-weight:700;color:var(--mu);font-size:11px;text-transform:uppercase;padding:10px 12px;text-align:left;border-bottom:2px solid var(--brd);white-space:nowrap;letter-spacing:0.3px;cursor:pointer;user-select:none}
th:hover{background:#e2e8f0}
th.sorted-asc::after{content:' ▲';font-size:9px;color:var(--bl)}
th.sorted-desc::after{content:' ▼';font-size:9px;color:var(--bl)}
td{padding:9px 12px;border-bottom:1px solid var(--brd);vertical-align:middle;transition:background .1s}
tr:last-child td{border-bottom:none}
tr:hover td{background:var(--hover)}
.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.center{text-align:center;white-space:nowrap}
tr.row-up td{background:#f0fdf4}
tr.row-up:hover td{background:#dcfce7}
tr.row-dn td{background:#fef2f2}
tr.row-dn:hover td{background:#fee2e2}
tr.row-near td{background:#fef9c3;border-left:3px solid var(--am)}
.badge{display:inline-block;padding:3px 10px;border-radius:6px;font-size:11px;font-weight:600;white-space:nowrap;letter-spacing:0.2px}
.badge.gr{background:#dcfce7;color:#15803d;border:1px solid #bbf7d0}
.badge.rd{background:#fee2e2;color:#b91c1c;border:1px solid #fecaca}
.badge.am{background:#fef9c3;color:#854d0e;border:1px solid #fde68a}
.badge.bl{background:#dbeafe;color:#1d4ed8;border:1px solid #bfdbfe}
.badge.mu{background:#f1f5f9;color:#64748b;border:1px solid var(--brd)}
.tier{display:inline-block;padding:3px 8px;border-radius:5px;font-size:11px;font-weight:700;letter-spacing:0.3px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.tier-A{background:#1e293b;color:#fbbf24;border:1px solid #334155}
.tier-B{background:#dbeafe;color:#1e40af;border:1px solid #93c5fd}
.tier-C{background:#f1f5f9;color:#64748b;border:1px solid var(--brd)}
.tier-D{background:#fee2e2;color:#991b1b;border:1px solid #fecaca}
.tier-note{font-size:10px;color:var(--mu);display:block;margin-top:2px}
.disclaimer{background:#fef9c3;border:1px solid #fde68a;color:#713f12;padding:10px 14px;border-radius:8px;font-size:11.5px;line-height:1.6;margin:14px 0}
.up{color:#16a34a;font-weight:700}
.dn{color:#dc2626;font-weight:700}
.empty{padding:40px;text-align:center;color:var(--mu);background:var(--card);border-radius:10px;border:1px dashed var(--brd)}
.ft{text-align:center;color:var(--mu);font-size:11.5px;padding:20px 28px;border-top:1px solid var(--brd);line-height:1.8;background:var(--card)}
@media(max-width:768px){.hdr,.stats,.pane{padding-left:16px;padding-right:16px}.tabs{padding:0 16px;overflow-x:auto}th,td{padding:8px 8px}.sc{min-width:110px;padding:10px 14px}.sc .n{font-size:22px}}
"""

JS = """
function showTab(name, el){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('pane-'+name).classList.add('active');
}
function sortTable(th){
  const table = th.closest('table');
  const tbody = table.querySelector('tbody');
  const idx = Array.from(th.parentNode.children).indexOf(th);
  const isNum = th.classList.contains('num');
  const cur = th.classList.contains('sorted-asc') ? 'asc' : (th.classList.contains('sorted-desc') ? 'desc' : null);
  const next = cur === 'asc' ? 'desc' : 'asc';
  table.querySelectorAll('th').forEach(h=>h.classList.remove('sorted-asc','sorted-desc'));
  th.classList.add('sorted-' + next);
  const rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort((a,b)=>{
    let va = a.children[idx].textContent.trim();
    let vb = b.children[idx].textContent.trim();
    if(isNum){ va = parseFloat(va.replace(/[%,+]/g,'')) || 0; vb = parseFloat(vb.replace(/[%,+]/g,'')) || 0; }
    return next === 'asc' ? (va > vb ? 1 : -1) : (va < vb ? 1 : -1);
  });
  rows.forEach(r => tbody.appendChild(r));
}
"""


def fmt_pct(v):
    if v is None: return '<td class="num">-</td>'
    cls = 'up' if v > 0 else ('dn' if v < 0 else '')
    sign = '+' if v > 0 else ''
    return f'<td class="num {cls}">{sign}{v:.2f}%</td>'


def fmt_num(v):
    if v is None: return '<td class="num">-</td>'
    return f'<td class="num">{v}</td>'


TIER_ORDER = {'A': 0, 'B': 1, 'C': 2, 'D': 3}

def render_s2_table(rows):
    if not rows:
        return '<div class="empty">今日無處置中股票</div>'
    # Sort by tier first, then by ret_pct
    body = []
    for r in sorted(rows, key=lambda x: (TIER_ORDER.get(x.get('tier', 'C'), 9), x['ret_pct'])):
        cls = ''
        if r['days_left'] <= 1:
            cls = 'row-near'
        elif r['ret_pct'] > 2:
            cls = 'row-up'
        elif r['ret_pct'] < -2:
            cls = 'row-dn'
        first_badge = '<span class="badge bl">首次</span>' if r['is_first'] else f'<span class="badge mu">第{r["disposition_cnt"]}次</span>'
        near_tag = '<span class="badge am">明日為解除日</span>' if r['days_left'] <= 1 else ''
        tier = r.get('tier', 'C')
        tier_html = f'<span class="tier tier-{tier}">{tier}</span><span class="tier-note">{r.get("tier_reason", "")}</span>'
        vol_str = f'{r["vol_ratio"]:.2f}x' if r.get('vol_ratio') is not None else '-'
        body.append(f'''<tr class="{cls}">
  <td class="center">{tier_html}</td>
  <td><b>{r["stock_id"]}</b></td>
  <td>{r["stock_name"]}</td>
  <td class="center">{first_badge}</td>
  <td class="center">{r["period_start"]}</td>
  <td class="center">{r["period_end"]} {near_tag}</td>
  <td class="num">{r["days_left"]}</td>
  <td class="num">{r["entry_price"]}</td>
  <td class="num">{r["last_price"]}</td>
  {fmt_pct(r["ret_pct"])}
  {fmt_pct(r["max_dd_pct"])}
  {fmt_pct(r["pre_5d_ret"])}
  <td class="num">{vol_str}</td>
</tr>''')
    return f'''<table><thead><tr>
  <th class="center" onclick="sortTable(this)">分組</th>
  <th onclick="sortTable(this)">股號</th>
  <th onclick="sortTable(this)">股名</th>
  <th class="center" onclick="sortTable(this)">處置次數</th>
  <th class="center" onclick="sortTable(this)">處置起</th>
  <th class="center" onclick="sortTable(this)">處置迄</th>
  <th class="num" onclick="sortTable(this)">剩餘天數</th>
  <th class="num" onclick="sortTable(this)">生效日收盤</th>
  <th class="num" onclick="sortTable(this)">最新價</th>
  <th class="num" onclick="sortTable(this)">區間報酬</th>
  <th class="num" onclick="sortTable(this)">區間最大回撤</th>
  <th class="num" onclick="sortTable(this)">前5日漲跌</th>
  <th class="num" onclick="sortTable(this)">進場日量比</th>
</tr></thead><tbody>{"".join(body)}</tbody></table>'''


def render_s3_table(rows):
    if not rows:
        return '<div class="empty">近 10 日無解除處置股票</div>'
    body = []
    for r in sorted(rows, key=lambda x: -(x['days_held'] or 0)):
        if r.get('status') == 'pending_open':
            body.append(f'''<tr>
  <td><b>{r["stock_id"]}</b></td>
  <td>{r["stock_name"]}</td>
  <td class="center">{r["period_end"]}</td>
  <td class="num">-</td><td class="num">-</td><td class="num">-</td>
  <td class="num">-</td>
  <td class="center"><span class="badge am">尚未開盤</span></td>
</tr>''')
            continue
        cls = 'row-up' if r['ret_pct'] > 2 else ('row-dn' if r['ret_pct'] < -2 else '')
        if not r['in_window']:
            badge = '<span class="badge mu">已過 10 日窗</span>'
        elif r['ret_pct'] > 0:
            badge = '<span class="badge gr">窗內 / 正</span>'
        else:
            badge = '<span class="badge am">窗內 / 負</span>'
        body.append(f'''<tr class="{cls}">
  <td><b>{r["stock_id"]}</b></td>
  <td>{r["stock_name"]}</td>
  <td class="center">{r["period_end"]}</td>
  <td class="num">{r["days_held"]} / {S3_TRACK_DAYS}</td>
  <td class="num">{r["entry_price"]}</td>
  <td class="num">{r["last_price"]}</td>
  {fmt_pct(r["ret_pct"])}
  <td class="center">{badge}</td>
</tr>''')
    return f'''<table><thead><tr>
  <th onclick="sortTable(this)">股號</th>
  <th onclick="sortTable(this)">股名</th>
  <th class="center" onclick="sortTable(this)">解除日</th>
  <th class="num" onclick="sortTable(this)">距解除日</th>
  <th class="num" onclick="sortTable(this)">解除次日開盤</th>
  <th class="num" onclick="sortTable(this)">最新價</th>
  <th class="num" onclick="sortTable(this)">區間報酬</th>
  <th class="center">區間狀態</th>
</tr></thead><tbody>{"".join(body)}</tbody></table>'''


def render_html(s2_rows, s3_rows, perf, today_str, new_today, lift_tomorrow):
    s2_n = len(s2_rows)
    s3_n = len([r for r in s3_rows if r.get('in_window')])

    def perf_str(avg, win, n):
        if avg is None: return ('-', '-')
        cls = 'gr' if avg > 0 else 'rd'
        return (f'<span class="{cls}">{"+" if avg>0 else ""}{avg}%</span>', f'勝 {win}% / N={n}')

    s2_perf, s2_perf_l = perf_str(perf['s2_avg'], perf['s2_win'], perf['s2_n'])
    s3_perf, s3_perf_l = perf_str(perf['s3_avg'], perf['s3_win'], perf['s3_n'])

    return f'''<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>處置股策略儀表板</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>處置股策略儀表板</h1>
  <div class="sub">更新：{today_str}（每交易日盤後自動更新）<a class="nav-link" href="index.html">→ 可轉債</a><a class="nav-link" href="etf_index.html">→ 主動式 ETF</a></div>
</div>
<div class="stats">
  <div class="sc"><div class="n">{s2_n}</div><div class="l">處置中股票</div></div>
  <div class="sc pp"><div class="n">{new_today}</div><div class="l">今日新進處置</div></div>
  <div class="sc am"><div class="n">{lift_tomorrow}</div><div class="l">明日解除</div></div>
  <div class="sc gr"><div class="n">{s3_n}</div><div class="l">S3 10 日區間內</div></div>
  <div class="sc"><div class="n">{s2_perf}</div><div class="l">近 30 日 S2 樣本均值<br/><span style="font-size:10px">{s2_perf_l}</span></div></div>
  <div class="sc"><div class="n">{s3_perf}</div><div class="l">近 30 日 S3 樣本均值<br/><span style="font-size:10px">{s3_perf_l}</span></div></div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('s2',this)">S2 處置期間樣本（{s2_n}）</div>
  <div class="tab" onclick="showTab('s3',this)">S3 解除後 10 日樣本（{len(s3_rows)}）</div>
  <div class="tab" onclick="showTab('about',this)">回測說明</div>
</div>
<div id="pane-s2" class="pane active">
  <div class="ttl">S2 處置期間樣本</div>
  <div class="desc">
    回測區間 2024-04 ~ 2026-04，N=994，全樣本勝率 59.3%、平均 +3.80%、PF 1.94。
    <span class="tag">還原權息價</span><span class="tag">區間中位數約 8.6 個交易日</span>
  </div>
  <div class="disclaimer">
    <b>免責聲明：</b>以下數據僅為歷史統計分析與分組勾稽，不構成任何交易建議或投資推薦。
    分組標籤（A/B/C/D）對應該樣本在歷史回測中的勝率區段，不代表未來績效。表頭可點擊排序。
  </div>
  <div class="box">
    <b>分組標籤（依歷史回測勝率分層）：</b><br/>
    <span class="tier tier-A">A</span> 量縮&lt;30% — 該分組歷史勝率 68.3% / PF 3.34 / N=350<br/>
    <span class="tier tier-B">B</span> 量縮&lt;60% + 紅K + 價≥20 — 該分組歷史勝率 65.0% / PF 2.93 / N=226<br/>
    <span class="tier tier-C">C</span> 一般（baseline） — 全樣本勝率 59.3% / PF 1.94<br/>
    <span class="tier tier-D">D</span> 反指標（量增 / 低於 20 元 / 進場日漲&gt;5%） — 該分組歷史勝率&lt;50%
  </div>
  <div class="box">
    <b>名詞定義：</b><br/>
    • <b>進場日量比</b> = 處置生效當日成交量 ÷ 該股前 10 個交易日的平均成交量<br/>
    &nbsp;&nbsp;&nbsp;例：量比 0.19 表示該日成交量僅為前 10 日均量的 19%（大幅量縮）；2.0 表示翻倍（爆量）<br/>
    • <b>紅K</b> = 處置生效當日收盤 &gt; 開盤<br/>
    • <b>進場日漲幅</b> = (生效日收盤 − 生效日開盤) ÷ 生效日開盤 × 100%<br/>
    • <b>區間報酬</b> = (最新收盤 − 生效日收盤) ÷ 生效日收盤 × 100%<br/>
    • <b>區間最大回撤</b> = (生效以來最低收盤 − 生效日收盤) ÷ 生效日收盤 × 100%
  </div>
  {render_s2_table(s2_rows)}
</div>
<div id="pane-s3" class="pane">
  <div class="ttl">S3 解除後 10 日樣本</div>
  <div class="desc">
    N=904，勝率 48.1%、平均 +3.55%、PF 1.63、σ 20.2%。
    <span class="tag">中位數 -0.62%</span><span class="tag">屬肥尾分布</span>
  </div>
  <div class="disclaimer">
    <b>免責聲明：</b>以下數據僅為歷史統計追蹤，不構成任何交易建議或投資推薦。
    S3 樣本中位數為負、平均為正 — 即少數大幅正報酬樣本拉高均值，個別樣本變異極大。
  </div>
  {render_s3_table(s3_rows)}
</div>
<div id="pane-about" class="pane">
  <div class="ttl">策略邏輯與回測</div>
  <div class="disclaimer" style="margin-top:14px">
    <b>免責聲明：</b>本儀表板僅為歷史資料統計與回測分析，所有數據均為已發生的歷史紀錄，
    不構成任何交易建議、投資推薦或財務指導。使用者應自行判斷與承擔任何決策風險。
  </div>
  <div class="desc" style="margin-top:14px">
    <b>處置股機制：</b>個股因連續異常波動觸發 TWSE 處置，期間以「人工管制撮合」（5 或 20 分鐘一次）+
    預收款券，散戶投機行為受限，籌碼換手率歷史上呈現下降，解除後歷史樣本常見回補。<br/><br/>

    <b>S2 處置期間區間樣本</b>（2024-04 ~ 2026-04，1316 處置事件，還原權息價）<br/>
    &nbsp;&nbsp;• 無篩選 N=994，勝率 59.3%，平均 +3.80%，PF 1.94，σ 15.8%<br/>
    &nbsp;&nbsp;• 首次處置 only：N=264，勝率 59.1%，平均 +4.00%，PF 2.05<br/>
    &nbsp;&nbsp;• 重複處置 only：N=730，勝率 59.3%，平均 +3.73%，PF 1.90<br/>
    &nbsp;&nbsp;• 時間穩定性：2024 PF 1.69 / 2025 PF 2.37 / 2026 PF 1.82<br/><br/>

    <b>S2 子分組</b><br/>
    &nbsp;&nbsp;• A 量縮&lt;30%：N=350，勝率 68.3%，PF 3.34<br/>
    &nbsp;&nbsp;• B 量縮+紅K+價≥20：N=226，勝率 65.0%，PF 2.93<br/>
    &nbsp;&nbsp;• D 反指標（量增 1~2x / 低於 20 元 / 進場日漲&gt;5%）：歷史勝率 &lt;50%<br/><br/>

    <b>S3 解除後 10 日區間樣本</b><br/>
    N=904，勝率 48.1%，平均 +3.55%，PF 1.63，σ 20.2%；中位數 -0.62%，典型肥尾分布<br/><br/>

    <b>方法學注意：</b>所有數據未含手續費及滑價。處置股因人工管制撮合，
    歷史成交量通常低於同股平日，實際買賣價差可能高於一般股票。樣本標準差 σ 15.8%（S2）、
    20.2%（S3），個別事件之變異甚大，平均值不代表單一事件之預期結果。
  </div>
</div>
<div class="ft">
  資料源：FinMind TaiwanStockDispositionSecuritiesPeriod / TaiwanStockPriceAdj<br/>
  本儀表板為歷史資料統計，不構成任何交易建議或投資推薦。
</div>
<script>{JS}</script>
</body></html>'''


# ── Persistence ──────────────────────────────────────────────────────────────

def save_snapshot(payload):
    os.makedirs(DATA_DIR, exist_ok=True)
    fp = os.path.join(DATA_DIR, f'{TODAY}.json')
    with open(fp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f'  → snapshot {fp}')

    with open(LATEST_CACHE, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f'  → latest {LATEST_CACHE}')


def cleanup_old_snapshots():
    if not os.path.isdir(DATA_DIR):
        return
    cutoff = TODAY - timedelta(days=MAX_HISTORY_DAYS)
    for fn in os.listdir(DATA_DIR):
        if not fn.endswith('.json'):
            continue
        try:
            d = datetime.strptime(fn[:-5], '%Y-%m-%d').date()
            if d < cutoff:
                os.remove(os.path.join(DATA_DIR, fn))
        except ValueError:
            continue


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    events = fetch_disposition_events()
    if not events:
        print('No events fetched; abort.')
        sys.exit(1)

    # Active S2 + recent-lifted S3 candidates → which stocks need K-line?
    s2_candidates = [e for e in events if is_active_today(e)]
    s3_candidates = [e for e in events if is_recently_lifted(e)]
    needed_stocks = sorted({e['stock_id'] for e in s2_candidates + s3_candidates})

    # Also fetch all events' stocks for "recent perf" calculation
    perf_stocks = sorted({e['stock_id'] for e in events})

    print(f'\nS2 active: {len(s2_candidates)}, S3 candidates: {len(s3_candidates)}')
    print(f'Need K-line for {len(perf_stocks)} unique stocks (active + perf calc)')

    prices = fetch_prices(perf_stocks)

    # Build rows
    s2_rows = [r for r in (compute_s2_row(e, prices) for e in s2_candidates) if r]
    # De-dup S2 rows: keep the latest period_start per stock
    s2_dedup = {}
    for r in s2_rows:
        k = r['stock_id']
        if k not in s2_dedup or r['period_start'] > s2_dedup[k]['period_start']:
            s2_dedup[k] = r
    s2_rows = list(s2_dedup.values())

    s3_rows = [r for r in (compute_s3_row(e, prices) for e in s3_candidates) if r]
    s3_dedup = {}
    for r in s3_rows:
        k = r['stock_id']
        if k not in s3_dedup or r['period_end'] > s3_dedup[k]['period_end']:
            s3_dedup[k] = r
    s3_rows = list(s3_dedup.values())

    perf = compute_recent_perf(events, prices, window_days=30)

    today_str = TODAY.strftime('%Y-%m-%d')
    today_iso = TODAY.strftime('%Y-%m-%d')
    tomorrow = (TODAY + timedelta(days=1)).strftime('%Y-%m-%d')
    new_today = sum(1 for r in s2_rows if r['period_start'] == today_iso)
    lift_tomorrow = sum(1 for r in s2_rows if r['period_end'] == tomorrow)

    payload = {
        'fetched_at': datetime.now().isoformat(timespec='seconds'),
        'date': today_str,
        's2_rows': s2_rows,
        's3_rows': s3_rows,
        'perf_30d': perf,
        'stats': {
            's2_active': len(s2_rows),
            's3_tracking': sum(1 for r in s3_rows if r.get('in_window')),
            'new_today': new_today,
            'lift_tomorrow': lift_tomorrow,
        },
    }

    save_snapshot(payload)
    cleanup_old_snapshots()

    html = render_html(s2_rows, s3_rows, perf, today_str, new_today, lift_tomorrow)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'\nDashboard → {OUTPUT_HTML}')
    print(f'\nSummary:')
    print(f'  S2 active stocks: {len(s2_rows)}')
    print(f'  S3 tracking stocks: {sum(1 for r in s3_rows if r.get("in_window"))}')
    print(f'  Recent 30d S2: avg {perf["s2_avg"]}%, win {perf["s2_win"]}%, N={perf["s2_n"]}')
    print(f'  Recent 30d S3: avg {perf["s3_avg"]}%, win {perf["s3_win"]}%, N={perf["s3_n"]}')


if __name__ == '__main__':
    main()
