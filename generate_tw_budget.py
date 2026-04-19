#!/usr/bin/env python3
"""TW Budget Bill Monitor — 每日監控台灣立法院法律提案（含預算相關），
依關鍵字字典分類到受益產業與個股，輸出 tw_budget_index.html dashboard。

資料來源：立法院開放資料 ID20 API（議案提案）
"""
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from html import escape
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
KEYWORDS_PATH = ROOT / "tw_budget_keywords.json"
CACHE_PATH = ROOT / "tw_budget_cache.json"
OUTPUT_HTML = ROOT / "tw_budget_index.html"
LATEST_JSON = ROOT / "tw_budget_latest.json"

LY_API = "https://data.ly.gov.tw/odw/ID20Action.action"
CURRENT_TERM = "11"
# 抓當前屆最近會期（11屆每會期半年，從 01 到 10）。
SESSION_PERIODS = ["02", "03", "04", "05", "06", "07", "08"]

# 「預算類」核心關鍵字：法案名稱包含任一即視為預算/撥款相關。
# 僅此類法案才入列（避免 is_budget OR sector 模式把無關法規例如「道路交通處罰條例」收進來）。
BUDGET_KEYWORDS_CORE = [
    "預算", "撥款", "特別條例", "特別預算", "預備金", "決算",
    "融通", "振興", "紓困", "歲入", "歲出", "墊付",
    "基金設置", "基金條例", "發放特別",
]

# 「產業政策」次要訊號：法案名稱含政策行動詞 + 產業關鍵字時也可視為受益訊號。
POLICY_ACTION_KEYWORDS = [
    "發展", "推動", "促進", "產業", "扶植", "獎勵",
    "基礎建設", "前瞻", "創新", "研發",
]

TPE = timezone(timedelta(hours=8))


def fetch_bills():
    """抓取當前屆多個會期的全部議案提案。"""
    all_bills = []
    seen_keys = set()
    for sp in SESSION_PERIODS:
        params = {
            "term": CURRENT_TERM,
            "sessionPeriod": sp,
            "sessionTimes": "",
            "meetingTimes": "",
            "billName": "",
            "billOrg": "",
            "billProposer": "",
            "billCosignatory": "",
            "fileType": "json",
        }
        try:
            r = requests.get(
                LY_API,
                params=params,
                timeout=30,
                headers={
                    "Accept-Encoding": "identity",
                    "User-Agent": "budget-monitor/1.0",
                },
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[warn] fetch session {sp} failed: {e}", file=sys.stderr)
            continue
        bills = data.get("dataList") or []
        for b in bills:
            key = b.get("billNo") or ""
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            all_bills.append(b)
        print(f"[info] session {sp}: {len(bills)} bills", file=sys.stderr)
    print(f"[info] total unique bills: {len(all_bills)}", file=sys.stderr)
    return all_bills


def classify(bill, sectors):
    """回傳 (matched_sectors, is_budget_related, is_policy)。

    - is_budget: 含核心預算關鍵字（預算、撥款、特別條例…）
    - is_policy: 含產業政策行動詞（發展、推動、促進…）+ 產業關鍵字
    """
    text = " ".join([
        bill.get("billName") or "",
        bill.get("billOrg") or "",
    ])
    is_budget = any(k in text for k in BUDGET_KEYWORDS_CORE)
    is_policy_action = any(k in text for k in POLICY_ACTION_KEYWORDS)
    matched = []
    for sector_name, sector_def in sectors.items():
        for kw in sector_def["keywords"]:
            if kw in text:
                matched.append(sector_name)
                break
    is_policy = is_policy_action and bool(matched)
    return matched, is_budget, is_policy


def parse_bill_date(bill_no):
    """billNo 前 8 碼通常為 yyyymmdd 提案日期。失敗回 None。"""
    if not bill_no or len(bill_no) < 8:
        return None
    prefix = bill_no[:8]
    if not prefix.isdigit():
        return None
    try:
        return datetime.strptime(prefix, "%Y%m%d").date()
    except ValueError:
        return None


def shorten(text, limit=140):
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def load_cache():
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def update_cache(bills, cache, today_iso):
    """更新快取並標記新案/狀態變更。回傳帶 change 資訊的 bills。"""
    annotated = []
    for b in bills:
        no = b.get("billNo") or ""
        status = b.get("billStatus") or ""
        prev = cache.get(no)
        change = "新提案"
        prev_status = None
        if prev:
            if prev.get("billStatus") != status:
                change = "狀態變更"
                prev_status = prev.get("billStatus")
            else:
                change = "無變化"
        cache[no] = {
            "billStatus": status,
            "first_seen": (prev or {}).get("first_seen") or today_iso,
            "last_changed": today_iso if change != "無變化" else (prev or {}).get("last_changed") or today_iso,
        }
        ann = dict(b)
        ann["_change"] = change
        ann["_prev_status"] = prev_status
        ann["_first_seen"] = cache[no]["first_seen"]
        ann["_last_changed"] = cache[no]["last_changed"]
        annotated.append(ann)
    return annotated


STATUS_COLORS = {
    "交付審查": "#4b6bfb",
    "排入院會討論": "#f59e0b",
    "一讀": "#8b5cf6",
    "二讀": "#ec4899",
    "三讀": "#10b981",
    "審查完畢": "#06b6d4",
    "撤回": "#6b7280",
}


def status_color(status):
    for key, color in STATUS_COLORS.items():
        if key in (status or ""):
            return color
    return "#6b7280"


def render_html(bills_with_sectors, sectors, stats, now_str):
    """輸出單頁 HTML dashboard。"""

    # 依產業分組
    by_sector = {name: [] for name in sectors.keys()}
    unmatched = []
    for row in bills_with_sectors:
        if row["sectors"]:
            for s in row["sectors"]:
                by_sector[s].append(row)
        else:
            unmatched.append(row)

    def bill_card(row):
        b = row["bill"]
        sectors_chips = "".join(
            f'<span class="chip chip-sector">{escape(s)}</span>' for s in row["sectors"]
        )
        type_chip = ""
        if row.get("is_budget"):
            type_chip = '<span class="chip chip-budget">💰 預算類</span>'
        elif row.get("is_policy"):
            type_chip = '<span class="chip chip-policy">📋 產業政策</span>'
        change_cls = {
            "新提案": "badge-new",
            "狀態變更": "badge-change",
            "無變化": "badge-stale",
        }.get(row["change"], "badge-stale")
        change_text = row["change"]
        if row["change"] == "狀態變更" and row.get("prev_status"):
            change_text = f"狀態變更：{row['prev_status']} → {b.get('billStatus', '')}"
        status_html = (
            f'<span class="status" style="background:{status_color(b.get("billStatus"))}">'
            f'{escape(b.get("billStatus") or "-")}</span>'
        )
        date = parse_bill_date(b.get("billNo") or "")
        date_str = date.isoformat() if date else "-"
        pdf = b.get("pdfUrl") or ""
        pdf_link = (
            f'<a class="pdf" href="{escape(pdf)}" target="_blank" rel="noopener">原文 PDF</a>'
            if pdf else ""
        )
        org = escape(shorten(b.get("billOrg") or "-", 60))
        proposer = escape(shorten(b.get("billProposer") or "", 80))
        return f"""
        <div class="bill">
          <div class="bill-head">
            <span class="badge {change_cls}">{escape(change_text)}</span>
            {status_html}
            <span class="bill-date">{escape(date_str)}</span>
          </div>
          <div class="bill-name">{escape(shorten(b.get('billName') or '', 200))}</div>
          <div class="bill-meta">
            <span class="bill-org">{org}</span>
            {f'<span class="bill-proposer">提案人：{proposer}</span>' if proposer else ''}
          </div>
          <div class="bill-chips">{type_chip}{sectors_chips}</div>
          <div class="bill-foot">
            <span class="bill-no">#{escape(b.get('billNo') or '')}</span>
            {pdf_link}
          </div>
        </div>
        """

    sector_sections = []
    for name, info in sectors.items():
        rows = by_sector.get(name, [])
        if not rows:
            continue
        # 新提案 + 狀態變更 排前面
        rows.sort(key=lambda r: (
            0 if r["change"] == "新提案" else 1 if r["change"] == "狀態變更" else 2,
            -(parse_bill_date(r["bill"].get("billNo") or "") or datetime(1900, 1, 1).date()).toordinal(),
        ))
        stock_chips = "".join(
            f'<span class="chip chip-stock">{escape(s["code"])} {escape(s["name"])}</span>'
            for s in info["stocks"]
        )
        new_count = sum(1 for r in rows if r["change"] == "新提案")
        chg_count = sum(1 for r in rows if r["change"] == "狀態變更")
        section = f"""
        <section class="sector">
          <div class="sector-head">
            <h2>{escape(name)}</h2>
            <div class="sector-meta">
              <span class="sector-stat">法案 {len(rows)}</span>
              {'<span class="sector-stat new">新提案 ' + str(new_count) + '</span>' if new_count else ''}
              {'<span class="sector-stat change">狀態變更 ' + str(chg_count) + '</span>' if chg_count else ''}
            </div>
          </div>
          <div class="stocks">
            <span class="stocks-label">受益個股候選：</span>{stock_chips}
          </div>
          <div class="bills">
            {''.join(bill_card(r) for r in rows)}
          </div>
        </section>
        """
        sector_sections.append(section)

    # Top KPI
    kpi = f"""
    <div class="kpis">
      <div class="kpi"><div class="kpi-label">預算/撥款類</div><div class="kpi-value">{stats['budget']}</div></div>
      <div class="kpi"><div class="kpi-label">產業政策類</div><div class="kpi-value">{stats['policy']}</div></div>
      <div class="kpi kpi-hot"><div class="kpi-label">今日新提案</div><div class="kpi-value">{stats['new']}</div></div>
      <div class="kpi kpi-warn"><div class="kpi-label">今日狀態變更</div><div class="kpi-value">{stats['changed']}</div></div>
    </div>
    """

    unmatched_block = ""
    if unmatched:
        hot = [r for r in unmatched if r["change"] in ("新提案", "狀態變更")]
        if hot:
            unmatched_block = f"""
            <section class="sector unmatched" id="sec-general">
              <div class="sector-head">
                <h2>一般預算 / 特別條例（未對應特定產業）</h2>
                <div class="sector-meta"><span class="sector-stat">{len(hot)}</span></div>
              </div>
              <div class="bills">
                {''.join(bill_card(r) for r in hot[:50])}
              </div>
            </section>
            """

    sector_nav = "".join(
        f'<a href="#sec-{i}">{escape(name)}</a>'
        for i, (name, rows) in enumerate(
            [(n, by_sector[n]) for n in sectors.keys() if by_sector[n]]
        )
    )

    html = f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>台灣預算法案監控｜產業＆個股對照</title>
<style>
*,*::before,*::after{{box-sizing:border-box}}
:root{{
  --bg:#0f1419;--panel:#1a2332;--panel-2:#243147;--ink:#e8eef7;--muted:#8b9bb4;
  --line:#2d3b52;--accent:#60a5fa;--accent-2:#93c5fd;
  --new:#10b981;--change:#f59e0b;--stale:#64748b;
}}
html,body{{margin:0;padding:0;background:var(--bg);color:var(--ink);
  font-family:"Noto Sans TC","PingFang TC","Microsoft JhengHei",system-ui,sans-serif;
  font-size:14px;line-height:1.6;-webkit-font-smoothing:antialiased}}
.wrap{{max-width:1200px;margin:0 auto;padding:20px}}
header{{margin-bottom:20px}}
h1{{font-size:24px;font-weight:800;margin:0 0 6px;line-height:1.3}}
.subtitle{{color:var(--muted);font-size:13px;margin-bottom:14px}}
.nav{{display:flex;flex-wrap:wrap;gap:6px;padding:10px 0;border-top:1px solid var(--line);border-bottom:1px solid var(--line);margin-bottom:18px}}
.nav a{{color:var(--accent);text-decoration:none;font-size:12px;padding:4px 10px;border:1px solid var(--line);border-radius:14px;transition:all .15s}}
.nav a:hover{{background:var(--panel-2);border-color:var(--accent)}}
.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:18px}}
@media (max-width:700px){{.kpis{{grid-template-columns:repeat(2,1fr)}}}}
.kpi{{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px 16px}}
.kpi-label{{font-size:12px;color:var(--muted);margin-bottom:4px}}
.kpi-value{{font-size:26px;font-weight:800;color:var(--ink)}}
.kpi-hot .kpi-value{{color:var(--new)}}
.kpi-warn .kpi-value{{color:var(--change)}}
.sector{{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:18px;margin-bottom:16px}}
.sector-head{{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:12px}}
.sector h2{{font-size:18px;font-weight:700;margin:0;color:var(--ink)}}
.sector-meta{{display:flex;gap:8px}}
.sector-stat{{font-size:12px;color:var(--muted);background:var(--panel-2);padding:3px 10px;border-radius:12px}}
.sector-stat.new{{color:var(--new);border:1px solid var(--new)}}
.sector-stat.change{{color:var(--change);border:1px solid var(--change)}}
.stocks{{margin-bottom:14px;padding:10px 12px;background:var(--panel-2);border-radius:8px;font-size:12px}}
.stocks-label{{color:var(--muted);margin-right:6px}}
.chip{{display:inline-block;padding:2px 9px;border-radius:10px;font-size:11px;margin:2px 3px;white-space:nowrap}}
.chip-stock{{background:#1e40af;color:#dbeafe}}
.chip-sector{{background:#065f46;color:#d1fae5}}
.chip-budget{{background:#7c2d12;color:#fed7aa}}
.chip-policy{{background:#4c1d95;color:#ddd6fe}}
.bills{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:10px}}
.bill{{background:var(--panel-2);border:1px solid var(--line);border-radius:8px;padding:12px}}
.bill-head{{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:8px}}
.badge{{font-size:11px;font-weight:700;padding:2px 8px;border-radius:10px}}
.badge-new{{background:var(--new);color:#02261a}}
.badge-change{{background:var(--change);color:#2a1a00}}
.badge-stale{{background:var(--stale);color:#1a2233;opacity:.7}}
.status{{font-size:11px;color:#fff;padding:2px 8px;border-radius:10px;font-weight:600}}
.bill-date{{font-size:11px;color:var(--muted);margin-left:auto}}
.bill-name{{font-size:13px;font-weight:600;color:var(--ink);line-height:1.5;margin-bottom:6px}}
.bill-meta{{font-size:11px;color:var(--muted);margin-bottom:6px}}
.bill-org{{display:block;margin-bottom:2px}}
.bill-proposer{{display:block;color:#6b7f9a}}
.bill-chips{{margin:6px 0}}
.bill-foot{{display:flex;justify-content:space-between;align-items:center;font-size:11px;color:var(--muted);margin-top:6px;padding-top:6px;border-top:1px dashed var(--line)}}
.bill-no{{font-family:ui-monospace,monospace}}
.pdf{{color:var(--accent);text-decoration:none}}
.pdf:hover{{text-decoration:underline}}
footer{{margin-top:24px;padding:14px 0;border-top:1px solid var(--line);color:var(--muted);font-size:11px;line-height:1.7}}
footer b{{color:#fbbf24}}
</style>
</head>
<body>
<div class="wrap">
<header>
<h1>台灣預算法案監控 <span style="color:var(--muted);font-weight:400;font-size:14px">Budget Bill Monitor</span></h1>
<div class="subtitle">每日自動抓取立法院議案提案 API，依關鍵字字典分類到受益產業與代表個股。更新於 {escape(now_str)}</div>
</header>
{kpi}
<nav class="nav">{sector_nav}</nav>
{''.join(f'<div id="sec-{i}"></div>{sec}' for i, sec in enumerate(sector_sections))}
{unmatched_block}
<footer>
<b>資料來源：</b>立法院開放資料平台 ID20 議案提案 API（data.ly.gov.tw）<br>
<b>分類方式：</b>以 budget_keywords.json 定義的關鍵字規則比對法案名稱與提案單位，未使用 LLM。<br>
<b>風險聲明：</b>本頁僅為資料整理與研究用途，個股清單為各產業代表上市櫃公司，不構成投資建議；法案通過與否、實際撥款金額與受益範圍請以官方公告為準。市場可能已提前反應。
</footer>
</div>
</body>
</html>
"""
    return html


def main():
    keywords_data = json.loads(KEYWORDS_PATH.read_text(encoding="utf-8"))
    sectors = keywords_data["sectors"]

    bills = fetch_bills()
    cache = load_cache()
    today_iso = datetime.now(TPE).date().isoformat()
    annotated = update_cache(bills, cache, today_iso)

    # 僅保留：預算/撥款類 OR 產業政策類（政策行動詞 + 產業關鍵字）
    rows = []
    for b in annotated:
        matched_sectors, is_budget, is_policy = classify(b, sectors)
        if not (is_budget or is_policy):
            continue
        rows.append({
            "bill": b,
            "sectors": matched_sectors,
            "is_budget": is_budget,
            "is_policy": is_policy,
            "change": b["_change"],
            "prev_status": b.get("_prev_status"),
        })

    stats = {
        "total": len(bills),
        "matched": len(rows),
        "budget": sum(1 for r in rows if r["is_budget"]),
        "policy": sum(1 for r in rows if r["is_policy"] and not r["is_budget"]),
        "new": sum(1 for r in rows if r["change"] == "新提案"),
        "changed": sum(1 for r in rows if r["change"] == "狀態變更"),
    }

    now_str = datetime.now(TPE).strftime("%Y-%m-%d %H:%M (UTC+8)")
    html = render_html(rows, sectors, stats, now_str)
    OUTPUT_HTML.write_text(html, encoding="utf-8")

    # Persist
    CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    LATEST_JSON.write_text(
        json.dumps({
            "generated_at": now_str,
            "stats": stats,
            "bills": [
                {
                    "billNo": r["bill"].get("billNo"),
                    "billName": r["bill"].get("billName"),
                    "billOrg": r["bill"].get("billOrg"),
                    "billStatus": r["bill"].get("billStatus"),
                    "pdfUrl": r["bill"].get("pdfUrl"),
                    "sectors": r["sectors"],
                    "is_budget": r["is_budget"],
                    "is_policy": r["is_policy"],
                    "change": r["change"],
                    "prev_status": r["prev_status"],
                    "first_seen": r["bill"].get("_first_seen"),
                    "last_changed": r["bill"].get("_last_changed"),
                }
                for r in rows
            ],
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        f"[done] total={stats['total']} matched={stats['matched']} "
        f"new={stats['new']} changed={stats['changed']}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
