#!/usr/bin/env python3
"""
美國聯邦預算法案監控 — 每日抓取法案 → Claude 判讀 → 對應受惠公司 → 量體校正
產出 budget_dashboard.html 供 WordPress 嵌入。
"""
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import anthropic
import requests
import yfinance as yf
from pydantic import BaseModel, Field

# ── 設定 ────────────────────────────────────────────────────────────────────

CONGRESS_API_KEY = os.environ.get("CONGRESS_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

MODEL = "claude-opus-4-7"
CG_BASE = "https://api.congress.gov/v3"
TODAY = datetime.now(timezone.utc).date()
OUTPUT_HTML = "budget_dashboard.html"
OUTPUT_JSON = "budget_latest.json"
CACHE_FILE = "budget_cache.json"
LOOKBACK_DAYS = 30
MAX_BILLS_PER_RUN = 60  # cost 控制：每次最多新處理這麼多件（快取命中的不算）

# Policy areas that map to investable industries (Congress.gov 官方分類)
POLICY_AREAS_OF_INTEREST = {
    "Appropriations", "Armed Forces and National Security", "Energy",
    "Health", "Science, Technology, Communications", "Transportation and Public Works",
    "Environmental Protection", "Agriculture and Food", "Economics and Public Finance",
    "Taxation", "Commerce", "Labor and Employment", "Education",
    "Housing and Community Development", "Water Resources Development",
    "Emergency Management",
}

# 命名類、紀念類等沒有投資意義的法案
SKIP_TITLE_PATTERNS = [
    r"\bto designate\b", r"\brename\w*\b", r"\bcommemorat\w*\b",
    r"\bcommend\w*\b", r"\bhonor\w* the\b",
    r"\bexpressing the (sense|sympathy|condolences)\b",
    r"\bday of recognition\b", r"\bnational .* month\b",
    r"\bpost office\b",
]

# 階段歷史通過率 (GovTrack 長期統計)
STAGE_BASELINE = {
    "introduced": 0.04, "referred": 0.05, "committee": 0.18,
    "reported": 0.35, "passed_one": 0.55, "passed_both": 0.92,
    "enrolled": 0.97, "signed": 1.00,
}

STAGE_LABEL_ZH = {
    "introduced": "提案",
    "referred": "交付委員會",
    "committee": "委員會審議",
    "reported": "委員會通過",
    "passed_one": "單院通過",
    "passed_both": "兩院通過",
    "enrolled": "送交總統",
    "signed": "已簽署",
}

BILL_TYPE_SLUG = {
    "hr": "house-bill", "s": "senate-bill",
    "hjres": "house-joint-resolution", "sjres": "senate-joint-resolution",
    "hconres": "house-concurrent-resolution", "sconres": "senate-concurrent-resolution",
    "hres": "house-resolution", "sres": "senate-resolution",
}


# ── Pydantic schemas (Claude 結構化輸出) ────────────────────────────────────

class BudgetProgram(BaseModel):
    name: str = Field(description="程式/條款名稱")
    amount_usd: Optional[float] = Field(description="該項預算 USD，找不到則 null")
    industry_hint: str = Field(description="受惠產業 (中文)")


class BudgetExtraction(BaseModel):
    total_amount_usd: Optional[float] = Field(description="法案總預算 USD，找不到 null")
    period_years: Optional[int] = Field(description="實施年限，單年或不明則 null")
    programs: List[BudgetProgram]
    is_appropriation: bool = Field(description="是否為實際撥款 (true) vs 只是授權 (false)")
    confidence: float = Field(description="0-1 信心度")
    notes: str = Field(description="1-2 句中文說明")


class PassageAdjustment(BaseModel):
    adjustment: float = Field(description="-0.30 到 +0.30 的通過機率調整")
    reasoning: str = Field(description="1-2 句中文說明關鍵因子")


class BeneficiaryCompany(BaseModel):
    ticker: str = Field(description="美股 ticker，必須是實際存在的")
    name: str
    estimated_share_pct: float = Field(description="估計可拿到法案預算的百分比 (0-100)")
    rationale: str = Field(description="1 句中文說明")
    confidence: float = Field(description="0-1 信心度")


class CompanyMapping(BaseModel):
    industries: List[str] = Field(description="受惠產業 (中文)")
    companies: List[BeneficiaryCompany]


# ── Congress.gov client ─────────────────────────────────────────────────────

def cg_get(path, params=None, max_retries=3):
    params = dict(params or {})
    params["api_key"] = CONGRESS_API_KEY
    params["format"] = "json"
    for i in range(max_retries):
        try:
            r = requests.get(f"{CG_BASE}{path}", params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                wait = int(r.headers.get("retry-after", 60))
                print(f"  rate limited, wait {wait}s")
                time.sleep(wait)
                continue
            print(f"  {path} → {r.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"  {path} error: {e}")
        time.sleep(2 ** i)
    return None


def fetch_recent_bills():
    """抓取最近 LOOKBACK_DAYS 天有動作的法案。"""
    from_dt = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT00:00:00Z")
    bills = []
    offset = 0
    page_size = 250
    while True:
        data = cg_get("/bill", params={
            "fromDateTime": from_dt, "limit": page_size, "offset": offset,
            "sort": "updateDate+desc",
        })
        if not data or not data.get("bills"):
            break
        page = data["bills"]
        bills.extend(page)
        if len(page) < page_size or len(bills) >= 1000:
            break
        offset += page_size
    return bills


def fetch_bill_detail(bill_ref):
    cg_id = bill_ref.get("congress")
    btype = (bill_ref.get("type") or "").lower()
    number = bill_ref.get("number")
    if not (cg_id and btype and number):
        return None
    base = f"/bill/{cg_id}/{btype}/{number}"
    detail = cg_get(base) or {}
    summaries = cg_get(f"{base}/summaries") or {}
    cosponsors = cg_get(f"{base}/cosponsors", params={"limit": 250}) or {}
    actions = cg_get(f"{base}/actions", params={"limit": 250}) or {}
    return {
        "congress": cg_id, "type": btype, "number": number,
        "detail": detail.get("bill", {}),
        "summaries": summaries.get("summaries", []),
        "cosponsors": cosponsors.get("cosponsors", []),
        "actions": actions.get("actions", []),
    }


# ── Filtering ───────────────────────────────────────────────────────────────

def is_material_bill(bill_ref):
    title = (bill_ref.get("title") or "").lower()
    for pat in SKIP_TITLE_PATTERNS:
        if re.search(pat, title):
            return False
    policy = ((bill_ref.get("policyArea") or {}).get("name"))
    if policy in POLICY_AREAS_OF_INTEREST:
        return True
    if re.search(r"\b(appropriation\w*|authoriz\w+ act|funding|budget|fiscal year \d{4}|chips|infrastructure|reauthoriz\w+)\b", title):
        return True
    return False


def current_stage(actions):
    if not actions:
        return "introduced"
    texts = [(a.get("text") or "").lower() for a in actions]
    joined = " | ".join(texts)
    if re.search(r"became public law|signed by president", joined):
        return "signed"
    if re.search(r"cleared for.*president|presented to president", joined):
        return "enrolled"
    passed_house = bool(re.search(r"passed[/ ]house|on passage.*agreed to.*house", joined))
    passed_senate = bool(re.search(r"passed[/ ]senate|on passage.*agreed to.*senate", joined))
    if passed_house and passed_senate:
        return "passed_both"
    if passed_house or passed_senate:
        return "passed_one"
    if re.search(r"reported by.*committee|committee.*reported", joined):
        return "reported"
    if re.search(r"committee consideration|markup|ordered to be reported", joined):
        return "committee"
    if re.search(r"referred to", joined):
        return "referred"
    return "introduced"


# ── Claude wrappers ─────────────────────────────────────────────────────────

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


BUDGET_EXTRACT_SYSTEM = """你是美國聯邦預算法案分析師。給定法案標題與摘要，抽取結構化的預算資訊。

判斷重點：
1. 總預算 (total_amount_usd)：整份法案的核心金額。多年計畫回報累計總額。
2. 實施年限 (period_years)：法案明確提到 N 年填入；單年或不明填 null。
3. 各 program：列出主要支出項目與估計金額（從條文或 CBO 估算）。
4. 授權 vs 撥款：
   - Appropriation 撥款法案 = 實際撥錢，is_appropriation=true
   - Authorization 授權法案 = 只核准上限，實際是否撥款看後續 appropriation，is_appropriation=false 並在 notes 標注
5. 政策宣示類法案（無具體金額）→ total_amount_usd=null, confidence 降低。

notes 輸出繁體中文，1-2 句。"""


PASSAGE_NUANCE_SYSTEM = """你是美國國會法案通過機率分析師。已有基於階段的歷史基準通過率 (baseline)，給出一個調整值 (-0.30 到 +0.30)。

評估因子：
- 兩黨共同提案人數（bipartisan = 最強正向訊號）
- 提案院別 vs 多數黨
- 是否為年度必過法案（NDAA, appropriations, 再授權類）
- 委員會動態（markup scheduled, reported favorably）
- 黨領袖表態
- 歷史同類法案命運

reasoning 輸出繁體中文，1-2 句。"""


COMPANY_MAP_SYSTEM = """你是美國政策 → 股市映射分析師。給定一份美國聯邦法案的資訊（標題、預算、摘要），列出具體可能受惠的美股上市公司。

核心原則：
1. **以歷史合約與產業龍頭為基礎**：優先列出歷史上拿過同類計畫合約的公司、或在該產業有顯著市占的公司。
2. **避免過度猜測**：通用性政策 → confidence 降低；具名指定（如 CHIPS Act 半導體）→ confidence 拉高。
3. **estimated_share_pct 合理估算**：所有公司 share 加總不需 100%（法案可能有非上市受惠方）。意思是「這家公司可能分到法案總預算的 X%」。
4. **特別標注小市值公司**：市值 < $5B 但可能拿到鉅額合約 → 獨立列出（budget/mcap 比值會高，alpha 強）。
5. **ticker 必須真實**：NYSE 或 Nasdaq 實際存在的 ticker。ADR 可。不要發明。

rationale 輸出繁體中文，1 句。列 5-15 家公司為佳。"""


def _parse_or_none(parse_fn, *args, **kwargs):
    """呼叫 Claude 的包裝器，失敗時印訊息回 None。"""
    try:
        return parse_fn(*args, **kwargs)
    except anthropic.APIStatusError as e:
        print(f"  Claude API error {e.status_code}: {str(e)[:200]}")
    except anthropic.APIError as e:
        print(f"  Claude error: {str(e)[:200]}")
    except Exception as e:
        print(f"  Claude unexpected: {type(e).__name__}: {str(e)[:200]}")
    return None


def extract_budget(summary_text, title):
    def call():
        resp = claude.messages.parse(
            model=MODEL,
            max_tokens=2000,
            system=[{
                "type": "text", "text": BUDGET_EXTRACT_SYSTEM,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": f"法案標題：{title}\n\n法案摘要：\n{summary_text}\n\n請抽取結構化預算資訊。"}],
            output_format=BudgetExtraction,
        )
        return resp.parsed_output
    return _parse_or_none(call)


def score_passage_nuance(title, stage, baseline, actions_summary, cosponsor_stats):
    def call():
        resp = claude.messages.parse(
            model=MODEL,
            max_tokens=1500,
            thinking={"type": "adaptive"},
            system=[{
                "type": "text", "text": PASSAGE_NUANCE_SYSTEM,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": f"""法案：{title}

當前階段：{stage}（歷史基準通過率 {baseline:.1%}）

近期 actions：
{actions_summary}

提案人概況：
{cosponsor_stats}

請給出調整值。"""}],
            output_format=PassageAdjustment,
        )
        return resp.parsed_output
    result = _parse_or_none(call)
    if result is None:
        return 0.0, ""
    return max(-0.30, min(0.30, result.adjustment)), result.reasoning


def map_companies(title, budget_json, summary):
    def call():
        resp = claude.messages.parse(
            model=MODEL,
            max_tokens=4000,
            thinking={"type": "adaptive"},
            system=[{
                "type": "text", "text": COMPANY_MAP_SYSTEM,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": f"""法案：{title}

預算結構化資料：
{budget_json}

法案摘要：
{summary}

請列出可能受惠的美股上市公司。"""}],
            output_format=CompanyMapping,
        )
        return resp.parsed_output
    return _parse_or_none(call)


# ── Passage probability scorer ─────────────────────────────────────────────

def compute_passage_prob(bill_detail, stage, claude_adjustment=0.0):
    base = STAGE_BASELINE.get(stage, 0.04)
    cosponsors = bill_detail.get("cosponsors") or []
    parties = {(c.get("party") or "").upper() for c in cosponsors} & {"R", "D"}
    bipartisan_boost = 0.10 if len(parties) == 2 else 0.0
    cosponsor_boost = min(0.10, len(cosponsors) / 200)
    title = (bill_detail.get("detail", {}).get("title") or "").lower()
    must_pass_boost = 0.15 if re.search(
        r"\bappropriations?\b|\bnational defense authorization\b|\breauthorization\b", title
    ) else 0.0
    prob = base + bipartisan_boost + cosponsor_boost + must_pass_boost + claude_adjustment
    return max(0.01, min(0.98, prob))


# ── Market cap lookup ──────────────────────────────────────────────────────

def fetch_ticker_info(ticker):
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}
        return {
            "ticker": ticker,
            "yf_name": info.get("longName") or info.get("shortName") or "",
            "market_cap": info.get("marketCap"),
            "revenue_ttm": info.get("totalRevenue"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "country": info.get("country"),
        }
    except Exception as e:
        print(f"  yfinance {ticker}: {type(e).__name__}")
        return {"ticker": ticker, "market_cap": None, "revenue_ttm": None}


def enrich_companies(all_tickers, workers=8):
    info_map = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_ticker_info, t): t for t in all_tickers}
        for fut in as_completed(futures):
            info = fut.result()
            info_map[info["ticker"]] = info
    return info_map


# ── Alpha computation ─────────────────────────────────────────────────────

def compute_alpha(company_share_usd, ticker_info, passage_prob, period_years):
    mcap = ticker_info.get("market_cap") or 0
    revenue = ticker_info.get("revenue_ttm") or 0
    period = max(1, period_years or 1)
    annual_share = company_share_usd / period
    budget_to_mcap = (annual_share / mcap) if mcap else 0
    budget_to_revenue = (annual_share / revenue) if revenue else 0
    expected_alpha = budget_to_mcap * passage_prob
    return {
        "annual_share_usd": annual_share,
        "budget_to_mcap_pct": budget_to_mcap * 100,
        "budget_to_revenue_pct": budget_to_revenue * 100,
        "expected_alpha_bp": expected_alpha * 10000,
    }


# ── Cache ──────────────────────────────────────────────────────────────────

def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def bill_needs_refresh(bill_id, bill_ref, cache):
    cached = cache.get(bill_id)
    if not cached:
        return True
    last_action = (bill_ref.get("latestAction") or {}).get("actionDate")
    return cached.get("last_action_date") != last_action


# ── Process single bill ────────────────────────────────────────────────────

def bill_id_of(bill_ref):
    return f"{bill_ref.get('congress')}-{(bill_ref.get('type') or '').lower()}-{bill_ref.get('number')}"


def bill_url(congress, btype, number):
    slug = BILL_TYPE_SLUG.get(btype, "house-bill")
    ordinal = f"{congress}th"
    return f"https://www.congress.gov/bill/{ordinal}-congress/{slug}/{number}"


def process_bill(bill_ref, cache):
    bid = bill_id_of(bill_ref)
    if not bill_needs_refresh(bid, bill_ref, cache):
        return cache[bid]

    print(f"  {bid}: {(bill_ref.get('title') or '')[:60]}")
    detail = fetch_bill_detail(bill_ref)
    if not detail:
        return None

    summaries = detail["summaries"] or []
    summary_text = ""
    if summaries:
        s_sorted = sorted(summaries, key=lambda s: s.get("actionDate") or "", reverse=True)
        summary_text = s_sorted[0].get("text") or ""
        summary_text = re.sub(r"<[^>]+>", "", summary_text)[:4000]

    title = detail["detail"].get("title") or bill_ref.get("title", "")
    stage = current_stage(detail["actions"])
    if not summary_text:
        summary_text = f"(尚無正式摘要) {title}"

    # Claude: extract budget
    budget = extract_budget(summary_text, title)

    # Cosponsor stats
    cosponsors = detail["cosponsors"]
    n_r = sum(1 for c in cosponsors if (c.get("party") or "") == "R")
    n_d = sum(1 for c in cosponsors if (c.get("party") or "") == "D")
    cosponsor_stats = f"共 {len(cosponsors)} 位共同提案人 (R={n_r}, D={n_d})"

    # Claude: passage nuance
    actions_preview = "\n".join(
        f"  {a.get('actionDate', '')}: {(a.get('text') or '')[:120]}"
        for a in (detail["actions"][:5] or [])
    )
    baseline = STAGE_BASELINE.get(stage, 0.04)
    nuance_adj, nuance_reason = score_passage_nuance(title, stage, baseline, actions_preview, cosponsor_stats)
    passage_prob = compute_passage_prob(detail, stage, nuance_adj)

    # Claude: map companies (有 budget 才打這個 call，節省成本)
    mapping = None
    if budget and budget.total_amount_usd:
        budget_json = budget.model_dump_json(indent=2)
        mapping = map_companies(title, budget_json, summary_text)

    record = {
        "bill_id": bid,
        "title": title,
        "congress": detail["congress"],
        "type": detail["type"],
        "number": detail["number"],
        "url": bill_url(detail["congress"], detail["type"], detail["number"]),
        "policy_area": (detail["detail"].get("policyArea") or {}).get("name"),
        "stage": stage,
        "stage_label": STAGE_LABEL_ZH.get(stage, stage),
        "passage_baseline": baseline,
        "passage_nuance_adj": nuance_adj,
        "passage_nuance_reason": nuance_reason,
        "passage_prob": passage_prob,
        "cosponsors_total": len(cosponsors),
        "cosponsors_R": n_r,
        "cosponsors_D": n_d,
        "budget": budget.model_dump() if budget else None,
        "companies_raw": mapping.model_dump() if mapping else None,
        "latest_action": bill_ref.get("latestAction"),
        "last_action_date": (bill_ref.get("latestAction") or {}).get("actionDate"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    return record


# ── HTML rendering ─────────────────────────────────────────────────────────

def _fmt_usd(v):
    if v is None or v == 0:
        return "—"
    if v >= 1e12:
        return f"${v/1e12:.1f}T"
    if v >= 1e9:
        return f"${v/1e9:.1f}B"
    if v >= 1e6:
        return f"${v/1e6:.1f}M"
    return f"${v:,.0f}"


def _fmt_pct(v):
    if v is None:
        return "—"
    return f"{v:.1f}%"


def _prob_class(p):
    if p >= 0.5:
        return "hi"
    if p >= 0.2:
        return "mi"
    return "lo"


def _alpha_class(bp):
    if bp >= 50:
        return "hi"
    if bp >= 10:
        return "mi"
    return "lo"


CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b;--card:#fff;--gold:#b45309}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:14px;line-height:1.5}
.hdr{background:linear-gradient(135deg,#0f172a 0%,#1e3a8a 50%,#2563eb 100%);color:#fff;padding:22px 28px 16px}
.hdr h1{font-size:20px;font-weight:800;margin-bottom:4px;letter-spacing:-0.3px}
.hdr .sub{font-size:11.5px;opacity:.85}
.nav-link{color:#93c5fd;font-size:11.5px;text-decoration:none;margin-left:14px;border-bottom:1px dashed #93c5fd;padding-bottom:1px}
.nav-link:hover{opacity:.7}
.summary{display:flex;gap:10px;padding:14px 28px;background:var(--card);border-bottom:1px solid var(--brd);flex-wrap:wrap}
.summary .mk{background:var(--bg);border:1px solid var(--brd);border-radius:8px;padding:10px 16px;min-width:160px}
.summary .lbl{font-size:10.5px;color:var(--mu);font-weight:600;letter-spacing:0.3px}
.summary .px{font-size:18px;font-weight:700;margin-top:2px}
.construction{background:linear-gradient(90deg,#fef3c7,#fde68a);border-bottom:2px solid #f59e0b;color:#78350f;padding:10px 28px;font-size:12.5px;font-weight:600;text-align:center}
.pane{padding:20px 28px}
.ttl{font-size:15px;font-weight:700;margin-bottom:4px}
.desc{font-size:12.5px;color:var(--mu);margin-bottom:16px;line-height:1.7}
.bills{display:flex;flex-direction:column;gap:10px}
.bill{background:var(--card);border:1px solid var(--brd);border-radius:10px;overflow:hidden}
.bill > summary{list-style:none;cursor:pointer;padding:14px 18px;display:grid;grid-template-columns:90px 1fr 140px 120px 100px;gap:14px;align-items:center;font-size:13px}
.bill > summary::-webkit-details-marker{display:none}
.bill > summary:hover{background:#f8fafc}
.bill[open] > summary{background:#eef2ff;border-bottom:1px solid var(--brd)}
.bid{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-weight:700;font-size:11.5px;color:var(--mu)}
.btitle{font-weight:600;line-height:1.4;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
.bstage{display:flex;flex-direction:column;gap:4px}
.bstage .pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10.5px;font-weight:700;text-align:center;width:fit-content}
.pill.stage{background:#e2e8f0;color:#475569}
.pill.stage.passed_one,.pill.stage.passed_both,.pill.stage.enrolled,.pill.stage.signed{background:#dcfce7;color:#15803d}
.pill.stage.committee,.pill.stage.reported{background:#fef3c7;color:#b45309}
.probbar{width:100%;height:6px;background:var(--brd);border-radius:3px;overflow:hidden;margin-top:2px}
.probbar > span{display:block;height:100%;background:var(--bl)}
.probbar.hi > span{background:var(--gr)}
.probbar.mi > span{background:var(--am)}
.probbar.lo > span{background:var(--rd)}
.probtxt{font-size:11px;color:var(--mu);margin-top:2px;font-variant-numeric:tabular-nums}
.bamt{text-align:right;font-weight:700;font-variant-numeric:tabular-nums}
.balpha{text-align:right}
.balpha .num{font-weight:800;font-variant-numeric:tabular-nums;font-size:15px}
.balpha .num.hi{color:var(--gold)}
.balpha .num.mi{color:var(--am)}
.balpha .num.lo{color:var(--mu)}
.balpha .lbl{font-size:10px;color:var(--mu);font-weight:600}
.detail{padding:16px 20px;background:#fafbff;display:grid;grid-template-columns:1.2fr 2fr;gap:18px}
@media(max-width:860px){.detail{grid-template-columns:1fr}.bill > summary{grid-template-columns:1fr 100px;grid-template-areas:"id alpha" "title title" "stage amt";gap:8px}.bid{grid-area:id}.btitle{grid-area:title}.bstage{grid-area:stage}.bamt{grid-area:amt;text-align:left}.balpha{grid-area:alpha}}
.detail .block h4{font-size:11px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:0.4px;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid var(--brd)}
.detail .kv{font-size:12.5px;line-height:1.9}
.detail .kv .k{color:var(--mu);display:inline-block;min-width:84px}
.detail .kv .v{font-weight:600}
.detail .notes{font-size:12.5px;color:#475569;margin-top:8px;line-height:1.7;padding:8px 10px;background:#fff;border-left:3px solid var(--bl);border-radius:0 6px 6px 0}
.progs{margin-top:10px;font-size:12px}
.progs .row{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px dashed #e2e8f0}
.progs .row:last-child{border-bottom:none}
.progs .nm{color:var(--txt)}
.progs .hint{color:var(--mu);font-size:11px;margin-left:6px}
.progs .amt{font-weight:700;font-variant-numeric:tabular-nums}
.cotable{width:100%;border-collapse:collapse;font-size:12px}
.cotable th,.cotable td{padding:6px 8px;text-align:left;border-bottom:1px dashed #e2e8f0}
.cotable th{font-size:10px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:0.3px;border-bottom:1px solid var(--brd)}
.cotable td.num{text-align:right;font-variant-numeric:tabular-nums}
.cotable .tk{font-weight:800;color:var(--bl)}
.cotable .tk a{color:inherit;text-decoration:none}
.cotable .tk a:hover{text-decoration:underline}
.cotable .ratio.hi{color:var(--gold);font-weight:700}
.cotable .ratio.mi{color:var(--am);font-weight:700}
.cotable .rationale{color:var(--mu);font-size:11px;margin-top:2px;line-height:1.4}
.cotable .conf{display:inline-block;padding:1px 5px;border-radius:6px;font-size:10px;font-weight:700;background:#e2e8f0;color:#475569}
.cotable .conf.hi{background:#dcfce7;color:#15803d}
.cotable .conf.lo{background:#fee2e2;color:#b91c1c}
.empty{text-align:center;color:var(--mu);padding:28px;font-size:13px;background:var(--card);border:1px dashed var(--brd);border-radius:10px}
.ft{text-align:center;color:var(--mu);font-size:11.5px;padding:20px 28px;border-top:1px solid var(--brd);line-height:1.8;background:var(--card)}
.biglink{color:var(--bl);text-decoration:none;font-size:11.5px;margin-left:4px}
.biglink:hover{text-decoration:underline}
@media(max-width:768px){.hdr,.summary,.pane{padding-left:16px;padding-right:16px}}
"""


def _render_bill(rec):
    budget = rec.get("budget") or {}
    beneficiaries = rec.get("beneficiaries") or []
    top_alpha = max((b.get("expected_alpha_bp", 0) for b in beneficiaries), default=0)

    # Summary row
    prob = rec["passage_prob"]
    prob_cls = _prob_class(prob)
    total_amt = budget.get("total_amount_usd")
    period = budget.get("period_years")
    amt_str = _fmt_usd(total_amt)
    if period:
        amt_str += f'<div style="font-size:10.5px;color:var(--mu);font-weight:500">{period} 年</div>'

    alpha_cls = _alpha_class(top_alpha)
    alpha_txt = f"{top_alpha:.0f}" if top_alpha else "—"

    stage_cls = rec["stage"]

    # Detail: budget block
    notes = (budget.get("notes") or "").strip()
    is_approp = budget.get("is_appropriation")
    approp_tag = ""
    if budget:
        approp_tag = '<span class="pill" style="background:#dbeafe;color:#1e3a8a;margin-left:6px">撥款</span>' if is_approp else '<span class="pill" style="background:#fef3c7;color:#b45309;margin-left:6px">授權</span>'

    progs_html = ""
    programs = budget.get("programs") or []
    if programs:
        rows = []
        for p in programs[:8]:
            rows.append(
                f'<div class="row"><span class="nm">{p.get("name", "")}<span class="hint">（{p.get("industry_hint", "")}）</span></span>'
                f'<span class="amt">{_fmt_usd(p.get("amount_usd"))}</span></div>'
            )
        progs_html = f'<div class="progs">{"".join(rows)}</div>'

    budget_block = f"""<div class="block">
<h4>📋 法案詳情 {approp_tag}</h4>
<div class="kv">
  <div><span class="k">Bill ID</span><span class="v">{rec["bill_id"].upper()}</span><a class="biglink" href="{rec['url']}" target="_blank" rel="noopener">Congress.gov ↗</a></div>
  <div><span class="k">政策領域</span><span class="v">{rec.get("policy_area") or "—"}</span></div>
  <div><span class="k">當前階段</span><span class="v">{rec["stage_label"]}</span></div>
  <div><span class="k">通過機率</span><span class="v">{prob*100:.1f}%</span>
    <span style="font-size:11px;color:var(--mu);margin-left:6px">= 基準 {rec["passage_baseline"]*100:.0f}% + 調整 {rec["passage_nuance_adj"]*100:+.0f}%</span></div>
  <div><span class="k">共同提案人</span><span class="v">{rec["cosponsors_total"]} 位</span>
    <span style="font-size:11px;color:var(--mu);margin-left:6px">R={rec["cosponsors_R"]} · D={rec["cosponsors_D"]}</span></div>
  <div><span class="k">總預算</span><span class="v">{_fmt_usd(total_amt)}{f" ({period} 年)" if period else ""}</span></div>
  <div><span class="k">最近行動</span><span class="v" style="font-size:11.5px">{(rec.get("latest_action") or {}).get("actionDate") or "—"}</span></div>
</div>
{f'<div class="notes"><b>Claude 判讀</b>：{notes}</div>' if notes else ''}
{f'<div class="notes" style="border-left-color:var(--am)"><b>通過機率依據</b>：{rec.get("passage_nuance_reason", "")}</div>' if rec.get("passage_nuance_reason") else ''}
{progs_html}
</div>"""

    # Companies table
    if not beneficiaries:
        cos_block = '<div class="block"><h4>🏢 受惠公司</h4><div class="empty" style="padding:20px">Claude 未對此法案產出公司映射（可能因無具體金額或政策方向過於廣泛）</div></div>'
    else:
        rows = []
        for b in beneficiaries[:15]:
            alpha_bp = b.get("expected_alpha_bp", 0) or 0
            ratio_cls = "hi" if alpha_bp >= 50 else "mi" if alpha_bp >= 10 else ""
            conf = b.get("confidence") or 0
            conf_cls = "hi" if conf >= 0.7 else "lo" if conf < 0.4 else ""
            tk = b.get("ticker", "")
            ticker_cell = f'<a href="https://finance.yahoo.com/quote/{tk}" target="_blank" rel="noopener">{tk}</a>'
            name = b.get("yf_name") or b.get("name") or ""
            rows.append(f"""<tr>
<td><div class="tk">{ticker_cell}</div><div style="font-size:10px;color:var(--mu)">{name[:32]}</div></td>
<td class="num">{_fmt_usd(b.get("market_cap"))}</td>
<td class="num">{b.get("estimated_share_pct", 0):.1f}%<div style="font-size:10px;color:var(--mu)">{_fmt_usd(b.get("share_usd"))}</div></td>
<td class="num ratio {ratio_cls}">{_fmt_pct(b.get("budget_to_mcap_pct"))}</td>
<td class="num"><span style="font-weight:800">{alpha_bp:.0f}</span><div style="font-size:10px;color:var(--mu)">bp</div></td>
<td><span class="conf {conf_cls}">{conf*100:.0f}%</span></td>
</tr>
<tr><td colspan="6" style="padding-top:0;padding-bottom:8px;border-bottom:1px solid var(--brd)"><span class="rationale">{b.get("rationale", "")}</span></td></tr>""")
        ind_str = "、".join((rec.get("companies_raw") or {}).get("industries", []))
        cos_block = f"""<div class="block">
<h4>🏢 受惠公司（預估）</h4>
{f'<div style="font-size:12px;color:var(--mu);margin-bottom:8px">受惠產業：{ind_str}</div>' if ind_str else ''}
<table class="cotable">
<thead><tr><th>公司</th><th style="text-align:right">市值</th><th style="text-align:right">佔比估算</th><th style="text-align:right">預算/市值</th><th style="text-align:right">期望 Alpha</th><th>信心</th></tr></thead>
<tbody>{"".join(rows)}</tbody>
</table>
<div style="font-size:10.5px;color:var(--mu);margin-top:8px;line-height:1.6">
• <b>期望 Alpha</b> = (年化預算/市值) × 通過機率，以 bp 顯示，越高代表這家公司相對其量體的預期影響越大<br>
• <b>佔比估算</b> = Claude 估計該公司可拿到法案預算的百分比（非官方合約），請自行交叉驗證
</div>
</div>"""

    return f"""<details class="bill">
<summary>
<span class="bid">{rec["bill_id"].upper()}</span>
<div class="btitle">{rec["title"]}</div>
<div class="bstage">
  <span class="pill stage {stage_cls}">{rec["stage_label"]}</span>
  <div class="probbar {prob_cls}"><span style="width:{prob*100:.0f}%"></span></div>
  <div class="probtxt">通過機率 {prob*100:.1f}%</div>
</div>
<div class="bamt">{amt_str}</div>
<div class="balpha"><div class="num {alpha_cls}">{alpha_txt}</div><div class="lbl">TOP α (bp)</div></div>
</summary>
<div class="detail">
{budget_block}
{cos_block}
</div>
</details>"""


def render_html(records):
    # Summary stats
    total_bills = len(records)
    total_budget = sum(
        (r.get("budget") or {}).get("total_amount_usd") or 0 for r in records
    )
    weighted_budget = sum(
        ((r.get("budget") or {}).get("total_amount_usd") or 0) * r.get("passage_prob", 0)
        for r in records
    )
    top_bills = sorted(
        records,
        key=lambda r: max((b.get("expected_alpha_bp", 0) for b in r.get("beneficiaries") or []), default=0),
        reverse=True,
    )

    if not records:
        body = '<div class="empty">本次執行未產出任何法案紀錄。等下次 GitHub Action 再跑看看，或檢查 API key 是否設定正確。</div>'
    else:
        body = '<div class="bills">' + "".join(_render_bill(r) for r in top_bills) + "</div>"

    return f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>美國預算法案監控 — {TODAY.isoformat()}</title>
<meta name="robots" content="noindex, nofollow"/>
<meta name="referrer" content="no-referrer"/>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>美國聯邦預算法案監控</h1>
  <div class="sub">更新：{TODAY.isoformat()}（每日自動抓 Congress.gov → Claude 判讀 → 量體校正）<a class="nav-link" href="us_index.html">→ 美股板塊</a><a class="nav-link" href="etf_index.html">→ ETF 資金流向</a><a class="nav-link" href="index.html">→ 可轉債</a></div>
</div>
<div class="construction">🚧 開發測試中｜Claude 產出的預算金額與公司映射有機率誤判，請務必交叉驗證 🚧</div>
<div class="summary">
  <div class="mk"><div class="lbl">追蹤法案</div><div class="px">{total_bills} 件</div></div>
  <div class="mk"><div class="lbl">預算總額（名目）</div><div class="px">{_fmt_usd(total_budget)}</div></div>
  <div class="mk"><div class="lbl">期望加權預算（×通過機率）</div><div class="px">{_fmt_usd(weighted_budget)}</div></div>
</div>
<div class="pane">
  <div class="ttl">法案清單（按 Top α 排序）</div>
  <div class="desc">每筆法案顯示當前階段、通過機率、總預算。展開後看 Claude 判讀的受惠公司與量體校正結果。期望 Alpha 越高代表該公司相對自身市值的預期影響越大——這是真正的 alpha 所在。</div>
  {body}
</div>
<div class="ft">資料來源：Congress.gov API · Claude Opus 4.7 判讀 · Yahoo Finance（yfinance） ｜ 僅供研究參考，不構成投資建議</div>
</body></html>"""


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    if not CONGRESS_API_KEY:
        print("::error::missing CONGRESS_API_KEY env var")
        sys.exit(1)
    if not ANTHROPIC_API_KEY:
        print("::error::missing ANTHROPIC_API_KEY env var")
        sys.exit(1)

    print(f"=== 預算法案監控 ({TODAY.isoformat()}) ===")

    cache = load_cache()
    print(f"快取中已有 {len(cache)} 筆法案歷史")

    print(f"抓取 Congress.gov 最近 {LOOKBACK_DAYS} 天法案...")
    all_bills = fetch_recent_bills()
    print(f"  收到 {len(all_bills)} 件")

    material = [b for b in all_bills if is_material_bill(b)]
    print(f"  過濾後（有投資意義）：{len(material)} 件")

    records = []
    processed_this_run = 0
    for bill_ref in material:
        bid = bill_id_of(bill_ref)
        if bid in cache and not bill_needs_refresh(bid, bill_ref, cache):
            records.append(cache[bid])
            continue
        if processed_this_run >= MAX_BILLS_PER_RUN:
            continue
        rec = process_bill(bill_ref, cache)
        if rec:
            records.append(rec)
            cache[bid] = rec
            processed_this_run += 1

    print(f"本次 Claude 新處理：{processed_this_run} 件（其餘走快取）")

    # 抓市值
    all_tickers = set()
    for rec in records:
        cr = rec.get("companies_raw") or {}
        for c in cr.get("companies") or []:
            tk = (c.get("ticker") or "").upper().strip()
            if tk and re.match(r"^[A-Z.]{1,6}$", tk):
                all_tickers.add(tk)
    print(f"抓取 {len(all_tickers)} 個 ticker 的 yfinance 市值...")
    ticker_info_map = enrich_companies(all_tickers) if all_tickers else {}

    # Alpha
    for rec in records:
        cr = rec.get("companies_raw") or {}
        budget = rec.get("budget") or {}
        total = budget.get("total_amount_usd") or 0
        period = budget.get("period_years") or 1
        prob = rec.get("passage_prob", 0)
        beneficiaries = []
        for c in cr.get("companies") or []:
            tk = (c.get("ticker") or "").upper().strip()
            info = ticker_info_map.get(tk, {})
            share_usd = total * (c.get("estimated_share_pct", 0) / 100)
            alpha = compute_alpha(share_usd, info, prob, period)
            beneficiaries.append({**c, "ticker": tk, "share_usd": share_usd, **info, **alpha})
        beneficiaries.sort(key=lambda x: x.get("expected_alpha_bp", 0), reverse=True)
        rec["beneficiaries"] = beneficiaries

    # 存 cache（含最新 beneficiaries）
    for rec in records:
        cache[rec["bill_id"]] = rec
    save_cache(cache)

    # 存 JSON 快取
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "records": records,
        }, f, ensure_ascii=False, indent=2)

    # 輸出 HTML
    html = render_html(records)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n完成：{len(records)} 件法案 → {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
