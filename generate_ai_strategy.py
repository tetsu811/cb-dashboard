#!/usr/bin/env python3
"""
AI 供應鏈策略儀表板
- 九巨頭（Mag7 + TSM + AVGO）為頂層追蹤
- 由上到下展開至供應鏈 bucket 與個股
- 資料源：FMP（基本面/分析師面）+ yfinance（價格/新聞/技術面）
- 兩源交叉驗證；任一失效自動 fallback
每個交易日收盤後執行，產出 ai_strategy.html。
"""
import glob
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import yfinance as yf

TODAY = datetime.now().date()
NOW_UTC = datetime.now(timezone.utc)
OUTPUT_HTML = "ai_strategy.html"
CACHE_FILE = "ai_latest.json"
HISTORY_DIR = "ai_data"
MAX_HISTORY_DAYS = 60

# FMP config
FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_KEY = os.environ.get("FMP_API_KEY", "").strip()
FMP_TIMEOUT = 15
FMP_CACHE_DIR = "ai_fmp_cache"
# 各端點的快取 TTL（小時）
FMP_TTL_HOURS = {
    "ratios": 72,               # 基本面比率 — 財報才動，3 天快取
    "key-metrics-ttm": 72,      # 同上
    "financial-scores": 168,    # 一週
    "price-target-consensus": 24,
    "grades-historical": 24,
    "analyst-estimates": 72,
    "profile": 168,
    "cash-flow-statement": 720, # 30 天（季報才動）
    "batch-quote": 0,           # 每次重抓
}

# Capex 週期分組：哪些巨頭 vs 哪些受惠 bucket
CAPEX_GROUPS = [
    ("hyperscaler", "雲端 Hyperscaler",
     ["MSFT", "GOOGL", "META", "AMZN"],
     ["ai_chip", "hbm", "network", "power"]),
    ("foundry", "晶圓代工（TSM）",
     ["TSM"],
     ["equipment", "eda"]),
]

# ── 九巨頭 ─────────────────────────────────────────────────────────────────
GIANTS = [
    ("NVDA",  "NVIDIA",     "AI 晶片"),
    ("MSFT",  "Microsoft",  "雲端 / Copilot"),
    ("GOOGL", "Alphabet",   "雲端 / Gemini"),
    ("META",  "Meta",       "Llama / 廣告"),
    ("AMZN",  "Amazon",     "AWS / Bedrock"),
    ("AAPL",  "Apple",      "終端 AI"),
    ("TSLA",  "Tesla",      "FSD / Dojo"),
    ("TSM",   "TSMC",       "全球晶圓代工"),
    ("AVGO",  "Broadcom",   "客製 AI ASIC"),
]

# ── 供應鏈 buckets（由上游至下游）────────────────────────────────────────
BUCKETS = [
    # (key, label, role, [symbols])
    ("equipment", "半導體設備", "最上游",
        ["ASML", "AMAT", "LRCX", "KLAC", "TOELY", "ONTO", "ENTG"]),
    ("eda",       "EDA 工具",   "晶片設計",
        ["SNPS", "CDNS", "ANSS"]),
    ("foundry",   "晶圓代工",   "製造核心",
        ["TSM", "GFS", "UMC"]),
    ("ai_chip",   "AI 晶片",    "運算核心",
        ["NVDA", "AVGO", "AMD", "MRVL", "QCOM", "INTC"]),
    ("hbm",       "HBM / 記憶體", "運算配套",
        ["MU", "WDC", "STX", "SIMO"]),
    ("network",   "網通 / 光通訊", "資料中心連結",
        ["ANET", "LITE", "COHR", "CIEN", "CSCO", "VIAV", "AXTI", "FN", "EXTR"]),
    ("power",     "電源 / 散熱 / 伺服器", "資料中心基建",
        ["VRT", "ETN", "SMCI", "DELL", "HPE", "APH"]),
    ("cloud_saas", "雲端 / 企業 SaaS", "下游平台",
        ["ORCL", "CRM", "NOW", "SNOW", "PLTR", "DDOG", "MDB"]),
    ("ai_app",    "AI 應用層", "終端應用",
        ["ADBE", "IBM", "AI", "PATH", "CRWD", "PANW"]),
]

# 每個巨頭在哪些 bucket 有重要依賴（強度 1-3）
DEPENDENCY = {
    "NVDA":  {"foundry": 3, "eda": 3, "equipment": 2, "hbm": 3, "network": 2, "power": 2},
    "MSFT":  {"ai_chip": 3, "foundry": 2, "hbm": 2, "network": 3, "power": 3, "cloud_saas": 1},
    "GOOGL": {"ai_chip": 2, "foundry": 3, "hbm": 2, "network": 3, "power": 3, "cloud_saas": 1},
    "META":  {"ai_chip": 3, "foundry": 2, "hbm": 2, "network": 3, "power": 3, "ai_app": 1},
    "AMZN":  {"ai_chip": 2, "foundry": 2, "hbm": 2, "network": 3, "power": 3, "cloud_saas": 1},
    "AAPL":  {"foundry": 3, "eda": 2, "equipment": 1, "ai_app": 1},
    "TSLA":  {"foundry": 2, "ai_chip": 1},
    "TSM":   {"equipment": 3, "eda": 2},
    "AVGO":  {"foundry": 3, "eda": 3, "network": 3},
}

# ── 抓取參數 ────────────────────────────────────────────────────────────
WORKERS = 8
NEWS_PER_STOCK = 3
NEWS_MAX_AGE_HOURS = 48
CROSS_CHECK_DIFF_PCT = 2.0  # 兩源差超過這個 % 就提示

# ── 紅黃綠燈門檻 ────────────────────────────────────────────────────────
TRAFFIC_LIGHT_RULES = {
    # 損益表 4 盞
    "revenue_growth_yoy": {"green": 0.15, "yellow": 0.05, "higher_better": True, "label": "營收年增"},
    "operating_margin":   {"green": 0.20, "yellow": 0.10, "higher_better": True, "label": "營業利益率"},
    "net_margin":         {"green": 0.15, "yellow": 0.07, "higher_better": True, "label": "淨利率"},
    "gross_margin":       {"green": 0.40, "yellow": 0.25, "higher_better": True, "label": "毛利率"},

    # 資產負債 4 盞
    "debt_to_equity":     {"green": 0.5,  "yellow": 1.5,  "higher_better": False, "label": "負債/股東權益"},
    "current_ratio":      {"green": 2.0,  "yellow": 1.0,  "higher_better": True,  "label": "流動比率"},
    "roe":                {"green": 0.15, "yellow": 0.08, "higher_better": True,  "label": "ROE"},
    "roic":               {"green": 0.15, "yellow": 0.08, "higher_better": True,  "label": "ROIC"},

    # 現金流 3 盞
    "fcf_margin":         {"green": 0.15, "yellow": 0.05, "higher_better": True, "label": "FCF / 營收"},
    "income_quality":     {"green": 0.90, "yellow": 0.70, "higher_better": True, "label": "盈餘品質(FCF/NI)"},
    "fcf_yield":          {"green": 0.04, "yellow": 0.02, "higher_better": True, "label": "FCF 殖利率"},

    # 估值 1 盞
    "forward_peg":        {"green": 1.2,  "yellow": 2.0,  "higher_better": False, "label": "Forward PEG"},
}


# ── FMP 客戶端（含檔案快取）─────────────────────────────────────────────

class FMPError(Exception): pass


def _cache_path(path, params):
    """根據 endpoint + params 產生快取檔名。"""
    os.makedirs(FMP_CACHE_DIR, exist_ok=True)
    safe_path = path.replace("/", "_")
    # 從 params 取主要識別（symbol / symbols / limit / period）
    parts = [safe_path]
    for k in ("symbol", "symbols", "period"):
        if k in params:
            parts.append(f"{k}={params[k]}")
    if "limit" in params:
        parts.append(f"limit={params['limit']}")
    fname = "_".join(parts)[:200] + ".json"
    return os.path.join(FMP_CACHE_DIR, fname)


def _cache_fresh(cache_file, ttl_hours):
    if ttl_hours <= 0 or not os.path.isfile(cache_file):
        return False
    age_s = time.time() - os.path.getmtime(cache_file)
    return age_s < ttl_hours * 3600


def fmp_get(path, **params):
    """GET /{path}?... 加上 apikey。成功回傳 JSON，失敗 raise。
    會優先檢查 file cache（依端點 TTL）。"""
    ttl = FMP_TTL_HOURS.get(path.split("/")[0], 24)
    cache_file = _cache_path(path, params)
    if _cache_fresh(cache_file, ttl):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    if not FMP_KEY:
        raise FMPError("FMP_API_KEY env 未設定")
    params["apikey"] = FMP_KEY
    url = f"{FMP_BASE}/{path}"
    try:
        r = requests.get(url, params=params, timeout=FMP_TIMEOUT)
    except requests.RequestException as e:
        raise FMPError(f"network: {e}")
    if r.status_code != 200:
        raise FMPError(f"HTTP {r.status_code}: {r.text[:120]}")
    try:
        data = r.json()
    except ValueError:
        raise FMPError(f"invalid JSON: {r.text[:120]}")
    if isinstance(data, dict) and "Error Message" in data:
        raise FMPError(data["Error Message"])
    if isinstance(data, str) and "Restricted" in data:
        raise FMPError("endpoint restricted on current plan")

    # 成功 → 存快取
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass
    return data


def fmp_safe(path, default=None, **params):
    """Graceful — 失敗回 default、不噴。"""
    try:
        return fmp_get(path, **params)
    except FMPError as e:
        print(f"  [FMP {path}] {e}")
        return default


def fmp_batch_quote(symbols):
    """用 batch-quote 一次抓多檔的 quote，省 API calls。"""
    if not symbols or not FMP_KEY:
        return {}
    syms = ",".join(sorted(set(symbols)))
    data = fmp_safe("batch-quote", [], symbols=syms)
    out = {}
    if isinstance(data, list):
        for row in data:
            if "symbol" in row:
                out[row["symbol"]] = row
    return out


# ── yfinance 工具 ──────────────────────────────────────────────────────

def yf_batch_bars(symbols):
    """批次抓近 2 年日線（足夠算 1y 報酬 + 200 日均線）。"""
    df = yf.download(
        list(symbols), period="2y", interval="1d",
        auto_adjust=False, progress=False, group_by="ticker", threads=True,
    )
    return df


def yf_metrics_for_symbol(bars_df, sym):
    """從批次 K 線抽個股指標：day%、MAs、RSI、量能比。"""
    if sym not in bars_df.columns.get_level_values(0):
        return None
    bars = bars_df[sym].dropna(subset=["Close"])
    if len(bars) < 2:
        return None
    closes, vols = bars["Close"], bars["Volume"]
    last, prev = closes.iloc[-1], closes.iloc[-2]
    day_pct = (last / prev - 1) * 100

    def pct_over(n):
        return float((last / closes.iloc[-(n + 1)] - 1) * 100) if len(closes) > n else None

    def ma(n):
        return float(closes.iloc[-n:].mean()) if len(closes) >= n else None

    vol_today = float(vols.iloc[-1]) if len(vols) else None
    vol_yday = float(vols.iloc[-2]) if len(vols) >= 2 else None
    vol_vs_yday = (vol_today / vol_yday) if (vol_yday and vol_today) else None

    return {
        "price": float(last),
        "day_pct": float(day_pct),
        "pct_5d": pct_over(5),
        "pct_20d": pct_over(20),
        "pct_60d": pct_over(60),
        "pct_252d": pct_over(252),  # 1 年
        "ma20": ma(20),
        "ma50": ma(50),
        "ma200": ma(200),
        "above_ma50": ma(50) and last > ma(50),
        "above_ma200": ma(200) and last > ma(200),
        "vol_vs_yday": float(vol_vs_yday) if vol_vs_yday else None,
        "rsi": _rsi(closes),
    }


def _rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    diffs = closes.diff().dropna()
    gains = diffs.clip(lower=0)
    losses = (-diffs).clip(lower=0)
    avg_g = gains.iloc[:period].mean()
    avg_l = losses.iloc[:period].mean()
    for g, l in zip(gains.iloc[period:], losses.iloc[period:]):
        avg_g = (avg_g * (period - 1) + g) / period
        avg_l = (avg_l * (period - 1) + l) / period
    if avg_l == 0:
        return 100.0
    return float(100 - 100 / (1 + avg_g / avg_l))


#
# ── 新聞分類器（語意）────────────────────────────────────────────────────
# 四大類高價值類別（訂單 / 指引 / 高管 / 財報）優先命中；都沒中才做情緒詞典 fallback。
#

NEWS_CATEGORY_RULES = [
    # (category, 正向關鍵字, 負向關鍵字, 類別偵測字 — 任一命中就進此類)
    ("order", "訂單 / 合約",
        ["wins", "awarded", "secures", "landed", "clinches", "deal", "contract", "order", "partnership", "supply agreement", "multi-year", "signs"],
        ["loses", "cancels", "cancelled", "terminated", "dropped", "ends partnership"],
        ["contract", "deal", "order", "agreement", "partnership", "supply"]),
    ("guidance", "指引修訂",
        ["raises guidance", "raised guidance", "upgrades", "upgraded", "raises target", "boost", "beats estimates", "lifts outlook", "raised price target", "outperform"],
        ["cuts guidance", "lowers guidance", "downgrades", "downgraded", "cuts target", "slashes", "warns", "below estimates", "lowered outlook", "underperform"],
        ["guidance", "outlook", "forecast", "target", "rating", "estimate", "analyst", "upgrade", "downgrade", "price target"]),
    ("insider", "高管動作",
        ["insider buying", "bought shares", "increased stake", "ceo buys", "purchases shares", "insider purchase", "accumulates"],
        ["insider selling", "sold shares", "dumped", "unloads", "cashes out", "ceo sells", "insider sale", "reduces stake"],
        ["insider", "ceo", "executive", "director", "stake", "holding"]),
    ("earnings", "財報",
        ["beats", "beat estimates", "tops estimates", "record revenue", "strong quarter", "exceeds", "blowout"],
        ["misses", "missed estimates", "falls short", "disappointing", "weak quarter", "shortfall"],
        ["earnings", "q1", "q2", "q3", "q4", "quarterly", "eps", "revenue of $", "reports earnings"]),
]

# 一般情緒詞典（非特定類別時用）
SENTIMENT_POS = {
    "surge", "soar", "jump", "rally", "rise", "gain", "climb", "advance", "boost",
    "strong", "record", "best", "wins", "bullish", "positive", "growth",
    "expand", "expansion", "optimistic", "breakthrough", "approved", "launch",
    "premium", "success", "bullish", "milestone", "all-time high", "tops",
    "powering", "dominates", "leading", "outperform", "exceeds",
}
SENTIMENT_NEG = {
    "fall", "drop", "plunge", "decline", "slump", "crash", "tumble", "slide",
    "cut", "weak", "loss", "worst", "loses", "hurt", "bearish", "negative",
    "shrink", "warn", "warning", "layoff", "lawsuit", "probe", "investigation",
    "recall", "delay", "halt", "below", "sinks", "plummets", "underperform",
    "selloff", "disappointing", "concerns", "risk", "tumbled",
}


# ── Claude LLM 新聞深度分析（可選） ─────────────────────────────────────

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = "claude-haiku-4-5"  # 成本低、速度快
LLM_CACHE_DIR = "ai_llm_cache"
LLM_CACHE_TTL_HOURS = 48
LLM_NEWS_SYSTEM_PROMPT = """你是美股財經新聞分析師。收到新聞標題後，回傳 JSON 評估該新聞對標的股價的即時影響。

輸出嚴格 JSON（不要有任何 markdown 或解釋文字），schema：
{
  "impact": "bullish" | "bearish" | "neutral",
  "magnitude": "high" | "medium" | "low",
  "category": "earnings" | "guidance" | "product" | "partnership" | "regulation" | "macro" | "competition" | "management" | "other",
  "zh": "<繁體中文摘要，12-20 字>"
}

判斷原則：
- bullish/bearish 判斷要考慮對「該標的」的影響，不是對整體市場
- magnitude: high = 會顯著推升/壓低股價（>2% 日幅），medium = 小幅（0.5-2%），low = 關聯度弱
- category: 精確歸類；若含多類別，選最主導的
- zh: 簡短白話，說明發生什麼事與方向，不要加解讀性評論"""


def _llm_cache_key(sym, title):
    import hashlib
    h = hashlib.sha256((sym + "|" + title).encode()).hexdigest()[:24]
    os.makedirs(LLM_CACHE_DIR, exist_ok=True)
    return os.path.join(LLM_CACHE_DIR, f"{h}.json")


def _llm_cache_fresh(path):
    if not os.path.isfile(path):
        return False
    age_s = time.time() - os.path.getmtime(path)
    return age_s < LLM_CACHE_TTL_HOURS * 3600


def call_claude_news_analysis(sym, title):
    """回傳 {impact, magnitude, category, zh} 或 None。"""
    cache_path = _llm_cache_key(sym, title)
    if _llm_cache_fresh(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    if not ANTHROPIC_API_KEY:
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": 200,
                "system": [{
                    "type": "text",
                    "text": LLM_NEWS_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                "messages": [{
                    "role": "user",
                    "content": f"Ticker: {sym}\nHeadline: {title}"
                }],
            },
            timeout=20,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        text = data["content"][0]["text"].strip()
        # 清掉可能的 markdown code fence
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:].strip()
        parsed = json.loads(text)
        # 存快取
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(parsed, f, ensure_ascii=False)
        return parsed
    except Exception as e:
        print(f"  [LLM {sym}] {e}")
        return None


def enrich_news_with_llm(stocks):
    """對九巨頭的 top N 新聞做 LLM 分析（成本可控）。"""
    if not ANTHROPIC_API_KEY:
        print("ℹ️  ANTHROPIC_API_KEY 未設定，跳過 LLM 新聞深度分析（keyword classifier 仍作用）")
        return 0

    giant_set = {code for code, _, _ in GIANTS}
    tasks = []
    for sym, s in stocks.items():
        if sym not in giant_set:
            continue
        for news_item in (s.get("news") or [])[:3]:
            tasks.append((sym, news_item))

    print(f"LLM：並行分析 {len(tasks)} 則九巨頭新聞...")
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(call_claude_news_analysis, sym, n["title"]): (sym, n) for sym, n in tasks}
        for fut in as_completed(futures):
            sym, news_item = futures[fut]
            result = fut.result()
            if result:
                news_item["llm"] = result
                done += 1
    print(f"  完成 {done}/{len(tasks)} · {time.time()-t0:.1f}s")
    return done


BENCHMARKS = [
    ("SPY",  "S&P 500"),
    ("QQQ",  "Nasdaq 100"),
    ("SOXX", "半導體 ETF"),
    ("VGT",  "科技 ETF"),
]


def fetch_benchmarks():
    """抓 benchmark ETF 的 2 年日線、算各期報酬。"""
    syms = [s for s, _ in BENCHMARKS]
    df = yf.download(syms, period="2y", interval="1d", auto_adjust=False, progress=False, group_by="ticker", threads=True)
    out = []
    for sym, label in BENCHMARKS:
        try:
            bars = df[sym].dropna(subset=["Close"]) if sym in df.columns.get_level_values(0) else None
            if bars is None or len(bars) < 2:
                out.append({"symbol": sym, "label": label, "price": None, "day_pct": None, "pct_252d": None})
                continue
            closes = bars["Close"]
            last = float(closes.iloc[-1])
            prev = float(closes.iloc[-2])
            day_pct = (last / prev - 1) * 100
            pct_252 = None
            if len(closes) > 252:
                pct_252 = float((last / closes.iloc[-253] - 1) * 100)
            out.append({"symbol": sym, "label": label, "price": last, "day_pct": day_pct, "pct_252d": pct_252})
        except Exception:
            out.append({"symbol": sym, "label": label, "price": None, "day_pct": None, "pct_252d": None})
    return out


def compute_portfolio_return(stocks, symbols, field="pct_252d"):
    """等權組合的報酬。"""
    rets = []
    for s in symbols:
        stk = stocks.get(s)
        if not stk:
            continue
        val = (stk.get("tech") or {}).get(field)
        if val is not None:
            rets.append(val)
    if not rets:
        return None
    return sum(rets) / len(rets)


def _classify_news(title, summary=""):
    """回傳 (category, category_label, sentiment, matched_words)。
    - category: 'order' | 'guidance' | 'insider' | 'earnings' | 'general'
    - sentiment: 'positive' | 'negative' | 'neutral'
    """
    text = (title + " " + (summary or "")).lower()
    matched = []

    # 1. 類別偵測（掃描 4 大類規則）
    for category, label, pos_kws, neg_kws, detectors in NEWS_CATEGORY_RULES:
        # 類別命中條件：detector 關鍵字 + 明確正/負向詞
        detected = any(d in text for d in detectors)
        if not detected:
            continue
        pos_hit = [w for w in pos_kws if w in text]
        neg_hit = [w for w in neg_kws if w in text]
        if pos_hit or neg_hit:
            matched = pos_hit + neg_hit
            if pos_hit and not neg_hit:
                return (category, label, "positive", matched)
            if neg_hit and not pos_hit:
                return (category, label, "negative", matched)
            # 正負都有 → 中性（混合訊號）
            return (category, label, "neutral", matched)

    # 2. 都沒中 → 跑一般情緒詞典
    pos_count = sum(1 for w in SENTIMENT_POS if w in text)
    neg_count = sum(1 for w in SENTIMENT_NEG if w in text)
    if pos_count > neg_count:
        return ("general", "一般新聞", "positive", [])
    if neg_count > pos_count:
        return ("general", "一般新聞", "negative", [])
    return ("general", "一般新聞", "neutral", [])


def _parse_news_item(raw, related_sym):
    c = raw.get("content") or {}
    if c.get("contentType") not in (None, "STORY", "VIDEO"):
        return None
    title = c.get("title") or ""
    if not title:
        return None
    summary = c.get("summary") or ""
    pub = c.get("pubDate") or c.get("displayTime")
    try:
        ts = datetime.fromisoformat(pub.replace("Z", "+00:00")) if pub else None
    except (ValueError, AttributeError):
        ts = None
    if ts and (NOW_UTC - ts).total_seconds() > NEWS_MAX_AGE_HOURS * 3600:
        return None

    category, cat_label, sentiment, matched = _classify_news(title, summary)

    return {
        "symbol": related_sym,
        "title": title,
        "summary": summary[:200] if summary else "",
        "url": (c.get("clickThroughUrl") or {}).get("url") or (c.get("canonicalUrl") or {}).get("url") or "",
        "provider": (c.get("provider") or {}).get("displayName", ""),
        "timestamp": ts.isoformat() if ts else None,
        "ts_epoch": ts.timestamp() if ts else 0,
        "category": category,
        "category_label": cat_label,
        "sentiment": sentiment,
        "matched_keywords": matched,
    }


def yf_fundamentals(sym):
    """從 yfinance .info 抽基本面欄位（當 FMP 失效的備援）。"""
    try:
        info = yf.Ticker(sym).info or {}
    except Exception:
        return {}
    return {
        # 利潤率
        "gross_margin": info.get("grossMargins"),
        "operating_margin": info.get("operatingMargins"),
        "net_margin": info.get("profitMargins"),
        # 成長性
        "revenue_growth_yoy": info.get("revenueGrowth"),
        # 資產負債
        "current_ratio": info.get("currentRatio"),
        "roe": info.get("returnOnEquity"),
        # yfinance 的 debtToEquity 是 0-400+ 的百分比刻度，要換算
        "debt_to_equity": (info["debtToEquity"] / 100.0) if info.get("debtToEquity") else None,
        # 現金流
        "fcf_ttm": info.get("freeCashflow"),
        "ocf_ttm": info.get("operatingCashflow"),
        "total_revenue": info.get("totalRevenue"),
        "market_cap": info.get("marketCap"),
        # 估值
        "forward_pe": info.get("forwardPE"),
        "trailing_pe": info.get("trailingPE"),
        "peg_ratio": info.get("pegRatio"),
        # 分析師
        "target_mean_price": info.get("targetMeanPrice"),
        "target_high_price": info.get("targetHighPrice"),
        "target_low_price": info.get("targetLowPrice"),
        "num_analysts": info.get("numberOfAnalystOpinions"),
        "recommendation_mean": info.get("recommendationMean"),  # 1=strong buy ... 5=strong sell
        "recommendation_key": info.get("recommendationKey"),
        # 其他
        "industry": info.get("industry"),
        "long_name": info.get("longName") or info.get("shortName"),
    }


def fetch_yf_fundamentals_bulk(symbols):
    """並行抓 yfinance .info 當 FMP 備援。"""
    out = {}
    syms = sorted(set(symbols))
    print(f"yfinance：並行抓 {len(syms)} 檔 .info 作為基本面備援...")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(yf_fundamentals, s): s for s in syms}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                out[sym] = fut.result()
            except Exception:
                out[sym] = {}
    print(f"  完成，耗時 {time.time()-t0:.1f}s")
    return out


def yf_news_for_symbol(sym):
    try:
        news = yf.Ticker(sym).news or []
    except Exception:
        return []
    items = []
    for raw in news:
        it = _parse_news_item(raw, sym)
        if it:
            items.append(it)
    items.sort(key=lambda x: x["ts_epoch"], reverse=True)
    return items[:NEWS_PER_STOCK]


# ── FMP 綜合抓取（每檔個股一組 API 呼叫）────────────────────────────────

def fmp_enrichment(sym, include_estimates):
    """抓單一個股的 FMP 資料（核心端點）。"""
    out = {
        "quote": None,          # 由 batch-quote 另外注入
        "ratios_latest": None,
        "ratios_history": [],
        "key_metrics_ttm": None,
        "financial_scores": None,
        "analyst_estimates_next": None,
        "price_target": None,
        "grades": None,
    }

    # 核心 5 端點（所有個股都抓）
    rh = fmp_safe("ratios", [], symbol=sym, period="annual", limit=5)
    if isinstance(rh, list) and rh:
        out["ratios_history"] = rh
        out["ratios_latest"] = rh[0]

    km = fmp_safe("key-metrics-ttm", [], symbol=sym)
    if isinstance(km, list) and km:
        out["key_metrics_ttm"] = km[0]

    fs = fmp_safe("financial-scores", [], symbol=sym)
    if isinstance(fs, list) and fs:
        out["financial_scores"] = fs[0]

    pt = fmp_safe("price-target-consensus", [], symbol=sym)
    if isinstance(pt, list) and pt:
        out["price_target"] = pt[0]

    gh = fmp_safe("grades-historical", [], symbol=sym, limit=1)
    if isinstance(gh, list) and gh:
        out["grades"] = gh[0]

    # analyst-estimates 只對九巨頭抓（省 quota）
    if include_estimates:
        ae = fmp_safe("analyst-estimates", [], symbol=sym, period="annual", limit=2)
        if isinstance(ae, list):
            for r in ae:
                try:
                    yr = int(r["date"][:4])
                    if yr >= TODAY.year:
                        out["analyst_estimates_next"] = r
                        break
                except (KeyError, ValueError):
                    pass

        # 九巨頭的季 Capex（8 季足夠算 TTM + YoY）
        cf = fmp_safe("cash-flow-statement", [], symbol=sym, period="quarter", limit=8)
        if isinstance(cf, list):
            out["capex_quarterly"] = [
                {
                    "date": r.get("date"),
                    "period": r.get("period"),
                    "fiscalYear": r.get("fiscalYear"),
                    "capex": abs(r.get("capitalExpenditure") or 0),
                }
                for r in cf
            ]

    return sym, out


def fetch_fmp_bulk(symbols):
    """並行抓所有個股的 FMP 資料。"""
    data = {}
    if not FMP_KEY:
        print("⚠️  FMP_API_KEY 未設定，將僅使用 yfinance")
        return data

    giant_set = {code for code, _, _ in GIANTS}
    syms = sorted(set(symbols))

    # 1. Batch quote 一次搞定（1 call）
    print(f"FMP：batch-quote {len(syms)} 檔...")
    quotes = fmp_batch_quote(syms)
    print(f"  收到 {len(quotes)} 檔 quote")

    # 2. 每檔 5 個核心端點（九巨頭多 1 個 analyst-estimates）
    print(f"FMP：平行抓每檔的基本面/分析師資料...")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fmp_enrichment, s, s in giant_set): s for s in syms}
        for fut in as_completed(futures):
            sym, result = fut.result()
            result["quote"] = quotes.get(sym)
            data[sym] = result
    print(f"  完成，耗時 {time.time()-t0:.1f}s")
    return data


def fetch_yf_bulk(symbols):
    """並行抓 yfinance 的新聞（價格走批次）。"""
    news_map = {}
    syms = sorted(set(symbols))
    print(f"yfinance：並行抓 {len(syms)} 檔個股新聞...")
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(yf_news_for_symbol, s): s for s in syms}
        for fut in as_completed(futures):
            sym = futures[fut]
            news_map[sym] = fut.result()
    return news_map


# ── 交叉驗證 ────────────────────────────────────────────────────────────

def _cross_check_fundamentals(fmp, yf_fund):
    """比對 FMP ratios 和 yfinance .info 的重疊欄位（當兩源都有）。"""
    warnings = []
    if not fmp or not yf_fund:
        return warnings
    r = fmp.get("ratios_latest") or {}
    pairs = [
        ("gross_margin", r.get("grossProfitMargin"), yf_fund.get("gross_margin")),
        ("operating_margin", r.get("operatingProfitMargin"), yf_fund.get("operating_margin")),
        ("net_margin", r.get("netProfitMargin"), yf_fund.get("net_margin")),
        ("current_ratio", r.get("currentRatio"), yf_fund.get("current_ratio")),
    ]
    for label, fmp_v, yf_v in pairs:
        if fmp_v is None or yf_v is None:
            continue
        avg = (abs(fmp_v) + abs(yf_v)) / 2
        if avg == 0:
            continue
        diff_pct = abs(fmp_v - yf_v) / avg * 100
        if diff_pct > 10:  # 基本面欄位差 10% 才 flag（因 fiscal year 不同很常見）
            warnings.append({"field": label, "yf": yf_v, "fmp": fmp_v, "diff_pct": round(diff_pct, 1)})
    return warnings


def cross_check(yf_tech, fmp):
    """兩源重疊欄位比對；回傳 warnings list。"""
    warnings = []
    if not fmp:
        return warnings
    q = fmp.get("quote") or {}
    prof = fmp.get("profile") or {}

    checks = []
    # 價格
    if yf_tech and q.get("price"):
        checks.append(("price", yf_tech["price"], q["price"]))
    # 市值
    yf_mc = None  # 可從 prof/quote 取，yfinance 也有，但我們這邊只存了 tech
    fmp_mc = q.get("marketCap") or prof.get("marketCap")
    # 日漲跌
    if yf_tech and q.get("changePercentage") is not None:
        checks.append(("day_change_%", yf_tech["day_pct"], q["changePercentage"]))
    # 50 日均
    if yf_tech and yf_tech.get("ma50") and q.get("priceAvg50"):
        checks.append(("MA50", yf_tech["ma50"], q["priceAvg50"]))
    # 200 日均
    if yf_tech and yf_tech.get("ma200") and q.get("priceAvg200"):
        checks.append(("MA200", yf_tech["ma200"], q["priceAvg200"]))

    for label, a, b in checks:
        if a is None or b is None:
            continue
        avg = (abs(a) + abs(b)) / 2
        if avg == 0:
            continue
        diff_pct = abs(a - b) / avg * 100
        if diff_pct > CROSS_CHECK_DIFF_PCT:
            warnings.append({
                "field": label,
                "yf": a,
                "fmp": b,
                "diff_pct": round(diff_pct, 2),
            })
    return warnings


# ── 紅黃綠燈計算 ────────────────────────────────────────────────────────

def _extract_fundamentals(fmp, yf_fund):
    """從 FMP 擷取紅黃綠燈欄位，缺的地方由 yfinance .info 補上。
    同時記錄每個欄位來自哪一源，供交叉驗證/透明度顯示。"""
    out = {}
    sources = {}
    fmp = fmp or {}
    yf_fund = yf_fund or {}
    r = fmp.get("ratios_latest") or {}
    km = fmp.get("key_metrics_ttm") or {}
    scores = fmp.get("financial_scores") or {}

    def _take(key, fmp_val, yf_val):
        """優先 FMP、缺了用 yf、都沒就 None。"""
        if fmp_val is not None:
            out[key] = fmp_val
            sources[key] = "fmp"
        elif yf_val is not None:
            out[key] = yf_val
            sources[key] = "yf"
        else:
            out[key] = None
            sources[key] = None

    # 損益
    _take("gross_margin", r.get("grossProfitMargin"), yf_fund.get("gross_margin"))
    _take("operating_margin", r.get("operatingProfitMargin"), yf_fund.get("operating_margin"))
    _take("net_margin", r.get("netProfitMargin"), yf_fund.get("net_margin"))

    # 營收年增率：FMP 5 年 ratios 裡有 revenuePerShare，yf 有 revenueGrowth
    fmp_rev_yoy = None
    rh = fmp.get("ratios_history") or []
    if len(rh) >= 2 and rh[0].get("revenuePerShare") and rh[1].get("revenuePerShare"):
        y1, y2 = rh[0]["revenuePerShare"], rh[1]["revenuePerShare"]
        if y2:
            fmp_rev_yoy = (y1 - y2) / y2
    _take("revenue_growth_yoy", fmp_rev_yoy, yf_fund.get("revenue_growth_yoy"))

    # 資產負債
    _take("debt_to_equity", r.get("debtToEquityRatio"), yf_fund.get("debt_to_equity"))
    _take("current_ratio", r.get("currentRatio"), yf_fund.get("current_ratio"))
    _take("roe", km.get("returnOnEquityTTM"), yf_fund.get("roe"))
    _take("roic", km.get("returnOnInvestedCapitalTTM"), None)  # yf 沒 ROIC

    # 現金流
    fmp_fcf_margin = None
    ocf_sales = r.get("operatingCashFlowSalesRatio")
    fcf_ocf = r.get("freeCashFlowOperatingCashFlowRatio")
    if ocf_sales and fcf_ocf:
        fmp_fcf_margin = ocf_sales * fcf_ocf
    yf_fcf_margin = None
    if yf_fund.get("fcf_ttm") and yf_fund.get("total_revenue"):
        yf_fcf_margin = yf_fund["fcf_ttm"] / yf_fund["total_revenue"]
    _take("fcf_margin", fmp_fcf_margin, yf_fcf_margin)

    _take("income_quality", km.get("incomeQualityTTM") or fcf_ocf, None)
    _take("fcf_yield", km.get("freeCashFlowYieldTTM"), None)

    # 估值
    fmp_fpeg = r.get("forwardPriceToEarningsGrowthRatio") or r.get("priceToEarningsGrowthRatio")
    _take("forward_peg", fmp_fpeg, yf_fund.get("peg_ratio"))

    # 附帶指標
    out["piotroski"] = scores.get("piotroskiScore")
    out["altman_z"] = scores.get("altmanZScore")
    out["forward_pe"] = yf_fund.get("forward_pe")  # yf 比較可靠
    out["trailing_pe"] = yf_fund.get("trailing_pe")

    return out, sources


def _light_color(metric, value, rules):
    """依 rules 回傳 'green'/'yellow'/'red'/'gray'"""
    if value is None:
        return "gray"
    rule = rules.get(metric)
    if not rule:
        return "gray"
    g, y = rule["green"], rule["yellow"]
    higher = rule["higher_better"]
    if higher:
        if value >= g:
            return "green"
        if value >= y:
            return "yellow"
        return "red"
    else:  # 越小越好
        if value <= g:
            return "green"
        if value <= y:
            return "yellow"
        return "red"


def compute_traffic_lights(fundamentals):
    """回傳 {metric_key: {value, color, label}}。"""
    out = {}
    for metric, rule in TRAFFIC_LIGHT_RULES.items():
        val = fundamentals.get(metric)
        color = _light_color(metric, val, TRAFFIC_LIGHT_RULES)
        out[metric] = {
            "value": val,
            "color": color,
            "label": rule["label"],
        }
    return out


def traffic_light_score(lights):
    """把 12 盞燈加權總分（綠=2 / 黃=1 / 紅/灰=0）。"""
    score = 0
    total_possible = 0
    for _, v in lights.items():
        c = v["color"]
        if c == "green":
            score += 2
        elif c == "yellow":
            score += 1
        total_possible += 2
    return score, total_possible


# ── 聚合 ────────────────────────────────────────────────────────────────

def build_stock_report(sym, name, bars_df, fmp_map, news_map, yf_fund_map):
    """建立單一個股的完整報告。"""
    fmp = fmp_map.get(sym) or {}
    yf_fund = yf_fund_map.get(sym) or {}
    yf_tech = yf_metrics_for_symbol(bars_df, sym)
    news = news_map.get(sym, [])

    # 基本價格欄位：yfinance 為主、FMP fallback
    q = fmp.get("quote") or {}
    price = (yf_tech or {}).get("price") or q.get("price")
    day_pct = (yf_tech or {}).get("day_pct") or q.get("changePercentage")
    market_cap = q.get("marketCap") or yf_fund.get("market_cap")
    industry = q.get("industry") or yf_fund.get("industry") or q.get("exchange")
    display_name = name or yf_fund.get("long_name") or q.get("name") or sym

    # 基本面欄位（FMP 為主、yf 為備援；sources 記錄每個欄位來自哪一源）
    fundamentals, f_sources = _extract_fundamentals(fmp, yf_fund)
    lights = compute_traffic_lights(fundamentals)
    # sources 也掛到 lights 上，前台可顯示
    for k in lights:
        lights[k]["source"] = f_sources.get(k)
    score, total = traffic_light_score(lights)

    # 交叉驗證
    warnings = cross_check(yf_tech, fmp)
    # yf vs fmp 基本面欄位交叉比對（當兩源都有值時）
    fund_warnings = _cross_check_fundamentals(fmp, yf_fund)
    warnings.extend(fund_warnings)

    # 分析師
    pt = fmp.get("price_target") or {}
    grades = fmp.get("grades") or {}
    upside = None
    if pt.get("targetConsensus") and price:
        upside = (pt["targetConsensus"] - price) / price * 100

    return {
        "symbol": sym,
        "name": display_name,
        "industry": industry,
        "price": price,
        "day_pct": day_pct,
        "market_cap": market_cap,
        "tech": yf_tech,
        "fundamentals": fundamentals,
        "lights": lights,
        "score": score,
        "score_max": total,
        "cross_check": warnings,
        "price_target": {
            "consensus": pt.get("targetConsensus"),
            "high": pt.get("targetHigh"),
            "low": pt.get("targetLow"),
            "upside_pct": upside,
        },
        "grades": {
            "strong_buy": grades.get("analystRatingsStrongBuy"),
            "buy": grades.get("analystRatingsBuy"),
            "hold": grades.get("analystRatingsHold"),
            "sell": grades.get("analystRatingsSell"),
            "strong_sell": grades.get("analystRatingsStrongSell"),
        } if grades else None,
        "analyst_estimates_next": fmp.get("analyst_estimates_next"),
        "piotroski": (fmp.get("financial_scores") or {}).get("piotroskiScore"),
        "altman_z": (fmp.get("financial_scores") or {}).get("altmanZScore"),
        "news": news,
    }


def collect_all_symbols():
    syms = set()
    for code, _, _ in GIANTS:
        syms.add(code)
    for _, _, _, stocks in BUCKETS:
        syms.update(stocks)
    return sorted(syms)


# ── Capex 週期計算 ──────────────────────────────────────────────────────

def compute_capex_trend(quarters):
    """從 8 季 capex 算 TTM + YoY + 最近一季 QoQ。"""
    if not quarters or len(quarters) < 4:
        return None
    quarters = quarters[:8]
    latest4 = [q["capex"] for q in quarters[:4] if q["capex"]]
    if len(latest4) < 4:
        return None
    ttm = sum(latest4)
    yoy = None
    if len(quarters) >= 8:
        prev4 = [q["capex"] for q in quarters[4:8] if q["capex"]]
        if len(prev4) == 4 and sum(prev4) > 0:
            yoy = (ttm - sum(prev4)) / sum(prev4) * 100
    qoq = None
    if len(latest4) >= 2 and latest4[1] > 0:
        qoq = (latest4[0] - latest4[1]) / latest4[1] * 100
    return {"ttm": ttm, "yoy_pct": yoy, "qoq_pct": qoq, "latest_quarter": quarters[0] if quarters else None}


def compute_capex_groups(fmp_map):
    """針對 CAPEX_GROUPS 定義，計算每組合計 TTM + YoY。"""
    out = []
    for key, label, giants, benefit_buckets in CAPEX_GROUPS:
        entries = []
        for g in giants:
            fmp = fmp_map.get(g) or {}
            q = fmp.get("capex_quarterly") or []
            trend = compute_capex_trend(q)
            if trend:
                entries.append({"symbol": g, **trend})
        if not entries:
            continue
        total_ttm = sum(e["ttm"] for e in entries)
        # 用各家的 YoY 組合：先算上年 TTM 合計
        prev_total = 0
        for e in entries:
            if e.get("yoy_pct") is not None:
                prev_total += e["ttm"] / (1 + e["yoy_pct"] / 100)
            else:
                prev_total += e["ttm"]  # 無歷史資料時視同持平
        agg_yoy = ((total_ttm - prev_total) / prev_total * 100) if prev_total else None
        out.append({
            "key": key,
            "label": label,
            "giants": entries,
            "total_ttm": total_ttm,
            "yoy_pct": agg_yoy,
            "benefit_buckets": benefit_buckets,
        })
    return out


# ── Alpha Scoring Model ─────────────────────────────────────────────────

# 6 個子分數，各 0-10，加權平均 × 10 = 0-100 分
ALPHA_WEIGHTS = {
    "fundamental": 0.25,   # 紅黃綠燈分數
    "momentum":    0.20,   # 60 日報酬 universe 排名
    "technical":   0.15,   # MA + RSI
    "capex_align": 0.15,   # 若屬於 Capex 受惠 bucket
    "analyst":     0.15,   # 目標價上行 + 買進評級
    "signals":     0.10,   # 連續訊號
}


def _rank_pct(value, all_values):
    """計算 value 在 all_values 中的 percentile（0-1）。"""
    if value is None:
        return 0.5
    sorted_vals = sorted([v for v in all_values if v is not None])
    if not sorted_vals:
        return 0.5
    below = sum(1 for v in sorted_vals if v < value)
    return below / len(sorted_vals)


def compute_alpha_scores(stocks, capex_groups, signals, stock_to_bucket):
    """給每檔個股算 alpha = 0-100 分。回傳 {sym: {alpha, components, tier}}。"""
    # 收集 universe 60 日報酬用於排序
    universe_60d = [(s.get("tech") or {}).get("pct_60d") for s in stocks.values()]

    # 哪些 bucket 受惠於 Capex 上升（YoY > 10）
    capex_benefit_buckets = set()
    for g in (capex_groups or []):
        yoy = g.get("yoy_pct")
        if yoy is not None and yoy > 5:
            capex_benefit_buckets.update(g["benefit_buckets"])

    out = {}
    for sym, s in stocks.items():
        tech = s.get("tech") or {}
        lights = s.get("lights") or {}
        grades = s.get("grades") or {}
        pt = s.get("price_target") or {}

        # 1. 基本面（紅黃綠燈 0-24 → 0-10）
        fund = (s.get("score", 0) / max(s.get("score_max", 24), 1)) * 10

        # 2. 動能（60d 報酬 percentile × 10）
        pct_60d = tech.get("pct_60d")
        momentum = _rank_pct(pct_60d, universe_60d) * 10

        # 3. 技術面（MA50 + MA200 + RSI 合理）
        tech_score = 0
        if tech.get("above_ma50"):
            tech_score += 3
        if tech.get("above_ma200"):
            tech_score += 3
        rsi = tech.get("rsi")
        if rsi is not None and 30 <= rsi <= 65:
            tech_score += 4
        elif rsi is not None and 65 < rsi <= 75:
            tech_score += 2
        # RSI >75 or <30 → 0 分

        # 4. Capex 對齊
        bucket_key = stock_to_bucket.get(sym)
        capex_align = 10.0 if bucket_key in capex_benefit_buckets else 5.0 if capex_benefit_buckets else 0.0

        # 5. 分析師面
        analyst = 0
        if pt.get("upside_pct") is not None:
            u = pt["upside_pct"]
            if u > 20:
                analyst += 6
            elif u > 10:
                analyst += 4
            elif u > 0:
                analyst += 2
        if grades:
            total = sum((grades.get(k) or 0) for k in ("strong_buy", "buy", "hold", "sell", "strong_sell"))
            buy_pct = (((grades.get("strong_buy") or 0) + (grades.get("buy") or 0)) / total * 100) if total else 0
            if buy_pct > 70:
                analyst += 4
            elif buy_pct > 50:
                analyst += 2
        analyst = min(analyst, 10)

        # 6. 連續訊號
        sig = (signals or {}).get(sym) or {}
        sig_score = 0
        if sig.get("up_streak", 0) >= 3:
            sig_score += 5
        elif sig.get("up_streak", 0) == 2:
            sig_score += 2
        if sig.get("top3_streak", 0) >= 2:
            sig_score += 5
        if sig.get("down_streak", 0) >= 3:
            sig_score -= 3
        if sig.get("bottom3_streak", 0) >= 2:
            sig_score -= 3
        sig_score = max(0, min(10, sig_score + 5))  # 中性基準線 +5，範圍 0-10

        # 加權合成
        components = {
            "fundamental": fund,
            "momentum": momentum,
            "technical": tech_score,
            "capex_align": capex_align,
            "analyst": analyst,
            "signals": sig_score,
        }
        alpha = sum(components[k] * ALPHA_WEIGHTS[k] for k in ALPHA_WEIGHTS) * 10  # 0-100
        # 分級
        if alpha >= 70:
            tier = "strong_buy"
            tier_label = "強勢買進"
        elif alpha >= 55:
            tier = "buy"
            tier_label = "買進"
        elif alpha >= 40:
            tier = "neutral"
            tier_label = "觀望"
        else:
            tier = "weak"
            tier_label = "弱勢"

        out[sym] = {
            "alpha": round(alpha, 1),
            "components": {k: round(v, 1) for k, v in components.items()},
            "tier": tier,
            "tier_label": tier_label,
            "capex_benefit": bucket_key in capex_benefit_buckets,
        }
    return out


def compute_bucket_rankings(stocks, alpha_scores):
    """對每個 bucket 內個股依 alpha 排序、標出 1/2/3 名。"""
    rankings = {}
    for key, label, role, syms in BUCKETS:
        stocks_in_bucket = [
            (sym, alpha_scores.get(sym, {}).get("alpha", 0))
            for sym in syms if sym in stocks
        ]
        stocks_in_bucket.sort(key=lambda x: -x[1])
        ranks = {}
        for i, (sym, _) in enumerate(stocks_in_bucket):
            if i == 0 and len(stocks_in_bucket) >= 2:
                ranks[sym] = {"rank": 1, "medal": "🥇", "label": "領頭"}
            elif i == 1 and len(stocks_in_bucket) >= 3:
                ranks[sym] = {"rank": 2, "medal": "🥈", "label": "跟隨"}
            elif i == 2 and len(stocks_in_bucket) >= 4:
                ranks[sym] = {"rank": 3, "medal": "🥉", "label": "第三"}
            elif i == len(stocks_in_bucket) - 1 and len(stocks_in_bucket) >= 4:
                ranks[sym] = {"rank": -1, "medal": "🐌", "label": "落後"}
            else:
                ranks[sym] = {"rank": 0, "medal": "", "label": ""}
        rankings[key] = {
            "ordered": stocks_in_bucket,
            "ranks": ranks,
        }
    return rankings


# ── 歷史快照 ────────────────────────────────────────────────────────────

def save_history_snapshot(stocks):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    slim = {
        s["symbol"]: {
            "day_pct": s.get("day_pct"),
            "price": s.get("price"),
            "score": s.get("score"),
        } for s in stocks.values()
    }
    payload = {
        "trade_date": TODAY.isoformat(),
        "fetched_at": datetime.now().isoformat(),
        "stocks": slim,
    }
    with open(os.path.join(HISTORY_DIR, f"{TODAY.isoformat()}.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_history(days=30):
    """讀近 N 天 snapshot，排序由舊到新。"""
    if not os.path.isdir(HISTORY_DIR):
        return []
    snaps = []
    for p in sorted(glob.glob(os.path.join(HISTORY_DIR, "*.json"))):
        try:
            with open(p, "r", encoding="utf-8") as f:
                snaps.append(json.load(f))
        except Exception:
            continue
    snaps.sort(key=lambda s: s.get("trade_date", ""))
    return snaps[-days:]


def compute_consecutive_signals(history, stocks):
    """依歷史快照計算每檔個股的連續訊號。"""
    if not history:
        return {}
    # 組成 [(date, {sym: {day_pct, score}})]
    series = [(h["trade_date"], h.get("stocks", {})) for h in history]
    if history[-1].get("trade_date") != TODAY.isoformat():
        # 把今天補上
        today_slim = {k: {"day_pct": v.get("day_pct"), "score": v.get("score")} for k, v in stocks.items()}
        series.append((TODAY.isoformat(), today_slim))

    signals = {}
    for sym in stocks:
        up_streak, down_streak = 0, 0
        top3_streak, bottom3_streak = 0, 0
        score_up_streak = 0
        prev_score = None
        for _, daily in reversed(series):
            snap = daily.get(sym)
            if not snap:
                break
            pct = snap.get("day_pct")
            score = snap.get("score")

            # 連續漲/跌
            if pct is None:
                break
            if pct > 0 and down_streak == 0:
                up_streak += 1
            elif pct < 0 and up_streak == 0:
                down_streak += 1
            else:
                break

        # Top-3 / bottom-3 streak（獨立一輪）
        for _, daily in reversed(series):
            snap = daily.get(sym)
            if not snap or snap.get("day_pct") is None:
                break
            ranked = sorted(
                [(c, v.get("day_pct")) for c, v in daily.items() if v.get("day_pct") is not None],
                key=lambda x: x[1], reverse=True,
            )
            codes = [c for c, _ in ranked]
            if sym in codes[:3]:
                if bottom3_streak == 0:
                    top3_streak += 1
                else:
                    break
            elif sym in codes[-3:]:
                if top3_streak == 0:
                    bottom3_streak += 1
                else:
                    break
            else:
                break

        signals[sym] = {
            "up_streak": up_streak,
            "down_streak": down_streak,
            "top3_streak": top3_streak,
            "bottom3_streak": bottom3_streak,
        }
    return signals


def cleanup_history():
    if not os.path.isdir(HISTORY_DIR):
        return
    cutoff = TODAY - timedelta(days=MAX_HISTORY_DAYS)
    for p in glob.glob(os.path.join(HISTORY_DIR, "*.json")):
        name = os.path.splitext(os.path.basename(p))[0]
        try:
            d = datetime.strptime(name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < cutoff:
            try:
                os.remove(p)
            except OSError:
                pass


# ── HTML 渲染（下一 section）─────────────────────────────────────────

CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b;--card:#fff;--hover:#eef2ff}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:14px;line-height:1.5}
.hdr{background:linear-gradient(135deg,#0f172a 0%,#1e3a8a 50%,#2563eb 100%);color:#fff;padding:22px 28px 16px}
.hdr h1{font-size:20px;font-weight:800;margin-bottom:4px;letter-spacing:-0.3px}
.hdr .sub{font-size:11.5px;opacity:.85}
.nav-link{color:#93c5fd;font-size:11.5px;text-decoration:none;margin-left:14px;border-bottom:1px dashed #93c5fd;padding-bottom:1px}
.nav-link:hover{opacity:.7}
.construction{background:linear-gradient(90deg,#fef3c7,#fde68a);border-bottom:2px solid #f59e0b;color:#78350f;padding:10px 28px;font-size:12.5px;font-weight:600;text-align:center}
.health-badge{display:flex;gap:8px;flex-wrap:wrap;padding:10px 28px;background:#f8fafc;border-bottom:1px solid var(--brd);font-size:11.5px;align-items:center}
.hb-item{display:inline-flex;align-items:center;padding:3px 10px;border-radius:14px;font-weight:600;border:1px solid}
.hb-item.hb-ok{background:#f0fdf4;color:#15803d;border-color:#bbf7d0}
.hb-item.hb-warn{background:#fef3c7;color:#92400e;border-color:#fde68a}
.hb-item.hb-bad{background:#fee2e2;color:#b91c1c;border-color:#fecaca}
.hb-item.hb-off{background:#e2e8f0;color:#64748b;border-color:#cbd5e1}
.hb-item.hb-ts{margin-left:auto;background:transparent;border-color:transparent;color:var(--mu);font-weight:500}
@media(max-width:768px){.health-badge{padding-left:16px;padding-right:16px;font-size:10.5px}.hb-item.hb-ts{margin-left:0;width:100%;text-align:center}}
.pane{padding:22px 28px}
.ttl{font-size:15px;font-weight:700;margin-bottom:4px}
.desc{font-size:12.5px;color:var(--mu);margin-bottom:16px;line-height:1.7}

/* 巨頭熱力圖 */
.giants{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px;margin-bottom:24px}
.giant-card{background:var(--card);border:1px solid var(--brd);border-radius:10px;padding:14px 16px;cursor:pointer;transition:transform .12s,box-shadow .12s;position:relative;overflow:hidden}
.giant-card:hover{transform:translateY(-2px);box-shadow:0 6px 18px rgba(0,0,0,.08)}
.giant-card .sym{font-size:11px;font-weight:700;color:var(--mu);letter-spacing:0.4px}
.giant-card .name{font-size:14px;font-weight:800;margin-top:1px}
.giant-card .role{font-size:10.5px;color:var(--mu);margin-top:2px}
.giant-card .px{font-size:18px;font-weight:800;margin-top:8px;letter-spacing:-0.3px}
.giant-card .chg{font-size:12px;font-weight:700;margin-top:1px}
.giant-card .chg.up{color:var(--gr)}.giant-card .chg.dn{color:var(--rd)}
.giant-card .score{position:absolute;top:10px;right:12px;font-size:11px;font-weight:700;padding:2px 8px;border-radius:10px}
.score.high{background:#dcfce7;color:#15803d}
.score.mid{background:#fef3c7;color:#b45309}
.score.low{background:#fee2e2;color:#b91c1c}

/* 供應鏈區 */
.chain{margin-top:10px}
.bucket{background:var(--card);border:1px solid var(--brd);border-radius:12px;margin-bottom:12px;overflow:hidden}
.bucket > summary{list-style:none;cursor:pointer;padding:14px 18px;display:flex;align-items:center;justify-content:space-between;background:linear-gradient(90deg,#f8fafc 0%,#fff 100%);border-bottom:1px solid var(--brd)}
.bucket > summary::-webkit-details-marker{display:none}
.bucket > summary:hover{background:var(--hover)}
.bucket .b-left{display:flex;align-items:center;gap:12px}
.bucket .b-title{font-size:14px;font-weight:800}
.bucket .b-role{font-size:10.5px;color:var(--mu);background:#e2e8f0;padding:2px 8px;border-radius:10px;font-weight:600}
.bucket .b-stocks-count{font-size:11px;color:var(--mu)}
.bucket .b-giants{display:flex;gap:4px;flex-wrap:wrap;max-width:40%}
.bucket .b-giant-chip{font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;background:#dbeafe;color:#1e40af}
.bucket .b-giant-chip.s3{background:#1e40af;color:#fff}
.bucket .b-giant-chip.s2{background:#60a5fa;color:#fff}
.bucket .b-giant-chip.s1{background:#bfdbfe;color:#1e3a8a}
.bucket .chev{color:var(--mu);font-size:11px;transition:transform .15s;margin-left:8px}
.bucket[open] > summary .chev{transform:rotate(180deg)}

/* 個股列 */
.b-body{padding:10px 14px}
details.stock{border-bottom:1px dashed #eef2f7;margin:0}
details.stock:last-child{border-bottom:none}
details.stock > summary{list-style:none;cursor:pointer;padding:10px 4px;display:flex;align-items:center;gap:10px;font-size:13px;transition:background .1s}
details.stock > summary::-webkit-details-marker{display:none}
details.stock > summary:hover{background:#f8fafc}
details.stock .sym{font-weight:700;min-width:56px}
details.stock .nm{color:var(--mu);font-size:12px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
details.stock .lights-mini{display:flex;gap:2px;align-items:center}
.light{width:12px;height:12px;border-radius:50%;display:inline-block;border:1px solid rgba(0,0,0,0.08)}
.light.green{background:#16a34a}
.light.yellow{background:#eab308}
.light.red{background:#dc2626}
.light.gray{background:#cbd5e1}
.score-chip{font-size:11px;font-weight:700;padding:2px 7px;border-radius:6px;min-width:38px;text-align:center}
.up{color:var(--gr)}.dn{color:var(--rd)}.mu{color:var(--mu)}
.pct{font-variant-numeric:tabular-nums;font-weight:700;min-width:60px;text-align:right}

/* 個股詳情面板 */
.stock-detail{background:#f8fafc;border-left:3px solid var(--bl);border-radius:0 8px 8px 0;margin:4px 0 12px 12px;padding:14px 16px}
.sd-grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
@media(max-width:720px){.sd-grid2{grid-template-columns:1fr}}
.sd-block{background:#fff;border:1px solid var(--brd);border-radius:8px;padding:12px 14px}
.sd-block h5{font-size:11px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px;display:flex;align-items:center;gap:6px}
.lights-full{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:5px}
.light-row{display:flex;align-items:center;gap:8px;font-size:11.5px;padding:3px 0}
.light-row .k{color:var(--mu);flex:1}
.light-row .v{font-weight:700;font-variant-numeric:tabular-nums}

.kv-row{display:flex;justify-content:space-between;padding:4px 0;font-size:12px;border-bottom:1px dashed #eef2f7}
.kv-row:last-child{border-bottom:none}
.kv-row .k{color:var(--mu)}
.kv-row .v{font-weight:700;font-variant-numeric:tabular-nums}
.pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10.5px;font-weight:700}
.pill.up{background:#dcfce7;color:#15803d}
.pill.dn{background:#fee2e2;color:#dc2626}
.pill.am{background:#fef3c7;color:#b45309}
.pill.mu{background:#e2e8f0;color:#475569}
.warn-box{background:#fef3c7;border:1px solid #fde68a;color:#92400e;border-radius:6px;padding:6px 10px;font-size:11.5px;margin-top:8px;line-height:1.5}
.news-item{padding:7px 0;border-bottom:1px dashed #eef2f7;font-size:12px;line-height:1.45}
.news-item:last-child{border-bottom:none}
.news-item a{color:var(--txt);text-decoration:none}
.news-item a:hover{color:var(--bl);text-decoration:underline}
.news-meta{font-size:10.5px;color:var(--mu);margin-top:2px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.news-tags{display:flex;gap:4px;align-items:center;margin-bottom:3px}
.cat-pill{font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;letter-spacing:0.2px}
.cat-order{background:#fce7f3;color:#9f1239}
.cat-guidance{background:#dbeafe;color:#1e40af}
.cat-insider{background:#e9d5ff;color:#6b21a8}
.cat-earnings{background:#fef3c7;color:#92400e}
.cat-general{background:#e2e8f0;color:#475569}
.sent-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:3px}
.sent-positive{background:#16a34a}
.sent-negative{background:#dc2626}
.sent-neutral{background:#94a3b8}
.sent-label{font-size:10px;font-weight:600}
.sent-positive-label{color:#15803d}
.sent-negative-label{color:#b91c1c}
.sent-neutral-label{color:#64748b}
.llm-tag{display:inline-flex;align-items:center;margin-left:auto;padding:1.5px 6px;background:#f0f9ff;border:1px dashed #7dd3fc;border-radius:4px}
.llm-summary{background:linear-gradient(90deg,#f0f9ff,#fff);border-left:2px solid #0ea5e9;padding:4px 8px;margin:3px 0;font-size:11px;color:#0c4a6e;border-radius:0 4px 4px 0;line-height:1.5}
.analyst-bar{display:flex;gap:2px;height:18px;border-radius:4px;overflow:hidden;margin-top:3px}
.analyst-bar > div{transition:opacity .15s}
.analyst-bar .sb{background:#15803d}
.analyst-bar .b{background:#4ade80}
.analyst-bar .h{background:#cbd5e1}
.analyst-bar .s{background:#fca5a5}
.analyst-bar .ss{background:#b91c1c}
.ft{text-align:center;color:var(--mu);font-size:11.5px;padding:20px 28px;border-top:1px solid var(--brd);line-height:1.8;background:var(--card)}
/* Capex 週期 */
.capex-panel{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-bottom:18px}
.capex-card{background:var(--card);border:1px solid var(--brd);border-radius:10px;padding:14px 16px;border-left:4px solid var(--bl)}
.capex-card.rising{border-left-color:var(--gr)}
.capex-card.falling{border-left-color:var(--rd)}
.capex-card h4{font-size:12px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:0.4px;margin-bottom:4px}
.capex-card .big{font-size:24px;font-weight:800;letter-spacing:-0.3px;margin:4px 0}
.capex-card .yoy{font-size:13px;font-weight:700}
.capex-card .members{font-size:10.5px;color:var(--mu);margin-top:8px;line-height:1.6}
.capex-card .signal{font-size:11.5px;color:var(--txt);background:#f0fdf4;padding:6px 10px;border-radius:6px;margin-top:10px;line-height:1.5}
.capex-card.falling .signal{background:#fef2f2}
/* 圖例 */
.legend-box{background:var(--card);border:1px solid var(--brd);border-radius:10px;padding:12px 16px;margin-bottom:16px;font-size:12px}
.legend-box summary{list-style:none;cursor:pointer;font-weight:700;color:var(--txt);font-size:13px}
.legend-box summary::-webkit-details-marker{display:none}
.legend-body{margin-top:10px;display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px}
.legend-group{background:#f8fafc;border-radius:6px;padding:8px 10px}
.legend-group h6{font-size:11px;font-weight:700;color:var(--mu);text-transform:uppercase;margin-bottom:5px}
.legend-row{font-size:11px;display:flex;gap:6px;padding:1.5px 0}
.legend-row .k{flex:1}
.legend-row .v{color:var(--mu)}
/* Bucket summary */
.b-stats{display:flex;gap:12px;align-items:center;font-size:11px;margin-right:10px}
.b-stats .stat-pill{padding:2px 7px;border-radius:10px;font-weight:700}
.b-stats .stat-pill.up{background:#dcfce7;color:#15803d}
.b-stats .stat-pill.dn{background:#fee2e2;color:#b91c1c}
.b-stats .stat-pill.mu{background:#e2e8f0;color:#475569}
/* 連續訊號 */
.signals{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin-bottom:16px}
.sig-card{background:var(--card);border:1px solid var(--brd);border-radius:10px;padding:10px 14px;border-left:3px solid var(--bl)}
.sig-card.pos{border-left-color:var(--gr)}
.sig-card.neg{border-left-color:var(--rd)}
.sig-card.warn{border-left-color:var(--am)}
.sig-card.bot{border-left-color:#7c3aed}
.sig-card h5{font-size:11.5px;font-weight:700;color:var(--txt);margin-bottom:6px}
.sig-card .sig-body{display:flex;flex-wrap:wrap;gap:3px}
.streak-tag{display:inline-block;padding:2px 7px;font-size:10.5px;font-weight:700;border-radius:10px}
.streak-tag.pos{background:#dcfce7;color:#15803d;border:1px solid #bbf7d0}
.streak-tag.neg{background:#fee2e2;color:#b91c1c;border:1px solid #fecaca}
.streak-tag.warn{background:#fef3c7;color:#92400e;border:1px solid #fde68a}
.streak-tag.bot{background:#ede9fe;color:#6b21a8;border:1px solid #ddd6fe}
/* Sankey SVG */
.sankey-wrap{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:10px;margin-bottom:16px;overflow-x:auto}
.sankey-svg{width:100%;height:auto;min-width:920px}
.sankey-svg path.sankey-link{transition:opacity .15s, stroke-width .15s}
.sankey-svg path.sankey-link:hover{opacity:1 !important}
.sankey-svg path.sankey-link.active{opacity:1 !important}
.sankey-svg path.sankey-link.dimmed{opacity:0.06 !important}
.sankey-svg .sankey-node{transition:opacity .15s}
.sankey-svg .sankey-node:hover rect{stroke:var(--bl);stroke-width:2}
.sankey-svg .sankey-node.active rect{stroke:var(--bl);stroke-width:2.5}
.sankey-svg .sankey-node.dimmed{opacity:0.3}
.sankey-info-empty{text-align:center;font-size:11.5px;color:var(--mu);padding:8px 0;border-top:1px solid var(--brd);margin-top:6px}
.sankey-info-active{padding:10px 14px;background:#f0f9ff;border:1px solid #bfdbfe;border-radius:8px;margin:8px 4px 0;font-size:12.5px;line-height:1.7}
.sankey-info-active .si-head{font-weight:800;font-size:13.5px;color:#1e3a8a;margin-bottom:4px}
.sankey-info-active .si-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:4px 14px;margin-top:6px}
.sankey-info-active .si-grid .k{color:var(--mu);font-size:11.5px}
.sankey-info-active .si-grid .v{font-weight:700;font-variant-numeric:tabular-nums}
.sankey-info-active .si-chips{display:flex;flex-wrap:wrap;gap:6px;margin-top:6px}
.sankey-info-active .si-chip{display:inline-flex;align-items:center;gap:6px;background:#fff;border:1px solid #bfdbfe;border-radius:20px;padding:4px 10px;font-size:11.5px;font-weight:600;color:#1e3a8a;cursor:pointer;transition:all .15s}
.sankey-info-active .si-chip:hover{background:#dbeafe;border-color:#60a5fa;transform:translateY(-1px);box-shadow:0 2px 6px rgba(59,130,246,0.15)}
.sankey-info-active .si-chip .si-stars{color:#b45309;font-size:10.5px;letter-spacing:-1px}
.sankey-info-active .si-chip .si-chip-pct{font-variant-numeric:tabular-nums;font-weight:700}
.sankey-svg .sankey-node.clickable-target rect{stroke:#60a5fa;stroke-width:2;stroke-dasharray:4 3;animation:pulse-stroke 1.6s ease-in-out infinite}
@keyframes pulse-stroke {
  0%, 100% { stroke-opacity: 0.5; }
  50% { stroke-opacity: 1; }
}
/* Backtest table */
.bt-table{width:100%;border-collapse:collapse;background:var(--card);border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.bt-table th{background:#f1f5f9;font-size:11px;color:var(--mu);font-weight:700;padding:8px 12px;text-transform:uppercase;text-align:left}
.bt-table td{padding:8px 12px;font-size:12.5px;border-top:1px solid var(--brd)}
.bt-table td.num{text-align:right;font-variant-numeric:tabular-nums;font-weight:700}
.tier-examples{font-size:10.5px;color:var(--mu);font-weight:500;margin-top:2px}
/* Alpha panel */
.alpha-wrap{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 16px;margin-bottom:18px}
.alpha-wrap h3{font-size:14px;font-weight:800;margin-bottom:10px}
.alpha-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px}
.alpha-card{background:var(--bg);border:1px solid var(--brd);border-radius:8px;padding:10px 12px;position:relative;border-left:3px solid var(--bl)}
.alpha-card.strong_buy{border-left-color:#15803d;background:linear-gradient(90deg,#dcfce7,#fff)}
.alpha-card.buy{border-left-color:#16a34a;background:linear-gradient(90deg,#f0fdf4,#fff)}
.alpha-card.neutral{border-left-color:#d97706}
.alpha-card.weak{border-left-color:#dc2626;background:linear-gradient(90deg,#fef2f2,#fff)}
.alpha-card .sym{font-size:12px;font-weight:700}
.alpha-card .name{font-size:10px;color:var(--mu);margin-bottom:4px}
.alpha-card .alpha-num{font-size:22px;font-weight:800;letter-spacing:-0.5px}
.alpha-card .tier-label{font-size:10px;font-weight:700;padding:1.5px 7px;border-radius:10px;position:absolute;top:8px;right:10px}
.alpha-card.strong_buy .tier-label{background:#15803d;color:#fff}
.alpha-card.buy .tier-label{background:#86efac;color:#14532d}
.alpha-card.neutral .tier-label{background:#fef3c7;color:#92400e}
.alpha-card.weak .tier-label{background:#fee2e2;color:#b91c1c}
.alpha-card .breakdown{font-size:9.5px;color:var(--mu);margin-top:4px;line-height:1.4}
/* Bucket ranking medal */
.rank-medal{font-size:11.5px;margin-right:4px}
.rank-pill{font-size:10px;font-weight:700;padding:1.5px 5px;border-radius:4px;margin-right:4px}
.rank-pill.r1{background:#fef3c7;color:#92400e}
.rank-pill.r2{background:#e5e7eb;color:#374151}
.rank-pill.r3{background:#fef3c7;color:#a16207;opacity:0.85}
.rank-pill.r-laggard{background:#fee2e2;color:#991b1b}
.alpha-pill{font-size:10.5px;font-weight:700;padding:2px 8px;border-radius:10px;min-width:46px;text-align:center}
.alpha-pill.strong_buy{background:#15803d;color:#fff}
.alpha-pill.buy{background:#dcfce7;color:#15803d}
.alpha-pill.neutral{background:#fef3c7;color:#92400e}
.alpha-pill.weak{background:#fee2e2;color:#b91c1c}
.b-leader{font-size:10.5px;color:#92400e;background:#fef3c7;padding:2px 7px;border-radius:10px;font-weight:700;margin-left:6px}
.empty-msg{color:var(--mu);font-size:12.5px;padding:10px 0;text-align:center}
@media(max-width:768px){.hdr,.pane{padding-left:16px;padding-right:16px}.giants{grid-template-columns:repeat(2,1fr)}}
"""


# ── HTML 工具 ───────────────────────────────────────────────────────────

def _fmt_pct(v, show_plus=True, digits=2):
    if v is None:
        return "─"
    sign = "+" if v > 0 and show_plus else ""
    return f"{sign}{v:.{digits}f}%"


def _fmt_price(v):
    if v is None:
        return "─"
    return f"${v:,.2f}"


def _fmt_mc(v):
    if v is None:
        return "─"
    if v >= 1e12:
        return f"${v/1e12:.2f}T"
    if v >= 1e9:
        return f"${v/1e9:.1f}B"
    if v >= 1e6:
        return f"${v/1e6:.1f}M"
    return f"${v:,.0f}"


def _pct_cls(v):
    if v is None:
        return "mu"
    return "up" if v > 0 else ("dn" if v < 0 else "mu")


def _light_cls(c):
    return f"light {c}"


def _score_cls(score, total):
    if total == 0:
        return "low"
    pct = score / total
    if pct >= 0.65:
        return "high"
    if pct >= 0.4:
        return "mid"
    return "low"


def _fmt_light_value(metric, value):
    """依 metric 決定顯示格式（% / 倍數 / 原值）。"""
    if value is None:
        return "─"
    pct_metrics = {"revenue_growth_yoy", "operating_margin", "net_margin", "gross_margin",
                   "roe", "roic", "fcf_margin", "fcf_yield"}
    if metric in pct_metrics:
        return f"{value*100:.1f}%"
    if metric in ("income_quality",):
        return f"{value:.2f}"
    if metric in ("current_ratio", "debt_to_equity", "forward_peg"):
        return f"{value:.2f}"
    return f"{value:.2f}"


def _news_age(ts_iso):
    if not ts_iso:
        return ""
    try:
        ts = datetime.fromisoformat(ts_iso)
    except ValueError:
        return ""
    age = (NOW_UTC - ts).total_seconds()
    if age < 3600:
        return f"{int(age // 60)} 分鐘前"
    if age < 86400:
        return f"{int(age // 3600)} 小時前"
    return f"{int(age // 86400)} 天前"


# ── HTML 區塊 ───────────────────────────────────────────────────────────

def _gen_health_badge(health):
    """頁面頂部的資料品質狀態條。"""
    if not health:
        return ""
    parts = []

    # FMP 基本面
    fmp_pct = health["fmp_ok_pct"]
    fmp_cls = "hb-ok" if fmp_pct >= 80 else "hb-warn" if fmp_pct >= 40 else "hb-bad"
    parts.append(f'<span class="hb-item {fmp_cls}" title="紅黃綠燈基本面資料取得率（FMP 主源 / yfinance 備援）">基本面 {health["fmp_ok_count"]}/{health["stock_count"]}</span>')

    # 新聞
    news_age = health.get("avg_news_age_hours", 0)
    news_cls = "hb-ok" if news_age < 24 else "hb-warn" if news_age < 36 else "hb-bad"
    parts.append(f'<span class="hb-item {news_cls}" title="{health["news_total"]} 則新聞，平均 {news_age:.1f} 小時前">新聞 {health["news_total"]} 則（均齡 {news_age:.0f}h）</span>')

    # LLM
    if health.get("llm_enabled"):
        llm_count = health.get("llm_enriched_count", 0)
        llm_cls = "hb-ok" if llm_count > 0 else "hb-warn"
        parts.append(f'<span class="hb-item {llm_cls}" title="Claude Haiku 深度分析">LLM 分析 {llm_count}</span>')
    else:
        parts.append(f'<span class="hb-item hb-off" title="ANTHROPIC_API_KEY 未設定，使用 keyword classifier">LLM 關閉</span>')

    # Capex
    capex_cls = "hb-ok" if health["capex_groups_ok"] >= 2 else "hb-warn"
    parts.append(f'<span class="hb-item {capex_cls}" title="Capex 週期追蹤組數">Capex 週期 {health["capex_groups_ok"]}/{len(CAPEX_GROUPS)}</span>')

    # 歷史
    hist_cls = "hb-ok" if health["history_days"] >= 3 else "hb-warn"
    parts.append(f'<span class="hb-item {hist_cls}" title="歷史快照累積天數 — 連續訊號需至少 3 天">歷史 {health["history_days"]} 天</span>')

    # 最後更新時間
    try:
        fetched = datetime.fromisoformat(health["fetched_at"].replace("Z", "+00:00"))
        age_m = (datetime.now(timezone.utc) - fetched).total_seconds() / 60
        if age_m < 60:
            time_str = f"{int(age_m)} 分鐘前"
        elif age_m < 1440:
            time_str = f"{int(age_m/60)} 小時前"
        else:
            time_str = f"{int(age_m/1440)} 天前"
    except Exception:
        time_str = "─"
    parts.append(f'<span class="hb-item hb-ts" title="資料抓取時間 {health.get("fetched_at", "─")}">🕐 更新於 {time_str}</span>')

    return f'<div class="health-badge">{ "".join(parts) }</div>'


def _gen_alpha_panel(alpha_scores, stocks):
    """Top alpha 排行 — 前 12 強。"""
    if not alpha_scores:
        return ""
    items = [(sym, sc["alpha"], sc["tier"], sc["tier_label"], sc["components"]) for sym, sc in alpha_scores.items()]
    items.sort(key=lambda x: -x[1])
    top = items[:12]

    # 分級統計
    tier_counts = {"strong_buy": 0, "buy": 0, "neutral": 0, "weak": 0}
    for _, _, t, _, _ in items:
        tier_counts[t] += 1
    stats = f'<div style="font-size:11.5px;color:var(--mu);margin-bottom:10px">共 {len(items)} 檔：<b style="color:#15803d">{tier_counts["strong_buy"]} 強勢買進</b> / <b style="color:#16a34a">{tier_counts["buy"]} 買進</b> / <b style="color:#d97706">{tier_counts["neutral"]} 觀望</b> / <b style="color:#dc2626">{tier_counts["weak"]} 弱勢</b></div>'

    cards = ""
    for sym, alpha, tier, tlabel, comps in top:
        name = stocks.get(sym, {}).get("name", sym)
        breakdown = " · ".join(
            f"{k[:3]}{v:.0f}" for k, v in comps.items()
            if v is not None
        )
        cards += f'''<div class="alpha-card {tier}">
  <div class="tier-label">{tlabel}</div>
  <div class="sym">{sym}</div>
  <div class="name">{name[:14]}</div>
  <div class="alpha-num">{alpha}</div>
  <div class="breakdown" title="基本/動能/技術/Capex/分析師/訊號 各 0-10 分">{breakdown}</div>
</div>'''

    return f'''<div class="alpha-wrap">
  <h3>⚡ Alpha 評分排行 Top 12</h3>
  {stats}
  <div class="alpha-grid">{cards}</div>
</div>'''


def _gen_signals_panel(signals, stocks):
    """連續訊號：找出連續漲/跌/在 top3 的股票。"""
    if not signals:
        return ""
    up_items = sorted([(s, v["up_streak"]) for s, v in signals.items() if v["up_streak"] >= 2],
                      key=lambda x: -x[1])
    down_items = sorted([(s, v["down_streak"]) for s, v in signals.items() if v["down_streak"] >= 2],
                        key=lambda x: -x[1])
    top3_items = sorted([(s, v["top3_streak"]) for s, v in signals.items() if v["top3_streak"] >= 2],
                       key=lambda x: -x[1])
    bottom3_items = sorted([(s, v["bottom3_streak"]) for s, v in signals.items() if v["bottom3_streak"] >= 2],
                          key=lambda x: -x[1])

    def _tags(items, cls):
        if not items:
            return '<span style="color:var(--mu);font-size:11.5px">—</span>'
        parts = []
        for sym, n in items[:8]:
            name = stocks.get(sym, {}).get("name", sym) if stocks.get(sym) else sym
            parts.append(f'<span class="streak-tag {cls}" title="{name}">{sym} {n}天</span>')
        return "".join(parts)

    boxes = []
    if up_items:
        boxes.append(f'<div class="sig-card pos"><h5>🔥 連續收紅</h5><div class="sig-body">{_tags(up_items, "pos")}</div></div>')
    if down_items:
        boxes.append(f'<div class="sig-card neg"><h5>❄️ 連續收黑</h5><div class="sig-body">{_tags(down_items, "neg")}</div></div>')
    if top3_items:
        boxes.append(f'<div class="sig-card warn"><h5>🏆 連續進 Top-3</h5><div class="sig-body">{_tags(top3_items, "warn")}</div></div>')
    if bottom3_items:
        boxes.append(f'<div class="sig-card bot"><h5>📉 連續墊底 Top-3</h5><div class="sig-body">{_tags(bottom3_items, "bot")}</div></div>')

    if not boxes:
        return '<div class="empty-msg" style="margin:10px 0">連續訊號累積中（需至少 3 個交易日的歷史資料）</div>'

    return f'<div class="signals">{ "".join(boxes) }</div>'


def _gen_sankey_svg(stocks, bucket_lookup):
    """供應鏈依賴關係視覺化（SVG）。
    左側 9 巨頭，右側 9 bucket，連線粗細 = 依賴強度，顏色 = bucket 平均表現。
    """
    giants_list = list(GIANTS)  # (code, name, role)
    buckets_list = list(BUCKETS)

    W, H = 880, 520
    left_x = 130
    right_x = 780
    top_pad = 30
    row_h = 52

    # 計算每個 bucket 的平均日漲跌 → 決定連線顏色
    bucket_avg_pct = {}
    for key, _, _, syms in buckets_list:
        ds = [stocks[s]["day_pct"] for s in syms if s in stocks and stocks[s].get("day_pct") is not None]
        bucket_avg_pct[key] = sum(ds) / len(ds) if ds else 0

    def _color_for_pct(pct):
        if pct is None or abs(pct) < 0.1:
            return "#94a3b8"
        return "#16a34a" if pct > 0 else "#dc2626"

    # 巨頭當日漲跌也著色
    giant_y = {g[0]: top_pad + i * row_h + row_h / 2 for i, g in enumerate(giants_list)}
    bucket_y = {b[0]: top_pad + i * row_h + row_h / 2 for i, b in enumerate(buckets_list)}

    svg_parts = [f'<svg class="sankey-svg" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg">']

    # 畫連線（先畫，讓 node 在上面）。每條 path 帶 data-giant / data-bucket，供 JS 高亮
    for giant_code, deps in DEPENDENCY.items():
        for bucket_key, strength in deps.items():
            y1 = giant_y.get(giant_code)
            y2 = bucket_y.get(bucket_key)
            if y1 is None or y2 is None:
                continue
            width = strength * 1.4
            opacity = 0.25 + strength * 0.15
            bucket_pct = bucket_avg_pct.get(bucket_key, 0)
            color = _color_for_pct(bucket_pct)
            midx = (left_x + right_x) / 2
            path = f"M{left_x + 10},{y1} C{midx},{y1} {midx},{y2} {right_x - 10},{y2}"
            bucket_label = bucket_lookup.get(bucket_key, bucket_key)
            giant_pct = (stocks.get(giant_code) or {}).get("day_pct")
            svg_parts.append(
                f'<path class="sankey-link" data-giant="{giant_code}" data-bucket="{bucket_key}" '
                f'data-giant-pct="{giant_pct if giant_pct is not None else 0:.2f}" '
                f'data-bucket-pct="{bucket_pct:.2f}" data-bucket-label="{bucket_label}" '
                f'data-strength="{strength}" data-default-opacity="{opacity:.2f}" data-default-width="{width:.1f}" '
                f'd="{path}" stroke="{color}" stroke-width="{width:.1f}" fill="none" opacity="{opacity:.2f}" '
                f'onclick="highlightSankey(this, event)" style="cursor:pointer">'
                f'<title>{giant_code} → {bucket_label} · 依賴 {"★" * strength}</title>'
                f'</path>'
            )

    # 巨頭節點（左）
    for i, (code, _, role) in enumerate(giants_list):
        y = giant_y[code]
        stk = stocks.get(code) or {}
        day_pct = stk.get("day_pct")
        score = stk.get("score", 0)
        score_max = stk.get("score_max", 24)
        dot_color = _color_for_pct(day_pct)
        svg_parts.append(f'<g class="sankey-node giant-node" data-giant="{code}" onclick="selectGiant(\'{code}\', event)" style="cursor:pointer">')
        svg_parts.append(f'<rect x="{left_x - 115}" y="{y - 18}" width="115" height="36" rx="6" fill="#fff" stroke="#cbd5e1" stroke-width="1"/>')
        svg_parts.append(f'<circle cx="{left_x - 105}" cy="{y}" r="5" fill="{dot_color}"/>')
        svg_parts.append(f'<text x="{left_x - 95}" y="{y - 3}" font-size="13" font-weight="700" fill="#1e293b">{code}</text>')
        svg_parts.append(f'<text x="{left_x - 95}" y="{y + 11}" font-size="10" fill="#64748b">{_fmt_pct(day_pct)} · {score}/{score_max}</text>')
        svg_parts.append(f'</g>')

    # Bucket 節點（右）
    for i, (key, label, role, syms) in enumerate(buckets_list):
        y = bucket_y[key]
        avg_pct = bucket_avg_pct.get(key)
        dot_color = _color_for_pct(avg_pct)
        svg_parts.append(f'<g class="sankey-node bucket-node" data-bucket="{key}" data-bucket-label="{label}" onclick="selectBucket(\'{key}\', event)" style="cursor:pointer">')
        svg_parts.append(f'<rect x="{right_x}" y="{y - 18}" width="130" height="36" rx="6" fill="#fff" stroke="#cbd5e1" stroke-width="1"/>')
        svg_parts.append(f'<circle cx="{right_x + 10}" cy="{y}" r="5" fill="{dot_color}"/>')
        svg_parts.append(f'<text x="{right_x + 20}" y="{y - 3}" font-size="12" font-weight="700" fill="#1e293b">{label}</text>')
        svg_parts.append(f'<text x="{right_x + 20}" y="{y + 11}" font-size="10" fill="#64748b">{_fmt_pct(avg_pct)} · {len(syms)} 檔</text>')
        svg_parts.append(f'</g>')

    svg_parts.append('</svg>')
    info_box = '''<div id="sankey-info" class="sankey-info-empty">👆 點擊左側巨頭或右側 Bucket 節點，查看其所有供應鏈關係；或點一條線看單一關係</div>'''
    return '<div class="sankey-wrap" onclick="if(!event.target.closest(\'g.sankey-node\') && event.target.tagName!==\'path\') resetSankey()">' + "".join(svg_parts) + info_box + '</div>'


def _gen_capex_panel(capex_groups, bucket_lookup):
    if not capex_groups:
        return ''
    html = '<div class="capex-panel">'
    for grp in capex_groups:
        yoy = grp["yoy_pct"]
        trend_cls = "rising" if (yoy is not None and yoy > 5) else "falling" if (yoy is not None and yoy < -5) else ""
        arrow = "📈" if (yoy and yoy > 5) else "📉" if (yoy and yoy < -5) else "→"
        yoy_str = _fmt_pct(yoy) if yoy is not None else "─"
        yoy_cls = _pct_cls(yoy)
        members = " · ".join(f'{e["symbol"]}({_fmt_mc(e["ttm"])})' for e in grp["giants"])

        # 受惠 bucket 的名稱
        benefit_names = [bucket_lookup[b] for b in grp["benefit_buckets"] if b in bucket_lookup]
        signal = ""
        if yoy is not None:
            if yoy > 20:
                signal = f'🚀 <b>加速期</b> — 上游受惠：{" · ".join(benefit_names)}'
            elif yoy > 5:
                signal = f'📈 <b>擴張期</b> — 上游穩定受惠：{" · ".join(benefit_names)}'
            elif yoy > -5:
                signal = f'→ <b>持平</b> — 上游需求穩定，無大動作'
            else:
                signal = f'⚠️ <b>放緩期</b> — 上游承壓：{" · ".join(benefit_names)}'

        html += f'''<div class="capex-card {trend_cls}">
  <h4>🏭 {grp["label"]}（{len(grp["giants"])} 家）</h4>
  <div class="big">{_fmt_mc(grp["total_ttm"])}</div>
  <div class="yoy {yoy_cls}">TTM YoY {yoy_str} {arrow}</div>
  <div class="members">{members}</div>
  <div class="signal">{signal}</div>
</div>'''
    html += '</div>'
    return html


def _gen_legend():
    groups = [
        ("📊 損益表 (4)", [
            ("營收年增率", "≥15% 綠 · 5-15% 黃 · <5% 紅"),
            ("營業利益率", "≥20% 綠 · 10-20% 黃 · <10% 紅"),
            ("淨利率", "≥15% 綠 · 7-15% 黃 · <7% 紅"),
            ("毛利率", "≥40% 綠 · 25-40% 黃 · <25% 紅"),
        ]),
        ("🏦 資產負債 (4)", [
            ("負債/股東權益", "≤0.5 綠 · 0.5-1.5 黃 · >1.5 紅"),
            ("流動比率", "≥2 綠 · 1-2 黃 · <1 紅"),
            ("ROE", "≥15% 綠 · 8-15% 黃 · <8% 紅"),
            ("ROIC", "≥15% 綠 · 8-15% 黃 · <8% 紅"),
        ]),
        ("💰 現金流 (3)", [
            ("FCF / 營收", "≥15% 綠 · 5-15% 黃 · <5% 紅"),
            ("盈餘品質 FCF/NI", "≥0.9 綠 · 0.7-0.9 黃 · <0.7 紅"),
            ("FCF 殖利率", "≥4% 綠 · 2-4% 黃 · <2% 紅"),
        ]),
        ("💲 估值 (1)", [
            ("Forward PEG", "≤1.2 綠 · 1.2-2 黃 · >2 紅"),
        ]),
    ]
    body = ""
    for title, rows in groups:
        rows_html = "".join(f'<div class="legend-row"><span class="k">{k}</span><span class="v">{v}</span></div>' for k, v in rows)
        body += f'<div class="legend-group"><h6>{title}</h6>{rows_html}</div>'
    return f'''<details class="legend-box">
  <summary>🚦 12 盞燈解說（依照下方個股列上的 12 點排序，左→右）— 點開查看各指標門檻</summary>
  <div class="legend-body">{body}</div>
</details>'''


def _gen_backtest(stocks, benchmarks):
    """Score 與 1 年報酬的相關性 + 組合 vs 大盤比較。"""
    items = [
        (s["symbol"], s["name"], s["score"], (s.get("tech") or {}).get("pct_252d"))
        for s in stocks.values()
        if (s.get("tech") or {}).get("pct_252d") is not None
    ]
    if len(items) < 10:
        return ""
    items.sort(key=lambda x: -x[2])
    top10 = items[:10]
    bot10 = items[-10:]
    top_avg = sum(x[3] for x in top10) / len(top10)
    bot_avg = sum(x[3] for x in bot10) / len(bot10)
    diff = top_avg - bot_avg

    # Bucket by score tier
    tiers = [
        ("🟢 高分（≥ 14）", [x for x in items if x[2] >= 14]),
        ("🟡 中分（10-13）", [x for x in items if 10 <= x[2] <= 13]),
        ("🔴 低分（< 10）", [x for x in items if x[2] < 10]),
    ]
    tier_rows = ""
    for label, arr in tiers:
        if not arr:
            continue
        avg = sum(x[3] for x in arr) / len(arr)
        cls = _pct_cls(avg)
        examples = " · ".join(x[0] for x in arr[:3])
        tier_rows += f'<tr><td>{label}<div class="tier-examples">例：{examples}</div></td><td class="num">{len(arr)}</td><td class="num {cls}">{_fmt_pct(avg)}</td></tr>'

    # 組合 vs 大盤
    giant_syms = [code for code, _, _ in GIANTS]
    high_score_syms = [s["symbol"] for s in stocks.values() if s["score"] >= 14]
    portfolio_rows = [
        ("AI 九巨頭（等權）", compute_portfolio_return(stocks, giant_syms)),
        (f"高分組合（≥14 分，{len(high_score_syms)} 檔等權）", compute_portfolio_return(stocks, high_score_syms)),
    ]
    bench_rows = [(b["label"] + f" ({b['symbol']})", b.get("pct_252d")) for b in (benchmarks or [])]

    cmp_rows = ""
    for label, val in portfolio_rows + bench_rows:
        if val is None:
            cmp_rows += f'<tr><td>{label}</td><td class="num mu">─</td></tr>'
        else:
            cls = _pct_cls(val)
            cmp_rows += f'<tr><td>{label}</td><td class="num {cls}">{_fmt_pct(val)}</td></tr>'

    valid = "✓ 評分系統有效" if diff > 10 else "⚠️ 評分與 1 年報酬相關性弱（可能因上市時間短或市場剛反轉）" if diff < 0 else "→ 相關性尚可"

    return f'''<div class="ttl">📊 組合比較 + 評分有效性</div>
<div class="desc">左：把宇宙內的股票依目前紅黃綠燈分數分三級，看該分級所有股票過去 1 年的「等權平均報酬」。右：策略組合（九巨頭等權 + 高分等權）vs 大盤 ETF 過去 1 年報酬</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:18px">
  <div>
    <div style="font-size:11px;font-weight:700;color:var(--mu);margin-bottom:6px">🚦 依分數等級 · 該等級股票過去 1 年等權平均報酬</div>
    <table class="bt-table"><tr><th>等級</th><th class="num">檔數</th><th class="num">過去 1 年報酬（等權組合）</th></tr>{tier_rows}</table>
    <div style="margin-top:8px;background:var(--card);border:1px solid var(--brd);border-radius:6px;padding:8px 12px;font-size:11.5px;line-height:1.6">
      Top10 均：<b class="{_pct_cls(top_avg)}">{_fmt_pct(top_avg)}</b> · Bottom10 均：<b class="{_pct_cls(bot_avg)}">{_fmt_pct(bot_avg)}</b>
      · 差距 <b>{_fmt_pct(diff)}</b> · <span style="color:var(--mu)">{valid}</span>
    </div>
  </div>
  <div>
    <div style="font-size:11px;font-weight:700;color:var(--mu);margin-bottom:6px">📈 組合 vs 大盤（1 年）</div>
    <table class="bt-table"><tr><th>組合 / 基準</th><th class="num">1 年報酬</th></tr>{cmp_rows}</table>
  </div>
</div>'''


def _gen_giants_grid(stocks):
    html = '<div class="giants">'
    for code, name, role in GIANTS:
        s = stocks.get(code)
        if not s:
            html += f'<div class="giant-card"><div class="sym">{code}</div><div class="name">{name}</div><div class="role">{role}</div><div class="empty-msg">資料不足</div></div>'
            continue
        chg_cls = _pct_cls(s["day_pct"])
        score_cls = _score_cls(s["score"], s["score_max"])
        html += f'''<a href="#stock-{code}" class="giant-card" style="text-decoration:none;color:inherit">
  <span class="score score-chip {score_cls}">{s["score"]}/{s["score_max"]}</span>
  <div class="sym">{code}</div>
  <div class="name">{name}</div>
  <div class="role">{role}</div>
  <div class="px">{_fmt_price(s["price"])}</div>
  <div class="chg {chg_cls}">{_fmt_pct(s["day_pct"])}</div>
</a>'''
    html += '</div>'
    return html


def _gen_supply_chain(stocks, alpha_scores=None, bucket_rankings=None):
    html = '<div class="chain">'
    # 建反向索引：bucket → 哪些巨頭依賴它（含強度）
    bucket_deps = {}
    for giant, deps in DEPENDENCY.items():
        for b, strength in deps.items():
            bucket_deps.setdefault(b, []).append((giant, strength))

    for key, label, role, syms in BUCKETS:
        deps = sorted(bucket_deps.get(key, []), key=lambda x: -x[1])
        giant_chips = "".join(
            f'<span class="b-giant-chip s{strength}" title="依賴強度 {"★" * strength}">{g}</span>'
            for g, strength in deps
        )
        # 依 alpha（若有）排序個股，否則用紅黃綠燈分數
        if alpha_scores:
            sorted_stocks = sorted(
                [stocks[s] for s in syms if s in stocks],
                key=lambda x: -alpha_scores.get(x["symbol"], {}).get("alpha", 0),
            )
        else:
            sorted_stocks = sorted(
                [stocks[s] for s in syms if s in stocks],
                key=lambda x: -x["score"] if x else 0,
            )
        # 計算 bucket 統計
        b_stats = ""
        if sorted_stocks:
            scores = [s["score"] for s in sorted_stocks]
            day_pcts = [s["day_pct"] for s in sorted_stocks if s.get("day_pct") is not None]
            avg_score = sum(scores) / len(scores) if scores else 0
            avg_score_max = sorted_stocks[0]["score_max"] if sorted_stocks else 24
            avg_day = (sum(day_pcts) / len(day_pcts)) if day_pcts else None
            score_cls = "up" if avg_score / avg_score_max >= 0.6 else "mu" if avg_score / avg_score_max >= 0.4 else "dn"
            day_cls = _pct_cls(avg_day)
            b_stats = f'<div class="b-stats"><span class="stat-pill {score_cls}" title="平均分數">🚦 {avg_score:.1f}/{avg_score_max}</span><span class="stat-pill {day_cls}" title="平均日漲跌">{_fmt_pct(avg_day)}</span></div>'

        # 取得 bucket 內 top alpha 的領頭股
        leader_info = ""
        if bucket_rankings and key in bucket_rankings and bucket_rankings[key]["ordered"]:
            top_sym, top_alpha = bucket_rankings[key]["ordered"][0]
            if top_alpha > 0:
                leader_info = f'<span class="b-leader" title="Bucket 內 alpha 最高">🥇 {top_sym} ({top_alpha:.0f})</span>'

        ranks_for_bucket = (bucket_rankings or {}).get(key, {}).get("ranks", {})

        html += f'''<details class="bucket">
  <summary>
    <div class="b-left">
      <span class="b-role">{role}</span>
      <span class="b-title">{label}</span>
      <span class="b-stocks-count">· {len(sorted_stocks)} 檔</span>
      {leader_info}
    </div>
    {b_stats}
    <div class="b-giants">{giant_chips}</div>
    <span class="chev">▾</span>
  </summary>
  <div class="b-body">
    {_gen_bucket_body(sorted_stocks, alpha_scores, ranks_for_bucket)}
  </div>
</details>'''
    html += '</div>'
    return html


def _gen_bucket_body(stocks, alpha_scores=None, ranks_for_bucket=None):
    if not stocks:
        return '<div class="empty-msg">此 bucket 暫無資料</div>'
    return "".join(_gen_stock_row(s, alpha_scores, ranks_for_bucket) for s in stocks)


def _gen_stock_row(s, alpha_scores=None, ranks_for_bucket=None):
    code = s["symbol"]
    light_parts = []
    for k, v in s["lights"].items():
        val_str = _fmt_light_value(k, v["value"])
        label = v["label"]
        light_parts.append(f'<span class="{_light_cls(v["color"])}" title="{label}: {val_str}"></span>')
    lights_mini = "".join(light_parts)
    score_cls = _score_cls(s["score"], s["score_max"])
    day_cls = _pct_cls(s["day_pct"])

    # 排名獎牌
    rank_info = (ranks_for_bucket or {}).get(code, {})
    medal = rank_info.get("medal", "")
    medal_html = f'<span class="rank-medal" title="Bucket 內 {rank_info.get("label", "")}">{medal}</span>' if medal else ""

    # Alpha 分數 pill
    alpha_pill = ""
    if alpha_scores and code in alpha_scores:
        a = alpha_scores[code]
        tier = a["tier"]
        alpha_pill = f'<span class="alpha-pill {tier}" title="Alpha {a["tier_label"]}">α {a["alpha"]:.0f}</span>'

    return f'''<details class="stock" id="stock-{code}">
  <summary>
    {medal_html}
    <span class="sym">{code}</span>
    <span class="nm">{s["name"]}</span>
    <span class="lights-mini" title="紅黃綠燈摘要">{lights_mini}</span>
    <span class="score-chip {score_cls}" title="12 盞燈綜合分">{s["score"]}/{s["score_max"]}</span>
    {alpha_pill}
    <span class="pct {day_cls}">{_fmt_pct(s["day_pct"])}</span>
  </summary>
  {_gen_stock_detail(s, alpha_scores)}
</details>'''


def _gen_stock_detail(s, alpha_scores=None):
    alpha_block = ""
    if alpha_scores and s["symbol"] in alpha_scores:
        a = alpha_scores[s["symbol"]]
        comps = a["components"]
        labels = {
            "fundamental": "基本面",
            "momentum": "動能",
            "technical": "技術",
            "capex_align": "Capex 對齊",
            "analyst": "分析師",
            "signals": "訊號",
        }
        rows = "".join(
            f'<div class="light-row"><span class="k">{labels[k]} ({ALPHA_WEIGHTS[k]*100:.0f}%)</span><span class="v">{v:.1f}/10</span></div>'
            for k, v in comps.items()
        )
        alpha_block = f'''<div class="sd-block" style="grid-column:1/-1">
  <h5>⚡ Alpha 評分拆解 — {a["alpha"]:.0f}/100 · <span class="alpha-pill {a["tier"]}">{a["tier_label"]}</span></h5>
  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:4px 14px">{rows}</div>
</div>'''

    return f'''<div class="stock-detail">
  <div class="sd-grid2">
    {_gen_tech_block(s)}
    {_gen_lights_block(s)}
    {_gen_valuation_block(s)}
    {_gen_news_block(s)}
    {alpha_block}
  </div>
  {_gen_cross_check_block(s)}
</div>'''


def _gen_tech_block(s):
    t = s.get("tech") or {}
    rows = []

    def ma_pill(lbl, above, val):
        if val is None or above is None:
            return f'<span class="pill mu">{lbl} ─</span>'
        cls = "up" if above else "dn"
        arrow = "↑" if above else "↓"
        return f'<span class="pill {cls}">{lbl} {arrow} {_fmt_price(val)}</span>'

    rows.append(f'<div class="kv-row"><span class="k">收盤</span><span class="v">{_fmt_price(t.get("price") or s.get("price"))}</span></div>')
    rows.append(f'<div class="kv-row"><span class="k">市值</span><span class="v">{_fmt_mc(s.get("market_cap"))}</span></div>')
    rows.append(f'<div class="kv-row"><span class="k">近 5 日 / 20 日 / 60 日</span><span class="v"><span class="{_pct_cls(t.get("pct_5d"))}">{_fmt_pct(t.get("pct_5d"))}</span> / <span class="{_pct_cls(t.get("pct_20d"))}">{_fmt_pct(t.get("pct_20d"))}</span> / <span class="{_pct_cls(t.get("pct_60d"))}">{_fmt_pct(t.get("pct_60d"))}</span></span></div>')
    rows.append(f'<div class="kv-row"><span class="k">近 1 年</span><span class="v {_pct_cls(t.get("pct_252d"))}">{_fmt_pct(t.get("pct_252d"))}</span></div>')
    rsi = t.get("rsi")
    rsi_str = "─" if rsi is None else f"{rsi:.0f} {'超買' if rsi>=70 else '超賣' if rsi<=30 else '中性'}"
    rows.append(f'<div class="kv-row"><span class="k">RSI(14)</span><span class="v">{rsi_str}</span></div>')
    rows.append(f'<div class="kv-row"><span class="k">均線位置</span><span class="v">{ma_pill("MA50", t.get("above_ma50"), t.get("ma50"))} {ma_pill("MA200", t.get("above_ma200"), t.get("ma200"))}</span></div>')

    return f'<div class="sd-block"><h5>📐 技術面 (yfinance)</h5>{"".join(rows)}</div>'


def _gen_lights_block(s):
    lights = s["lights"]
    # 分組顯示
    groups = [
        ("損益", ["revenue_growth_yoy", "operating_margin", "net_margin", "gross_margin"]),
        ("資產負債", ["debt_to_equity", "current_ratio", "roe", "roic"]),
        ("現金流", ["fcf_margin", "income_quality", "fcf_yield"]),
        ("估值", ["forward_peg"]),
    ]
    html = ""
    for group_name, keys in groups:
        html += f'<div style="font-size:11px;color:var(--mu);font-weight:700;margin:6px 0 3px 0;text-transform:uppercase;letter-spacing:0.3px">{group_name}</div>'
        for k in keys:
            v = lights.get(k, {})
            color = v.get("color", "gray")
            label = v.get("label", k)
            value = _fmt_light_value(k, v.get("value"))
            html += f'<div class="light-row"><span class="{_light_cls(color)}"></span><span class="k">{label}</span><span class="v">{value}</span></div>'
    # 附 Piotroski + Altman Z
    pio = s.get("piotroski")
    az = s.get("altman_z")
    html += '<div style="font-size:11px;color:var(--mu);font-weight:700;margin:10px 0 3px 0;text-transform:uppercase">綜合指標</div>'
    if pio is not None:
        pio_cls = "up" if pio >= 7 else "am" if pio >= 5 else "dn"
        html += f'<div class="kv-row"><span class="k">Piotroski F-Score (0-9)</span><span class="v"><span class="pill {pio_cls}">{pio}</span></span></div>'
    if az is not None:
        az_cls = "up" if az >= 3 else "am" if az >= 1.8 else "dn"
        html += f'<div class="kv-row"><span class="k">Altman Z-Score</span><span class="v"><span class="pill {az_cls}">{az:.2f}</span></span></div>'

    return f'<div class="sd-block"><h5>🚦 紅黃綠燈 (FMP)</h5>{html}</div>'


def _gen_valuation_block(s):
    pt = s.get("price_target") or {}
    grades = s.get("grades") or {}
    est = s.get("analyst_estimates_next") or {}
    rows = []

    if pt.get("consensus") is not None:
        upside = pt.get("upside_pct")
        upside_str = _fmt_pct(upside) if upside is not None else "─"
        upside_cls = _pct_cls(upside)
        rows.append(f'<div class="kv-row"><span class="k">目標價共識</span><span class="v">{_fmt_price(pt["consensus"])} <span class="{upside_cls}">({upside_str})</span></span></div>')
        rows.append(f'<div class="kv-row"><span class="k">目標範圍</span><span class="v mu">{_fmt_price(pt.get("low"))} – {_fmt_price(pt.get("high"))}</span></div>')

    if grades:
        total = sum(v or 0 for v in grades.values())
        if total:
            sb = (grades.get("strong_buy") or 0)
            b = (grades.get("buy") or 0)
            h = (grades.get("hold") or 0)
            sell = (grades.get("sell") or 0)
            ss = (grades.get("strong_sell") or 0)
            bar = ""
            for cls, n in [("sb", sb), ("b", b), ("h", h), ("s", sell), ("ss", ss)]:
                if n:
                    pct = n / total * 100
                    bar += f'<div class="{cls}" style="width:{pct:.1f}%" title="{cls} {n} 人"></div>'
            rows.append(f'<div class="kv-row"><span class="k">分析師 {total} 人</span><span class="v mu" style="font-size:11px">{sb}S/{b}B/{h}H/{sell}S/{ss}SS</span></div>')
            rows.append(f'<div class="analyst-bar">{bar}</div>')

    if est:
        rev_avg = est.get("revenueAvg")
        eps_avg = est.get("epsAvg")
        num_a = est.get("numAnalystsRevenue")
        if rev_avg:
            rows.append(f'<div class="kv-row"><span class="k">明年營收預估（{num_a} 位分析師）</span><span class="v">{_fmt_mc(rev_avg)}</span></div>')
        if eps_avg is not None:
            rows.append(f'<div class="kv-row"><span class="k">明年 EPS 預估</span><span class="v">${eps_avg:.2f}</span></div>')

    body = "".join(rows) if rows else '<div class="empty-msg">分析師資料暫無</div>'
    return f'<div class="sd-block"><h5>🎯 分析師面 (FMP)</h5>{body}</div>'


SENT_LABEL = {"positive": "正向", "negative": "負向", "neutral": "中性"}


def _gen_news_block(s):
    news = s.get("news") or []
    if not news:
        return '<div class="sd-block"><h5>📰 新聞 (yfinance · 48h)</h5><div class="empty-msg">暫無相關新聞</div></div>'

    # 摘要：各類別/情緒的分布
    sent_counts = {"positive": 0, "negative": 0, "neutral": 0}
    cat_counts = {}
    for n in news:
        sent_counts[n.get("sentiment", "neutral")] += 1
        cat_counts[n.get("category_label", "一般新聞")] = cat_counts.get(n.get("category_label", "一般新聞"), 0) + 1
    summary = f'<div style="font-size:10.5px;color:var(--mu);margin-bottom:6px">綜合情緒：<span class="sent-positive-label">↑{sent_counts["positive"]}</span> / <span class="sent-neutral-label">={sent_counts["neutral"]}</span> / <span class="sent-negative-label">↓{sent_counts["negative"]}</span></div>'

    items = ""
    for n in news:
        age = _news_age(n.get("timestamp"))
        provider = n.get("provider") or ""
        url = n.get("url") or "#"
        open_ = f'<a href="{url}" target="_blank" rel="noopener">' if url != "#" else "<span>"
        close_ = "</a>" if url != "#" else "</span>"

        cat = n.get("category", "general")
        cat_label = n.get("category_label", "一般新聞")
        sentiment = n.get("sentiment", "neutral")
        sent_label = SENT_LABEL.get(sentiment, "中性")
        matched = n.get("matched_keywords", [])
        matched_title = (" · 關鍵字: " + ", ".join(matched[:3])) if matched else ""

        tags = f'<div class="news-tags"><span class="cat-pill cat-{cat}" title="{cat_label}{matched_title}">{cat_label}</span><span class="sent-dot sent-{sentiment}"></span><span class="sent-label sent-{sentiment}-label">{sent_label}</span>'

        # LLM 分析輸出（若有）
        llm = n.get("llm") or {}
        if llm:
            impact = llm.get("impact", "neutral")
            mag = llm.get("magnitude", "low")
            llm_cat = llm.get("category", "other")
            mag_emoji = "🔥" if mag == "high" else "⚡" if mag == "medium" else "·"
            impact_cls = "sent-positive" if impact == "bullish" else "sent-negative" if impact == "bearish" else "sent-neutral"
            tags += f'<span class="llm-tag" title="AI 深度分析 · {llm_cat}"><span class="{impact_cls}" style="font-size:10px">🤖 {mag_emoji} {impact}</span></span>'

        tags += '</div>'

        zh_summary = ""
        if llm and llm.get("zh"):
            zh_summary = f'<div class="llm-summary">💡 {llm["zh"]}</div>'

        meta_parts = [x for x in (provider, age) if x]
        meta = f'<div class="news-meta"><span>{" · ".join(meta_parts)}</span></div>'

        items += f'<div class="news-item">{tags}{open_}{n["title"]}{close_}{zh_summary}{meta}</div>'

    return f'<div class="sd-block"><h5>📰 新聞 (yfinance · 48h)</h5>{summary}{items}</div>'


def _gen_cross_check_block(s):
    w = s.get("cross_check") or []
    if not w:
        return '<div style="font-size:11px;color:var(--mu);margin-top:8px;padding:4px 8px;background:#f0fdf4;border-radius:4px">✓ 兩源資料一致（差異 &lt; 2%）</div>'
    parts = []
    for warning in w:
        parts.append(f'<b>{warning["field"]}</b> 差 {warning["diff_pct"]}% (yf: {warning["yf"]:.2f} / fmp: {warning["fmp"]:.2f})')
    return f'<div class="warn-box">⚠️ 兩源差異偵測：{" ; ".join(parts)}</div>'


def generate_html(stocks, capex_groups, signals, benchmarks, alpha_scores, bucket_rankings, health=None):
    bucket_lookup = {k: label for k, label, _, _ in BUCKETS}
    giants_grid = _gen_giants_grid(stocks)
    supply_chain = _gen_supply_chain(stocks, alpha_scores, bucket_rankings)
    capex_html = _gen_capex_panel(capex_groups, bucket_lookup)
    legend_html = _gen_legend()
    backtest_html = _gen_backtest(stocks, benchmarks)
    signals_html = _gen_signals_panel(signals, stocks)
    sankey_html = _gen_sankey_svg(stocks, bucket_lookup)
    alpha_html = _gen_alpha_panel(alpha_scores, stocks)
    health_badge = _gen_health_badge(health)

    n_green = sum(1 for s in stocks.values() for l in s["lights"].values() if l["color"] == "green")
    n_red = sum(1 for s in stocks.values() for l in s["lights"].values() if l["color"] == "red")
    n_total_lights = sum(len(s["lights"]) for s in stocks.values())

    fmp_status = "FMP + yfinance 雙源交叉驗證" if FMP_KEY else "僅 yfinance（FMP 未設定）"

    return f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<meta name="robots" content="noindex, nofollow"/>
<title>AI 供應鏈策略儀表板 — {TODAY.isoformat()}</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>AI 供應鏈策略儀表板</h1>
  <div class="sub">9 檔巨頭 + 供應鏈上下游 × 基本面紅黃綠燈 · 更新 {TODAY.isoformat()} · {fmp_status} <a class="nav-link" href="us_index.html">→ 美股板塊復盤</a> <a class="nav-link" href="etf_index.html">→ ETF 資金流</a></div>
</div>
{health_badge}
<div class="pane">
  <div class="ttl">🏭 AI 資本支出週期（Capex 領先指標）</div>
  <div class="desc">雲端 Hyperscaler（MSFT+GOOGL+META+AMZN）合計 Capex 年增決定上游供應鏈（AI 晶片／HBM／網通／電源）需求強度。TSM Capex 則是設備商（ASML／AMAT／LRCX／KLAC）的訂單訊號</div>
  {capex_html}

  <div class="ttl">🏛 AI 九巨頭</div>
  <div class="desc">Mag7 + TSM（全球晶圓代工）+ AVGO（客製 AI ASIC）。角標分數為 12 盞燈綜合分（綠 2 分 / 黃 1 分 · 最高 24 分）</div>
  {giants_grid}

  <div class="ttl">🔄 連續訊號（歷史累積）</div>
  <div class="desc">從每日快照累積，找出連續漲/跌、連續進 Top-3 的標的（需至少 3 個交易日歷史）</div>
  {signals_html}

  {backtest_html}

  <div class="ttl">🕸 供應鏈依賴關係圖（巨頭 ↔ Bucket）</div>
  <div class="desc">連線粗細 = 依賴強度（★/★★/★★★）。連線顏色 = 該 bucket 當日平均漲跌（綠漲紅跌）。節點顯示當日 % 與紅黃綠燈分數</div>
  {sankey_html}

  <div class="ttl">🔗 供應鏈（由上游至下游）</div>
  <div class="desc">每個 bucket 標示哪些巨頭依賴它（★=弱、★★=中、★★★=關鍵）。點 bucket 展開個股清單，個股依紅黃綠燈分數排序。再點個股看完整技術/基本面/分析師/新聞分析</div>
  {legend_html}
  {supply_chain}
</div>
<script>
var __sankeyState = {{giant: null, bucket: null}};

function __fmtPct(p){{
  if(p==null || isNaN(p)) return '—';
  var s = (p>=0?'+':'') + p.toFixed(2) + '%';
  var cls = p>0.05 ? 'up' : p<-0.05 ? 'dn' : 'mu';
  return '<span class="'+cls+'">'+s+'</span>';
}}
function __stars(n){{ return '★'.repeat(n) + '☆'.repeat(3-n); }}
function __strengthLabel(n){{ return n===3 ? '關鍵依賴' : n===2 ? '重要' : '次要'; }}

function resetSankey(){{
  __sankeyState = {{giant: null, bucket: null}};
  document.querySelectorAll('.sankey-svg path.sankey-link').forEach(p=>{{
    p.classList.remove('active','dimmed');
    p.setAttribute('stroke-width', p.dataset.defaultWidth);
    p.style.opacity = '';
  }});
  document.querySelectorAll('.sankey-svg .sankey-node').forEach(n=>{{
    n.classList.remove('active','dimmed','clickable-target');
  }});
  var info = document.getElementById('sankey-info');
  if(info){{
    info.className='sankey-info-empty';
    info.innerHTML='👆 點擊左側巨頭或右側 Bucket 節點，查看其所有供應鏈關係；或點一條線看單一關係';
  }}
}}

function __applyPathFilter(matchFn){{
  document.querySelectorAll('.sankey-svg path.sankey-link').forEach(p=>{{
    if(matchFn(p)){{
      p.classList.add('active'); p.classList.remove('dimmed');
      p.setAttribute('stroke-width', (parseFloat(p.dataset.defaultWidth)+1.0).toFixed(1));
    }} else {{
      p.classList.add('dimmed'); p.classList.remove('active');
      p.setAttribute('stroke-width', p.dataset.defaultWidth);
    }}
  }});
}}

function __collectRelations(giantCode){{
  // 從 DOM 收集 NVDA 的所有 bucket 關聯
  var out = [];
  document.querySelectorAll('.sankey-svg path.sankey-link[data-giant="'+giantCode+'"]').forEach(p=>{{
    out.push({{
      bucket: p.dataset.bucket,
      label: p.dataset.bucketLabel,
      strength: parseInt(p.dataset.strength),
      bucketPct: parseFloat(p.dataset.bucketPct),
    }});
  }});
  out.sort(function(a,b){{ return b.strength - a.strength; }});
  return out;
}}

function __collectGiantsForBucket(bucketKey){{
  var out = [];
  document.querySelectorAll('.sankey-svg path.sankey-link[data-bucket="'+bucketKey+'"]').forEach(p=>{{
    out.push({{
      giant: p.dataset.giant,
      strength: parseInt(p.dataset.strength),
      giantPct: parseFloat(p.dataset.giantPct),
    }});
  }});
  out.sort(function(a,b){{ return b.strength - a.strength; }});
  return out;
}}

function selectGiant(code, ev){{
  if(ev && ev.stopPropagation) ev.stopPropagation();
  __sankeyState = {{giant: code, bucket: null}};
  var rels = __collectRelations(code);
  var relBuckets = new Set(rels.map(function(r){{return r.bucket;}}));
  __applyPathFilter(function(p){{ return p.dataset.giant===code; }});
  document.querySelectorAll('.sankey-svg .sankey-node').forEach(n=>{{
    var isGiant = n.dataset.giant===code;
    var isRelBucket = !!(n.dataset.bucket && relBuckets.has(n.dataset.bucket));
    n.classList.toggle('active', isGiant);
    n.classList.toggle('clickable-target', isRelBucket);
    n.classList.toggle('dimmed', !isGiant && !isRelBucket);
  }});
  var info = document.getElementById('sankey-info');
  if(info){{
    var chips = rels.map(function(r){{
      return '<span class="si-chip" onclick="selectBucket(\\''+r.bucket+'\\', event)" title="'+__strengthLabel(r.strength)+' · Bucket 平均 '+(r.bucketPct>=0?'+':'')+r.bucketPct.toFixed(2)+'%"><span class="si-stars">'+__stars(r.strength)+'</span> '+r.label+' <span class="si-chip-pct">'+__fmtPct(r.bucketPct)+'</span></span>';
    }}).join('');
    info.className='sankey-info-active';
    info.innerHTML =
      '<div class="si-head">🎯 '+code+' 的上下游依賴（'+rels.length+' 個 bucket）</div>'+
      '<div style="font-size:11.5px;color:#64748b;margin-bottom:6px">點下方任一 Bucket 查看單一關係細節</div>'+
      '<div class="si-chips">'+chips+'</div>';
  }}
}}

function selectBucket(key, ev){{
  if(ev && ev.stopPropagation) ev.stopPropagation();
  // 若已選擇 giant，顯示 giant→bucket 單一關係
  if(__sankeyState.giant){{
    var giant = __sankeyState.giant;
    var path = document.querySelector('.sankey-svg path.sankey-link[data-giant="'+giant+'"][data-bucket="'+key+'"]');
    if(path){{ highlightSankey(path, ev); return; }}
  }}
  // 否則顯示該 bucket 所有依賴它的巨頭
  __sankeyState = {{giant: null, bucket: key}};
  var deps = __collectGiantsForBucket(key);
  var depGiants = new Set(deps.map(function(d){{return d.giant;}}));
  __applyPathFilter(function(p){{ return p.dataset.bucket===key; }});
  var bucketLabel = '';
  document.querySelectorAll('.sankey-svg .sankey-node').forEach(n=>{{
    var isBucket = n.dataset.bucket===key;
    if(isBucket) bucketLabel = n.dataset.bucketLabel || '';
    var isDepGiant = !!(n.dataset.giant && depGiants.has(n.dataset.giant));
    n.classList.toggle('active', isBucket);
    n.classList.toggle('clickable-target', isDepGiant);
    n.classList.toggle('dimmed', !isBucket && !isDepGiant);
  }});
  var info = document.getElementById('sankey-info');
  if(info){{
    var chips = deps.map(function(d){{
      return '<span class="si-chip" onclick="selectGiant(\\''+d.giant+'\\', event)" title="'+__strengthLabel(d.strength)+' · 巨頭當日 '+(d.giantPct>=0?'+':'')+d.giantPct.toFixed(2)+'%"><span class="si-stars">'+__stars(d.strength)+'</span> '+d.giant+' <span class="si-chip-pct">'+__fmtPct(d.giantPct)+'</span></span>';
    }}).join('');
    info.className='sankey-info-active';
    info.innerHTML =
      '<div class="si-head">🎯 '+bucketLabel+' 被 '+deps.length+' 個巨頭依賴</div>'+
      '<div style="font-size:11.5px;color:#64748b;margin-bottom:6px">點下方任一巨頭查看它對此 bucket 的依賴細節</div>'+
      '<div class="si-chips">'+chips+'</div>';
  }}
}}

function highlightSankey(el, ev){{
  if(ev && ev.stopPropagation) ev.stopPropagation();
  var giant = el.dataset.giant, bucket = el.dataset.bucket;
  __sankeyState = {{giant: giant, bucket: bucket}};
  __applyPathFilter(function(p){{ return p===el; }});
  document.querySelectorAll('.sankey-svg .sankey-node').forEach(n=>{{
    var isMatch = n.dataset.giant===giant || n.dataset.bucket===bucket;
    n.classList.toggle('active', !!isMatch);
    n.classList.toggle('clickable-target', false);
    n.classList.toggle('dimmed', !isMatch);
  }});
  var info = document.getElementById('sankey-info');
  if(!info) return;
  var strength = parseInt(el.dataset.strength);
  var gpct = parseFloat(el.dataset.giantPct);
  var bpct = parseFloat(el.dataset.bucketPct);
  var label = el.dataset.bucketLabel;
  info.className='sankey-info-active';
  info.innerHTML =
    '<div class="si-head">'+giant+'  →  '+label+'</div>'+
    '<div style="font-size:12px;color:#475569;margin-bottom:6px">'+__stars(strength)+' '+__strengthLabel(strength)+'（'+strength+'/3）</div>'+
    '<div class="si-grid">'+
      '<div><span class="k">巨頭當日</span><span class="v">'+__fmtPct(gpct)+'</span></div>'+
      '<div><span class="k">Bucket 平均當日</span><span class="v">'+__fmtPct(bpct)+'</span></div>'+
    '</div>'+
    '<div style="margin-top:8px;font-size:11.5px"><a href="#" onclick="selectGiant(\\''+giant+'\\', event); return false;" style="color:var(--bl)">← 回到 '+giant+' 全部依賴</a></div>';
}}
</script>
</body></html>"""


# ── Main ────────────────────────────────────────────────────────────────

def main():
    symbols = collect_all_symbols()
    print(f"=== AI 供應鏈策略 ({TODAY.isoformat()}) ===")
    print(f"追蹤 {len(symbols)} 檔個股（9 巨頭 + {len(symbols)-9} 檔供應鏈）\n")

    # 1. yfinance 批次 K 線（技術面資料）
    print("yfinance：批次抓 2 年 K 線...")
    bars_df = yf_batch_bars(symbols)
    print("  完成\n")

    # 2. FMP 平行抓（基本面 + 分析師面）
    fmp_map = fetch_fmp_bulk(symbols)

    # 3. yfinance 基本面備援 + 新聞
    yf_fund_map = fetch_yf_fundamentals_bulk(symbols)
    news_map = fetch_yf_bulk(symbols)

    # 4. 組合每檔個股的報告
    stocks = {}
    name_map = {code: name for code, name, _ in GIANTS}
    for sym in symbols:
        name = name_map.get(sym) or sym
        rep = build_stock_report(sym, name, bars_df, fmp_map, news_map, yf_fund_map)
        stocks[sym] = rep

    # 摘要
    print("\n=== 紅黃綠燈分數排行 ===")
    rank = sorted(stocks.values(), key=lambda s: -s["score"])[:10]
    for s in rank:
        print(f"  {s['symbol']:6s} {s['score']:2d}/{s['score_max']:2d} · {s['name']}")

    # 5. LLM 新聞深度分析（九巨頭 only，gated by ANTHROPIC_API_KEY）
    enrich_news_with_llm(stocks)

    # 6. Capex 週期
    capex_groups = compute_capex_groups(fmp_map)
    if capex_groups:
        print("\n=== Capex 週期 ===")
        for g in capex_groups:
            yoy = g["yoy_pct"]
            yoy_s = f"{yoy:+.1f}%" if yoy is not None else "─"
            print(f"  {g['label']}: TTM ${g['total_ttm']/1e9:.1f}B · YoY {yoy_s}")

    # 7. Benchmark ETFs
    print("\n抓 benchmark ETFs...")
    benchmarks = fetch_benchmarks()

    # 8. 歷史快照 + 連續訊號
    save_history_snapshot(stocks)
    cleanup_history()
    history = load_history()
    signals = compute_consecutive_signals(history, stocks) if history else {}
    print(f"\n連續訊號計算：歷史 {len(history)} 天")

    # 9. Alpha scoring model + bucket rankings
    stock_to_bucket = {}
    for key, _, _, syms in BUCKETS:
        for sym in syms:
            stock_to_bucket[sym] = key
    alpha_scores = compute_alpha_scores(stocks, capex_groups, signals, stock_to_bucket)
    bucket_rankings = compute_bucket_rankings(stocks, alpha_scores)
    # 打印 alpha top 5
    print("\n=== Alpha Top 5 ===")
    for sym, sc in sorted(alpha_scores.items(), key=lambda x: -x[1]["alpha"])[:5]:
        print(f"  {sym:6s} {sc['alpha']:5.1f}  {sc['tier_label']}")

    # 10. Monitoring summary — 供 debug / 健康監控用
    health = compute_health_summary(stocks, fmp_map, news_map, capex_groups, signals, alpha_scores, benchmarks)
    with open("ai_strategy_health.json", "w", encoding="utf-8") as f:
        json.dump(health, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n=== 資料品質 ===")
    print(f"  FMP 基本面成功: {health['fmp_ok_count']}/{health['stock_count']}")
    print(f"  yfinance 新聞: {health['news_total']} 則")
    print(f"  LLM 深度分析: {health['llm_enriched_count']}")
    print(f"  Capex 週期組: {health['capex_groups_ok']}/{len(CAPEX_GROUPS)}")
    print(f"  歷史快照: {health['history_days']} 天")

    # 11. HTML
    html = generate_html(stocks, capex_groups, signals, benchmarks, alpha_scores, bucket_rankings, health)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n儀表板輸出至 {OUTPUT_HTML}")

    # 12. latest cache（debug 用）
    def to_slim(s):
        return {k: v for k, v in s.items() if k not in ("tech", "fundamentals", "insider_recent", "news", "analyst_estimates_next")}
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump({"trade_date": TODAY.isoformat(), "stocks": {k: to_slim(v) for k, v in stocks.items()}}, f, ensure_ascii=False, indent=2, default=str)


def compute_health_summary(stocks, fmp_map, news_map, capex_groups, signals, alpha_scores, benchmarks):
    """計算本次執行的資料品質摘要，寫入 ai_strategy_health.json。"""
    stock_count = len(stocks)
    fmp_ok_count = sum(1 for sym in stocks if (fmp_map.get(sym) or {}).get("ratios_latest"))
    news_total = sum(len(v) for v in (news_map or {}).values())
    llm_enriched_count = sum(
        1 for s in stocks.values()
        for n in (s.get("news") or [])
        if n.get("llm")
    )
    capex_groups_ok = len(capex_groups or [])
    alpha_tiers = {"strong_buy": 0, "buy": 0, "neutral": 0, "weak": 0}
    for v in (alpha_scores or {}).values():
        alpha_tiers[v["tier"]] = alpha_tiers.get(v["tier"], 0) + 1
    bench_ok = sum(1 for b in (benchmarks or []) if b.get("pct_252d") is not None)

    # 歷史快照天數
    history_days = 0
    if os.path.isdir(HISTORY_DIR):
        history_days = len([p for p in glob.glob(os.path.join(HISTORY_DIR, "*.json"))])

    # 資料新鮮度：新聞平均年齡
    news_ages = []
    for v in (news_map or {}).values():
        for n in v:
            ts_epoch = n.get("ts_epoch")
            if ts_epoch:
                age_h = (NOW_UTC.timestamp() - ts_epoch) / 3600
                if age_h > 0:
                    news_ages.append(age_h)
    avg_news_age = sum(news_ages) / len(news_ages) if news_ages else 0

    return {
        "trade_date": TODAY.isoformat(),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "stock_count": stock_count,
        "fmp_ok_count": fmp_ok_count,
        "fmp_ok_pct": round(fmp_ok_count / stock_count * 100, 1) if stock_count else 0,
        "news_total": news_total,
        "avg_news_age_hours": round(avg_news_age, 1),
        "llm_enriched_count": llm_enriched_count,
        "llm_enabled": bool(ANTHROPIC_API_KEY),
        "capex_groups_ok": capex_groups_ok,
        "bench_ok_count": bench_ok,
        "signals_count": len(signals or {}),
        "alpha_tiers": alpha_tiers,
        "history_days": history_days,
    }


if __name__ == "__main__":
    main()
