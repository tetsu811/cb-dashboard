#!/usr/bin/env python3
"""
美股復盤儀表板 — 板塊熱力圖 + 歸因
每個交易日收盤後執行，產出單檔 HTML 供 WordPress 嵌入。
"""
import glob
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

TODAY = datetime.now().date()
NOW_UTC = datetime.now(timezone.utc)
OUTPUT_HTML = 'us_index.html'
CACHE_FILE = 'us_latest.json'
HISTORY_DIR = 'us_data'
MAX_HISTORY_DAYS = 60

# 11 SPDR 行業 ETF
SECTORS = [
    ("XLK", "科技",   "Technology"),
    ("XLF", "金融",   "Financials"),
    ("XLV", "醫療",   "Health Care"),
    ("XLY", "非必需消費", "Consumer Discretionary"),
    ("XLC", "通訊服務", "Communication Services"),
    ("XLI", "工業",   "Industrials"),
    ("XLP", "必需消費", "Consumer Staples"),
    ("XLE", "能源",   "Energy"),
    ("XLU", "公用事業", "Utilities"),
    ("XLRE", "房地產", "Real Estate"),
    ("XLB", "原物料", "Materials"),
]

MARKET_TICKERS = [
    ("SPY", "S&P 500"),
    ("QQQ", "Nasdaq 100"),
    ("DIA", "Dow 30"),
    ("IWM", "Russell 2000"),
    ("^VIX", "VIX"),
]

TOP_N_HOLDINGS = 10         # 每個板塊抓幾檔成分股
CONTRIB_SHOW = 3            # 展示幾檔最大貢獻 / 拖累
VOLUME_SURGE_THRESHOLD = 1.5      # ETF 板塊放量：今日量 ≥ 20 日均量 × 此倍數
STOCK_VOL_SURGE_THRESHOLD = 1.2   # 個股放量：今日量 ≥ 昨日量 × 此倍數（+20%）
FETCH_DELAY = 0.3           # 抓成分股個股資料的節流
RSI_PERIOD = 14             # RSI 計算週期

# 新聞 / 財報 / 趨勢參數
NEWS_PER_SECTOR = 6            # 每個板塊展開區顯示的新聞數
NEWS_PER_STOCK = 3             # 每檔個股詳情面板顯示的新聞數
NEWS_MAX_AGE_HOURS = 48        # 只顯示 48 小時內新聞
EARNINGS_PAST_DAYS = 3         # 過去 N 個交易日內公布的財報
EARNINGS_UPCOMING_DAYS = 7     # 未來 N 個交易日內將公布的財報
STREAK_LOOKBACK = 10           # 連續性訊號回看天數
NEWS_WORKERS = 8               # 抓新聞並發數


# ── 資料抓取 ────────────────────────────────────────────────────────────────

def fetch_sector_and_market_bars():
    """一次抓取所有 ETF 與大盤指標的 60 日 K 線，用於算 1d/5d/20d/50d 指標。"""
    tickers = [s[0] for s in SECTORS] + [m[0] for m in MARKET_TICKERS]
    df = yf.download(
        tickers, period="1y", interval="1d",
        auto_adjust=False, progress=False, group_by="ticker", threads=True,
    )
    return df


def _ticker_bars(df, ticker):
    if ticker not in df.columns.get_level_values(0):
        return None
    bars = df[ticker].dropna(subset=["Close"])
    return bars if len(bars) >= 2 else None


def compute_etf_metrics(bars):
    """從日線 K 線計算 1d/5d 漲幅、量能比、均線位置。"""
    closes = bars["Close"]
    volumes = bars["Volume"]
    last, prev = closes.iloc[-1], closes.iloc[-2]
    day_pct = (last / prev - 1) * 100

    def pct_over(n):
        if len(closes) <= n:
            return None
        return (last / closes.iloc[-(n + 1)] - 1) * 100

    def ma(n):
        if len(closes) < n:
            return None
        return closes.iloc[-n:].mean()

    vol_today = volumes.iloc[-1]
    vol_avg20 = volumes.iloc[-21:-1].mean() if len(volumes) >= 21 else volumes.mean()
    vol_ratio = (vol_today / vol_avg20) if vol_avg20 else None

    ma20, ma50, ma200 = ma(20), ma(50), ma(200)

    return {
        "close": float(last),
        "day_pct": float(day_pct),
        "pct_5d": pct_over(5),
        "pct_20d": pct_over(20),
        "volume": int(vol_today),
        "vol_ratio": float(vol_ratio) if vol_ratio else None,
        "ma20": float(ma20) if ma20 else None,
        "ma50": float(ma50) if ma50 else None,
        "ma200": float(ma200) if ma200 else None,
        "above_ma20": bool(ma20 and last > ma20),
        "above_ma50": bool(ma50 and last > ma50),
        "above_ma200": bool(ma200 and last > ma200),
    }


def fetch_sector_holdings(sector_code):
    """抓取單一板塊 ETF 的 top holdings。返回 [(symbol, name, weight), ...]"""
    try:
        t = yf.Ticker(sector_code)
        fd = t.get_funds_data()
        th = fd.top_holdings
    except Exception as e:
        print(f"  [{sector_code}] holdings 抓取失敗: {e}")
        return []
    out = []
    for sym, row in th.head(TOP_N_HOLDINGS).iterrows():
        out.append((str(sym), str(row["Name"]), float(row["Holding Percent"])))
    return out


def _compute_rsi(closes, period=RSI_PERIOD):
    """簡化版 RSI（Wilder smoothing）。"""
    if len(closes) < period + 1:
        return None
    diffs = closes.diff().dropna()
    gains = diffs.clip(lower=0)
    losses = (-diffs).clip(lower=0)
    # Wilder smoothing
    avg_gain = gains.iloc[:period].mean()
    avg_loss = losses.iloc[:period].mean()
    for g, l in zip(gains.iloc[period:], losses.iloc[period:]):
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - 100 / (1 + rs))


def fetch_stocks_bars(symbols):
    """批次抓取多檔個股的近 1 年 K 線，算完整技術面指標。"""
    if not symbols:
        return {}
    df = yf.download(
        list(symbols), period="1y", interval="1d",
        auto_adjust=False, progress=False, group_by="ticker", threads=True,
    )
    out = {}
    for sym in symbols:
        try:
            bars = df[sym].dropna(subset=["Close"]) if sym in df.columns.get_level_values(0) else None
            if bars is None or len(bars) < 2:
                continue
            closes = bars["Close"]
            volumes = bars["Volume"]
            last, prev = closes.iloc[-1], closes.iloc[-2]
            day_pct = (last / prev - 1) * 100
            vol_today = volumes.iloc[-1]
            vol_yday = volumes.iloc[-2] if len(volumes) >= 2 else None
            vol_avg20 = volumes.iloc[-21:-1].mean() if len(volumes) >= 21 else volumes.mean()
            vol_vs_yday = (vol_today / vol_yday) if vol_yday else None
            vol_ratio = (vol_today / vol_avg20) if vol_avg20 else None

            def pct_over(n):
                if len(closes) <= n:
                    return None
                return float((last / closes.iloc[-(n + 1)] - 1) * 100)

            def ma(n):
                if len(closes) < n:
                    return None
                return float(closes.iloc[-n:].mean())

            ma20, ma50, ma200 = ma(20), ma(50), ma(200)
            rsi = _compute_rsi(closes)

            out[sym] = {
                "close": float(last),
                "day_pct": float(day_pct),
                "pct_5d": pct_over(5),
                "pct_20d": pct_over(20),
                "volume": int(vol_today),
                "vol_yesterday": int(vol_yday) if vol_yday else None,
                "vol_vs_yday": float(vol_vs_yday) if vol_vs_yday else None,
                "vol_ratio": float(vol_ratio) if vol_ratio else None,
                "ma20": ma20,
                "ma50": ma50,
                "ma200": ma200,
                "above_ma20": bool(ma20 and last > ma20),
                "above_ma50": bool(ma50 and last > ma50),
                "above_ma200": bool(ma200 and last > ma200),
                "rsi": rsi,
            }
        except Exception:
            continue
    return out


# ── 歸因計算 ────────────────────────────────────────────────────────────────

def build_sector_report(sector_code, sector_name_zh, sector_name_en, bars_df):
    """整合單一板塊的所有資訊。"""
    bars = _ticker_bars(bars_df, sector_code)
    if bars is None:
        return {
            "code": sector_code, "name_zh": sector_name_zh, "name_en": sector_name_en,
            "status": "error", "day_pct": 0, "holdings": [],
        }

    metrics = compute_etf_metrics(bars)
    holdings = fetch_sector_holdings(sector_code)
    time.sleep(FETCH_DELAY)

    holding_syms = [h[0] for h in holdings]
    stock_metrics = fetch_stocks_bars(holding_syms) if holding_syms else {}

    # 算每檔個股對板塊的「貢獻 = 權重 × 日漲幅」，並把完整技術指標一起帶進去
    contributions = []
    for sym, name, weight in holdings:
        m = stock_metrics.get(sym)
        if not m:
            continue
        contrib = weight * m["day_pct"]   # 權重為 0~1，貢獻以百分點近似
        contributions.append({
            "symbol": sym,
            "name": name,
            "weight": weight,
            "day_pct": m["day_pct"],
            "contribution": contrib,
            "tech": {
                "close": m["close"],
                "pct_5d": m.get("pct_5d"),
                "pct_20d": m.get("pct_20d"),
                "vol_vs_yday": m.get("vol_vs_yday"),
                "vol_ratio": m.get("vol_ratio"),
                "above_ma20": m.get("above_ma20"),
                "above_ma50": m.get("above_ma50"),
                "above_ma200": m.get("above_ma200"),
                "ma20": m.get("ma20"),
                "ma50": m.get("ma50"),
                "ma200": m.get("ma200"),
                "rsi": m.get("rsi"),
            },
        })

    contributions.sort(key=lambda x: x["contribution"], reverse=True)
    # 只有 真正負貢獻 的個股才進「今日拖累」
    negatives = [c for c in contributions if c["contribution"] < 0]
    gainers = [c for c in contributions if c["contribution"] > 0][:CONTRIB_SHOW]
    losers = sorted(negatives, key=lambda x: x["contribution"])[:CONTRIB_SHOW]

    # 放量個股：今日量 ≥ 昨日量 × 1.2（即 +20%）
    surges = [c for c in contributions
              if c["tech"].get("vol_vs_yday") and c["tech"]["vol_vs_yday"] >= STOCK_VOL_SURGE_THRESHOLD]
    surges.sort(key=lambda x: x["tech"]["vol_vs_yday"] or 0, reverse=True)

    return {
        "code": sector_code,
        "name_zh": sector_name_zh,
        "name_en": sector_name_en,
        "status": "ok",
        **metrics,
        "holdings_count": len(holdings),
        "gainers": gainers,
        "losers": losers,
        "surges": surges[:CONTRIB_SHOW],
        "all_holdings": contributions,
    }


def build_market_report(bars_df):
    """S&P / Nasdaq / Dow / Russell / VIX 概況。"""
    out = []
    for code, label in MARKET_TICKERS:
        bars = _ticker_bars(bars_df, code)
        if bars is None:
            out.append({"code": code, "label": label, "status": "error"})
            continue
        m = compute_etf_metrics(bars)
        out.append({"code": code, "label": label, "status": "ok", **m})
    return out


# ── 新聞 / 財報 抓取 ────────────────────────────────────────────────────────

def _parse_news_item(raw, related_sym):
    """yfinance 新版 news 結構是 {id, content:{...}}，抽出展示欄位。"""
    c = raw.get("content") or {}
    if c.get("contentType") and c["contentType"] not in ("STORY", "VIDEO"):
        return None
    title = c.get("title") or ""
    if not title:
        return None
    pub = c.get("pubDate") or c.get("displayTime")
    try:
        ts = datetime.fromisoformat(pub.replace("Z", "+00:00")) if pub else None
    except (ValueError, AttributeError):
        ts = None
    if ts and (NOW_UTC - ts).total_seconds() > NEWS_MAX_AGE_HOURS * 3600:
        return None
    url = (c.get("clickThroughUrl") or {}).get("url") or (c.get("canonicalUrl") or {}).get("url") or ""
    provider = (c.get("provider") or {}).get("displayName", "")
    return {
        "symbol": related_sym,
        "title": title,
        "url": url,
        "provider": provider,
        "timestamp": ts.isoformat() if ts else None,
        "ts_epoch": ts.timestamp() if ts else 0,
    }


def _fetch_symbol_news(symbol):
    try:
        news = yf.Ticker(symbol).news or []
    except Exception as e:
        print(f"  [{symbol}] news err: {e}")
        return symbol, []
    items = []
    for raw in news:
        item = _parse_news_item(raw, symbol)
        if item:
            items.append(item)
    return symbol, items


def _fetch_symbol_chip(symbol):
    """回傳 (symbol, {insider_buys, insider_sells, insider_net_shares, institutional_pct})。"""
    out = {"insider_buys": None, "insider_sells": None, "insider_net_shares": None, "institutional_pct": None}
    try:
        t = yf.Ticker(symbol)
        ip = t.insider_purchases
        if ip is not None and len(ip) > 0:
            # insider_purchases 結構：第 1 欄文字標籤，第 2 欄 Shares，第 3 欄 Trans
            label_col = ip.columns[0]
            for _, row in ip.iterrows():
                label = str(row[label_col]).strip()
                if "Purchases" == label or label == "Purchases":
                    out["insider_buys"] = int(row["Trans"]) if pd.notna(row.get("Trans")) else None
                elif "Sales" == label:
                    out["insider_sells"] = int(row["Trans"]) if pd.notna(row.get("Trans")) else None
                elif "Net Shares" in label and "Purchased" in label:
                    shares = row.get("Shares")
                    if pd.notna(shares):
                        out["insider_net_shares"] = float(shares)
        mh = t.major_holders
        if mh is not None and len(mh) > 0:
            # major_holders 的 index 是 label，value column is "Value"
            try:
                inst = mh.loc["institutionsPercentHeld", "Value"]
                out["institutional_pct"] = float(inst) * 100 if pd.notna(inst) else None
            except (KeyError, TypeError):
                pass
    except Exception as e:
        pass
    return symbol, out


def _fetch_symbol_earnings(symbol):
    """回傳 (symbol, {past: {...} or None, upcoming: {...} or None})。"""
    try:
        ed = yf.Ticker(symbol).earnings_dates
    except Exception as e:
        print(f"  [{symbol}] earnings err: {e}")
        return symbol, {"past": None, "upcoming": None}
    if ed is None or len(ed) == 0:
        return symbol, {"past": None, "upcoming": None}

    now_local = pd.Timestamp.now(tz="America/New_York")
    past_cutoff = now_local - pd.Timedelta(days=EARNINGS_PAST_DAYS + 1)
    upcoming_cutoff = now_local + pd.Timedelta(days=EARNINGS_UPCOMING_DAYS)

    past_row, upcoming_row = None, None
    for ts, row in ed.iterrows():
        try:
            ts = ts.tz_convert("America/New_York") if ts.tzinfo else ts.tz_localize("America/New_York")
        except Exception:
            continue
        entry = {
            "date": ts.strftime("%Y-%m-%d"),
            "eps_est": float(row["EPS Estimate"]) if pd.notna(row.get("EPS Estimate")) else None,
            "eps_actual": float(row["Reported EPS"]) if pd.notna(row.get("Reported EPS")) else None,
            "surprise_pct": float(row["Surprise(%)"]) if pd.notna(row.get("Surprise(%)")) else None,
        }
        is_past = entry["eps_actual"] is not None
        if is_past and past_cutoff <= ts <= now_local:
            if past_row is None or ts > pd.Timestamp(past_row["_ts"]):
                entry["_ts"] = ts.isoformat()
                past_row = entry
        elif not is_past and now_local < ts <= upcoming_cutoff:
            if upcoming_row is None or ts < pd.Timestamp(upcoming_row["_ts"]):
                entry["_ts"] = ts.isoformat()
                upcoming_row = entry
    for r in (past_row, upcoming_row):
        if r:
            r.pop("_ts", None)
    return symbol, {"past": past_row, "upcoming": upcoming_row}


def fetch_stock_enrichment(symbols):
    """並行抓新聞 + 財報 + 籌碼。回傳 (news_map, earn_map, chip_map)。"""
    symbols = sorted(set(symbols))
    if not symbols:
        return {}, {}, {}
    print(f"抓取 {len(symbols)} 檔個股的新聞／財報／籌碼...")
    news_map, earn_map, chip_map = {}, {}, {}
    with ThreadPoolExecutor(max_workers=NEWS_WORKERS) as ex:
        for sym, items in ex.map(_fetch_symbol_news, symbols):
            news_map[sym] = items
    with ThreadPoolExecutor(max_workers=NEWS_WORKERS) as ex:
        for sym, entry in ex.map(_fetch_symbol_earnings, symbols):
            earn_map[sym] = entry
    with ThreadPoolExecutor(max_workers=NEWS_WORKERS) as ex:
        for sym, entry in ex.map(_fetch_symbol_chip, symbols):
            chip_map[sym] = entry
    total_news = sum(len(v) for v in news_map.values())
    total_earn = sum(1 for v in earn_map.values() if v["past"] or v["upcoming"])
    total_chip = sum(1 for v in chip_map.values() if v.get("insider_buys") is not None)
    print(f"  新聞 {total_news} 則 · 財報 {total_earn} 檔 · 籌碼 {total_chip} 檔")
    return news_map, earn_map, chip_map


# 向下相容舊名稱
def fetch_news_and_earnings(symbols):
    n, e, _ = fetch_stock_enrichment(symbols)
    return n, e


def attach_enrichment(sectors, news_map, earn_map, chip_map):
    """把 新聞 / 財報 / 籌碼 掛到板塊、以及個別個股（gainers/losers/surges）上。"""
    def _attach_to_stock(c):
        sym = c["symbol"]
        c["news"] = news_map.get(sym, [])[:NEWS_PER_STOCK]
        c["chip"] = chip_map.get(sym, {}) or {}
        earn = earn_map.get(sym) or {}
        c["earnings"] = {"past": earn.get("past"), "upcoming": earn.get("upcoming")}

    for s in sectors:
        if s.get("status") != "ok":
            continue
        # 掛到 all_holdings、gainers、losers、surges（同 dict 會共用引用）
        for group in ("all_holdings", "gainers", "losers", "surges"):
            for c in s.get(group, []) or []:
                if "news" not in c:
                    _attach_to_stock(c)

        # 板塊層級彙整新聞（gainers + losers 的去重標題）
        top_syms = [c["symbol"] for c in (s.get("gainers", []) + s.get("losers", []))]
        seen_titles = set()
        sector_news = []
        for sym in top_syms:
            for item in news_map.get(sym, []):
                key = item["title"][:80]
                if key in seen_titles:
                    continue
                seen_titles.add(key)
                sector_news.append(item)
        sector_news.sort(key=lambda x: x["ts_epoch"], reverse=True)
        s["news"] = sector_news[:NEWS_PER_SECTOR]

        # 板塊層級的財報彙整
        earnings = []
        for c in s.get("all_holdings", []):
            e = earn_map.get(c["symbol"]) or {}
            if e.get("past") or e.get("upcoming"):
                earnings.append({
                    "symbol": c["symbol"],
                    "name": c["name"],
                    "weight": c["weight"],
                    "day_pct": c["day_pct"],
                    "past": e.get("past"),
                    "upcoming": e.get("upcoming"),
                })
        earnings.sort(key=lambda x: (0 if x["past"] else 1, -x["weight"]))
        s["earnings"] = earnings


# 舊呼叫點仍能運作
def attach_news_and_earnings(sectors, news_map, earn_map):
    attach_enrichment(sectors, news_map, earn_map, {})


# ── 歷史快照與趨勢 ──────────────────────────────────────────────────────────

def save_history_snapshot(sectors, market):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    # 精簡結構：只存趨勢運算需要的欄位
    slim_sectors = {
        s["code"]: {
            "day_pct": s.get("day_pct"),
            "pct_5d": s.get("pct_5d"),
            "pct_20d": s.get("pct_20d"),
            "vol_ratio": s.get("vol_ratio"),
            "close": s.get("close"),
            "above_ma20": s.get("above_ma20"),
            "above_ma50": s.get("above_ma50"),
            "above_ma200": s.get("above_ma200"),
        }
        for s in sectors if s.get("status") == "ok"
    }
    slim_market = {m["code"]: m.get("day_pct") for m in market if m.get("status") == "ok"}
    payload = {
        "trade_date": TODAY.isoformat(),
        "fetched_at": datetime.now().isoformat(),
        "sectors": slim_sectors,
        "market": slim_market,
    }
    path = os.path.join(HISTORY_DIR, f"{TODAY.isoformat()}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def cleanup_history(keep_days=MAX_HISTORY_DAYS):
    if not os.path.isdir(HISTORY_DIR):
        return
    cutoff = TODAY - timedelta(days=keep_days)
    for path in glob.glob(os.path.join(HISTORY_DIR, "*.json")):
        name = os.path.splitext(os.path.basename(path))[0]
        try:
            d = datetime.strptime(name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < cutoff:
            try:
                os.remove(path)
            except OSError:
                pass


def load_history(days=STREAK_LOOKBACK):
    """回傳近 N 天（含今日之前）的 snapshot list，依日期由舊到新。"""
    if not os.path.isdir(HISTORY_DIR):
        return []
    snaps = []
    for path in sorted(glob.glob(os.path.join(HISTORY_DIR, "*.json"))):
        try:
            with open(path, "r", encoding="utf-8") as f:
                snaps.append(json.load(f))
        except Exception:
            continue
    snaps.sort(key=lambda s: s.get("trade_date", ""))
    return snaps[-days:]


def compute_sector_trends(history, today_sectors):
    """從歷史 + 今日資料，為每個板塊算連續性訊號。"""
    today_map = {s["code"]: s for s in today_sectors if s.get("status") == "ok"}
    if not today_map:
        return {}

    # 組合成 [(date, {code: snapshot})] 序列（含今日）
    series = [(snap["trade_date"], snap["sectors"]) for snap in history if snap.get("trade_date") != TODAY.isoformat()]
    series.append((TODAY.isoformat(), {code: {
        "day_pct": s["day_pct"], "vol_ratio": s.get("vol_ratio"),
    } for code, s in today_map.items()}))

    trends = {}
    for code in today_map:
        up_streak, down_streak, surge_streak = 0, 0, 0
        top3_streak, bottom3_streak = 0, 0

        # 由最新往回數，遇到斷點即停
        for date_str, daily in reversed(series):
            snap = daily.get(code)
            if not snap or snap.get("day_pct") is None:
                break
            pct = snap["day_pct"]
            vr = snap.get("vol_ratio")
            if pct > 0:
                if down_streak == 0:
                    up_streak += 1
                else:
                    break
            elif pct < 0:
                if up_streak == 0:
                    down_streak += 1
                else:
                    break
            else:
                break

        # 單獨跑放量連續
        for date_str, daily in reversed(series):
            snap = daily.get(code)
            if not snap:
                break
            vr = snap.get("vol_ratio")
            if vr is not None and vr >= 1.2:
                surge_streak += 1
            else:
                break

        # top-3 / bottom-3 連續
        for date_str, daily in reversed(series):
            ranked = sorted(
                [(c, v.get("day_pct")) for c, v in daily.items() if v.get("day_pct") is not None],
                key=lambda x: x[1], reverse=True,
            )
            codes_ranked = [c for c, _ in ranked]
            if code in codes_ranked[:3]:
                if bottom3_streak == 0:
                    top3_streak += 1
                else:
                    break
            elif code in codes_ranked[-3:]:
                if top3_streak == 0:
                    bottom3_streak += 1
                else:
                    break
            else:
                break

        # 5 日累積漲幅（從歷史推算，若不足則 None）
        cum_5d = None
        if len(series) >= 5:
            tail = series[-5:]
            vals = []
            for _, daily in tail:
                snap = daily.get(code) or {}
                if snap.get("day_pct") is None:
                    vals = None
                    break
                vals.append(snap["day_pct"])
            if vals:
                cum_5d = sum(vals)

        trends[code] = {
            "up_streak": up_streak,
            "down_streak": down_streak,
            "surge_streak": surge_streak,
            "top3_streak": top3_streak,
            "bottom3_streak": bottom3_streak,
            "cum_5d": cum_5d,
        }
    return trends


# ── HTML 渲染 ───────────────────────────────────────────────────────────────

CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b;--card:#fff;--hover:#eef2ff}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:14px;line-height:1.5}
.hdr{background:linear-gradient(135deg,#0f172a 0%,#1e3a8a 50%,#2563eb 100%);color:#fff;padding:22px 28px 16px}
.hdr h1{font-size:20px;font-weight:800;margin-bottom:4px;letter-spacing:-0.3px}
.hdr .sub{font-size:11.5px;opacity:.85}
.nav-link{color:#93c5fd;font-size:11.5px;text-decoration:none;margin-left:14px;border-bottom:1px dashed #93c5fd;padding-bottom:1px}
.nav-link:hover{opacity:.7}
.market{display:flex;gap:10px;padding:14px 28px;background:var(--card);border-bottom:1px solid var(--brd);flex-wrap:wrap}
.mk{background:var(--bg);border:1px solid var(--brd);border-radius:8px;padding:10px 16px;min-width:140px}
.mk .lbl{font-size:10.5px;color:var(--mu);font-weight:600;letter-spacing:0.3px}
.mk .px{font-size:18px;font-weight:700;margin-top:2px}
.mk .chg{font-size:12px;font-weight:600;margin-top:1px}
.chg.up{color:var(--gr)}.chg.dn{color:var(--rd)}.chg.nu{color:var(--mu)}
.construction{background:linear-gradient(90deg,#fef3c7,#fde68a);border-bottom:2px solid #f59e0b;color:#78350f;padding:10px 28px;font-size:12.5px;font-weight:600;text-align:center}
.pane{padding:20px 28px}
.ttl{font-size:15px;font-weight:700;margin-bottom:4px}
.desc{font-size:12.5px;color:var(--mu);margin-bottom:16px;line-height:1.7}
.heat{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px}
.sector-card{border-radius:10px;padding:14px 16px;cursor:pointer;color:#fff;position:relative;transition:transform .12s,box-shadow .12s;border:1px solid rgba(0,0,0,.08)}
.sector-card:hover{transform:translateY(-2px);box-shadow:0 6px 18px rgba(0,0,0,.14)}
.sector-card.active{outline:3px solid #111;outline-offset:2px}
.sector-card .code{font-size:11px;font-weight:700;opacity:0.85;letter-spacing:0.4px}
.sector-card .name{font-size:15px;font-weight:800;margin-top:2px}
.sector-card .chg{font-size:22px;font-weight:800;margin-top:8px;letter-spacing:-0.5px}
.sector-card .sub{font-size:10.5px;opacity:.85;margin-top:3px}
.drill{margin-top:20px;padding:18px 20px;background:var(--card);border:1px solid var(--brd);border-radius:12px;display:none}
.drill.active{display:block}
.drill h2{font-size:17px;font-weight:800;margin-bottom:4px}
.drill .meta{font-size:12px;color:var(--mu);margin-bottom:14px;line-height:1.7}
.grid-4{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px}
.col h4{font-size:12px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;padding-bottom:6px;border-bottom:2px solid var(--brd)}
.col.pos h4{color:var(--gr);border-bottom-color:#bbf7d0}
.col.neg h4{color:var(--rd);border-bottom-color:#fecaca}
.col.vol h4{color:var(--am);border-bottom-color:#fed7aa}
.col.tech h4{color:var(--bl);border-bottom-color:#bfdbfe}
.row-item{display:flex;align-items:center;justify-content:space-between;padding:6px 0;border-bottom:1px dashed #eef2f7;font-size:12.5px}
.row-item:last-child{border-bottom:none}
.row-item .sym{font-weight:700;color:var(--txt);min-width:46px}
.row-item .nm{flex:1;color:var(--mu);font-size:11.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin:0 8px}
.row-item .val{font-weight:700;font-variant-numeric:tabular-nums;min-width:52px;text-align:right}
.val.up{color:var(--gr)}.val.dn{color:var(--rd)}.val.am{color:var(--am)}
.tech-row{font-size:12px;padding:4px 0;display:flex;justify-content:space-between}
.tech-row .k{color:var(--mu)}
.tech-row .v{font-weight:600}
.pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10.5px;font-weight:700}
.pill.up{background:#dcfce7;color:#15803d}
.pill.dn{background:#fee2e2;color:#dc2626}
.pill.am{background:#fef3c7;color:#b45309}
.pill.mu{background:#e2e8f0;color:#475569}
.ft{text-align:center;color:var(--mu);font-size:11.5px;padding:20px 28px;border-top:1px solid var(--brd);line-height:1.8;background:var(--card)}
.trends{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-bottom:18px}
.trend-box{background:var(--card);border:1px solid var(--brd);border-left:3px solid var(--bl);border-radius:10px;padding:12px 16px}
.trend-box.pos{border-left-color:var(--gr)}
.trend-box.neg{border-left-color:var(--rd)}
.trend-box.warn{border-left-color:var(--am)}
.trend-box h5{font-size:11.5px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:0.4px;margin-bottom:6px}
.trend-box .body{font-size:13px;line-height:1.8}
.trend-tag{display:inline-block;background:#eef2ff;color:#1e3a8a;border:1px solid #c7d2fe;padding:2px 8px;border-radius:10px;font-size:11.5px;font-weight:700;margin:2px 4px 2px 0}
.trend-tag.pos{background:#dcfce7;color:#15803d;border-color:#bbf7d0}
.trend-tag.neg{background:#fee2e2;color:#b91c1c;border-color:#fecaca}
.trend-tag.warn{background:#fef3c7;color:#92400e;border-color:#fde68a}
.news-section{margin-top:18px;padding-top:14px;border-top:1px dashed var(--brd)}
.news-section h4{font-size:12px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px}
.news-item{display:flex;gap:10px;align-items:flex-start;padding:8px 0;border-bottom:1px dashed #eef2f7;font-size:13px}
.news-item:last-child{border-bottom:none}
.news-item .sym-pill{flex-shrink:0;font-weight:700;font-size:11px;color:var(--bl);background:#dbeafe;padding:2px 7px;border-radius:4px;min-width:46px;text-align:center}
.news-item .meta{font-size:11px;color:var(--mu);margin-top:2px}
.news-item a{color:var(--txt);text-decoration:none;line-height:1.5}
.news-item a:hover{color:var(--bl);text-decoration:underline}
.earn-section{margin-top:14px;padding-top:12px;border-top:1px dashed var(--brd)}
.earn-section h4{font-size:12px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px}
.earn-row{display:flex;align-items:center;gap:10px;padding:6px 0;font-size:12.5px;border-bottom:1px dashed #eef2f7}
.earn-row:last-child{border-bottom:none}
.earn-row .sym{font-weight:700;min-width:52px}
.earn-row .nm{flex:1;color:var(--mu);font-size:11.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.earn-row .pill-past.beat{background:#dcfce7;color:#15803d}
.earn-row .pill-past.miss{background:#fee2e2;color:#b91c1c}
.earn-row .pill-past.inline{background:#e2e8f0;color:#475569}
.earn-row .pill-up{background:#e0e7ff;color:#3730a3}
details.stock-row{border-bottom:1px dashed #eef2f7;margin:0}
details.stock-row:last-child{border-bottom:none}
details.stock-row > summary{list-style:none;cursor:pointer;padding:7px 0;display:flex;align-items:center;justify-content:space-between;font-size:12.5px;transition:background .1s}
details.stock-row > summary::-webkit-details-marker{display:none}
details.stock-row > summary:hover{background:#f8fafc}
details.stock-row > summary .chev{flex:0 0 auto;color:var(--mu);font-size:10px;transition:transform .15s;margin-left:4px}
details.stock-row[open] > summary .chev{transform:rotate(180deg)}
.stock-detail{background:#f8fafc;border-left:3px solid var(--bl);border-radius:0 6px 6px 0;margin:2px 0 10px 6px;padding:10px 12px}
.stock-detail .sd-block{margin-bottom:10px}
.stock-detail .sd-block:last-child{margin-bottom:0}
.stock-detail h5{font-size:11px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:0.3px;margin-bottom:6px;display:flex;align-items:center;gap:6px}
.stock-detail .sd-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:4px 10px;font-size:11.5px}
.stock-detail .sd-grid > div{display:flex;justify-content:space-between;gap:6px;padding:2px 0}
.stock-detail .sd-grid .k{color:var(--mu)}
.stock-detail .sd-grid .v{font-weight:600;font-variant-numeric:tabular-nums}
.stock-detail .sd-grid .v.up{color:var(--gr)}
.stock-detail .sd-grid .v.dn{color:var(--rd)}
.stock-detail .sd-grid .v.am{color:var(--am)}
.stock-detail .sd-grid .v.mu{color:var(--txt)}
.stock-detail .sd-ma{margin-top:6px;display:flex;flex-wrap:wrap;gap:3px}
.stock-detail .sd-note{font-size:10.5px;color:var(--mu);margin-top:4px;font-style:italic}
.stock-detail .sd-news-item{padding:5px 0;border-bottom:1px dashed #e2e8f0;font-size:11.5px;line-height:1.45}
.stock-detail .sd-news-item:last-child{border-bottom:none}
.stock-detail .sd-news-item a{color:var(--txt);text-decoration:none}
.stock-detail .sd-news-item a:hover{color:var(--bl);text-decoration:underline}
.stock-detail .sd-news-meta{font-size:10.5px;color:var(--mu);margin-top:1px}
.empty-msg{color:var(--mu);font-size:13px;padding:16px 0;text-align:center}
@media(max-width:768px){.hdr,.market,.pane{padding-left:16px;padding-right:16px}.heat{grid-template-columns:repeat(2,1fr)}}
"""

JS = """
function selectSector(code){
  document.querySelectorAll('.sector-card').forEach(function(c){
    c.classList.toggle('active', c.dataset.code===code);
  });
  document.querySelectorAll('.drill').forEach(function(d){
    d.classList.toggle('active', d.dataset.code===code);
  });
  var drill = document.querySelector('.drill.active');
  if(drill) drill.scrollIntoView({behavior:'smooth', block:'start'});
}
"""


def _heat_color(pct):
    """日漲跌 → 熱力圖顏色（綠紅漸層）。pct 絕對值 >= 2.5 為最深。"""
    if pct is None:
        return "#94a3b8"  # 灰
    clamped = max(-2.5, min(2.5, pct))
    intensity = abs(clamped) / 2.5  # 0~1
    if pct >= 0:
        # 綠色系: #dcfce7 → #14532d
        r = int(220 - 200 * intensity)
        g = int(252 - 170 * intensity)
        b = int(231 - 186 * intensity)
    else:
        # 紅色系: #fee2e2 → #7f1d1d
        r = int(254 - 127 * intensity)
        g = int(226 - 197 * intensity)
        b = int(226 - 197 * intensity)
    return f"rgb({r},{g},{b})"


def _fmt_pct(v, show_plus=True):
    if v is None:
        return "─"
    sign = "+" if v > 0 and show_plus else ""
    return f"{sign}{v:.2f}%"


def _fmt_price(v):
    if v is None:
        return "─"
    return f"${v:,.2f}"


def _pct_cls(v):
    if v is None:
        return "nu"
    return "up" if v > 0 else ("dn" if v < 0 else "nu")


def _gen_market_row(market):
    html = ""
    for m in market:
        if m["status"] != "ok":
            html += f'<div class="mk"><div class="lbl">{m["label"]}</div><div class="px">─</div></div>'
            continue
        chg_cls = _pct_cls(m["day_pct"])
        html += f'''<div class="mk">
  <div class="lbl">{m["label"]} ({m["code"]})</div>
  <div class="px">{_fmt_price(m["close"])}</div>
  <div class="chg {chg_cls}">{_fmt_pct(m["day_pct"])}</div>
</div>'''
    return html


def _gen_sector_cards(sectors):
    # 依日漲跌排序（大到小）
    sorted_secs = sorted(sectors, key=lambda s: s.get("day_pct", 0) or 0, reverse=True)
    html = '<div class="heat">'
    for s in sorted_secs:
        if s["status"] != "ok":
            html += f'<div class="sector-card" style="background:#94a3b8"><div class="code">{s["code"]}</div><div class="name">{s["name_zh"]}</div><div class="chg">─</div></div>'
            continue
        bg = _heat_color(s["day_pct"])
        text_color = "#0f172a" if abs(s["day_pct"]) < 1.2 else "#fff"
        sub_bits = []
        if s.get("vol_ratio"):
            sub_bits.append(f'量×{s["vol_ratio"]:.2f}')
        if s.get("above_ma50") is not None:
            sub_bits.append("↑MA50" if s["above_ma50"] else "↓MA50")
        sub = " · ".join(sub_bits)
        html += f'''<div class="sector-card" data-code="{s["code"]}" style="background:{bg};color:{text_color}" onclick="selectSector('{s["code"]}')">
  <div class="code">{s["code"]}</div>
  <div class="name">{s["name_zh"]}</div>
  <div class="chg">{_fmt_pct(s["day_pct"])}</div>
  <div class="sub">{sub}</div>
</div>'''
    html += '</div>'
    return html


def _rsi_cls(rsi):
    if rsi is None:
        return "mu"
    if rsi >= 70:
        return "dn"   # 超買（紅）
    if rsi <= 30:
        return "up"   # 超賣（綠）
    return "mu"


def _rsi_label(rsi):
    if rsi is None:
        return "─"
    tag = "超買" if rsi >= 70 else ("超賣" if rsi <= 30 else "中性")
    return f"{rsi:.0f} {tag}"


def _gen_stock_detail(c):
    """個股展開後的詳情面板：技術面 + 籌碼 + 新聞。"""
    t = c.get("tech", {})
    chip = c.get("chip", {}) or {}
    news = c.get("news", []) or []
    earn = c.get("earnings", {}) or {}

    # 技術面
    def ma_pill(label, above, val):
        if above is None or val is None:
            return f'<span class="pill mu">{label} ─</span>'
        return f'<span class="pill {"up" if above else "dn"}">{label} {"↑" if above else "↓"} {_fmt_price(val)}</span>'

    vr_yday = t.get("vol_vs_yday")
    vr_yday_cls = "am" if (vr_yday and vr_yday >= STOCK_VOL_SURGE_THRESHOLD) else "mu"
    vr_yday_str = f"×{vr_yday:.2f}" if vr_yday else "─"

    vr_20 = t.get("vol_ratio")
    vr_20_str = f"×{vr_20:.2f}" if vr_20 else "─"

    rsi = t.get("rsi")
    tech_html = f'''<div class="sd-block">
  <h5>📐 技術面</h5>
  <div class="sd-grid">
    <div><span class="k">近 5 日</span><span class="v {_pct_cls(t.get("pct_5d"))}">{_fmt_pct(t.get("pct_5d"))}</span></div>
    <div><span class="k">近 20 日</span><span class="v {_pct_cls(t.get("pct_20d"))}">{_fmt_pct(t.get("pct_20d"))}</span></div>
    <div><span class="k">量 / 昨日</span><span class="v {vr_yday_cls}">{vr_yday_str}</span></div>
    <div><span class="k">量 / 20 日均</span><span class="v mu">{vr_20_str}</span></div>
    <div><span class="k">RSI(14)</span><span class="v {_rsi_cls(rsi)}">{_rsi_label(rsi)}</span></div>
  </div>
  <div class="sd-ma">{ma_pill("MA20", t.get("above_ma20"), t.get("ma20"))} {ma_pill("MA50", t.get("above_ma50"), t.get("ma50"))} {ma_pill("MA200", t.get("above_ma200"), t.get("ma200"))}</div>
</div>'''

    # 籌碼
    ib = chip.get("insider_buys")
    isell = chip.get("insider_sells")
    inet = chip.get("insider_net_shares")
    inst = chip.get("institutional_pct")

    if ib is None and isell is None and inst is None:
        chip_body = '<div class="empty-msg" style="font-size:11.5px;padding:6px 0">籌碼資料暫無</div>'
    else:
        net_str = "─"
        net_cls = "mu"
        if inet is not None:
            if inet > 0:
                net_str = f"淨買進 {abs(int(inet)):,} 股"
                net_cls = "up"
            elif inet < 0:
                net_str = f"淨賣出 {abs(int(inet)):,} 股"
                net_cls = "dn"
            else:
                net_str = "持平"
        chip_body = f'''<div class="sd-grid">
  <div><span class="k">內部人買</span><span class="v up">{ib if ib is not None else "─"} 筆</span></div>
  <div><span class="k">內部人賣</span><span class="v dn">{isell if isell is not None else "─"} 筆</span></div>
  <div><span class="k">內部人淨動</span><span class="v {net_cls}">{net_str}</span></div>
  <div><span class="k">機構持股</span><span class="v mu">{f"{inst:.1f}%" if inst is not None else "─"}</span></div>
</div>
<div class="sd-note">近 6 個月內部人申報（SEC Form 4）</div>'''

    # 財報徽章
    earn_html = ""
    if earn.get("past"):
        p = earn["past"]
        sur = p.get("surprise_pct")
        if sur is not None:
            cls = "up" if sur >= 0 else "dn"
            earn_html = f'<span class="pill {cls}">財報 {p["date"]} EPS {"+" if sur>=0 else ""}{sur:.1f}%</span>'
        else:
            earn_html = f'<span class="pill mu">財報 {p["date"]}</span>'
    elif earn.get("upcoming"):
        u = earn["upcoming"]
        earn_html = f'<span class="pill" style="background:#e0e7ff;color:#3730a3">📅 即將公布 {u["date"]}</span>'

    chip_block = f'''<div class="sd-block">
  <h5>📊 籌碼 {earn_html}</h5>
  {chip_body}
</div>'''

    # 新聞
    if not news:
        news_body = '<div class="empty-msg" style="font-size:11.5px;padding:6px 0">48 小時內無相關新聞</div>'
    else:
        items = []
        for n in news:
            age = _fmt_news_age(n.get("timestamp"))
            provider = n.get("provider") or ""
            meta = " · ".join([b for b in (provider, age) if b])
            url = n.get("url") or "#"
            link_open = f'<a href="{url}" target="_blank" rel="noopener">' if url != "#" else "<span>"
            link_close = "</a>" if url != "#" else "</span>"
            items.append(f'<div class="sd-news-item">{link_open}{n["title"]}{link_close}<div class="sd-news-meta">{meta}</div></div>')
        news_body = "".join(items)

    news_block = f'''<div class="sd-block">
  <h5>📰 新聞</h5>
  {news_body}
</div>'''

    return f'<div class="stock-detail">{tech_html}{chip_block}{news_block}</div>'


def _gen_stock_row(c, mode="contrib"):
    """以 <details> 產生可展開的個股列。mode: 'contrib' | 'surge'"""
    cls = "up" if c["day_pct"] >= 0 else "dn"
    weight_pct = c["weight"] * 100
    vr_yday = c.get("tech", {}).get("vol_vs_yday")
    vol_tag = ""
    if vr_yday and vr_yday >= STOCK_VOL_SURGE_THRESHOLD:
        vol_tag = f' <span class="pill am" title="量 ÷ 昨日 = {vr_yday:.2f}">放量</span>'

    if mode == "surge":
        vr_txt = f"×{vr_yday:.2f}" if vr_yday else "─"
        right = f'<span class="val am">{vr_txt}</span><span class="val {cls}" style="min-width:52px">{_fmt_pct(c["day_pct"])}</span>'
    else:
        right = f'<span class="val {cls}">{_fmt_pct(c["day_pct"])}</span><span class="val mu" style="color:var(--mu);font-weight:500;min-width:42px" title="權重 {weight_pct:.2f}%">{weight_pct:.1f}%</span>'

    summary = f'''<summary class="row-item">
  <span class="sym">{c["symbol"]}</span>
  <span class="nm" title="{c["name"]}">{c["name"]}{vol_tag}</span>
  {right}
  <span class="chev">▾</span>
</summary>'''
    return f'<details class="stock-row">{summary}{_gen_stock_detail(c)}</details>'


def _gen_contrib_list(items, is_positive):
    if not items:
        msg = "今日無拖累個股（所有成分股皆上漲）" if not is_positive else "無資料"
        return f'<div class="empty-msg" style="font-size:12px">{msg}</div>'
    return "".join(_gen_stock_row(c, "contrib") for c in items)


def _gen_surge_list(items):
    if not items:
        return '<div class="empty-msg" style="font-size:12px">今日無明顯放量個股（門檻：量 ≥ 昨日 ×1.2）</div>'
    return "".join(_gen_stock_row(c, "surge") for c in items)


def _gen_tech_panel(s):
    """技術面小面板：均線位置 + 近期表現。"""
    def ma_pill(label, above, val):
        if above is None or val is None:
            return f'<span class="pill mu">{label} ─</span>'
        return f'<span class="pill {"up" if above else "dn"}">{label} {"↑" if above else "↓"} {_fmt_price(val)}</span>'

    rows = []
    rows.append(f'<div class="tech-row"><span class="k">收盤</span><span class="v">{_fmt_price(s.get("close"))}</span></div>')
    rows.append(f'<div class="tech-row"><span class="k">近 5 日</span><span class="v {_pct_cls(s.get("pct_5d"))}">{_fmt_pct(s.get("pct_5d"))}</span></div>')
    rows.append(f'<div class="tech-row"><span class="k">近 20 日</span><span class="v {_pct_cls(s.get("pct_20d"))}">{_fmt_pct(s.get("pct_20d"))}</span></div>')
    vr = s.get("vol_ratio")
    vr_cls = "am" if (vr and vr >= VOLUME_SURGE_THRESHOLD) else "mu"
    vr_str = f"×{vr:.2f}" if vr else "─"
    rows.append(f'<div class="tech-row"><span class="k">量 / 20 日均</span><span class="v {vr_cls}">{vr_str}</span></div>')
    rows.append(f'<div class="tech-row"><span class="k">均線位置</span><span class="v">{ma_pill("MA20", s.get("above_ma20"), s.get("ma20"))} {ma_pill("MA50", s.get("above_ma50"), s.get("ma50"))} {ma_pill("MA200", s.get("above_ma200"), s.get("ma200"))}</span></div>')
    return "".join(rows)


def _fmt_news_age(ts_iso):
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


def _gen_news_panel(news):
    if not news:
        return ""
    html = '<div class="news-section"><h4>📰 相關新聞（Top 貢獻股，48 小時內）</h4>'
    for n in news:
        age = _fmt_news_age(n.get("timestamp"))
        provider = n.get("provider") or ""
        meta_bits = [b for b in (provider, age) if b]
        meta = " · ".join(meta_bits)
        url = n.get("url") or "#"
        link_open = f'<a href="{url}" target="_blank" rel="noopener">' if url != "#" else "<span>"
        link_close = "</a>" if url != "#" else "</span>"
        html += f'''<div class="news-item">
  <span class="sym-pill">{n["symbol"]}</span>
  <div style="flex:1;min-width:0">
    {link_open}{n["title"]}{link_close}
    <div class="meta">{meta}</div>
  </div>
</div>'''
    html += '</div>'
    return html


def _gen_earnings_panel(earnings):
    if not earnings:
        return ""
    html = '<div class="earn-section"><h4>💰 本週財報（過去 3 日 ／ 未來 7 日）</h4>'
    for e in earnings:
        sym = e["symbol"]
        name = e["name"]
        weight_pct = e["weight"] * 100
        day_cls = _pct_cls(e["day_pct"])
        day_str = _fmt_pct(e["day_pct"])
        past = e.get("past")
        up = e.get("upcoming")
        pills = ""
        if past:
            sur = past.get("surprise_pct")
            act = past.get("eps_actual")
            est = past.get("eps_est")
            if sur is not None and est is not None:
                cls = "beat" if sur >= 0 else "miss"
                label = f'已公布 {past["date"]}：EPS ${act:.2f} vs 預期 ${est:.2f}（{"+" if sur>=0 else ""}{sur:.1f}%）'
            elif act is not None:
                cls = "inline"
                label = f'已公布 {past["date"]}：EPS ${act:.2f}'
            else:
                cls = "inline"
                label = f'已公布 {past["date"]}'
            pills += f'<span class="pill pill-past {cls}" style="padding:3px 9px;border-radius:6px;font-size:11px;font-weight:700">{label}</span>'
        if up:
            est = up.get("eps_est")
            est_str = f'（預期 EPS ${est:.2f}）' if est is not None else ""
            pills += f'<span class="pill pill-up" style="padding:3px 9px;border-radius:6px;font-size:11px;font-weight:700;margin-left:6px">即將公布 {up["date"]}{est_str}</span>'
        html += f'''<div class="earn-row">
  <span class="sym">{sym}</span>
  <span class="nm" title="{name}">{name} · 權重 {weight_pct:.1f}%</span>
  <span class="val {day_cls}" style="min-width:60px;text-align:right">{day_str}</span>
  <span style="flex:0 0 auto">{pills}</span>
</div>'''
    html += '</div>'
    return html


def _gen_drill_panels(sectors):
    html = ""
    for s in sectors:
        if s["status"] != "ok":
            html += f'<div class="drill" data-code="{s["code"]}"><h2>{s["code"]} {s["name_zh"]}</h2><div class="empty-msg">資料抓取失敗</div></div>'
            continue
        pct_cls = _pct_cls(s["day_pct"])
        news_html = _gen_news_panel(s.get("news", []))
        earn_html = _gen_earnings_panel(s.get("earnings", []))
        html += f'''<div class="drill" data-code="{s["code"]}">
  <h2>{s["code"]} {s["name_zh"]} <span class="val {pct_cls}" style="font-size:18px;margin-left:8px">{_fmt_pct(s["day_pct"])}</span></h2>
  <div class="meta">{s["name_en"]} · 收盤 {_fmt_price(s["close"])} · 追蹤 Top {s["holdings_count"]} 成分股貢獻度</div>
  <div class="grid-4">
    <div class="col pos">
      <h4>▲ 今日推升</h4>
      {_gen_contrib_list(s["gainers"], True)}
    </div>
    <div class="col neg">
      <h4>▼ 今日拖累</h4>
      {_gen_contrib_list(s["losers"], False)}
    </div>
    <div class="col vol">
      <h4>⚡ 放量個股</h4>
      {_gen_surge_list(s["surges"])}
    </div>
    <div class="col tech">
      <h4>📐 技術面</h4>
      {_gen_tech_panel(s)}
    </div>
  </div>
  {earn_html}
  {news_html}
</div>'''
    return html


SECTOR_NAMES = {code: zh for code, zh, _ in SECTORS}


def _gen_trends_panel(trends, sectors):
    """頂部趨勢摘要：連續領漲/領跌、放量、反轉。"""
    if not trends:
        return ''
    today_map = {s["code"]: s for s in sectors if s.get("status") == "ok"}

    # 連續領漲（top3_streak ≥ 2）
    leaders = sorted(
        [(code, t["top3_streak"], t.get("cum_5d"))
         for code, t in trends.items() if t["top3_streak"] >= 2],
        key=lambda x: -x[1],
    )
    laggards = sorted(
        [(code, t["bottom3_streak"], t.get("cum_5d"))
         for code, t in trends.items() if t["bottom3_streak"] >= 2],
        key=lambda x: -x[1],
    )
    surges = sorted(
        [(code, t["surge_streak"]) for code, t in trends.items() if t["surge_streak"] >= 2],
        key=lambda x: -x[1],
    )
    # 反轉：連續漲 ≥ 3 今天轉跌，或連續跌 ≥ 3 今天轉漲
    reversals = []
    for code, t in trends.items():
        today_pct = today_map.get(code, {}).get("day_pct", 0) or 0
        if t["up_streak"] == 1 and t["down_streak"] == 0:
            # 今日只漲 1 天（代表前一天之前在跌）
            pass
        # 用連續 <= 今日判斷有點噁心，改用歷史前一天
    # 簡化版：拿 streak > 1 且今日反向的情況（前一天仍同向）
    # 我們已經讓 up_streak 和 down_streak 互斥，所以直接看有沒有 streak == 1 + 先前長 streak
    # 省事起見：只在有 >= 5 天歷史時才顯示，否則留白
    # （後續再迭代）

    def _tag(code, n, cls):
        zh = SECTOR_NAMES.get(code, code)
        return f'<span class="trend-tag {cls}">{code} {zh} {n}天</span>'

    boxes = []
    if leaders:
        body = "".join(_tag(c, n, "pos") for c, n, _ in leaders[:5])
        boxes.append(f'<div class="trend-box pos"><h5>🔥 連續領漲板塊</h5><div class="body">{body}</div></div>')
    if laggards:
        body = "".join(_tag(c, n, "neg") for c, n, _ in laggards[:5])
        boxes.append(f'<div class="trend-box neg"><h5>❄️ 連續領跌板塊</h5><div class="body">{body}</div></div>')
    if surges:
        body = "".join(_tag(c, n, "warn") for c, n in surges[:5])
        boxes.append(f'<div class="trend-box warn"><h5>⚡ 連續放量板塊（≥1.2×）</h5><div class="body">{body}</div></div>')

    if not boxes:
        return '<div class="trend-box" style="border-left-color:var(--mu)"><h5>📅 趨勢追蹤</h5><div class="body" style="color:var(--mu);font-size:12.5px">系統正在累積歷史資料，3 個交易日後即可呈現連續性訊號（領漲／領跌／放量）</div></div>'
    return f'<div class="trends">{"".join(boxes)}</div>'


def generate_html(sectors, market, trends):
    market_row = _gen_market_row(market)
    heatmap = _gen_sector_cards(sectors)
    drill = _gen_drill_panels(sectors)
    trend_panel = _gen_trends_panel(trends, sectors)

    return f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>美股板塊復盤 — {TODAY.isoformat()}</title>
<meta name="robots" content="noindex, nofollow"/>
<meta name="referrer" content="no-referrer"/>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>美股板塊復盤</h1>
  <div class="sub">更新：{TODAY.isoformat()}（每個交易日收盤後自動更新）<a class="nav-link" href="etf_index.html">→ ETF 資金流向</a><a class="nav-link" href="index.html">→ 可轉債儀表板</a></div>
</div>
<div class="construction">🚧 開發測試中｜新聞／業績／趨勢資料正在累積，連續性訊號需數日歷史 🚧</div>
<div class="market">{market_row}</div>
<div class="pane">
  {trend_panel}
  <div class="ttl">板塊熱力圖</div>
  <div class="desc">11 檔 SPDR 行業 ETF 當日漲跌排序。顏色越深表示漲/跌幅越大。點擊任一板塊查看「為什麼」——包含推升股、拖累股、放量個股、技術面、本週財報、相關新聞</div>
  {heatmap}
  {drill}
</div>
<div class="ft">資料來源：Yahoo Finance（yfinance）｜ 僅供研究參考，不構成投資建議 ｜ 美股板塊復盤監測</div>
<script>{JS}</script>
</body></html>"""


# ── 持久化（快取今日資料以便 debug） ────────────────────────────────────────

def save_cache(sectors, market):
    # 清理成 json-safe
    def clean(o):
        if isinstance(o, dict):
            return {k: clean(v) for k, v in o.items() if k != "all_holdings"}
        if isinstance(o, list):
            return [clean(x) for x in o]
        return o
    payload = {
        "fetched_at": datetime.now().isoformat(),
        "trade_date": TODAY.isoformat(),
        "sectors": clean(sectors),
        "market": clean(market),
    }
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print(f"=== 美股板塊復盤 ({TODAY.isoformat()}) ===")
    print(f"抓取 {len(SECTORS)} 個板塊 + {len(MARKET_TICKERS)} 個大盤指標的 K 線...")
    bars_df = fetch_sector_and_market_bars()
    print("K 線資料完成\n")

    sectors = []
    for code, zh, en in SECTORS:
        print(f"處理 {code} {zh}...")
        report = build_sector_report(code, zh, en, bars_df)
        if report["status"] == "ok":
            print(f"  {_fmt_pct(report['day_pct'])} · 量×{report.get('vol_ratio') or 0:.2f} · {report['holdings_count']} 檔成分股")
        else:
            print(f"  失敗")
        sectors.append(report)

    market = build_market_report(bars_df)

    # 蒐集所有需要新聞/財報的個股（top gainers + losers + full top-10 for earnings）
    enrich_syms = set()
    for s in sectors:
        if s.get("status") != "ok":
            continue
        for c in s.get("gainers", []) + s.get("losers", []):
            enrich_syms.add(c["symbol"])
        for c in s.get("all_holdings", []):
            enrich_syms.add(c["symbol"])
    news_map, earn_map, chip_map = fetch_stock_enrichment(enrich_syms)
    attach_enrichment(sectors, news_map, earn_map, chip_map)

    # 持久化今日 snapshot，計算趨勢
    save_history_snapshot(sectors, market)
    cleanup_history()
    history = load_history()
    trends = compute_sector_trends(history, sectors)
    print(f"歷史快照 {len(history)} 天，趨勢計算完成")

    html = generate_html(sectors, market, trends)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n儀表板輸出至 {OUTPUT_HTML}")

    save_cache(sectors, market)
    print(f"快取輸出至 {CACHE_FILE}")

    ok_count = sum(1 for s in sectors if s["status"] == "ok")
    if ok_count == 0:
        print("::error::所有板塊抓取失敗", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
