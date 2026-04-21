#!/usr/bin/env python3
"""
台股每日大盤復盤儀表板
每個交易日台股收盤後執行（14:30 後），產出單頁 HTML 供 WordPress 嵌入。

資料來源：
  - yfinance        → TAIEX、OTC、台股代表 ETF
  - CNN dataviz API → 貪婪與恐懼指數 (Fear & Greed)
  - FRED CSV        → 隔夜逆回購 RRP (RRPONTSYD)
  - TWSE openAPI    → 漲跌家數（可選）
"""
import csv
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd
import requests
import yfinance as yf

# ─── 設定 ───────────────────────────────────────────────────────────────────

TODAY = datetime.now().date()
NOW_UTC = datetime.now(timezone.utc)
OUTPUT_HTML = "tw_market.html"
CACHE_FILE = "tw_market_latest.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# 大盤指數 (yfinance tickers)
# ^TPKE 已從 yfinance 下架，暫以 TWO 代替（有時也無法取得，gracefully skip）
TW_MARKET_TICKERS = [
    ("^TWII",   "加權指數",   "TAIEX"),
]

# 台股代表 ETF（板塊代理）
TW_ETFS = [
    ("0050.TW",   "元大台灣50",       "大盤"),
    ("006208.TW", "富邦台50",         "大盤"),
    ("00891.TW",  "中信關鍵半導體",   "半導體"),
    ("00892.TW",  "富邦台灣半導體",   "半導體"),
    ("00881.TW",  "國泰台灣5G+",      "通訊"),
    ("00893.TW",  "國泰智能電動車",   "電動車"),
    ("00895.TW",  "富邦未來車",       "電動車"),
    ("00900.TW",  "富邦特選高股息",   "高息"),
    ("00919.TW",  "群益台灣精選高息", "高息"),
]

RRP_HISTORY_DAYS = 90   # 顯示最近幾天的 RRP 折線圖


# ─── 資料抓取 ────────────────────────────────────────────────────────────────

def fetch_market_indices():
    """抓 TAIEX 近 5 日資料，算今日漲跌。每個 ticker 單獨抓以避免多層欄問題。"""
    results = []
    for code, zh, en in TW_MARKET_TICKERS:
        try:
            bars = yf.download(
                code, period="5d", interval="1d",
                auto_adjust=False, progress=False,
            )
            # 若欄位是 MultiIndex，壓平取值
            if isinstance(bars.columns, pd.MultiIndex):
                bars.columns = bars.columns.get_level_values(0)
            bars = bars.dropna(subset=["Close"])
            if len(bars) < 2:
                results.append({"code": code, "zh": zh, "en": en, "status": "error"})
                continue
            close  = float(bars["Close"].iloc[-1])
            prev   = float(bars["Close"].iloc[-2])
            volume = float(bars["Volume"].iloc[-1]) if not pd.isna(bars["Volume"].iloc[-1]) else None
            results.append({
                "code":    code,
                "zh":      zh,
                "en":      en,
                "status":  "ok",
                "close":   close,
                "day_pct": (close / prev - 1) * 100,
                "day_chg": close - prev,
                "volume":  volume,
                "date":    bars.index[-1].date().isoformat(),
            })
        except Exception as ex:
            print(f"  [{code}] 解析失敗: {ex}")
            results.append({"code": code, "zh": zh, "en": en, "status": "error"})
    return results


def fetch_tw_etfs():
    """批次抓台股ETF當日行情。"""
    codes = [e[0] for e in TW_ETFS]
    try:
        df = yf.download(
            codes, period="5d", interval="1d",
            auto_adjust=False, progress=False,
            group_by="ticker", threads=True,
        )
    except Exception as e:
        print(f"ETF 抓取失敗: {e}")
        return []

    results = []
    for code, name, sector in TW_ETFS:
        try:
            if len(codes) > 1:
                bars = df[code] if code in df.columns.get_level_values(0) else None
                if bars is None:
                    continue
            else:
                bars = df.copy()
                if isinstance(bars.columns, pd.MultiIndex):
                    bars.columns = bars.columns.get_level_values(0)
            bars = bars.dropna(subset=["Close"])
            if len(bars) < 2:
                continue
            close  = float(bars["Close"].iloc[-1])
            prev   = float(bars["Close"].iloc[-2])
            vol    = float(bars["Volume"].iloc[-1]) if not pd.isna(bars["Volume"].iloc[-1]) else None
            vol_20 = float(bars["Volume"].iloc[-21:-1].mean()) if len(bars) >= 21 else None
            results.append({
                "code":      code.replace(".TW", ""),
                "name":      name,
                "sector":    sector,
                "close":     close,
                "day_pct":   (close / prev - 1) * 100,
                "volume":    vol,
                "vol_ratio": (vol / vol_20) if (vol and vol_20) else None,
            })
        except Exception:
            continue
    return results


def fetch_fear_greed():
    """從 CNN dataviz API 抓貪婪與恐懼指數。回傳 dict 或 None。"""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    try:
        r = requests.get(url, timeout=15, headers={
            **HEADERS,
            "Referer": "https://edition.cnn.com/",
            "Origin":  "https://edition.cnn.com",
        })
        r.raise_for_status()
        data = r.json()
        fg   = data.get("fear_and_greed", {})
        # 歷史趨勢（最近 30 天）
        hist = []
        for pt in data.get("fear_and_greed_historical", {}).get("data", [])[-30:]:
            try:
                hist.append({
                    "x": pt.get("x"),          # timestamp ms
                    "y": round(float(pt["y"]), 1),
                    "r": pt.get("rating", ""),
                })
            except Exception:
                continue
        return {
            "score":            round(float(fg["score"]), 1) if fg.get("score") is not None else None,
            "rating":           fg.get("rating", ""),
            "timestamp":        fg.get("timestamp", ""),
            "previous_close":   round(float(fg["previous_close"]), 1) if fg.get("previous_close") is not None else None,
            "previous_1_week":  round(float(fg["previous_1_week"]), 1) if fg.get("previous_1_week") is not None else None,
            "previous_1_month": round(float(fg["previous_1_month"]), 1) if fg.get("previous_1_month") is not None else None,
            "previous_1_year":  round(float(fg["previous_1_year"]), 1) if fg.get("previous_1_year") is not None else None,
            "history":          hist,
        }
    except Exception as e:
        print(f"Fear & Greed 抓取失敗: {e}")
        return None


def fetch_rrp(days=RRP_HISTORY_DAYS):
    """從 FRED 免 key CSV 端點抓隔夜逆回購 RRPONTSYD。"""
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=RRPONTSYD"
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        r.raise_for_status()
        reader = csv.DictReader(StringIO(r.text))
        # FRED CSV 有兩種常見格式：date列可能是 "DATE" 或 "observation_date"
        rows = []
        for row in reader:
            val = row.get("RRPONTSYD", "")
            date_key = "observation_date" if "observation_date" in row else "DATE"
            if val and val.strip() not in ("", "."):
                try:
                    rows.append({"date": row[date_key], "value": float(val)})
                except (ValueError, KeyError):
                    pass
        rows = rows[-days:]
        if not rows:
            return None
        latest = rows[-1]
        prev   = rows[-2] if len(rows) >= 2 else None
        wk_ago = rows[-6] if len(rows) >= 6 else None
        return {
            "current":      latest["value"],
            "current_date": latest["date"],
            "prev":         prev["value"] if prev else None,
            "prev_date":    prev["date"] if prev else None,
            "week_ago":     wk_ago["value"] if wk_ago else None,
            "history":      rows,
        }
    except Exception as e:
        print(f"RRP 抓取失敗: {e}")
        return None


def fetch_twse_breadth():
    """嘗試從 TWSE API 抓今日漲跌家數。失敗回傳 None。"""
    try:
        date_str = TODAY.strftime("%Y%m%d")
        url = (
            f"https://www.twse.com.tw/exchangeReport/MI_INDEX"
            f"?response=json&date={date_str}&type=MS"
        )
        r = requests.get(url, timeout=10, headers=HEADERS)
        data = r.json()
        if data.get("stat") != "OK":
            return None
        # 找漲跌家數表
        for table in data.get("tables", []):
            fields = table.get("fields", [])
            rows   = table.get("data", [])
            # 尋找包含「漲停」欄位的表
            if any("漲停" in str(f) for f in fields) and rows:
                row = rows[0] if rows else []
                # 典型欄位: 漲停, 漲, 持平, 跌, 跌停, 無比價
                mapping = {f: v for f, v in zip(fields, row)}
                def to_int(k):
                    try: return int(str(mapping.get(k, "0")).replace(",", ""))
                    except: return 0
                up   = to_int("漲") + to_int("漲停")
                flat = to_int("持平")
                dn   = to_int("跌") + to_int("跌停")
                return {"up": up, "flat": flat, "down": dn}
        return None
    except Exception as e:
        print(f"TWSE 漲跌家數: {e}")
        return None


# ─── HTML 輔助函式 ───────────────────────────────────────────────────────────

def _pct_cls(v):
    if v is None: return "nu"
    return "up" if v > 0 else ("dn" if v < 0 else "nu")

def _fmt_pct(v):
    if v is None: return "─"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%"

def _fmt_price(v, decimals=2):
    if v is None: return "─"
    if v >= 10000:
        return f"{v:,.0f}"
    return f"{v:,.{decimals}f}"

def _fmt_vol(v):
    """格式化成交量：億/萬 單位。"""
    if v is None or v == 0: return "─"
    if v >= 1e8:   return f"{v/1e8:.1f} 億"
    if v >= 1e4:   return f"{v/1e4:.1f} 萬"
    return f"{v:.0f}"

def _fg_rating_zh(rating):
    m = {
        "extreme fear":  "極度恐懼",
        "fear":          "恐懼",
        "neutral":       "中性",
        "greed":         "貪婪",
        "extreme greed": "極度貪婪",
    }
    return m.get((rating or "").lower(), rating)

def _fg_color(score):
    """依分數回傳色系。"""
    if score is None:  return "#94a3b8"
    if score <= 25:    return "#ef4444"
    if score <= 45:    return "#f97316"
    if score <= 55:    return "#eab308"
    if score <= 75:    return "#84cc16"
    return "#16a34a"

def _fg_bg(score):
    if score is None:  return "#f1f5f9"
    if score <= 25:    return "#fee2e2"
    if score <= 45:    return "#ffedd5"
    if score <= 55:    return "#fef9c3"
    if score <= 75:    return "#ecfccb"
    return "#dcfce7"


# ─── SVG 圖表產生 ────────────────────────────────────────────────────────────

def _gen_fg_gauge(score):
    """SVG 半圓儀表（0-100），指針指向當前分數，無圓餅。"""
    if score is None:
        return '<div class="fg-gauge-na">數據暫無</div>'

    # 半圓設定：角度從 180° 到 0°（左到右）
    cx, cy, r_outer, r_inner = 130, 100, 90, 56
    stroke_w = r_outer - r_inner

    def arc_path(start_pct, end_pct, color):
        """產生一段弧形 path。"""
        import math
        start_deg = 180 - start_pct * 180
        end_deg   = 180 - end_pct * 180
        # 從 start_deg 到 end_deg 的弧
        r_mid = (r_outer + r_inner) / 2
        sx = cx + r_mid * math.cos(math.radians(start_deg))
        sy = cy - r_mid * math.sin(math.radians(start_deg))
        ex = cx + r_mid * math.cos(math.radians(end_deg))
        ey = cy - r_mid * math.sin(math.radians(end_deg))
        large = 1 if (end_pct - start_pct) > 0.5 else 0
        # 外弧順時針、內弧逆時針
        ox1 = cx + r_outer * math.cos(math.radians(start_deg))
        oy1 = cy - r_outer * math.sin(math.radians(start_deg))
        ox2 = cx + r_outer * math.cos(math.radians(end_deg))
        oy2 = cy - r_outer * math.sin(math.radians(end_deg))
        ix1 = cx + r_inner * math.cos(math.radians(end_deg))
        iy1 = cy - r_inner * math.sin(math.radians(end_deg))
        ix2 = cx + r_inner * math.cos(math.radians(start_deg))
        iy2 = cy - r_inner * math.sin(math.radians(start_deg))
        d = (
            f"M {ox1:.1f} {oy1:.1f} "
            f"A {r_outer} {r_outer} 0 {large} 0 {ox2:.1f} {oy2:.1f} "
            f"L {ix1:.1f} {iy1:.1f} "
            f"A {r_inner} {r_inner} 0 {large} 1 {ix2:.1f} {iy2:.1f} Z"
        )
        return f'<path d="{d}" fill="{color}" />'

    # 5 個色帶
    bands = [
        (0.00, 0.25, "#ef4444"),
        (0.25, 0.45, "#f97316"),
        (0.45, 0.55, "#eab308"),
        (0.55, 0.75, "#84cc16"),
        (0.75, 1.00, "#16a34a"),
    ]
    band_paths = "".join(arc_path(s, e, c) for s, e, c in bands)

    # 指針
    import math
    pct = score / 100
    needle_deg = 180 - pct * 180
    nx = cx + (r_inner - 8) * math.cos(math.radians(needle_deg))
    ny = cy - (r_inner - 8) * math.sin(math.radians(needle_deg))
    needle = f'<line x1="{cx}" y1="{cy}" x2="{nx:.1f}" y2="{ny:.1f}" stroke="#1e293b" stroke-width="3" stroke-linecap="round"/>'
    dot    = f'<circle cx="{cx}" cy="{cy}" r="5" fill="#1e293b"/>'

    # 刻度標籤
    labels = [
        (0,   "0"),
        (0.25,"25"),
        (0.5, "50"),
        (0.75,"75"),
        (1.0, "100"),
    ]
    label_paths = ""
    for pct_l, txt in labels:
        deg = 180 - pct_l * 180
        lx = cx + (r_outer + 8) * math.cos(math.radians(deg))
        ly = cy - (r_outer + 8) * math.sin(math.radians(deg))
        label_paths += f'<text x="{lx:.1f}" y="{ly:.1f}" font-size="9" fill="#64748b" text-anchor="middle" dominant-baseline="middle">{txt}</text>'

    svg = f'''<svg viewBox="0 0 260 115" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-width:280px;display:block;margin:0 auto">
  {band_paths}
  {needle}
  {dot}
  {label_paths}
  <text x="{cx}" y="{cy - 22}" font-size="26" font-weight="800" fill="{_fg_color(score)}" text-anchor="middle">{score:.0f}</text>
</svg>'''
    return svg


def _gen_rrp_chart(rrp):
    """SVG 折線圖 for RRP，無圓餅。"""
    if not rrp or not rrp.get("history"):
        return '<div class="rrp-na">數據暫無</div>'

    pts = rrp["history"]
    vals = [p["value"] for p in pts]
    dates = [p["date"] for p in pts]
    n = len(vals)
    if n < 2:
        return ""

    W, H, pad_l, pad_r, pad_t, pad_b = 480, 130, 50, 16, 12, 28
    vmin, vmax = min(vals), max(vals)
    span = vmax - vmin if vmax != vmin else 1

    def sx(i): return pad_l + (i / (n - 1)) * (W - pad_l - pad_r)
    def sy(v): return pad_t + (1 - (v - vmin) / span) * (H - pad_t - pad_b)

    # 折線
    polyline = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(vals))
    line_path = f'<polyline points="{polyline}" fill="none" stroke="#3b82f6" stroke-width="2" stroke-linejoin="round"/>'

    # 填色區域
    area_pts = (
        f"{sx(0):.1f},{H - pad_b} "
        + " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(vals))
        + f" {sx(n-1):.1f},{H - pad_b}"
    )
    area = f'<polygon points="{area_pts}" fill="#dbeafe" opacity="0.5"/>'

    # Y 軸刻度 (3 條)
    grid = ""
    for frac in [0, 0.5, 1]:
        yv = vmin + frac * span
        yy = sy(yv)
        grid += f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{W - pad_r}" y2="{yy:.1f}" stroke="#e2e8f0" stroke-width="1"/>'
        grid += f'<text x="{pad_l - 4}" y="{yy:.1f}" font-size="9" fill="#94a3b8" text-anchor="end" dominant-baseline="middle">{yv:.0f}B</text>'

    # X 軸日期標籤（首尾 + 中間）
    xticks = ""
    for idx in [0, n // 2, n - 1]:
        xv = sx(idx)
        label = dates[idx][5:]  # MM-DD
        xticks += f'<text x="{xv:.1f}" y="{H - pad_b + 14}" font-size="9" fill="#94a3b8" text-anchor="middle">{label}</text>'

    # 最後一點高亮
    last_x, last_y = sx(n - 1), sy(vals[-1])
    dot = f'<circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="4" fill="#3b82f6"/>'

    svg = f'''<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" style="width:100%;display:block">
  {grid}
  {area}
  {line_path}
  {dot}
  {xticks}
</svg>'''
    return svg


def _gen_fg_history_mini(fg):
    """Fear & Greed 最近 30 天走勢折線（細）。"""
    hist = (fg or {}).get("history", [])
    if len(hist) < 2:
        return ""
    vals = [p["y"] for p in hist]
    n = len(vals)
    W, H = 240, 50
    vmin, vmax = 0, 100

    def sx(i): return (i / (n - 1)) * W
    def sy(v): return H - (v / 100) * H

    pts = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(vals))
    # 底部填色（按分數段上色）
    last_v = vals[-1]
    fill_color = _fg_color(last_v) + "33"  # 透明
    area_pts = f"0,{H} " + pts + f" {W},{H}"
    area = f'<polygon points="{area_pts}" fill="{fill_color}"/>'
    line = f'<polyline points="{pts}" fill="none" stroke="{_fg_color(last_v)}" stroke-width="1.5"/>'
    dot_x, dot_y = sx(n-1), sy(last_v)
    dot = f'<circle cx="{dot_x:.1f}" cy="{dot_y:.1f}" r="3" fill="{_fg_color(last_v)}"/>'

    return f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-width:240px;display:block;margin-top:8px">{area}{line}{dot}</svg>'


# ─── HTML 區塊產生 ───────────────────────────────────────────────────────────

def _gen_market_bar(indices, breadth):
    """頂部大盤指數橫條。"""
    cards = ""
    for m in indices:
        if m["status"] != "ok":
            cards += f'<div class="mk"><div class="lbl">{m["zh"]}</div><div class="px">─</div></div>'
            continue
        cls = _pct_cls(m["day_pct"])
        chg = m["day_chg"]
        sign = "+" if chg > 0 else ""
        cards += f'''<div class="mk">
  <div class="lbl">{m["zh"]} <span class="lbl-en">{m["en"]}</span></div>
  <div class="px">{_fmt_price(m["close"], 2 if m["close"] < 10000 else 0)}</div>
  <div class="chg {cls}">{sign}{chg:,.1f} &nbsp; {_fmt_pct(m["day_pct"])}</div>
  <div class="vol-lbl">成交量 {_fmt_vol(m.get("volume"))}</div>
</div>'''

    breadth_html = ""
    if breadth:
        up, flat, dn = breadth["up"], breadth["flat"], breadth["down"]
        total = up + flat + dn or 1
        breadth_html = f'''<div class="mk breadth-card">
  <div class="lbl">漲跌家數</div>
  <div class="breadth-row">
    <span class="b-up">▲ {up}</span>
    <span class="b-flat">── {flat}</span>
    <span class="b-dn">▼ {dn}</span>
  </div>
  <div class="breadth-bar">
    <div class="bb-up"   style="width:{up/total*100:.1f}%"></div>
    <div class="bb-flat" style="width:{flat/total*100:.1f}%"></div>
    <div class="bb-dn"   style="width:{dn/total*100:.1f}%"></div>
  </div>
</div>'''

    return f'<div class="market">{cards}{breadth_html}</div>'


def _gen_fear_greed_panel(fg):
    """貪婪與恐懼指數面板。"""
    if not fg or fg.get("score") is None:
        return '<div class="panel"><div class="panel-ttl">😱 貪婪與恐懼指數</div><div class="na">數據暫無</div></div>'

    score   = fg["score"]
    rating  = _fg_rating_zh(fg["rating"])
    color   = _fg_color(score)
    bg      = _fg_bg(score)
    gauge   = _gen_fg_gauge(score)
    mini    = _gen_fg_history_mini(fg)

    def cmp_row(label, val):
        if val is None: return ""
        diff = score - val
        diff_cls = "up" if diff > 0 else ("dn" if diff < 0 else "nu")
        sign = "+" if diff > 0 else ""
        return (
            f'<div class="cmp-row">'
            f'<span class="cmp-lbl">{label}</span>'
            f'<span class="cmp-val">{val:.0f}</span>'
            f'<span class="cmp-diff {diff_cls}">{sign}{diff:.1f}</span>'
            f'</div>'
        )

    cmp_block = (
        cmp_row("昨日收盤",  fg.get("previous_close"))
        + cmp_row("一週前",   fg.get("previous_1_week"))
        + cmp_row("一個月前", fg.get("previous_1_month"))
        + cmp_row("一年前",   fg.get("previous_1_year"))
    )

    return f'''<div class="panel fg-panel" style="background:{bg};border-color:{color}40">
  <div class="panel-ttl">😱 貪婪與恐懼指數 <span class="src-note">CNN Business</span></div>
  <div class="fg-body">
    <div class="fg-left">
      {gauge}
      <div class="fg-rating" style="color:{color}">{rating}</div>
      <div class="fg-sub">CNN Fear &amp; Greed</div>
      {mini}
    </div>
    <div class="fg-right">
      <div class="cmp-title">與歷史相比</div>
      {cmp_block}
      <div class="fg-note">
        ℹ️ 指數由市場動能、強弱、期權比率、垃圾債券需求等 7 項子指標合成，
        0 = 極度恐懼，100 = 極度貪婪。
      </div>
    </div>
  </div>
</div>'''


def _gen_rrp_panel(rrp):
    """隔夜逆回購 RRP 面板。"""
    if not rrp:
        return '<div class="panel"><div class="panel-ttl">🏦 隔夜逆回購 (RRP)</div><div class="na">數據暫無</div></div>'

    cur  = rrp["current"]
    prev = rrp.get("prev")
    wk   = rrp.get("week_ago")
    diff = cur - prev if prev is not None else None
    diff_cls = _pct_cls(diff)
    sign = "+" if diff and diff > 0 else ""
    diff_txt = f"{sign}{diff:.1f}B" if diff is not None else "─"
    chart = _gen_rrp_chart(rrp)

    wk_txt = f"{wk:.1f}B" if wk else "─"
    wk_diff = cur - wk if wk else None
    wk_sign = "+" if wk_diff and wk_diff > 0 else ""
    wk_cls = _pct_cls(wk_diff)
    wk_chg = f"{wk_sign}{wk_diff:.1f}B" if wk_diff is not None else ""

    return f'''<div class="panel rrp-panel">
  <div class="panel-ttl">🏦 隔夜逆回購 (RRP) <span class="src-note">FRED · RRPONTSYD</span></div>
  <div class="rrp-top">
    <div class="rrp-main">
      <div class="rrp-val">{cur:.1f}<span class="rrp-unit">B USD</span></div>
      <div class="rrp-date">{rrp["current_date"]}</div>
      <div class="rrp-changes">
        <span class="rc-row">日變動：<span class="{diff_cls}">{diff_txt}</span></span>
        <span class="rc-sep">·</span>
        <span class="rc-row">週前：<span class="mu">{wk_txt}</span><span class="{wk_cls}"> {wk_chg}</span></span>
      </div>
    </div>
    <div class="rrp-desc">
      <div class="rrp-desc-title">指標意義</div>
      <ul>
        <li>聯準會 ON RRP：銀行把隔夜閒置資金停放聯準會</li>
        <li>數字上升 → 市場資金過剩，流動性充裕</li>
        <li>數字下降 → 資金轉向風險資產 / 國庫券</li>
      </ul>
    </div>
  </div>
  <div class="rrp-chart">{chart}</div>
</div>'''


def _gen_etf_panel(etfs):
    """台股代表 ETF 板塊漲跌。"""
    if not etfs:
        return '<div class="panel"><div class="panel-ttl">📊 台股 ETF 板塊概覽</div><div class="na">數據暫無</div></div>'

    sorted_etfs = sorted(etfs, key=lambda e: e["day_pct"], reverse=True)
    cards = ""
    for e in sorted_etfs:
        pct = e["day_pct"]
        cls = _pct_cls(pct)
        intensity = min(abs(pct) / 3, 1)

        if pct > 0:
            r = int(220 - intensity * 80)
            g = int(220 + intensity * 35)
            b = int(210 - intensity * 210)
            bg = f"rgb({r},{g},{b})"
        elif pct < 0:
            r = int(210 + intensity * 45)
            g = int(210 - intensity * 180)
            b = int(210 - intensity * 180)
            bg = f"rgb({r},{g},{b})"
        else:
            bg = "#f1f5f9"

        txt_color = "#0f172a" if intensity < 0.5 else "#fff"
        vol_badge = ""
        if e.get("vol_ratio") and e["vol_ratio"] > 1.5:
            vol_badge = f'<span class="vol-pill">量×{e["vol_ratio"]:.1f}</span>'

        cards += f'''<div class="etf-card" style="background:{bg};color:{txt_color}">
  <div class="etf-code">{e["code"]}</div>
  <div class="etf-name">{e["name"]}</div>
  <div class="etf-pct">{_fmt_pct(pct)}</div>
  <div class="etf-sub">{e["sector"]} {vol_badge}</div>
</div>'''

    return f'''<div class="panel etf-panel">
  <div class="panel-ttl">📊 台股 ETF 板塊概覽</div>
  <div class="etf-grid">{cards}</div>
</div>'''


# ─── CSS + JS ────────────────────────────────────────────────────────────────

CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b;--card:#fff}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:14px;line-height:1.5}
.hdr{background:linear-gradient(135deg,#0f172a 0%,#065f46 60%,#059669 100%);color:#fff;padding:22px 28px 16px}
.hdr h1{font-size:20px;font-weight:800;margin-bottom:4px}
.hdr .sub{font-size:11.5px;opacity:.85}
/* 大盤橫條 */
.market{display:flex;gap:10px;padding:14px 28px;background:var(--card);border-bottom:1px solid var(--brd);flex-wrap:wrap}
.mk{background:var(--bg);border:1px solid var(--brd);border-radius:8px;padding:10px 16px;min-width:160px}
.mk .lbl{font-size:10.5px;color:var(--mu);font-weight:600;letter-spacing:.3px}
.lbl-en{font-size:9.5px;opacity:.7;font-weight:400}
.mk .px{font-size:20px;font-weight:800;margin-top:2px}
.mk .chg{font-size:12.5px;font-weight:600;margin-top:1px}
.chg.up{color:var(--gr)}.chg.dn{color:var(--rd)}.chg.nu{color:var(--mu)}
.vol-lbl{font-size:10.5px;color:var(--mu);margin-top:3px}
/* 漲跌家數 */
.breadth-card{min-width:170px}
.breadth-row{display:flex;gap:12px;font-size:14px;font-weight:700;margin-top:6px}
.b-up{color:var(--gr)}.b-flat{color:var(--mu)}.b-dn{color:var(--rd)}
.breadth-bar{display:flex;height:6px;border-radius:3px;overflow:hidden;margin-top:6px}
.bb-up{background:var(--gr)}.bb-flat{background:#94a3b8}.bb-dn{background:var(--rd)}
/* 面板通用 */
.panels{padding:18px 28px;display:flex;flex-direction:column;gap:16px}
.panel{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:18px 20px}
.panel-ttl{font-size:14px;font-weight:800;margin-bottom:14px;display:flex;align-items:center;gap:8px}
.src-note{font-size:10.5px;font-weight:400;color:var(--mu);background:var(--bg);padding:2px 8px;border-radius:10px;border:1px solid var(--brd)}
.na{color:var(--mu);font-style:italic;font-size:13px}
/* Fear & Greed */
.fg-panel{border-left-width:4px}
.fg-body{display:flex;gap:24px;flex-wrap:wrap}
.fg-left{display:flex;flex-direction:column;align-items:center;min-width:190px;flex:0 0 auto}
.fg-right{flex:1;min-width:200px}
.fg-rating{font-size:18px;font-weight:800;margin-top:4px;text-align:center}
.fg-sub{font-size:10.5px;color:var(--mu);text-align:center;margin-top:2px}
.fg-gauge-na{color:var(--mu);font-style:italic;font-size:13px;padding:40px 0;text-align:center}
.cmp-title{font-size:11px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;padding-bottom:6px;border-bottom:2px solid var(--brd)}
.cmp-row{display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px dashed #f1f5f9;font-size:13px}
.cmp-row:last-child{border-bottom:none}
.cmp-lbl{color:var(--mu);flex:1}
.cmp-val{font-weight:700;min-width:36px;text-align:right}
.cmp-diff{font-weight:600;font-size:12px;min-width:46px;text-align:right}
.cmp-diff.up{color:var(--gr)}.cmp-diff.dn{color:var(--rd)}.cmp-diff.nu{color:var(--mu)}
.fg-note{margin-top:14px;font-size:11.5px;color:var(--mu);background:var(--bg);border-radius:8px;padding:10px 12px;line-height:1.7}
/* RRP */
.rrp-panel{}
.rrp-top{display:flex;gap:24px;flex-wrap:wrap;margin-bottom:14px}
.rrp-main{min-width:160px}
.rrp-val{font-size:28px;font-weight:800;color:var(--bl);line-height:1.1}
.rrp-unit{font-size:14px;font-weight:400;color:var(--mu);margin-left:4px}
.rrp-date{font-size:10.5px;color:var(--mu);margin-top:2px}
.rrp-changes{font-size:12px;margin-top:8px;display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.rc-row{white-space:nowrap}
.rc-sep{color:var(--mu)}
.rrp-desc{flex:1;min-width:180px}
.rrp-desc-title{font-size:11px;font-weight:700;color:var(--mu);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
.rrp-desc ul{padding-left:14px}
.rrp-desc li{font-size:12px;color:var(--mu);line-height:1.7}
.rrp-chart{margin-top:4px}
.rrp-na{color:var(--mu);font-style:italic}
/* ETF 板塊 */
.etf-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px}
.etf-card{border-radius:10px;padding:12px 14px;border:1px solid rgba(0,0,0,.06);transition:transform .12s}
.etf-card:hover{transform:translateY(-2px);box-shadow:0 4px 14px rgba(0,0,0,.1)}
.etf-code{font-size:11px;font-weight:700;opacity:.85}
.etf-name{font-size:14px;font-weight:700;margin-top:2px}
.etf-pct{font-size:20px;font-weight:800;margin-top:6px}
.etf-sub{font-size:10.5px;opacity:.85;margin-top:3px;display:flex;align-items:center;gap:6px}
.vol-pill{font-size:9.5px;font-weight:700;background:rgba(0,0,0,.12);padding:1px 6px;border-radius:8px}
/* 工具 */
.up{color:var(--gr)}.dn{color:var(--rd)}.mu{color:var(--mu)}
@media(max-width:600px){
  .fg-body,.rrp-top{flex-direction:column}
  .market{padding:10px 14px}
  .panels{padding:12px 14px}
}
"""

JS = """
// 無互動邏輯需求 — 保留空白
"""


# ─── HTML 組合 ───────────────────────────────────────────────────────────────

def generate_html(indices, breadth, fg, rrp, etfs):
    market_bar  = _gen_market_bar(indices, breadth)
    fg_panel    = _gen_fear_greed_panel(fg)
    rrp_panel   = _gen_rrp_panel(rrp)
    etf_panel   = _gen_etf_panel(etfs)

    data_date = ""
    for m in indices:
        if m.get("date"):
            data_date = m["date"]
            break
    if not data_date:
        data_date = TODAY.isoformat()

    return f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>台股每日大盤復盤 — {data_date}</title>
<meta name="robots" content="noindex, nofollow"/>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>台股每日大盤復盤</h1>
  <div class="sub">更新：{data_date}（台股收盤後自動更新）</div>
</div>
{market_bar}
<div class="panels">
  {fg_panel}
  {rrp_panel}
  {etf_panel}
</div>
<script>{JS}</script>
</body></html>"""


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print(f"=== 台股每日大盤復盤 ({TODAY.isoformat()}) ===")

    # 並發抓取（不互相依賴的資料源）
    results = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(fetch_market_indices): "indices",
            pool.submit(fetch_tw_etfs):        "etfs",
            pool.submit(fetch_fear_greed):     "fg",
            pool.submit(fetch_rrp):            "rrp",
            pool.submit(fetch_twse_breadth):   "breadth",
        }
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                results[key] = fut.result()
                print(f"  [{key}] OK")
            except Exception as ex:
                print(f"  [{key}] ERROR: {ex}")
                results[key] = None

    indices = results.get("indices") or []
    etfs    = results.get("etfs")    or []
    fg      = results.get("fg")
    rrp     = results.get("rrp")
    breadth = results.get("breadth")

    if fg:
        print(f"  Fear & Greed: {fg['score']} ({fg['rating']})")
    if rrp:
        print(f"  RRP: {rrp['current']:.1f}B ({rrp['current_date']})")

    html = generate_html(indices, breadth, fg, rrp, etfs)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n輸出至 {OUTPUT_HTML}")

    # 快取 JSON
    cache = {
        "generated_at": datetime.now().isoformat(),
        "date": TODAY.isoformat(),
        "indices": indices,
        "fear_greed": fg,
        "rrp_latest": {k: v for k, v in (rrp or {}).items() if k != "history"},
        "etfs": etfs,
        "breadth": breadth,
    }
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    if not indices and not fg and not rrp:
        print("::error::所有數據來源均失敗", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
