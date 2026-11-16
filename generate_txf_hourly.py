#!/usr/bin/env python3
"""Generate a public, read-only TX futures hourly support/resistance page."""

from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from zoneinfo import ZoneInfo

import requests


ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "txf_hourly.html"
LATEST = ROOT / "txf_hourly_latest.json"
HISTORY = ROOT / "txf_hourly_history.json"
API = "https://mis.taifex.com.tw/futures/api"
TZ = ZoneInfo("Asia/Taipei")


def normalise_exchange_time(trade_date, hhmmss: str, *, overnight: bool) -> datetime:
    """Parse TAIFEX times, including transient 13:60/24:xx style values."""
    digits = "".join(ch for ch in str(hhmmss) if ch.isdigit()).zfill(6)[-6:]
    hours, minutes, seconds = int(digits[:2]), int(digits[2:4]), int(digits[4:6])
    total_seconds = hours * 3600 + minutes * 60 + seconds
    day_offset, seconds_of_day = divmod(total_seconds, 24 * 3600)
    hours, remainder = divmod(seconds_of_day, 3600)
    minutes, seconds = divmod(remainder, 60)
    if overnight and day_offset == 0 and hours < 15:
        day_offset = 1
    return datetime.combine(trade_date + timedelta(days=day_offset), datetime.min.time(), TZ).replace(
        hour=hours, minute=minutes, second=seconds
    )


def fetch_rows() -> list[dict]:
    rows: list[dict] = []
    month_codes = "ABCDEFGHIJKL"
    headers = {"Referer": "https://mis.taifex.com.tw/futures/", "User-Agent": "Mozilla/5.0"}
    for market, suffix in (("0", "F"), ("1", "M")):
        quote = requests.post(f"{API}/getQuoteList", json={"MarketType": market, "SymbolType": "F", "KindID": "1"}, headers=headers, timeout=45).json()
        candidates = [q for q in (quote.get("RtData") or {}).get("QuoteList", []) if q.get("SymbolID", "").startswith("TXF") and q.get("SymbolID", "").endswith(f"-{suffix}")]
        candidates.sort(key=lambda q: float(q.get("CTotalVolume") or 0), reverse=True)
        if not candidates:
            continue
        q = candidates[0]
        symbol = q["SymbolID"]
        code = symbol[3:5]
        month = month_codes.index(code[0]) + 1
        year_digit = int(code[1])
        now_year = datetime.now(TZ).year
        year = min(y for y in range(now_year, now_year + 10) if y % 10 == year_digit)
        contract = f"{year}{month:02d}"
        trade_date = datetime.strptime(q["CDate"], "%Y%m%d").date()
        chart = requests.post(f"{API}/getChartData1M", json={"SymbolID": symbol}, headers=headers, timeout=45).json()
        data = chart.get("RtData") or {}
        if chart.get("RtCode") != "0":
            continue
        for tick in data.get("Ticks", []):
            hhmmss, o, h, low, c, volume = tick[:6]
            dt = normalise_exchange_time(trade_date, hhmmss, overnight=market == "1")
            rows.append({
                "timestamp": dt.isoformat(),
                "trade_date": str(trade_date),
                "session": "night" if market == "1" else "day",
                "open": o,
                "high": h,
                "low": low,
                "close": c,
                "volume": volume,
                "contract_date": contract,
            })
    if not rows:
        raise RuntimeError("TAIFEX returned no TX futures minute data")
    return rows


def parse_time(row: dict) -> datetime:
    if row.get("timestamp"):
        dt = datetime.fromisoformat(str(row["timestamp"]))
        return dt.replace(tzinfo=TZ) if dt.tzinfo is None else dt.astimezone(TZ)
    raw = f"{row.get('date', '')} {row.get('minute') or row.get('time') or ''}".strip()
    date_part, time_part = raw.split(maxsplit=1)
    base_date = datetime.fromisoformat(date_part).date()
    return normalise_exchange_time(base_date, time_part.replace(":", ""), overnight=False)


def select_contract(rows: list[dict]) -> tuple[str, list[dict]]:
    volumes: dict[str, float] = defaultdict(float)
    for row in rows:
        contract = str(row.get("contract_date", ""))
        if len(contract) == 6 and contract.isdigit():
            volumes[contract] += float(row.get("volume") or 0)
    if not volumes:
        raise RuntimeError("No active TX contract was returned")
    contract = max(volumes, key=volumes.get)
    return contract, [r for r in rows if str(r.get("contract_date")) == contract]


def hourly_bars(rows: list[dict], cutoff: datetime, seed_bars: list[dict] | None = None) -> list[dict]:
    buckets: dict[datetime, dict] = {}
    live_hours = {parse_time(row).replace(minute=0, second=0, microsecond=0) for row in rows if parse_time(row) < cutoff}
    for row in seed_bars or []:
        t = datetime.fromisoformat(str(row["t"])).astimezone(TZ)
        if t < cutoff and t not in live_hours:
            buckets[t] = {"t": t, "o": float(row["o"]), "h": float(row["h"]), "l": float(row["l"]), "c": float(row["c"]), "v": float(row["v"])}
    for row in sorted(rows, key=parse_time):
        dt = parse_time(row)
        if dt >= cutoff:
            continue
        hour = dt.replace(minute=0, second=0, microsecond=0)
        o = float(row.get("open") or row.get("close"))
        h = float(row.get("high") or o)
        l = float(row.get("low") or o)
        c = float(row.get("close") or o)
        v = float(row.get("volume") or 0)
        if hour not in buckets:
            buckets[hour] = {"t": hour, "o": o, "h": h, "l": l, "c": c, "v": v}
        else:
            bar = buckets[hour]
            bar["h"] = max(bar["h"], h)
            bar["l"] = min(bar["l"], l)
            bar["c"] = c
            bar["v"] += v
    bars = sorted(buckets.values(), key=lambda b: b["t"])[-72:]
    if len(bars) < 6:
        raise RuntimeError(f"Only {len(bars)} completed hourly bars are available")
    return bars


def atr(bars: list[dict], period: int = 14) -> float:
    sample = bars[-period:]
    values = []
    for i, bar in enumerate(sample):
        if i == 0:
            values.append(bar["h"] - bar["l"])
        else:
            prev = sample[i - 1]["c"]
            values.append(max(bar["h"] - bar["l"], abs(bar["h"] - prev), abs(bar["l"] - prev)))
    return mean(values)


def calculate_levels(bars: list[dict]) -> dict:
    cur = bars[-1]["c"]
    a = max(1.0, atr(bars))
    prev = bars[-9:-1]
    high, low, close = max(x["h"] for x in prev), min(x["l"] for x in prev), prev[-1]["c"]
    pivot = (high + low + close) / 3
    avg_volume = max(1.0, mean(x["v"] for x in bars))
    candidates = [
        [2 * pivot - low, 3.2, {"pivot"}], [pivot + high - low, 3.8, {"pivot"}],
        [high + 2 * (pivot - low), 3.0, {"pivot"}], [2 * pivot - high, 3.2, {"pivot"}],
        [pivot - (high - low), 3.8, {"pivot"}], [low - 2 * (high - pivot), 3.0, {"pivot"}],
    ]
    for i in range(2, len(bars) - 2):
        bar = bars[i]
        volume_ratio = max(0.35, bar["v"] / avg_volume)
        recent = 1.25 if i > len(bars) - 13 else 1.0
        if bar["h"] > bars[i - 1]["h"] and bar["h"] > bars[i + 1]["h"]:
            candidates.append([bar["h"], (3 + 4 * math.sqrt(volume_ratio)) * recent, {"volume-swing"}])
        if bar["l"] < bars[i - 1]["l"] and bar["l"] < bars[i + 1]["l"]:
            candidates.append([bar["l"], (3 + 4 * math.sqrt(volume_ratio)) * recent, {"volume-swing"}])
        span = max(1.0, bar["h"] - bar["l"])
        upper = bar["h"] - max(bar["o"], bar["c"])
        lower = min(bar["o"], bar["c"]) - bar["l"]
        if upper / span > 0.34:
            candidates.append([bar["h"], 2 + 6 * upper / span * math.sqrt(volume_ratio), {"rejection"}])
        if lower / span > 0.34:
            candidates.append([bar["l"], 2 + 6 * lower / span * math.sqrt(volume_ratio), {"rejection"}])

    step = max(20, round(a / 50) * 10)
    profile: dict[int, float] = defaultdict(float)
    for bar in bars:
        lo = math.floor(bar["l"] / step) * step
        hi = math.ceil(bar["h"] / step) * step
        bins = max(1, (hi - lo) // step + 1)
        typical = (bar["h"] + bar["l"] + bar["c"]) / 3
        for price in range(lo, hi + step, step):
            center_bias = 0.55 + 0.45 * (1 - min(1, abs(price - typical) / max(step, bar["h"] - bar["l"])))
            profile[price] += bar["v"] / bins * center_bias
    peaks = sorted(profile.items(), key=lambda item: item[1], reverse=True)[:10]
    max_peak = peaks[0][1] if peaks else 1
    for index, (price, volume) in enumerate(peaks):
        candidates.append([price, 5 + 10 * volume / max_peak + (2 if index < 3 else 0), {"volume-profile"}])

    merged = []
    for price, weight, tags in sorted(candidates, key=lambda x: x[0]):
        if merged and abs(merged[-1]["p"] - price) < a * 0.16:
            item = merged[-1]
            item["p"] = (item["p"] * item["w"] + price * weight) / (item["w"] + weight)
            item["w"] += weight
            item["tags"].update(tags)
        else:
            merged.append({"p": price, "w": weight, "tags": set(tags)})

    def choose(direction: int, count: int) -> list[dict]:
        pool = []
        for item in merged:
            if (direction > 0 and item["p"] <= cur + a * 0.07) or (direction < 0 and item["p"] >= cur - a * 0.07):
                continue
            score = item["w"] * 1.7 - abs(item["p"] - cur) / a
            score += 5 if "volume-profile" in item["tags"] else 0
            score += 3 if "volume-swing" in item["tags"] else 0
            pool.append({**item, "score": score})
        pool = sorted(pool, key=lambda x: x["score"], reverse=True)[:count]
        pool.sort(key=lambda x: x["p"], reverse=direction < 0)
        while len(pool) < count:
            last = pool[-1]["p"] if pool else cur
            pool.append({"p": last + direction * a * 0.72, "w": 1, "score": 0, "tags": {"atr"}})
        return pool

    up, down = choose(1, 6), choose(-1, 4)
    return {
        "pressure": list(reversed(up[3:6])),
        "short_pressure": up[:3],
        "short_support": down[:2],
        "support": list(reversed(down[2:4])),
        "atr": a,
    }


def clean_level(items: list[dict]) -> list[int]:
    return sorted((round(x["p"]) for x in items), reverse=True)


def load_json(path: Path, fallback):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


def scheduled_cutoffs(rows: list[dict], now: datetime) -> list[dict]:
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        groups[(str(row["trade_date"]), str(row["session"]))].append(row)
    due: list[dict] = []
    for (trade_date_text, session), session_rows in groups.items():
        trade_date = datetime.fromisoformat(trade_date_text).date()
        if session == "day":
            # Day session checkpoints requested by the product: opening, each
            # full hour, and the actual 13:45 close.  Do not invent a 14:00
            # checkpoint for the already-closed day session.
            slots = [(8, 45, "日盤開盤")]
            slots += [(hour, 0, f"{hour:02d}:00") for hour in range(9, 14)]
            slots += [(13, 45, "日盤收盤")]
        else:
            slots = [(15, 0, "夜盤開盤")] + [(hour, 0, f"{hour:02d}:00") for hour in range(16, 24)]
            slots += [(24 + hour, 0, f"{hour:02d}:00") for hour in range(0, 6)]
        ordered = sorted(session_rows, key=parse_time)
        first_tick, last_tick = parse_time(ordered[0]), parse_time(ordered[-1])
        for hour, minute, label in slots:
            cutoff = datetime.combine(trade_date, datetime.min.time(), TZ) + timedelta(hours=hour, minutes=minute)
            if cutoff > now:
                continue
            is_open = minute == 45 or hour == 15
            if is_open:
                window_end = cutoff + timedelta(minutes=5)
                first_in_window = next((parse_time(r) for r in ordered if cutoff <= parse_time(r) <= window_end), None)
                if first_in_window is None:
                    continue
                analysis_cutoff = first_in_window + timedelta(minutes=1)
            else:
                ready_at = cutoff - timedelta(minutes=1)
                if last_tick < ready_at:
                    continue
                analysis_cutoff = cutoff
            due.append({
                "trade_date": trade_date_text,
                "session": session,
                "cutoff": cutoff,
                "analysis_cutoff": analysis_cutoff,
                "label": label,
                "first_tick": first_tick,
                "last_tick": last_tick,
            })
    return sorted(due, key=lambda item: item["cutoff"])


def build_snapshot(contract: str, rows: list[dict], slot: dict, seed_bars: list[dict], now: datetime) -> dict:
    bars = hourly_bars(rows, slot["analysis_cutoff"], seed_bars)
    result = calculate_levels(bars)
    last = bars[-1]
    return {
        "id": f"{slot['trade_date']}|{slot['session']}|{slot['cutoff'].strftime('%H:%M')}",
        "contract": contract,
        "trade_date": slot["trade_date"],
        "session": slot["session"],
        "session_name": "日盤" if slot["session"] == "day" else "夜盤",
        "label": slot["label"],
        "snapshot_time": slot["cutoff"].strftime("%Y-%m-%d %H:%M"),
        "market_time": last["t"].strftime("%Y-%m-%d %H:00"),
        "price": round(last["c"]),
        "high": round(last["h"]),
        "low": round(last["l"]),
        "volume": round(last["v"]),
        "atr": round(result["atr"]),
        "pressure": clean_level(result["pressure"]),
        "short_pressure": clean_level(result["short_pressure"]),
        "short_support": clean_level(result["short_support"]),
        "support": clean_level(result["support"]),
        "updated_at": now.strftime("%Y-%m-%d %H:%M"),
        "bars": [{k: round(v) if k != "t" else v.isoformat() for k, v in bar.items()} for bar in bars[-32:]],
    }


def merge_history(existing: list[dict], additions: list[dict]) -> list[dict]:
    by_id = {str(item.get("id")): item for item in existing if item.get("id")}
    for item in additions:
        by_id[item["id"]] = item
    return sorted(by_id.values(), key=lambda item: item["snapshot_time"])[-160:]


def render(data: dict, history: list[dict]) -> str:
    snapshots_json = json.dumps(history[-48:], ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return f'''<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#080b10"><title>台指期 60 分 K｜整點壓力支撐</title><style>
:root{{color-scheme:dark;--bg:#080b10;--panel:#101720;--line:#202a36;--ink:#edf4f1;--muted:#84918f;--green:#4fe0b5}}*{{box-sizing:border-box}}body{{margin:0;background:radial-gradient(circle at 70% -10%,#173129 0,transparent 35%),var(--bg);color:var(--ink);font:16px/1.5 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans TC",sans-serif;min-height:100vh}}button{{font:inherit}}main{{max-width:1120px;margin:auto;padding:32px 22px 48px}}header{{display:flex;justify-content:space-between;gap:18px;align-items:end;margin-bottom:18px}}.eyebrow{{color:var(--green);font:700 .74rem ui-monospace,monospace;letter-spacing:.14em}}h1{{font-size:clamp(1.55rem,4vw,2.25rem);margin:4px 0 0;letter-spacing:-.04em}}.status{{text-align:right;color:var(--muted);font-size:.78rem}}.status b{{display:block;color:#c9d5d1;font-size:.88rem}}.timeline{{display:flex;gap:8px;overflow:auto;padding:4px 1px 14px;scrollbar-color:#2a3945 transparent}}.snap{{min-width:104px;border:1px solid var(--line);background:#0d141c;color:var(--muted);border-radius:11px;padding:9px 11px;text-align:left;cursor:pointer}}.snap b,.snap small{{display:block}}.snap b{{color:var(--ink);font-size:.82rem}}.snap small{{font-size:.7rem}}.snap[aria-pressed="true"]{{border-color:var(--green);background:#153027;color:#b8d5cd;box-shadow:0 0 0 1px #4fe0b533}}.grid{{display:grid;grid-template-columns:1.2fr .8fr;gap:18px}}.panel{{background:linear-gradient(145deg,#111922,#0c1118);border:1px solid var(--line);border-radius:18px;box-shadow:0 24px 70px #0008}}.market{{padding:22px}}.price{{font:720 clamp(2.4rem,7vw,4.2rem)/1 ui-monospace,monospace;letter-spacing:-.07em;margin:10px 0 5px}}.meta{{color:var(--muted);font-size:.8rem}}#chart{{width:100%;height:330px;margin-top:18px;border-top:1px solid var(--line)}}.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin-top:12px}}.stat{{border:1px solid var(--line);border-radius:10px;padding:10px}}.stat span{{display:block;color:var(--muted);font-size:.68rem}}.stat b{{font:650 .93rem ui-monospace,monospace}}.levels{{padding:4px 20px}}.group{{padding:15px 0;border-bottom:1px solid var(--line)}}.group:last-child{{border:0}}.head{{display:flex;justify-content:space-between;align-items:center;margin-bottom:5px}}.head h2{{font-size:.76rem;letter-spacing:.12em;color:var(--muted);margin:0}}.head span{{font:650 .65rem ui-monospace,monospace;color:var(--muted)}}.level{{display:flex;justify-content:space-between;align-items:baseline;padding:4px 0}}.level b{{font:650 1.34rem ui-monospace,monospace;color:var(--c)}}.level small{{color:var(--muted);font:500 .7rem ui-monospace,monospace}}.history{{margin-top:18px;padding:18px 20px}}.history h2{{margin:0 0 12px;font-size:1rem}}.table-wrap{{overflow:auto}}table{{width:100%;border-collapse:collapse;font-size:.78rem}}th,td{{padding:10px 8px;border-top:1px solid var(--line);text-align:right;white-space:nowrap}}th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){{text-align:left}}tbody tr{{cursor:pointer}}tbody tr:hover{{background:#15202a}}.note{{margin-top:16px;color:#71807c;font-size:.73rem;display:flex;justify-content:space-between;gap:16px}}@media(max-width:760px){{header{{align-items:flex-start;flex-direction:column}}.status{{text-align:left}}.grid{{grid-template-columns:1fr}}#chart{{height:270px}}.stats{{grid-template-columns:repeat(2,1fr)}}}}
</style></head><body><main><header><div><div class="eyebrow">TXF · HOURLY SNAPSHOT</div><h1>台指期 60 分 K 壓力支撐</h1></div><div class="status"><b>開盤＋每個交易整點自動更新</b><span id="snapshotTime"></span><br><span id="updatedAt"></span></div></header><div class="grid"><section class="panel market"><div class="meta" id="contractMeta"></div><div class="price" id="price"></div><div class="meta">成交量加權：密集成交帶、放量轉折、影線拒絕、Pivot／ATR</div><svg id="chart" viewBox="0 0 760 330" role="img" aria-label="最近 60 分 K 走勢"></svg><div class="stats"><div class="stat"><span>60 分高</span><b id="barHigh"></b></div><div class="stat"><span>60 分低</span><b id="barLow"></b></div><div class="stat"><span>60 分量</span><b id="barVolume"></b></div><div class="stat"><span>ATR（14）</span><b id="atr"></b></div></div></section><aside class="panel levels" id="levels"></aside></div><section class="panel history"><h2>最近整點計算</h2><div class="table-wrap"><table><thead><tr><th>盤別</th><th>資料截至</th><th>價格</th><th>短壓</th><th>短撐</th><th>60 分量</th></tr></thead><tbody id="historyRows"></tbody></table></div></section><div class="note"><span>08:45 與 15:00 保存開盤快照；其後每個交易整點重新計算。每個價位是區域估算，不是精確反轉點。</span><span>僅供研究，不構成投資建議。</span></div></main><script>
const snapshots={snapshots_json},groups=[['壓力','pressure','#ff9ca0','STRUCTURE'],['短壓','short_pressure','#f2bd5a','SHORT'],['短撐','short_support','#77a8ff','SHORT'],['支撐','support','#4fe0b5','STRUCTURE']],fmt=n=>Number(n).toLocaleString('zh-TW');
function drawChart(b){{const s=document.querySelector('#chart'),W=760,H=330,p={{l:8,r:58,t:14,b:24}},lo=Math.min(...b.map(x=>x.l)),hi=Math.max(...b.map(x=>x.h)),span=Math.max(1,hi-lo),x=i=>p.l+i*(W-p.l-p.r)/Math.max(1,b.length-1),y=v=>p.t+(hi-v)/span*(H-p.t-p.b);let z='';for(let i=0;i<5;i++){{const yy=p.t+i*(H-p.t-p.b)/4,val=Math.round(hi-i*span/4);z+=`<line x1="0" y1="${{yy}}" x2="${{W}}" y2="${{yy}}" stroke="#202a36"/><text x="${{W-4}}" y="${{yy-5}}" fill="#687572" font-size="11" text-anchor="end">${{fmt(val)}}</text>`}}b.forEach((q,i)=>{{const c=q.c>=q.o?'#4fe0b5':'#ff6b72',xx=x(i),bw=Math.max(4,(W-p.l-p.r)/b.length*.5);z+=`<line x1="${{xx}}" y1="${{y(q.h)}}" x2="${{xx}}" y2="${{y(q.l)}}" stroke="${{c}}"/><rect x="${{xx-bw/2}}" y="${{y(Math.max(q.o,q.c))}}" width="${{bw}}" height="${{Math.max(2,Math.abs(y(q.o)-y(q.c)))}}" rx="1" fill="${{c}}"/>`}});s.innerHTML=z}}
function show(i){{const d=snapshots[i];document.querySelector('#snapshotTime').textContent=`資料截至 ${{d.snapshot_time}}`;document.querySelector('#updatedAt').textContent=`產製時間 ${{d.updated_at}}`;document.querySelector('#contractMeta').textContent=`TX ${{d.contract}} 近月 · ${{d.session_name}} · 最近 K 棒 ${{d.market_time}}`;document.querySelector('#price').textContent=fmt(d.price);document.querySelector('#barHigh').textContent=fmt(d.high);document.querySelector('#barLow').textContent=fmt(d.low);document.querySelector('#barVolume').textContent=fmt(d.volume);document.querySelector('#atr').textContent=fmt(d.atr);document.querySelector('#levels').innerHTML=groups.map(([title,key,color,tag])=>`<section class="group"><div class="head"><h2>${{title}}</h2><span>${{tag}}</span></div>${{d[key].map((v,j)=>`<div class="level" style="--c:${{color}}"><b>${{fmt(v)}}</b><small>${{String(j+1).padStart(2,'0')}}</small></div>`).join('')}}</section>`).join('');drawChart(d.bars)}}
document.querySelector('#historyRows').innerHTML=[...snapshots].reverse().slice(0,30).map((d,j)=>`<tr data-i="${{snapshots.length-1-j}}"><td>${{d.session_name}}</td><td>${{d.snapshot_time}}</td><td>${{fmt(d.price)}}</td><td>${{fmt(d.short_pressure[0])}}</td><td>${{fmt(d.short_support[0])}}</td><td>${{fmt(d.volume)}}</td></tr>`).join('');document.addEventListener('click',e=>{{const target=e.target.closest('[data-i]');if(target)show(Number(target.dataset.i))}});show(snapshots.length-1);
async function pollForUpdate(){{try{{const r=await fetch(`txf_hourly_latest.json?t=${{Date.now()}}`,{{cache:'no-store'}}),latest=await r.json();if(latest.snapshot_time>snapshots[snapshots.length-1].snapshot_time)location.replace(`${{location.pathname}}?v=${{Date.now()}}`)}}catch(e){{}}}}setInterval(pollForUpdate,300000);
</script></body></html>'''


def main() -> None:
    rows = fetch_rows()
    contract, selected = select_contract(rows)
    now = datetime.now(TZ)
    previous = load_json(LATEST, {})
    seed_bars = previous.get("bars", [])
    existing_history = load_json(HISTORY, [])
    known_ids = {item.get("id") for item in existing_history}
    additions = []
    for slot in scheduled_cutoffs(selected, now):
        snapshot_id = f"{slot['trade_date']}|{slot['session']}|{slot['cutoff'].strftime('%H:%M')}"
        if snapshot_id in known_ids:
            continue
        additions.append(build_snapshot(contract, selected, slot, seed_bars, now))
    history = merge_history(existing_history, additions)
    if not history:
        raise RuntimeError("No scheduled TXF snapshot is available yet")
    data = history[-1]
    LATEST.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    HISTORY.write_text(json.dumps(history, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    OUTPUT.write_text(render(data, history), encoding="utf-8")
    print(f"Generated {OUTPUT.name}: TX {contract}, snapshot {data['snapshot_time']}; added {len(additions)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
