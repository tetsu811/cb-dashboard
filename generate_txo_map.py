#!/usr/bin/env python3
"""Build a Taiwan index-option positioning map from official TAIFEX daily data."""

from __future__ import annotations

import json
import math
import csv
from datetime import date, datetime, time, timedelta
from io import StringIO
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "txo_map.html"
LATEST = ROOT / "txo_map_latest.json"
OPTION_API = "https://openapi.taifex.com.tw/v1/DailyMarketReportOpt"
FUTURES_API = "https://openapi.taifex.com.tw/v1/DailyMarketReportFut"
OPTION_REPORT = "https://www.taifex.com.tw/cht/3/optDailyMarketReport"
FUTURES_REPORT = "https://www.taifex.com.tw/cht/3/futDailyMarketReport"
TZ = ZoneInfo("Asia/Taipei")
RISK_FREE = 0.015


def _number(value: object) -> float:
    text = str(value or "").replace(",", "").strip()
    return float(text) if text not in {"", "-"} else 0.0


def _iso_date(value: str) -> str:
    text = str(value).strip()
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _response_records(response: requests.Response) -> list[dict]:
    """TAIFEX may serve the same endpoint as JSON or downloadable CSV."""
    text = response.content.decode("utf-8-sig").lstrip()
    if text.startswith("["):
        return response.json()
    return list(csv.DictReader(StringIO(text)))


def api_rows(dataset: str, data_id: str, start: date, end: date) -> list[dict]:
    """Read the latest official TAIFEX daily files and normalize their fields."""
    if dataset == "TaiwanOptionDaily":
        response = requests.get(OPTION_API, timeout=75)
        response.raise_for_status()
        rows = []
        for row in _response_records(response):
            if (row.get("契約") or row.get("Contract")) != data_id:
                continue
            rows.append({
                "date": _iso_date(row.get("日期") or row.get("Date")),
                "contract_date": row.get("到期月份(週別)") or row.get("ContractMonth(Week)"),
                "strike_price": _number(row.get("履約價") or row.get("StrikePrice")),
                "call_put": "call" if (row.get("買賣權") or row.get("CallPut")) in {"買權", "Call"} else "put",
                "open_interest": int(_number(row.get("未沖銷契約量") or row.get("OpenInterest"))),
                "volume": int(_number(row.get("成交量") or row.get("Volume"))),
                "settlement_price": _number(row.get("結算價") or row.get("SettlementPrice")),
                "close": _number(row.get("最後成交價") or row.get("Close")),
                "trading_session": "position" if (row.get("交易時段") or row.get("TradingSession")) == "一般" else "afterhours",
            })
        return rows
    response = requests.get(FUTURES_API, timeout=75)
    response.raise_for_status()
    rows = []
    for row in _response_records(response):
        if row.get("Contract") != data_id or "/" in str(row.get("ContractMonth(Week)", "")):
            continue
        rows.append({
            "date": _iso_date(row["Date"]),
            "contract_date": row["ContractMonth(Week)"],
            "close": _number(row.get("Last") or row.get("SettlementPrice")),
            "volume": int(_number(row.get("Volume"))),
            "trading_session": "position" if row.get("TradingSession") == "一般" else "afterhours",
        })
    return rows


def report_rows(dataset: str, data_id: str, trade_date: date) -> list[dict]:
    """Fallback to the official daily-report page when OpenAPI lags or is empty."""
    is_option = dataset == "TaiwanOptionDaily"
    url = OPTION_REPORT if is_option else FUTURES_REPORT
    response = requests.get(
        url,
        params={
            "queryDate": trade_date.strftime("%Y/%m/%d"),
            "commodity_id": data_id,
            "marketCode": "0",
            "MarketCode": "0",
        },
        timeout=90,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    marker = "履約價" if is_option else "結算價"
    table = next((table for table in soup.find_all("table") if marker in table.get_text()), None)
    if table is None:
        return []
    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
        if is_option:
            if len(cells) < 20 or cells[0] != data_id:
                continue
            rows.append({
                "date": trade_date.isoformat(),
                "contract_date": cells[1],
                "strike_price": _number(cells[3]),
                "call_put": cells[4].lower(),
                "open_interest": int(_number(cells[15])),
                "volume": int(_number(cells[13])),
                "settlement_price": _number(cells[9]),
                "close": _number(cells[8]),
                "trading_session": "position",
            })
        else:
            if len(cells) < 17 or cells[0] != data_id or "/" in cells[1]:
                continue
            rows.append({
                "date": trade_date.isoformat(),
                "contract_date": cells[1],
                "close": _number(cells[5] or cells[11]),
                "volume": int(_number(cells[9])),
                "trading_session": "position",
            })
    return rows


def expected_report_date() -> date:
    now = datetime.now(TZ)
    candidate = now.date() if now.time() >= time(16, 30) else now.date() - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def snapshot_rows() -> list[dict]:
    return []


def nth_weekday(year: int, month: int, weekday: int, nth: int) -> date:
    first = date(year, month, 1)
    shift = (weekday - first.weekday()) % 7
    return first + timedelta(days=shift + 7 * (nth - 1))


def expiry_of(contract: str) -> date:
    year, month = int(contract[:4]), int(contract[4:6])
    suffix = contract[6:]
    if suffix.startswith("W"):
        return nth_weekday(year, month, 2, int(suffix[1:]))
    if suffix.startswith("F"):
        return nth_weekday(year, month, 4, int(suffix[1:]))
    return nth_weekday(year, month, 2, 3)


def contract_label(contract: str) -> str:
    suffix = contract[6:]
    if suffix.startswith("W"):
        return f"週三選 {int(suffix[1:])}"
    if suffix.startswith("F"):
        return f"週五選 {int(suffix[1:])}"
    return f"月選 {contract[4:6]}"


def norm_cdf(value: float) -> float:
    return 0.5 * (1 + math.erf(value / math.sqrt(2)))


def option_price(spot: float, strike: float, years: float, vol: float, kind: str) -> float:
    if years <= 0 or vol <= 0:
        return max(0.0, spot - strike) if kind == "call" else max(0.0, strike - spot)
    root_t = math.sqrt(years)
    d1 = (math.log(spot / strike) + (RISK_FREE + 0.5 * vol * vol) * years) / (vol * root_t)
    d2 = d1 - vol * root_t
    if kind == "call":
        return spot * norm_cdf(d1) - strike * math.exp(-RISK_FREE * years) * norm_cdf(d2)
    return strike * math.exp(-RISK_FREE * years) * norm_cdf(-d2) - spot * norm_cdf(-d1)


def implied_vol(price: float, spot: float, strike: float, years: float, kind: str) -> float | None:
    intrinsic = max(0.0, spot - strike) if kind == "call" else max(0.0, strike - spot)
    if price <= intrinsic + 0.02 or price <= 0 or years <= 0:
        return None
    low, high = 0.02, 4.0
    for _ in range(60):
        mid = (low + high) / 2
        if option_price(spot, strike, years, mid, kind) > price:
            high = mid
        else:
            low = mid
    value = (low + high) / 2
    return value if 0.025 <= value <= 3.5 else None


def gamma(spot: float, strike: float, years: float, vol: float) -> float:
    root_t = math.sqrt(max(years, 1e-8))
    d1 = (math.log(spot / strike) + (RISK_FREE + 0.5 * vol * vol) * years) / (vol * root_t)
    density = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
    return density / (spot * vol * root_t)


def delta(spot: float, strike: float, years: float, vol: float, kind: str) -> float:
    """Black-Scholes delta used only for the clearly labelled OI scenario model."""
    root_t = math.sqrt(max(years, 1e-8))
    d1 = (math.log(spot / strike) + (RISK_FREE + 0.5 * vol * vol) * years) / (vol * root_t)
    call_delta = norm_cdf(d1)
    return call_delta if kind == "call" else call_delta - 1


def current_price(futures: list[dict], option_date: str) -> tuple[float, str]:
    eligible = [r for r in futures if r.get("date") == option_date and r.get("trading_session") == "position"]
    if not eligible:
        eligible = [r for r in futures if r.get("date") == option_date]
    eligible = [r for r in eligible if float(r.get("close") or 0) > 0]
    eligible.sort(key=lambda r: float(r.get("volume") or 0), reverse=True)
    if not eligible:
        raise RuntimeError("No matching TX futures close")
    return float(eligible[0]["close"]), str(eligible[0].get("contract_date") or "")


def live_price(rows: list[dict]) -> tuple[float | None, str | None]:
    usable = [r for r in rows if float(r.get("close") or 0) > 0 and str(r.get("date") or "")]
    if not usable:
        return None, None
    latest_time = max(str(r["date"]) for r in usable)
    latest = [r for r in usable if str(r["date"]) == latest_time]
    # The median of active near-ATM call+put strikes is not an underlying price;
    # use snapshot only to report freshness. Intraday position is supplied by TX futures tooling.
    return None, latest_time


def make_contract(rows: list[dict], contract: str, spot: float, option_day: date) -> dict:
    selected = [r for r in rows if r.get("contract_date") == contract and r.get("trading_session") == "position"]
    expiry = expiry_of(contract)
    years = max((datetime.combine(expiry, time(13, 30), TZ) - datetime.combine(option_day, time(13, 30), TZ)).total_seconds() / (365 * 86400), 1 / (365 * 24))
    by_strike: dict[float, dict] = {}
    model_rows: list[tuple[float, str, int, float]] = []
    for row in selected:
        strike = float(row["strike_price"])
        kind = str(row["call_put"]).lower()
        item = by_strike.setdefault(strike, {"strike": strike, "call_oi": 0, "put_oi": 0, "call_vol": 0, "put_vol": 0, "call_gamma": 0.0, "put_gamma": 0.0, "call_delta": 0.0, "put_delta": 0.0})
        oi = int(float(row.get("open_interest") or 0))
        volume = int(float(row.get("volume") or 0))
        price = float(row.get("settlement_price") or row.get("close") or 0)
        if kind == "call":
            item["call_oi"] += oi
            item["call_vol"] += volume
        else:
            item["put_oi"] += oi
            item["put_vol"] += volume
        iv = implied_vol(price, spot, strike, years, kind)
        if iv is not None and oi:
            model_rows.append((strike, kind, oi, iv))
            exposure = gamma(spot, strike, years, iv) * oi * spot * spot * 50 / 100
            item[f"{kind}_gamma"] += exposure
            delta_exposure = delta(spot, strike, years, iv, kind) * oi * spot * 50
            item[f"{kind}_delta"] += delta_exposure

    values = sorted(by_strike.values(), key=lambda x: x["strike"])
    if not values:
        raise ValueError(f"{contract} has no position rows")
    visible = [x for x in values if abs(x["strike"] - spot) <= max(1800, spot * 0.045)]
    if len(visible) < 12:
        visible = values
    call_wall = max(values, key=lambda x: x["call_oi"])["strike"]
    put_wall = max(values, key=lambda x: x["put_oi"])["strike"]
    for item in values:
        item["net_gamma"] = item["call_gamma"] - item["put_gamma"]
        item["net_delta"] = item["call_delta"] + item["put_delta"]

    def total_gamma(test_spot: float) -> float:
        total = 0.0
        for strike, kind, oi, iv in model_rows:
            sign = 1 if kind == "call" else -1
            total += sign * gamma(test_spot, strike, years, iv) * oi * test_spot * test_spot * 50 / 100
        return total

    grid = sorted({x["strike"] for x in values if abs(x["strike"] - spot) <= spot * 0.06})
    crossings: list[float] = []
    for left, right in zip(grid, grid[1:]):
        gl, gr = total_gamma(left), total_gamma(right)
        if gl == 0:
            crossings.append(left)
        elif gl * gr < 0:
            crossings.append(left + (right - left) * abs(gl) / (abs(gl) + abs(gr)))
    gamma_flip = min(crossings, key=lambda x: abs(x - spot)) if crossings else None
    call_oi = sum(x["call_oi"] for x in values)
    put_oi = sum(x["put_oi"] for x in values)
    call_vol = sum(x["call_vol"] for x in values)
    put_vol = sum(x["put_vol"] for x in values)
    atm = min(values, key=lambda x: abs(x["strike"] - spot))
    atm_rows = [r for r in selected if float(r["strike_price"]) == atm["strike"]]
    atm_prices = {str(r["call_put"]).lower(): float(r.get("settlement_price") or r.get("close") or 0) for r in atm_rows}
    expected = atm_prices.get("call", 0) + atm_prices.get("put", 0)
    call_dex = sum(x["call_delta"] for x in values)
    put_dex = abs(sum(x["put_delta"] for x in values))
    net_dex = call_dex - put_dex
    gross_dex = call_dex + put_dex
    call_gex = sum(x["call_gamma"] for x in values)
    put_gex = sum(x["put_gamma"] for x in values)
    net_gex = call_gex - put_gex
    gross_gex = call_gex + put_gex
    delta_bias = 100 * net_dex / gross_dex if gross_dex else 0
    gamma_bias = 100 * net_gex / gross_gex if gross_gex else 0

    if net_gex >= 0:
        volatility_state = "較偏震盪收斂"
        volatility_copy = "在目前的持倉方向假設下，價格變動後的避險需求較可能抵銷部分波動。"
    else:
        volatility_state = "較偏波動放大"
        volatility_copy = "在目前的持倉方向假設下，價格變動後的避險需求較可能順勢增加波動。"
    if abs(delta_bias) < 8:
        direction_state = "方向大致平衡"
        direction_copy = "Call 與 Put 的 Delta 加權結果接近，沒有明顯單邊傾向。"
    elif delta_bias > 0:
        direction_state = "Call 側權重較高"
        direction_copy = "Delta 加權後較偏 Call 側，但這不等於市場一定上漲。"
    else:
        direction_state = "Put 側權重較高"
        direction_copy = "Delta 加權後較偏 Put 側，但這不等於市場一定下跌。"

    if spot >= call_wall:
        structure_state = "站上 Call Wall"
        structure_copy = "價格高於最大 Call 未平倉履約價，觀察牆位能否轉為支撐。"
    elif spot <= put_wall:
        structure_state = "跌破 Put Wall"
        structure_copy = "價格低於最大 Put 未平倉履約價，留意波動是否擴大。"
    else:
        structure_state = "位於主要牆位之間"
        structure_copy = "價格仍在主要 Put Wall 與 Call Wall 之間。"
    return {
        "contract": contract,
        "label": contract_label(contract),
        "expiry": expiry.isoformat(),
        "days": max(0, (expiry - option_day).days),
        "call_wall": round(call_wall),
        "put_wall": round(put_wall),
        "gamma_flip": round(gamma_flip) if gamma_flip is not None else None,
        "oi_pcr": round(put_oi / call_oi, 3) if call_oi else None,
        "volume_pcr": round(put_vol / call_vol, 3) if call_vol else None,
        "expected_move": round(expected),
        "net_dex": round(net_dex),
        "gross_dex": round(gross_dex),
        "delta_bias": round(delta_bias, 1),
        "net_gex": round(net_gex),
        "gross_gex": round(gross_gex),
        "gamma_bias": round(gamma_bias, 1),
        "structure_state": structure_state,
        "structure_copy": structure_copy,
        "volatility_state": volatility_state,
        "volatility_copy": volatility_copy,
        "direction_state": direction_state,
        "direction_copy": direction_copy,
        "call_oi": call_oi,
        "put_oi": put_oi,
        "strikes": [{k: (round(v, 2) if isinstance(v, float) else v) for k, v in x.items()} for x in visible],
    }


def build() -> dict:
    today = datetime.now(TZ).date()
    start = today - timedelta(days=10)
    options = api_rows("TaiwanOptionDaily", "TXO", start, today + timedelta(days=1))
    expected = expected_report_date()
    if not options or max(str(r["date"]) for r in options) < expected.isoformat():
        fallback = report_rows("TaiwanOptionDaily", "TXO", expected)
        if fallback:
            options = fallback
    if not options:
        raise RuntimeError("TAIFEX returned no TXO daily rows")
    option_date = max(str(r["date"]) for r in options)
    option_day = date.fromisoformat(option_date)
    options = [r for r in options if r.get("date") == option_date]
    futures = api_rows("TaiwanFuturesDaily", "TX", start, today + timedelta(days=1))
    if not futures or max(str(r["date"]) for r in futures) < option_date:
        fallback = report_rows("TaiwanFuturesDaily", "TX", option_day)
        if fallback:
            futures = fallback
    spot, futures_contract = current_price(futures, option_date)
    snapshots = snapshot_rows()
    _, quote_time = live_price(snapshots)
    contracts = sorted({str(r["contract_date"]) for r in options}, key=expiry_of)
    # Expired same-day series are removed after the close; begin with the next live expiry.
    contracts = [c for c in contracts if expiry_of(c) > option_day][:5]
    built_contracts = []
    skipped_contracts = []
    for contract in contracts:
        try:
            built_contracts.append(make_contract(options, contract, spot, option_day))
        except ValueError as exc:
            skipped_contracts.append({"contract": contract, "reason": str(exc)})
    if not built_contracts:
        raise RuntimeError(f"No usable TXO contracts for {option_date}; skipped {len(skipped_contracts)}")
    payload = {
        "as_of": option_date,
        "generated_at": datetime.now(TZ).isoformat(timespec="minutes"),
        "quote_time": quote_time,
        "spot": round(spot),
        "futures_contract": futures_contract,
        "contracts": built_contracts,
        "skipped_contracts": skipped_contracts,
        "method_note": "OI、成交量與權利金來自期交所官方日行情；Delta、Gamma、DEX、GEX 與中性位置為模型計算。",
    }
    LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def render(payload: dict) -> None:
    data = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    html = r'''<!doctype html>
<html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>台指選擇權結構地圖</title>
<style>
:root{--bg:#07100f;--panel:#101a1d;--line:#25363a;--text:#eef4f0;--muted:#899b96;--mint:#50dfba;--red:#ff7d86;--amber:#ffc85e;--blue:#75a8ff;--ink:#07100f}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 70% 0,#0e2721 0,transparent 36%),var(--bg);color:var(--text);font-family:Inter,"Noto Sans TC",system-ui,sans-serif}.wrap{max-width:1500px;margin:auto;padding:34px}.eyebrow{color:var(--mint);font-weight:850;letter-spacing:.18em;font-size:13px}.beta{display:inline-flex;margin-left:10px;padding:4px 8px;border:1px solid var(--amber);border-radius:999px;color:var(--amber);font-size:11px;letter-spacing:.08em;vertical-align:middle}.head{display:flex;align-items:flex-end;justify-content:space-between;gap:28px;margin:10px 0 24px}.head h1{font-size:clamp(34px,5vw,66px);line-height:1;margin:0}.fresh{text-align:right;color:var(--muted);line-height:1.65}.fresh b{color:var(--text)}.fresh .rule{display:block;margin-top:5px;color:#b9c8c3;font-size:13px}.switches{display:flex;gap:10px;overflow:auto;padding:3px 0 18px}.switch{border:1px solid var(--line);background:#0d171a;color:var(--muted);border-radius:14px;padding:13px 18px;min-width:132px;text-align:left;cursor:pointer}.switch strong{display:block;color:var(--text);font-size:16px}.switch.active{border-color:var(--mint);background:#12352c;color:#b9eada;box-shadow:0 0 0 1px #50dfba55}.grid{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(350px,.7fr);gap:20px}.panel{background:linear-gradient(145deg,#101b20,#0c1418);border:1px solid var(--line);border-radius:24px;padding:28px}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:18px}.metric{border:1px solid var(--line);border-radius:15px;padding:15px;background:#0b1417}.metric span{color:var(--muted);font-size:12px}.metric b{font-size:23px;display:block;margin-top:7px}.metric small{display:block;color:var(--muted);font-size:12px;line-height:1.45;margin-top:7px}.metric.red b{color:var(--red)}.metric.mint b{color:var(--mint)}.metric.amber b{color:var(--amber)}.map-head{display:flex;justify-content:space-between;color:var(--muted);margin-bottom:18px}.chart{min-height:590px;position:relative}.row{display:grid;grid-template-columns:70px 1fr 95px 1fr;gap:10px;align-items:center;height:38px;border-top:1px solid #ffffff08}.strike{font-variant-numeric:tabular-nums;color:#aab9b4}.bar{height:16px;position:relative}.bar i{display:block;height:100%;border-radius:4px}.call{text-align:right}.call i{background:linear-gradient(90deg,#175848,var(--mint));margin-left:auto}.put i{background:linear-gradient(90deg,var(--red),#67252d)}.mid{text-align:center;font-weight:800;font-variant-numeric:tabular-nums}.row.spot .mid{color:var(--ink);background:var(--mint);padding:5px 8px;border-radius:7px}.row.wall-call{background:#50dfba0d}.row.wall-put{background:#ff7d860d}.legend{display:flex;gap:18px;flex-wrap:wrap;color:var(--muted);font-size:13px;margin-top:18px}.dot{width:9px;height:9px;display:inline-block;border-radius:50%;margin-right:6px}.reading h2{margin:0 0 20px}.state{border-radius:18px;padding:20px;background:#dff9a8;color:#163326;margin-bottom:18px}.state small{display:block;opacity:.7}.state b{display:block;font-size:26px;margin-top:4px}.levels{display:grid;gap:10px}.level{border-bottom:1px solid var(--line);padding:13px 0;display:flex;justify-content:space-between;gap:16px}.level span{color:var(--muted)}.level b{font-size:22px;text-align:right}.explain{margin-top:20px;padding:18px;border:1px solid var(--line);border-radius:16px;color:#c9d4d0;line-height:1.7}.explain strong{color:var(--text)}.learn{grid-column:1/-1;display:grid;grid-template-columns:1fr 1fr;gap:20px}.learn-card{background:linear-gradient(145deg,#111e21,#0c1518);border:1px solid var(--line);border-radius:22px;padding:24px}.learn-card h3{font-size:21px;margin:0 0 8px}.learn-card .plain{color:var(--text);font-size:17px;line-height:1.65;margin:0 0 16px}.formula{background:#07100f;border:1px solid #314448;border-radius:13px;padding:14px 16px;color:#dce8e4;font-variant-numeric:tabular-nums;line-height:1.65}.formula b{color:var(--amber)}.guide{display:grid;gap:8px;margin-top:14px}.guide div{display:grid;grid-template-columns:72px 1fr;gap:12px;color:#c4d0cc;line-height:1.55}.guide strong{color:var(--text)}.caution{color:var(--muted);font-size:13px;line-height:1.6;margin:14px 0 0}.foot{display:flex;justify-content:space-between;gap:30px;color:var(--muted);font-size:13px;margin-top:20px}.empty{padding:60px;text-align:center;color:var(--muted)}@media(max-width:900px){.wrap{padding:20px}.head{align-items:flex-start;flex-direction:column}.fresh{text-align:left}.grid{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(2,1fr)}.panel{padding:18px}.chart{min-height:auto}.row{grid-template-columns:58px 1fr 78px 1fr}.learn{grid-template-columns:1fr}.foot{flex-direction:column}}@media(max-width:520px){.metrics{grid-template-columns:1fr}.guide div{grid-template-columns:62px 1fr}}
.greeks{grid-column:1/-1}.greeks h2{margin:0 0 6px}.greeks-intro{color:var(--muted);margin:0 0 20px;line-height:1.6}.greek-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}.greek-card{background:#0b1417;border:1px solid var(--line);border-radius:18px;padding:20px}.greek-top{display:flex;justify-content:space-between;gap:18px}.greek-top span{color:var(--muted);font-size:13px}.greek-top b{display:block;font-size:24px;margin-top:4px}.greek-copy{color:#c9d4d0;line-height:1.65;margin:14px 0}.meter{height:12px;border-radius:999px;background:linear-gradient(90deg,var(--red),#3b4242 50%,var(--mint));position:relative;margin:18px 0 8px}.meter i{position:absolute;top:50%;width:18px;height:18px;border-radius:50%;background:#fff;border:4px solid var(--amber);transform:translate(-50%,-50%);box-shadow:0 0 0 3px #0007}.meter-labels{display:flex;justify-content:space-between;color:var(--muted);font-size:12px}.source-key{grid-column:1/-1;display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:2px}.source-key div{border:1px solid var(--line);border-radius:14px;padding:14px;color:#cbd6d2;line-height:1.5}.source-key b{display:block;color:var(--text);margin-bottom:4px}.source-key .real{border-color:#50dfba66}.source-key .model{border-color:#ffc85e66}.source-key .assume{border-color:#75a8ff66}@media(max-width:900px){.greek-grid,.source-key{grid-template-columns:1fr}}
</style></head><body><main class="wrap">
<div class="eyebrow">TXO · POSITIONING MAP <span class="beta">BETA</span></div><div class="head"><h1>台指選擇權<br>結構地圖</h1><div class="fresh"><b>盤後未平倉結構</b><br>OI／權利金資料：<span id="asof"></span> 日盤收盤<br>指數基準：TX <span id="futureContract"></span> 同日收盤<br>頁面產製：<span id="generated"></span><span class="rule">交易日盤後更新；夜盤沿用最近確認的盤後結構</span></div></div>
<div class="switches" id="switches"></div><div class="grid"><section class="panel"><div class="metrics" id="metrics"></div><div class="map-head"><b>Put OI</b><span>履約價與未平倉量</span><b>Call OI</b></div><div class="chart" id="chart"></div><div class="legend"><span><i class="dot" style="background:var(--red)"></i>Put 未平倉</span><span><i class="dot" style="background:var(--mint)"></i>Call 未平倉</span><span>條長只比較目前所選契約</span></div></section><aside class="panel reading"><h2>現在怎麼讀？</h2><div class="state" id="state"></div><div class="levels" id="levels"></div><div class="explain" id="explain"></div></aside><section class="panel greeks"><h2>Delta × Gamma：先看方向，再看波動</h2><p class="greeks-intro">Delta 像方向盤，描述選擇權對指數漲跌有多敏感；Gamma 像方向盤變靈敏的速度，描述 Delta 會變多快。下面把全部未平倉量加權，幫你看整體結構。</p><div class="greek-grid"><article class="greek-card" id="deltaCard"></article><article class="greek-card" id="gammaCard"></article><div class="source-key"><div class="real"><b>實際市場資料</b>履約價、未平倉量、成交量、權利金。</div><div class="model"><b>模型計算</b>IV、Delta、Gamma、DEX、GEX 與 Gamma 中性位置。</div><div class="assume"><b>情境假設</b>公開資料沒有持有人多空方向，因此結果是結構觀察，不是造市商真實部位。</div></div></div></section><section class="learn" aria-label="選擇權名詞說明"><article class="learn-card"><h3>Delta、DEX 是什麼？</h3><p class="plain"><strong>Delta</strong> 可以想成「指數動 1 點，選擇權價格大約跟著動多少」。<strong>DEX</strong> 則把每個履約價的 Delta 乘上未平倉量後加總。</p><div class="formula" id="deltaFormula"></div><p class="caution">正值代表模型下 Call 側權重較大，負值代表 Put 側權重較大。這是敏感度結構，不是漲跌預測。</p></article><article class="learn-card"><h3>Gamma、GEX 是什麼？</h3><p class="plain"><strong>Gamma</strong> 描述 Delta 改變的速度。<strong>GEX</strong> 把 Gamma 與未平倉量加總，用來觀察避險交易可能讓市場較穩定，或讓波動被放大。</p><div class="formula" id="gammaFormula"></div><p class="caution">此處以 Call 正向、Put 負向作情境計算。交易所沒有公開造市商的實際多空部位。</p></article><article class="learn-card"><h3>跨式預期幅度是什麼？</h3><p class="plain">把最接近目前指數的 Call 與 Put 權利金相加，當成市場為「到期前可能波動」開出的價格。</p><div class="formula" id="straddleFormula"></div><p class="caution">這是權利金反映的估算區間，不是保證會到、也不是統計上的必然範圍。實際損益還會受進場價格、時間價值與交易成本影響。</p></article><article class="learn-card"><h3>OI P/C 是什麼？</h3><p class="plain"><strong>OI</strong> 是尚未平倉、仍留在市場中的契約數；<strong>P/C</strong> 是 Put 除以 Call。</p><div class="formula" id="pcrFormula"></div><div class="guide"><div><strong>大於 1</strong><span>Put 未平倉比 Call 多。</span></div><div><strong>等於 1</strong><span>Put 與 Call 未平倉大致相當。</span></div><div><strong>小於 1</strong><span>Call 未平倉比 Put 多。</span></div></div><p class="caution">未平倉量包含買方、賣方與避險部位，所以 P/C 高不一定直接等於看空，低也不一定直接等於看多；要搭配價格位置與牆位一起看。</p></article></section></div><div class="foot"><span id="method"></span><span>區域估算，並非精確反轉點；不構成投資建議。</span></div></main>
<script>const DATA=__DATA__;let active=0;const nf=new Intl.NumberFormat('zh-TW');
function n(v){return v==null?'—':nf.format(v)}
function render(){const c=DATA.contracts[active];document.getElementById('asof').textContent=DATA.as_of;document.getElementById('futureContract').textContent=DATA.futures_contract;document.getElementById('generated').textContent=DATA.generated_at.replace('T',' ');document.getElementById('method').textContent=DATA.method_note;
document.getElementById('switches').innerHTML=DATA.contracts.map((x,i)=>`<button class="switch ${i===active?'active':''}" onclick="active=${i};render()"><strong>${x.label}</strong>${x.expiry} · ${x.days}天</button>`).join('');
document.getElementById('metrics').innerHTML=`<div class="metric red"><span>PUT WALL</span><b>${n(c.put_wall)}</b></div><div class="metric mint"><span>CALL WALL</span><b>${n(c.call_wall)}</b></div><div class="metric amber"><span>OI P/C</span><b>${c.oi_pcr??'—'}</b><small>Put OI ÷ Call OI</small></div><div class="metric"><span>跨式預期幅度</span><b>±${n(c.expected_move)} 點</b><small>價平 Call + Put 權利金</small></div>`;
const closest=c.strikes.reduce((a,b)=>Math.abs(b.strike-DATA.spot)<Math.abs(a.strike-DATA.spot)?b:a);const ci=c.strikes.findIndex(x=>x.strike===closest.strike);const plot=c.strikes.slice(Math.max(0,ci-12),Math.min(c.strikes.length,ci+13));const max=Math.max(...plot.flatMap(x=>[x.call_oi,x.put_oi]),1);document.getElementById('chart').innerHTML=plot.slice().reverse().map(x=>{const cls=[x.strike===c.call_wall?'wall-call':'',x.strike===c.put_wall?'wall-put':'',x.strike===closest.strike?'spot':''].join(' ');return `<div class="row ${cls}"><span class="strike">${n(x.put_oi)}</span><div class="bar put"><i style="width:${Math.max(1,x.put_oi/max*100)}%"></i></div><div class="mid">${n(x.strike)}</div><div class="bar call"><i style="width:${Math.max(1,x.call_oi/max*100)}%"></i></div></div>`}).join('');
const state=c.structure_state;document.getElementById('state').innerHTML=`<small>TX ${DATA.futures_contract} · OI 同期收盤</small><b>${n(DATA.spot)} · ${state}</b>`;document.getElementById('levels').innerHTML=`<div class="level"><span>上方主要牆位</span><b style="color:var(--mint)">${n(c.call_wall)}</b></div><div class="level"><span>Gamma 中性位置估算</span><b style="color:var(--amber)">${n(c.gamma_flip)}</b></div><div class="level"><span>下方主要牆位</span><b style="color:var(--red)">${n(c.put_wall)}</b></div><div class="level"><span>跨式推算區間</span><b>${n(DATA.spot-c.expected_move)}–${n(DATA.spot+c.expected_move)}</b></div>`;document.getElementById('explain').innerHTML=`<strong>${state}</strong><br>${c.structure_copy}<br><br><strong>${c.direction_state}</strong><br>${c.direction_copy}<br><br><strong>${c.volatility_state}</strong><br>${c.volatility_copy}`;
const deltaPos=Math.max(0,Math.min(100,(c.delta_bias+100)/2));const gammaPos=Math.max(0,Math.min(100,(c.gamma_bias+100)/2));document.getElementById('deltaCard').innerHTML=`<div class="greek-top"><div><span>方向敏感度 · DEX</span><b>${c.direction_state}</b></div><strong>${c.delta_bias>0?'+':''}${c.delta_bias}%</strong></div><p class="greek-copy">${c.direction_copy}</p><div class="meter"><i style="left:${deltaPos}%"></i></div><div class="meter-labels"><span>Put 側較重</span><span>平衡</span><span>Call 側較重</span></div>`;document.getElementById('gammaCard').innerHTML=`<div class="greek-top"><div><span>波動敏感度 · GEX</span><b>${c.volatility_state}</b></div><strong>${c.gamma_bias>0?'+':''}${c.gamma_bias}%</strong></div><p class="greek-copy">${c.volatility_copy}</p><div class="meter"><i style="left:${gammaPos}%"></i></div><div class="meter-labels"><span>波動較易放大</span><span>中性</span><span>波動較易收斂</span></div>`;
document.getElementById('deltaFormula').innerHTML=`DEX 結構指標：<b>${c.delta_bias>0?'+':''}${c.delta_bias}%</b><br>正值偏 Call 側，負值偏 Put 側；越接近 0 代表兩邊越平衡。`;
document.getElementById('gammaFormula').innerHTML=`GEX 情境指標：<b>${c.gamma_bias>0?'+':''}${c.gamma_bias}%</b><br>正值較偏波動收斂，負值較偏波動放大；這不是損益金額。`;
document.getElementById('straddleFormula').innerHTML=`目前指數 ${n(DATA.spot)} ± 權利金合計 <b>${n(c.expected_move)} 點</b><br>推算區間：<b>${n(DATA.spot-c.expected_move)} ～ ${n(DATA.spot+c.expected_move)}</b>`;
const pcrTone=c.oi_pcr==null?'暫無數值':c.oi_pcr>1?'Put OI 較多':c.oi_pcr<1?'Call OI 較多':'兩邊大致相當';document.getElementById('pcrFormula').innerHTML=`Put OI ${n(c.put_oi)} ÷ Call OI ${n(c.call_oi)}<br>＝ <b>${c.oi_pcr??'—'}</b>（${pcrTone}）`;}
render();
async function pollForUpdate(){try{const r=await fetch(`txo_map_latest.json?t=${Date.now()}`,{cache:'no-store'}),latest=await r.json();if((latest.generated_at||'')>(DATA.generated_at||''))location.replace(`${location.pathname}?v=${Date.now()}`)}catch(e){}}
setInterval(pollForUpdate,60000);pollForUpdate();</script></body></html>'''.replace('__DATA__', data)
    OUTPUT.write_text(html, encoding="utf-8")


if __name__ == "__main__":
    result = build()
    render(result)
    print(f"wrote {OUTPUT.name}: {result['as_of']} / {len(result['contracts'])} contracts")
