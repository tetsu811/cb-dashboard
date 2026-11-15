#!/usr/bin/env python3
"""Build the Taiwan day-trading heat dashboard dataset from official sources."""

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import json
import math
import re
import ssl
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


BASE = Path(__file__).resolve().parent
OUT_PATH = BASE / "day_trading_ratio_latest.json"
STATE_PATH = BASE / "day_trading_ratio_state.json.gz"
UA = "Mozilla/5.0 (compatible; TetsuDayTradingDashboard/1.0)"
TIMEOUT = 35

try:
    import certifi

    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CONTEXT = ssl.create_default_context()


def fetch_json(url: str, attempts: int = 3):
    last_error = None
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": UA, "Accept": "application/json,text/plain,*/*"},
            )
            with urllib.request.urlopen(request, timeout=TIMEOUT, context=SSL_CONTEXT) as response:
                return json.loads(response.read().decode("utf-8-sig"))
        except Exception as exc:  # noqa: BLE001 - retry official endpoints uniformly
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(1.2 * (attempt + 1))
    raise RuntimeError(f"fetch failed: {url}: {last_error}")


def number(value, default=None):
    if value is None:
        return default
    text = str(value).strip().replace(",", "").replace("%", "")
    if text in {"", "-", "--", "---", "除權", "除息"}:
        return default
    text = text.replace("X", "").replace("x", "")
    try:
        return float(text)
    except ValueError:
        return default


def integer(value, default=0):
    parsed = number(value, None)
    return default if parsed is None else int(parsed)


def roc_date(value: str) -> date:
    digits = re.sub(r"\D", "", str(value))
    if len(digits) != 7:
        raise ValueError(f"unexpected ROC date: {value}")
    return date(int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:7]))


def roc_slash(day: date) -> str:
    return f"{day.year - 1911}/{day:%m/%d}"


def tpex_intraday_url(day: date) -> str:
    return (
        "https://www.tpex.org.tw/web/stock/trading/intraday_stat/"
        "intraday_trading_stat_result.php?"
        + urllib.parse.urlencode(
            {"l": "zh-tw", "d": roc_slash(day), "s": "0,asc,0", "o": "json"}
        )
    )


def market_stats(row):
    buy = integer(row[2])
    buy_pct = number(row[3], 0)
    sell = integer(row[4])
    sell_pct = number(row[5], 0)
    daytrade = round((buy + sell) / 2)
    totals = []
    if buy > 0 and buy_pct > 0:
        totals.append(buy / (buy_pct / 100))
    if sell > 0 and sell_pct > 0:
        totals.append(sell / (sell_pct / 100))
    turnover = round(sum(totals) / len(totals)) if totals else 0
    ratio = daytrade / turnover * 100 if turnover else None
    return {
        "daytrade_value": daytrade,
        "turnover": turnover,
        "ratio": round(ratio, 2) if ratio is not None else None,
        "buy_value": buy,
        "sell_value": sell,
    }


def asset_type(code: str, name: str) -> str:
    code = code.strip()
    name = name.strip()
    if re.fullmatch(r"\d{4}", code) and not code.startswith("0") and "-DR" not in name:
        return "stock"
    if code.startswith(("00", "01")) and re.fullmatch(r"[0-9A-Z]{4,6}", code):
        return "etf"
    return "other"


def pct_change(close, delta):
    if close is None or delta is None:
        return None
    previous = close - delta
    if previous <= 0:
        return None
    return round(delta / previous * 100, 2)


def find_table(payload, field_name: str):
    for table in payload.get("tables", []):
        if field_name in (table.get("fields") or []):
            return table
    return None


def fetch_twse(day: date):
    compact = day.strftime("%Y%m%d")
    intra_url = (
        "https://www.twse.com.tw/exchangeReport/TWTB4U?"
        + urllib.parse.urlencode({"response": "json", "date": compact, "selectType": "All"})
    )
    quote_url = (
        "https://www.twse.com.tw/exchangeReport/MI_INDEX?"
        + urllib.parse.urlencode({"response": "json", "date": compact, "type": "ALLBUT0999"})
    )
    intra = fetch_json(intra_url)
    if intra.get("stat") != "OK" or len(intra.get("tables", [])) < 2:
        return None
    market_table, stock_table = intra["tables"][0], intra["tables"][1]
    if not market_table.get("data") or not stock_table.get("data"):
        return None
    quotes = fetch_json(quote_url)
    quote_table = find_table(quotes, "證券代號")
    if not quote_table or not quote_table.get("data"):
        return None
    quote_fields = quote_table["fields"]
    qi = {name: quote_fields.index(name) for name in quote_fields}
    quote_map = {}
    for row in quote_table["data"]:
        code = str(row[qi["證券代號"]]).strip()
        close = number(row[qi["收盤價"]])
        delta = number(row[qi["漲跌價差"]])
        sign = str(row[qi["漲跌(+/-)"]])
        if delta is not None and ("-" in sign or "−" in sign):
            delta = -abs(delta)
        quote_map[code] = {
            "turnover": integer(row[qi["成交金額"]]),
            "close": close,
            "change_pct": pct_change(close, delta),
        }
    stocks = []
    for row in stock_table["data"]:
        code, name = str(row[0]).strip(), str(row[1]).strip()
        quote = quote_map.get(code)
        if not quote or not quote["turnover"]:
            continue
        daytrade = round((integer(row[4]) + integer(row[5])) / 2)
        kind = asset_type(code, name)
        if kind == "other":
            continue
        stocks.append(
            {
                "code": code,
                "name": name,
                "market": "TWSE",
                "asset_type": kind,
                "close": quote["close"],
                "change_pct": quote["change_pct"],
                "turnover": quote["turnover"],
                "daytrade_value": daytrade,
                "ratio": round(daytrade / quote["turnover"] * 100, 2),
            }
        )
    return {"date": day.isoformat(), "market": market_stats(market_table["data"][0]), "stocks": stocks}


def fetch_tpex(day: date):
    roc = roc_slash(day)
    intra_url = tpex_intraday_url(day)
    quote_url = (
        "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/"
        "stk_wn1430_result.php?"
        + urllib.parse.urlencode({"l": "zh-tw", "d": roc, "se": "EW", "o": "json"})
    )
    intra = fetch_json(intra_url)
    if len(intra.get("tables", [])) < 2:
        return None
    market_table, stock_table = intra["tables"][0], intra["tables"][1]
    if not market_table.get("data") or not stock_table.get("data"):
        return None
    quotes = fetch_json(quote_url)
    if not quotes.get("tables") or not quotes["tables"][0].get("data"):
        return None
    quote_table = quotes["tables"][0]
    quote_map = {}
    for row in quote_table["data"]:
        code = str(row[0]).strip()
        close = number(row[2])
        delta = number(row[3])
        quote_map[code] = {
            "turnover": integer(row[8]),
            "close": close,
            "change_pct": pct_change(close, delta),
        }
    stocks = []
    for row in stock_table["data"]:
        code, name = str(row[0]).strip(), str(row[1]).strip()
        quote = quote_map.get(code)
        if not quote or not quote["turnover"]:
            continue
        daytrade = round((integer(row[3]) + integer(row[4])) / 2)
        kind = asset_type(code, name)
        if kind == "other":
            continue
        stocks.append(
            {
                "code": code,
                "name": name,
                "market": "TPEX",
                "asset_type": kind,
                "close": quote["close"],
                "change_pct": quote["change_pct"],
                "turnover": quote["turnover"],
                "daytrade_value": daytrade,
                "ratio": round(daytrade / quote["turnover"] * 100, 2),
            }
        )
    return {"date": day.isoformat(), "market": market_stats(market_table["data"][0]), "stocks": stocks}


def fetch_tpex_market(day: date):
    try:
        payload = fetch_json(tpex_intraday_url(day))
        if not payload.get("tables") or not payload["tables"][0].get("data"):
            return None
        return {"date": day.isoformat(), **market_stats(payload["tables"][0]["data"][0])}
    except Exception as exc:  # noqa: BLE001
        print(f"skip TPEx market {day}: {exc}")
        return None


def fetch_day(day: date):
    try:
        twse = fetch_twse(day)
        tpex = fetch_tpex(day)
        if not twse or not tpex:
            return None
        return {"date": day.isoformat(), "TWSE": twse, "TPEX": tpex}
    except Exception as exc:  # noqa: BLE001 - a missing day should not abort all history
        print(f"skip {day}: {exc}")
        return None


def latest_trading_date() -> date:
    payload = fetch_json("https://www.tpex.org.tw/openapi/v1/tpex_intraday_trading_statistics")
    if not payload:
        raise RuntimeError("TPEx latest trading-date endpoint returned no rows")
    return max(roc_date(row["Date"]) for row in payload)


def load_state():
    if not STATE_PATH.exists():
        return {"days": {}, "tpex_market": {}}
    with gzip.open(STATE_PATH, "rt", encoding="utf-8") as handle:
        state = json.load(handle)
    if not isinstance(state.get("days"), dict):
        state["days"] = {}
    if not isinstance(state.get("tpex_market"), dict):
        state["tpex_market"] = {}
    return state


def save_state(state):
    with gzip.open(STATE_PATH, "wt", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, separators=(",", ":"))


def twse_month_history(latest: date, months: int = 13):
    month_starts = []
    cursor = latest.replace(day=1)
    for _ in range(months):
        month_starts.append(cursor)
        cursor = (cursor - timedelta(days=1)).replace(day=1)

    def fetch_month(month):
        url = (
            "https://www.twse.com.tw/exchangeReport/TWTB4U2?"
            + urllib.parse.urlencode(
                {"response": "json", "date": month.strftime("%Y%m%d"), "stockNo": ""}
            )
        )
        try:
            payload = fetch_json(url)
            table = payload.get("tables", [None])[0]
            if not table:
                return []
            result = []
            for row in table.get("data", []):
                result.append({"date": roc_date(row[0]).isoformat(), **market_stats(row[1:])})
            return result
        except Exception as exc:  # noqa: BLE001
            print(f"skip TWSE month {month:%Y-%m}: {exc}")
            return []

    output = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for rows in pool.map(fetch_month, month_starts):
            output.extend(rows)
    return {row["date"]: row for row in output}


def business_final_date(day: date) -> date:
    cursor = day
    added = 0
    while added < 2:
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            added += 1
    return cursor


def mean(values):
    values = [v for v in values if v is not None and math.isfinite(v)]
    return sum(values) / len(values) if values else None


def update_tpex_market_history(state, latest: date, workers: int, backfill_days: int = 380):
    history = state.setdefault("tpex_market", {})
    lookback = 9 if history else backfill_days
    start = latest - timedelta(days=lookback)
    candidates = [
        start + timedelta(days=offset)
        for offset in range((latest - start).days + 1)
        if (start + timedelta(days=offset)).weekday() < 5
    ]
    print(f"updating TPEx market history from {start} ({len(candidates)} weekday candidates)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(fetch_tpex_market, candidates):
            if result:
                history[result["date"]] = result
    cutoff = (latest - timedelta(days=400)).isoformat()
    state["tpex_market"] = {key: value for key, value in history.items() if key >= cutoff}


def repair_tpex_market_gaps(state, twse_history, workers: int = 3):
    history = state.setdefault("tpex_market", {})
    missing = [date.fromisoformat(key) for key in sorted(twse_history) if key not in history]
    if not missing:
        return
    print(f"repairing {len(missing)} TPEx market-history gaps with {workers} workers")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(fetch_tpex_market, missing):
            if result:
                history[result["date"]] = result


def build_output(state, latest: date, twse_history):
    days = state["days"]
    valid_dates = sorted(key for key, value in days.items() if value.get("TWSE") and value.get("TPEX"))
    if not valid_dates:
        raise RuntimeError("no complete trading days were collected")
    latest_key = max(key for key in valid_dates if key <= latest.isoformat())
    latest_day = days[latest_key]

    market_map = {key: {"date": key, "TWSE": value} for key, value in twse_history.items()}
    for key, value in state.get("tpex_market", {}).items():
        market_map.setdefault(key, {"date": key})["TPEX"] = value
    for key in valid_dates:
        row = market_map.setdefault(key, {"date": key})
        row["TWSE"] = days[key]["TWSE"]["market"]
        row["TPEX"] = days[key]["TPEX"]["market"]
    series = []
    for key in sorted(market_map):
        row = market_map[key]
        twse = row.get("TWSE")
        tpex = row.get("TPEX")
        item = {
            "date": key,
            "twse_ratio": twse.get("ratio") if twse else None,
            "tpex_ratio": tpex.get("ratio") if tpex else None,
            "combined_ratio": None,
        }
        if twse and tpex and twse.get("turnover") and tpex.get("turnover"):
            total_turnover = twse["turnover"] + tpex["turnover"]
            item["combined_ratio"] = round(
                (twse["daytrade_value"] + tpex["daytrade_value"]) / total_turnover * 100, 2
            )
        series.append(item)

    history_by_code = {}
    for key in valid_dates[-30:]:
        for market in ("TWSE", "TPEX"):
            for stock in days[key][market]["stocks"]:
                history_by_code.setdefault(f"{market}:{stock['code']}", []).append(
                    {"date": key, "ratio": stock["ratio"]}
                )

    stocks = []
    for market in ("TWSE", "TPEX"):
        for stock in latest_day[market]["stocks"]:
            history = history_by_code.get(f"{market}:{stock['code']}", [])[-20:]
            average = mean([item["ratio"] for item in history])
            result = dict(stock)
            result["avg20_ratio"] = round(average, 2) if average is not None else None
            result["vs_avg20_pp"] = (
                round(stock["ratio"] - average, 2) if average is not None and len(history) >= 5 else None
            )
            result["history"] = history
            stocks.append(result)
    stocks.sort(key=lambda item: (item["ratio"], item["turnover"]), reverse=True)

    twse_latest = latest_day["TWSE"]["market"]
    tpex_latest = latest_day["TPEX"]["market"]
    combined_turnover = twse_latest["turnover"] + tpex_latest["turnover"]
    combined_daytrade = twse_latest["daytrade_value"] + tpex_latest["daytrade_value"]
    combined = {
        "turnover": combined_turnover,
        "daytrade_value": combined_daytrade,
        "ratio": round(combined_daytrade / combined_turnover * 100, 2),
    }
    history_only = [item for item in series if item["date"] < latest_key]
    for label, value_key, target in (
        ("combined", "combined_ratio", combined),
        ("twse", "twse_ratio", twse_latest),
        ("tpex", "tpex_ratio", tpex_latest),
    ):
        values = [item[value_key] for item in history_only[-20:] if item[value_key] is not None]
        avg = mean(values)
        target["avg20_ratio"] = round(avg, 2) if avg is not None else None
        target["vs_avg20_pp"] = round(target["ratio"] - avg, 2) if avg is not None else None

    now = datetime.now(ZoneInfo("Asia/Taipei"))
    latest_date = date.fromisoformat(latest_key)
    return {
        "schema_version": 1,
        "as_of": latest_key,
        "generated_at": now.isoformat(timespec="seconds"),
        "status": "final" if now.date() >= business_final_date(latest_date) else "preliminary",
        "status_note": "T+2 後為最終資料；最近三個交易日會自動回補修正。",
        "markets": {"combined": combined, "twse": twse_latest, "tpex": tpex_latest},
        "series": series,
        "stocks": stocks,
        "universe": {
            "stocks": sum(item["asset_type"] == "stock" for item in stocks),
            "etfs": sum(item["asset_type"] == "etf" for item in stocks),
            "default_min_turnover": 500_000_000,
        },
        "methodology": {
            "daytrade_value": "(當沖買進成交金額 + 當沖賣出成交金額) ÷ 2",
            "ratio": "當沖成交值 ÷ 總成交金額 × 100%",
            "sources": [
                "臺灣證券交易所 TWTB4U／TWTB4U2／MI_INDEX",
                "證券櫃檯買賣中心現股當沖交易統計／每日收盤行情",
            ],
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill-days", type=int, default=45)
    parser.add_argument("--refresh-days", type=int, default=7)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    latest = latest_trading_date()
    state = load_state()
    has_history = bool(state["days"])
    lookback = args.refresh_days if has_history else args.backfill_days
    start = latest - timedelta(days=lookback)
    candidates = [
        start + timedelta(days=offset)
        for offset in range((latest - start).days + 1)
        if (start + timedelta(days=offset)).weekday() < 5
    ]
    print(f"collecting {len(candidates)} weekday candidates through {latest}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for result in pool.map(fetch_day, candidates):
            if result:
                state["days"][result["date"]] = result

    cutoff = (latest - timedelta(days=80)).isoformat()
    state["days"] = {key: value for key, value in state["days"].items() if key >= cutoff}
    update_tpex_market_history(state, latest, args.workers)
    twse_history = twse_month_history(latest)
    repair_tpex_market_gaps(state, twse_history)
    output = build_output(state, latest, twse_history)
    save_state(state)
    OUT_PATH.write_text(
        json.dumps(output, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )
    print(
        f"wrote {OUT_PATH.name}: {output['as_of']}, "
        f"{len(output['stocks'])} securities, {len(output['series'])} market rows"
    )


if __name__ == "__main__":
    main()
