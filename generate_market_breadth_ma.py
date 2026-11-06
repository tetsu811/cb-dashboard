#!/usr/bin/env python3
"""Build a full-market TWSE/TPEx moving-average breadth data set.

The first run uses --backfill START_DATE to warm up the 200-day averages from
official daily exchange files. Normal daily runs fetch only missing trading
days after the saved state. The public JSON contains aggregates only; the
rolling per-stock state is gzip-compressed for the next run.
"""

import argparse
import gzip
import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import requests


BASE = Path(__file__).resolve().parent
STATE_PATH = BASE / "market_breadth_ma_state.json.gz"
OUT_PATH = BASE / "market_breadth_ma_latest.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; tetsu811-market-breadth/1.0)"}
LIQUIDITY_THRESHOLD = 20_000_000
STATE_DAYS = 280
SERIES_DAYS = 260


def number(value):
    try:
        text = re.sub(r"<[^>]+>", "", str(value)).replace(",", "").strip()
        if not text or text in {"--", "---", "-"}:
            return None
        result = float(text)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def request_json(url, attempts=4):
    last = None
    for attempt in range(attempts):
        try:
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
    raise last


def company_universe():
    sources = [
        (
            "上市",
            "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
            "公司代號",
            "公司簡稱",
            "已發行普通股數或TDR原股發行股數",
        ),
        (
            "上櫃",
            "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
            "SecuritiesCompanyCode",
            "CompanyName",
            "IssueShares",
        ),
    ]
    result = {}
    for market, url, id_key, name_key, shares_key in sources:
        for row in request_json(url):
            sid = str(row.get(id_key, "")).strip()
            shares = number(row.get(shares_key))
            # 91xx are TDRs; six-character preferred shares and ETFs are also
            # excluded by the strict four-digit common-share code rule.
            if not re.fullmatch(r"[1-9]\d{3}", sid) or sid.startswith("91"):
                continue
            if not shares or shares <= 0:
                continue
            result[sid] = {
                "name": str(row.get(name_key, sid)).strip() or sid,
                "market": market,
                "shares": round(shares),
            }
    return result


def parse_twse(payload, companies):
    if payload.get("stat") != "OK":
        return {}, None
    market_table = next(
        (table for table in payload.get("tables", []) if "每日收盤行情" in str(table.get("title"))),
        None,
    )
    index_table = payload.get("tables", [{}])[0]
    if not market_table:
        return {}, None
    rows = {}
    for row in market_table.get("data", []):
        sid = str(row[0]).strip()
        if sid not in companies or companies[sid]["market"] != "上市":
            continue
        close, turnover = number(row[8]), number(row[4])
        if close and close > 0:
            rows[sid] = (close, max(0, turnover or 0))
    taiex = None
    for row in index_table.get("data", []):
        if str(row[0]).strip() == "發行量加權股價指數":
            taiex = number(row[1])
            break
    return rows, taiex


def parse_tpex(payload, companies):
    if str(payload.get("stat", "")).lower() != "ok" or not payload.get("tables"):
        return {}
    rows = {}
    for row in payload["tables"][0].get("data", []):
        sid = str(row[0]).strip()
        if sid not in companies or companies[sid]["market"] != "上櫃":
            continue
        close, turnover = number(row[2]), number(row[9])
        if close and close > 0:
            rows[sid] = (close, max(0, turnover or 0))
    return rows


def fetch_day(day, companies):
    compact = day.strftime("%Y%m%d")
    slash = day.strftime("%Y/%m/%d")
    twse_url = (
        "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
        f"?date={compact}&type=ALLBUT0999&response=json"
    )
    tpex_url = (
        "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"
        f"?date={slash}&id=&response=json"
    )
    twse_payload = request_json(twse_url)
    twse_rows, taiex = parse_twse(twse_payload, companies)
    if not twse_rows or not taiex:
        return None
    tpex_rows = parse_tpex(request_json(tpex_url), companies)
    if not tpex_rows:
        return None
    return {
        "date": day.isoformat(),
        "taiex": taiex,
        "prices": {**twse_rows, **tpex_rows},
    }


def latest_official_date():
    payload = request_json(
        "https://wwwc.twse.com.tw/rwd/marginTrading/BFIJ3U_TREND?response=json&days=10"
    )
    return datetime.strptime(payload["data"][-1]["date"], "%Y%m%d").date()


def blank_state(companies):
    return {
        "dates": [],
        "taiex": [],
        "stocks": {
            sid: {
                "n": item["name"],
                "m": item["market"],
                "s": item["shares"],
                "c": [],
                "t": [],
            }
            for sid, item in companies.items()
        },
    }


def load_state(companies):
    if not STATE_PATH.exists():
        return blank_state(companies)
    with gzip.open(STATE_PATH, "rt", encoding="utf-8") as handle:
        state = json.load(handle)
    width = len(state.get("dates", []))
    for sid, item in companies.items():
        if sid not in state["stocks"]:
            state["stocks"][sid] = {
                "n": item["name"],
                "m": item["market"],
                "s": item["shares"],
                "c": [None] * width,
                "t": [0] * width,
            }
        else:
            state["stocks"][sid].update(n=item["name"], m=item["market"], s=item["shares"])
    return state


def save_state(state):
    temp = STATE_PATH.with_suffix(".tmp")
    with gzip.open(temp, "wt", encoding="utf-8", compresslevel=9) as handle:
        json.dump(state, handle, ensure_ascii=False, separators=(",", ":"))
    temp.replace(STATE_PATH)


def append_day(state, day_payload, companies):
    target = day_payload["date"]
    if state["dates"] and target <= state["dates"][-1]:
        return False
    previous_width = len(state["dates"])
    prices = day_payload["prices"]
    for sid, item in companies.items():
        stock = state["stocks"].setdefault(
            sid,
            {
                "n": item["name"],
                "m": item["market"],
                "s": item["shares"],
                "c": [None] * previous_width,
                "t": [0] * previous_width,
            },
        )
        observed = prices.get(sid)
        previous_close = stock["c"][-1] if stock["c"] else None
        stock["c"].append(observed[0] if observed else previous_close)
        stock["t"].append(round(observed[1]) if observed else 0)
        stock.update(n=item["name"], m=item["market"], s=item["shares"])
    state["dates"].append(target)
    state["taiex"].append(day_payload["taiex"])
    if len(state["dates"]) > STATE_DAYS:
        drop = len(state["dates"]) - STATE_DAYS
        state["dates"] = state["dates"][drop:]
        state["taiex"] = state["taiex"][drop:]
        for stock in state["stocks"].values():
            stock["c"] = stock["c"][drop:]
            stock["t"] = stock["t"][drop:]
    return True


def mean(values):
    return sum(values) / len(values) if values else None


def calculate_latest(state, companies):
    idx = len(state["dates"]) - 1
    if idx < 20:
        return None
    eligible = []
    for sid, meta in companies.items():
        stock = state["stocks"].get(sid)
        if not stock or idx >= len(stock["c"]):
            continue
        current = stock["c"][idx]
        prior_turnover = stock["t"][max(0, idx - 20):idx]
        if current is None or len(prior_turnover) < 20:
            continue
        avg_turnover = mean(prior_turnover)
        if avg_turnover is None or avg_turnover <= LIQUIDITY_THRESHOLD:
            continue
        row = {
            "id": sid,
            "market": meta["market"],
            "shares": meta["shares"],
            "close": current,
            "avg_turnover20": avg_turnover,
            "ma": {},
        }
        for window in (20, 60, 200):
            values = stock["c"][idx - window + 1:idx + 1]
            if len(values) == window and all(value is not None for value in values):
                row["ma"][window] = mean(values)
        prior_close = stock["c"][idx - 1] if idx else None
        row["prior_market_cap"] = (prior_close or current) * meta["shares"]
        recent60 = stock["c"][max(0, idx - 59):idx + 1]
        if len(recent60) == 60 and all(value is not None for value in recent60):
            row["high60"] = current >= max(recent60)
            row["low60"] = current <= min(recent60)
        eligible.append(row)

    markets = {}
    for market in ("上市", "上櫃"):
        group = [row for row in eligible if row["market"] == market]
        metrics = {"eligible": len(group)}
        for window in (20, 60, 200):
            usable = [row for row in group if window in row["ma"]]
            above = sum(row["close"] > row["ma"][window] for row in usable)
            metrics[f"ma{window}_pct"] = round(above / len(usable) * 100, 2) if usable else None
            metrics[f"ma{window}_above"] = above
            metrics[f"ma{window}_denominator"] = len(usable)
        markets[market] = metrics

    usable60 = [row for row in eligible if 60 in row["ma"]]
    ranked = sorted(usable60, key=lambda row: row["prior_market_cap"], reverse=True)
    large, small = ranked[:100], ranked[100:]
    large_above = sum(row["close"] > row["ma"][60] for row in large)
    small_above = sum(row["close"] > row["ma"][60] for row in small)
    high_count = sum(bool(row.get("high60")) for row in usable60)
    low_count = sum(bool(row.get("low60")) for row in usable60)
    taiex_window = state["taiex"][max(0, idx - 249):idx + 1]
    return {
        "date": state["dates"][idx],
        "taiex": round(state["taiex"][idx], 2),
        "taiex_high250": len(taiex_window) == 250 and state["taiex"][idx] >= max(taiex_window),
        "markets": markets,
        "size60": {
            "large_pct": round(large_above / len(large) * 100, 2) if large else None,
            "large_above": large_above,
            "large_denominator": len(large),
            "small_pct": round(small_above / len(small) * 100, 2) if small else None,
            "small_above": small_above,
            "small_denominator": len(small),
        },
        "new_high60": high_count,
        "new_low60": low_count,
        "net_new_high60": high_count - low_count,
        "coverage": len(eligible),
    }


def load_existing_series():
    if not OUT_PATH.exists():
        return []
    try:
        return json.loads(OUT_PATH.read_text())["series"]
    except (KeyError, ValueError, TypeError):
        return []


def write_output(series, companies):
    series = series[-SERIES_DAYS:]
    net_values = [row["net_new_high60"] for row in series]
    for index, row in enumerate(series):
        values = net_values[max(0, index - 9):index + 1]
        row["net_new_high60_ma10"] = round(mean(values), 2)
    payload = {
        "as_of": series[-1]["date"],
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "universe": {
            "description": "上市與上櫃普通股；排除 ETF、ETN、權證、特別股及 TDR",
            "listed_companies": sum(item["market"] == "上市" for item in companies.values()),
            "otc_companies": sum(item["market"] == "上櫃" for item in companies.values()),
            "liquidity_rule": "前20個交易日（不含當日）平均成交金額大於新台幣2,000萬元",
            "price_basis": "交易所每日收盤價，未還原權息",
        },
        "series": series,
        "sources": [
            "TWSE MI_INDEX 每日收盤行情",
            "TPEx dailyQuotes 上櫃股票行情",
            "TWSE/TPEx 公開資訊觀測站公司基本資料",
        ],
        "disclaimer": "市場廣度為每日可重算的統計估計，不代表未來漲跌，也不構成投資建議。",
    }
    if OUT_PATH.exists():
        try:
            previous = json.loads(OUT_PATH.read_text())
            comparable_previous = {key: value for key, value in previous.items() if key != "generated_at"}
            comparable_current = {key: value for key, value in payload.items() if key != "generated_at"}
            if comparable_previous == comparable_current:
                payload["generated_at"] = previous.get("generated_at", payload["generated_at"])
        except (OSError, ValueError, TypeError):
            pass
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def weekdays(start, end):
    current = start
    while current <= end:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", metavar="YYYY-MM-DD")
    parser.add_argument("--end", metavar="YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    companies = company_universe()
    if len(companies) < 1500:
        raise SystemExit(f"Company universe unexpectedly small: {len(companies)}")
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else latest_official_date()

    if args.backfill:
        start = datetime.strptime(args.backfill, "%Y-%m-%d").date()
        state = blank_state(companies)
        series = []
        candidates = list(weekdays(start, end))
        completed = []
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {executor.submit(fetch_day, day, companies): day for day in candidates}
            for count, future in enumerate(as_completed(futures), 1):
                day = futures[future]
                try:
                    payload = future.result()
                    if payload:
                        completed.append(payload)
                except Exception as exc:
                    print(f"WARN {day}: {exc}", flush=True)
                if count % 20 == 0 or count == len(candidates):
                    print(f"downloaded {count}/{len(candidates)} weekdays", flush=True)
        for payload in sorted(completed, key=lambda item: item["date"]):
            append_day(state, payload, companies)
            metric = calculate_latest(state, companies)
            if metric:
                series.append(metric)
        state_changed = True
    else:
        state = load_state(companies)
        series = load_existing_series()
        state_changed = False
        if not state["dates"]:
            raise SystemExit("Missing rolling state; run with --backfill first")
        start = datetime.strptime(state["dates"][-1], "%Y-%m-%d").date() + timedelta(days=1)
        for day in weekdays(start, end):
            payload = fetch_day(day, companies)
            if not payload:
                continue
            if append_day(state, payload, companies):
                state_changed = True
                metric = calculate_latest(state, companies)
                if metric:
                    series.append(metric)

    if not series:
        raise SystemExit("No valid market-breadth series generated")
    deduped = {row["date"]: row for row in series}
    series = [deduped[key] for key in sorted(deduped)]
    if state_changed:
        save_state(state)
    write_output(series, companies)
    print(
        json.dumps(
            {
                "as_of": series[-1]["date"],
                "series": len(series),
                "companies": len(companies),
                "coverage": series[-1]["coverage"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
