#!/usr/bin/env python3
"""Backfill six months of estimated short-sale maintenance data.

History row schema:
  [date, close, volume, margin_maintenance, taiex, margin_balance,
   short_maintenance, short_balance]
"""
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

BASE = Path(__file__).resolve().parent
LATEST = BASE / "margin_strategy_latest.json"
CACHE = Path("/tmp/tetsu_margin_short_backfill")
HEADERS = {"User-Agent": "Mozilla/5.0"}
SHORT_MARGIN_RATE = 0.9


def number(value):
    try:
        return float(str(value).replace(",", "").replace("+", "").strip())
    except (TypeError, ValueError):
        return None


def get_json(url):
    last_error = None
    for attempt in range(4):
        try:
            response = requests.get(url, headers=HEADERS, timeout=45)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            last_error = error
            if attempt < 3:
                time.sleep(2**attempt)
    raise last_error


def fetch_date(date):
    CACHE.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE / f"{date}.json"
    if cache_path.exists():
        return date, json.loads(cache_path.read_text())
    compact = date.replace("-", "")
    slashed = date.replace("-", "/")
    twse = get_json(
        f"https://wwwc.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={compact}&selectType=STOCK&response=json"
    )
    tpex = get_json(
        f"https://www.tpex.org.tw/www/zh-tw/margin/balance?date={slashed}&id=&response=json"
    )
    twse_table = next(
        table for table in twse.get("tables", []) if "融資融券彙總" in (table.get("title") or "")
    )
    result = {}
    for row in twse_table.get("data", []):
        stock_id = str(row[0]).strip()
        result[stock_id] = (number(row[9]) or 0, number(row[12]) or 0)
    tpex_tables = tpex.get("tables", [])
    if tpex_tables:
        for row in tpex_tables[0].get("data", []):
            stock_id = str(row[0]).strip()
            result[stock_id] = (number(row[11]) or 0, number(row[14]) or 0)
    cache_path.write_text(json.dumps(result, separators=(",", ":")))
    return date, result


def main():
    paths = sorted(BASE.glob("margin_history_[1-9].json"))
    shards = {path: json.loads(path.read_text()) for path in paths}
    dates = sorted(
        {row[0] for shard in shards.values() for item in shard["stocks"].values() for row in item["h"]}
    )
    daily = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(fetch_date, date) for date in dates]
        for index, future in enumerate(as_completed(futures), 1):
            try:
                date, values = future.result()
                daily[date] = values
            except Exception as error:
                print(f"warning: one trading day unavailable: {error}", flush=True)
            if index % 20 == 0 or index == len(futures):
                print(f"downloaded {index}/{len(futures)} trading days", flush=True)

    latest_cost = {}
    for path, shard in shards.items():
        for stock_id, item in shard["stocks"].items():
            cost = None
            previous_balance = 0
            for row in item["h"]:
                close = number(row[1])
                values = daily.get(row[0])
                short_sell, balance = values.get(stock_id, (0, 0)) if values is not None else (0, previous_balance)
                if balance > 0 and close:
                    if cost is None:
                        cost = close
                    else:
                        previous_units = max(0, balance - short_sell)
                        cost = (cost * previous_units + close * short_sell) / balance
                    maintenance = cost * (1 + SHORT_MARGIN_RATE) / close * 100
                else:
                    cost = None
                    maintenance = None
                row[6:] = [round(maintenance, 2) if maintenance is not None else None, balance]
                previous_balance = balance
            latest_cost[stock_id] = cost
        path.write_text(json.dumps(shard, ensure_ascii=False, separators=(",", ":")))

    payload = json.loads(LATEST.read_text())
    latest_date = payload["as_of"]
    for stock in payload["stocks"]:
        stock_id = stock["id"]
        history = shards[BASE / f"margin_history_{stock_id[0]}.json"]["stocks"].get(stock_id, {}).get("h", [])
        recent = history[-21:]
        balance_history = [[row[0], row[7]] for row in recent]
        current_balance = balance_history[-1][1] if balance_history else 0
        base_balance = balance_history[0][1] if len(balance_history) >= 21 else None
        cost = latest_cost.get(stock_id)
        close = number(stock.get("close"))
        stock["short_balance"] = current_balance
        stock["short_balance_history"] = balance_history
        stock["short_change_20d"] = (
            current_balance / base_balance - 1 if current_balance and base_balance else None
        )
        stock["short_cost"] = cost
        stock["short_maintenance"] = (
            cost * (1 + SHORT_MARGIN_RATE) / close * 100 if cost and close else None
        )
        stock["as_of"] = latest_date
    payload["generated_at"] = datetime.now().isoformat(timespec="seconds")
    LATEST.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    print(json.dumps({"as_of": latest_date, "days": len(dates), "stocks": len(payload["stocks"])}, ensure_ascii=False))


if __name__ == "__main__":
    main()
