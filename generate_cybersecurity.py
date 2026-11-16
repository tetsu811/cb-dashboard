#!/usr/bin/env python3
"""Refresh the market data embedded in cybersecurity.html.

The editorial company profiles stay untouched. Only each company's completed
daily close, daily move, moving averages and sparkline are replaced.
"""

from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf


HTML_PATH = Path("cybersecurity.html")
STATUS_PATH = Path("cybersecurity_latest.json")
DATA_PATTERN = re.compile(r"const DATA=(\[.*?\]);\nconst layers=", re.S)
MIN_SUCCESS_RATIO = 0.85
FUNDAMENTAL_WORKERS = 6
NEW_YORK = ZoneInfo("America/New_York")
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
FINMIND_API = "https://api.finmindtrade.com/api/v4/data"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}


def load_finmind_token() -> str | None:
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    if not token:
        token_file = Path(
            os.environ.get(
                "FINMIND_TOKEN_FILE",
                Path(__file__).resolve().parents[2] / ".secrets" / "finmind-token",
            )
        )
        if token_file.exists():
            token = token_file.read_text(encoding="utf-8").strip()
    token = token.replace("\\_", "_")
    return token or None


def read_page() -> tuple[str, list[dict]]:
    html = HTML_PATH.read_text(encoding="utf-8")
    match = DATA_PATTERN.search(html)
    if not match:
        raise RuntimeError("cybersecurity.html 中找不到公司資料區塊")
    return html, json.loads(match.group(1))


def ticker_frame(download: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    if isinstance(download.columns, pd.MultiIndex):
        if ticker not in download.columns.get_level_values(0):
            return None
        frame = download[ticker]
    else:
        frame = download
    frame = frame.dropna(subset=["Close"])
    return frame if len(frame) >= 60 else None


def fetch_completed_close(ticker: str) -> dict:
    """Read Yahoo's timestamped regular-session close.

    Yahoo can expose the completed close through chart metadata several hours
    before the daily candle itself is populated. The timestamp check prevents
    an in-session price from being labelled as a completed daily close.
    """
    response = requests.get(
        YAHOO_CHART.format(ticker=ticker),
        params={"range": "5d", "interval": "5m", "includePrePost": "false"},
        headers=HTTP_HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()["chart"]["result"][0]
    meta = result["meta"]
    timestamp = datetime.fromtimestamp(int(meta["regularMarketTime"]), timezone.utc)
    market_time = timestamp.astimezone(NEW_YORK)
    if market_time.hour < 16:
        raise RuntimeError(f"{ticker} 尚未完成美股正規交易時段")
    price = float(meta["regularMarketPrice"])
    previous = float(meta["previousClose"])
    return {
        "date": market_time.date().isoformat(),
        "price": price,
        "previous": previous,
        "timestamp": timestamp.isoformat(),
    }


def fetch_finmind_minute_close(ticker: str, trade_date: str, token: str) -> dict:
    """Read the last completed regular-session minute from FinMind.

    FinMind restricts USStockPriceMinute to a single day, so end_date must not
    be sent. A normal US session has 390 one-minute rows (09:30–15:59 ET).
    """
    response = requests.get(
        FINMIND_API,
        params={
            "dataset": "USStockPriceMinute",
            "data_id": ticker,
            "start_date": trade_date,
        },
        headers={"Authorization": f"Bearer {token}"},
        timeout=45,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != 200:
        raise RuntimeError(payload.get("msg") or f"FinMind status {payload.get('status')}")
    rows = payload.get("data") or []
    rows = [row for row in rows if str(row.get("date", "")).startswith(trade_date)]
    # Thinly traded names can legitimately have fewer than 390 rows because
    # the feed omits minutes without a trade. Validate session coverage rather
    # than requiring one row for every minute.
    first_time = str(rows[0].get("date", "")) if rows else ""
    last_time = str(rows[-1].get("date", "")) if rows else ""
    if len(rows) < 200 or "13:3" not in first_time or not any(
        marker in last_time for marker in ("19:55", "19:56", "19:57", "19:58", "19:59")
    ):
        raise RuntimeError(
            f"{ticker} {trade_date} 分鐘資料時段不完整："
            f"{len(rows)} 筆，{first_time} 至 {last_time}"
        )
    last = rows[-1]
    return {
        "date": trade_date,
        "price": float(last["close"]),
        "timestamp": str(last["date"]),
        "rows": len(rows),
    }


def quote_from_frame(frame: pd.DataFrame, completed: dict | None = None) -> dict:
    closes = frame["Close"].astype(float)
    dates = [index.date().isoformat() for index in closes.index]
    if completed and completed["date"] > dates[-1]:
        previous = float(closes.iloc[-1])
        closes = pd.concat([closes, pd.Series([completed["price"]])], ignore_index=True)
        dates.append(completed["date"])
        last = completed["price"]
    else:
        last = float(closes.iloc[-1])
        previous = float(closes.iloc[-2])
    history = [
        {"date": date, "close": round(float(close), 4)}
        for date, close in zip(dates[-20:], closes.iloc[-20:])
    ]
    return {
        "price": round(last, 4),
        "change": round(last - previous, 4),
        "pct": round((last / previous - 1) * 100, 6),
        "date": dates[-1],
        "ma20": round(float(closes.iloc[-20:].mean()), 6),
        "ma60": round(float(closes.iloc[-60:].mean()), 6),
        "history": history,
    }


def _statement_value(frame: pd.DataFrame, names: tuple[str, ...], column):
    if frame is None or frame.empty or column is None:
        return None
    for name in names:
        if name in frame.index:
            value = frame.at[name, column]
            if pd.notna(value):
                return float(value)
    return None


def fetch_fundamentals(ticker: str, price: float | None) -> dict:
    stock = yf.Ticker(ticker)
    income = stock.quarterly_income_stmt
    cashflow = stock.quarterly_cashflow
    balance = stock.quarterly_balance_sheet
    columns = sorted(income.columns, reverse=True) if income is not None else []
    latest = columns[0] if columns else None

    revenue = _statement_value(income, ("Total Revenue",), latest)
    operating_income = _statement_value(income, ("Operating Income",), latest)
    eps_values = []
    for column in columns[:4]:
        value = _statement_value(income, ("Diluted EPS", "Basic EPS"), column)
        if value is not None:
            eps_values.append(value)
    eps_ttm = sum(eps_values) if len(eps_values) == 4 else None

    revenue_yoy = None
    if len(columns) >= 5 and revenue is not None:
        prior = _statement_value(income, ("Total Revenue",), columns[4])
        if prior:
            revenue_yoy = revenue / prior - 1

    free_cash_flow = None
    if cashflow is not None and not cashflow.empty:
        cash_columns = sorted(cashflow.columns, reverse=True)
        if cash_columns:
            free_cash_flow = _statement_value(
                cashflow,
                ("Free Cash Flow",),
                cash_columns[0],
            )

    market_cap = None
    try:
        market_cap = stock.fast_info.get("market_cap")
        market_cap = float(market_cap) if market_cap is not None else None
    except Exception:
        pass
    if market_cap is None and price is not None and balance is not None and not balance.empty:
        balance_columns = sorted(balance.columns, reverse=True)
        shares = _statement_value(
            balance,
            ("Ordinary Shares Number", "Share Issued"),
            balance_columns[0] if balance_columns else None,
        )
        if shares:
            market_cap = price * shares

    return {
        "period": latest.date().isoformat() if latest is not None else None,
        "revenue": revenue,
        "revenueYoy": revenue_yoy,
        "epsTtm": eps_ttm,
        "pe": (price / eps_ttm) if price is not None and eps_ttm and eps_ttm > 0 else None,
        "operatingMargin": (
            operating_income / revenue
            if operating_income is not None and revenue
            else None
        ),
        "freeCashFlow": free_cash_flow,
        "marketCap": market_cap,
        "updatedAt": datetime.now(timezone.utc).date().isoformat(),
    }


def main() -> None:
    html, companies = read_page()
    tickers = [company["ticker"] for company in companies]
    bars = yf.download(
        tickers,
        period="6mo",
        interval="1d",
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    completed_closes: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=FUNDAMENTAL_WORKERS) as pool:
        futures = {pool.submit(fetch_completed_close, ticker): ticker for ticker in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                completed_closes[ticker] = future.result()
            except Exception as exc:
                print(f"  [{ticker}] 完成收盤價查核失敗：{exc}")

    finmind_token = load_finmind_token()
    finmind_closes: dict[str, dict] = {}
    if finmind_token:
        with ThreadPoolExecutor(max_workers=FUNDAMENTAL_WORKERS) as pool:
            futures = {
                pool.submit(
                    fetch_finmind_minute_close,
                    ticker,
                    completed_closes[ticker]["date"],
                    finmind_token,
                ): ticker
                for ticker in tickers
                if ticker in completed_closes
            }
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    finmind_closes[ticker] = future.result()
                except Exception as exc:
                    print(f"  [{ticker}] FinMind 分鐘收盤查核失敗：{exc}")
    else:
        print("  未設定 FinMind token；本次使用已完成收盤備援資料")

    updated_dates: dict[str, str] = {}
    failed: list[str] = []
    for company in companies:
        ticker = company["ticker"]
        frame = ticker_frame(bars, ticker)
        if frame is None:
            failed.append(ticker)
            continue
        # FinMind is the primary current-session source. Yahoo's timestamped
        # metadata is retained only to identify that the regular session has
        # completed and to provide a safe fallback when FinMind is unavailable.
        completed = finmind_closes.get(ticker) or completed_closes.get(ticker)
        company["quote"] = quote_from_frame(frame, completed)
        updated_dates[ticker] = company["quote"]["date"]

    minimum = max(1, int(len(companies) * MIN_SUCCESS_RATIO))
    if len(updated_dates) < minimum:
        raise RuntimeError(
            f"行情成功數不足：{len(updated_dates)}/{len(companies)}；保留上一版"
        )
    newest_date = max(updated_dates.values())
    lagging = [ticker for ticker, date in updated_dates.items() if date != newest_date]
    if lagging:
        raise RuntimeError(
            f"行情日期不一致；最新為 {newest_date}，落後：{', '.join(lagging)}。保留上一版"
        )

    fundamental_failures: list[str] = []
    with ThreadPoolExecutor(max_workers=FUNDAMENTAL_WORKERS) as pool:
        futures = {
            pool.submit(
                fetch_fundamentals,
                company["ticker"],
                company.get("quote", {}).get("price"),
            ): company
            for company in companies
        }
        for future in as_completed(futures):
            company = futures[future]
            try:
                fundamental = future.result()
                if fundamental.get("period"):
                    company["fundamentals"] = fundamental
                else:
                    fundamental_failures.append(company["ticker"])
            except Exception as exc:
                print(f"  [{company['ticker']}] 基本面更新失敗：{exc}")
                fundamental_failures.append(company["ticker"])

    payload = json.dumps(companies, ensure_ascii=False, separators=(",", ":"))
    replacement = f"const DATA={payload};\nconst layers="
    refreshed = DATA_PATTERN.sub(lambda _: replacement, html, count=1)
    HTML_PATH.write_text(refreshed, encoding="utf-8")

    dates = sorted(set(updated_dates.values()))
    status = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "trade_date": max(dates),
        "quote_dates": dates,
        "completedCloseTimestamps": {
            ticker: data["timestamp"]
            for ticker, data in (finmind_closes or completed_closes).items()
        },
        "finmindUpdated": len(finmind_closes),
        "finmindRows": {ticker: data["rows"] for ticker, data in finmind_closes.items()},
        "updated": len(updated_dates),
        "total": len(companies),
        "failed": failed,
        "fundamentalsUpdated": sum(
            1 for company in companies if company.get("fundamentals", {}).get("period")
        ),
        "fundamentalsFailed": fundamental_failures,
        "tickers": updated_dates,
    }
    STATUS_PATH.write_text(
        json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(
        f"資安股行情已更新：{len(updated_dates)}/{len(companies)}，"
        f"最新交易日 {status['trade_date']}"
    )
    if failed:
        print("未更新：" + ", ".join(failed))


if __name__ == "__main__":
    main()
