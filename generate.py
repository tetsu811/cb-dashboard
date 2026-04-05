#!/usr/bin/env python3
"""
可轉債策略儀表板資料產生器 - TPEX data source
"""
import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta
from io import StringIO

import requests
from bs4 import BeautifulSoup

# Constants
TODAY = datetime.now().date()
OUTPUT_HTML = 'index.html'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# TPEX data source URLs
TPEX_CB_INFO_PATTERN = "https://www.tpex.org.tw/storage/bond_zone/tradeinfo/cb/{year}/{yearmonth}/RSdrs001.{yearmonthday}-C.csv"
TPEX_CB_TRADING_PATTERN = "https://www.tpex.org.tw/storage/bond_zone/tradeinfo/cb/{year}/{yearmonth}/RSta0113.{yearmonthday}-C.csv"
TPEX_ISSUANCE_API = "https://www.tpex.org.tw/openapi/v1/bond_ISSBD5_data"

# Field indices for CB info CSV (after BODY prefix)
CB_INFO = {
    "cb_code": 1, "cb_name": 2,
    "conversion_start": 3, "conversion_end": 4, "conversion_price": 5,
    "buyback_start": 7, "buyback_end": 8, "buyback_price": 9,
    "original_issue_amount": 14, "outstanding_amount": 15,
    "cb_reference_price": 16, "stock_price": 17,
    "trading_stop_end": 19, "coupon_rate": 20,
}

# Field indices for CB trading CSV
CB_TRADE = {
    "cb_code": 1, "cb_name": 2,
    "close_price": 3, "open_price": 5, "high_price": 6, "low_price": 7,
    "volume": 9, "amount": 10,
}

def parse_num(txt):
    """Parse number from string, handling commas and whitespace."""
    if not txt or not txt.strip():
        return None
    cleaned = txt.strip().replace(",", "").replace("，", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_date_tw(s):
    """Parse YYYY/MM/DD to YYYY-MM-DD."""
    if not s or not s.strip():
        return None
    try:
        return datetime.strptime(s.strip(), "%Y/%m/%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _fetch_tpex_csv(url):
    """Fetch Big5-encoded CSV from TPEX, return BODY rows."""
    resp = requests.get(url, timeout=15)
    resp.encoding = "big5"
    rows = []
    for row in csv.reader(StringIO(resp.text)):
        if row and row[0].strip() == "BODY":
            rows.append(row)
    return rows


def _get_latest_tpex_csv(pattern, max_days=7):
    """Try fetching TPEX CSV going back up to max_days."""
    base = datetime.now()
    for d in range(max_days):
        dt = base - timedelta(days=d)
        url = pattern.format(
            year=dt.strftime("%Y"),
            yearmonth=dt.strftime("%Y%m"),
            yearmonthday=dt.strftime("%Y%m%d"),
        )
        try:
            rows = _fetch_tpex_csv(url)
            if rows:
                print(f"✓ TPEX CSV fetched ({dt.strftime('%Y-%m-%d')})")
                return rows
        except Exception:
            continue
    return []


def fetch_issuance_dates():
    """從TPEX API取得CB上市日期與到期日期"""
    listing_map, maturity_map = {}, {}
    try:
        data = requests.get(TPEX_ISSUANCE_API, timeout=10).json()
        if isinstance(data, list):
            for item in data:
                code = item.get("BondCode", "").strip()
                if not code:
                    continue
                ld = item.get("ListingDate", "")
                md = item.get("MaturityDate", "")
                if ld:
                    try:
                        listing_map[code] = datetime.strptime(ld, "%Y/%m/%d").strftime("%Y-%m-%d")
                    except ValueError:
                        pass
                if md:
                    try:
                        maturity_map[code] = datetime.strptime(md, "%Y/%m/%d").strftime("%Y-%m-%d")
                    except ValueError:
                        pass
    except Exception:
        pass
    return listing_map, maturity_map


def derive_stock_code(cb_code):
    """Derive stock code from CB code."""
    if not cb_code:
        return None
    code = cb_code.strip()
    if len(code) == 5:
        return code[:4]
    if len(code) == 6 and code[0] in ("1", "2"):
        return code[:4]
    return None


def fetch_all_cbs():
    """從TPEX取得所有可轉債資料"""
    listing_map, maturity_map = fetch_issuance_dates()
    info_rows = _get_latest_tpex_csv(TPEX_CB_INFO_PATTERN)
    if not info_rows:
        raise ValueError("Failed to fetch CB info from TPEX")
    trade_rows = _get_latest_tpex_csv(TPEX_CB_TRADING_PATTERN)
    trade_map = {}
    if trade_rows:
        for row in trade_rows:
            try:
                trade_map[row[CB_TRADE["cb_code"]].strip()] = row
            except (IndexError, AttributeError):
                pass
    cbs = []
    for row in info_rows:
        try:
            cb_code = row[CB_INFO["cb_code"]].strip()
            cb_name = row[CB_INFO["cb_name"]].strip()
            cb_price = parse_num(row[CB_INFO["cb_reference_price"]])
            stock_price = parse_num(row[CB_INFO["stock_price"]])
            conversion_price = parse_num(row[CB_INFO["conversion_price"]])
            conversion_end = parse_date_tw(row[CB_INFO["conversion_end"]])
            original_issue = parse_num(row[CB_INFO["original_issue_amount"]])
            outstanding = parse_num(row[CB_INFO["outstanding_amount"]])
            trading_stop_end = parse_date_tw(row[CB_INFO["trading_stop_end"]])
            maturity_date = maturity_map.get(cb_code) or trading_stop_end or conversion_end
            volume = None
            if cb_code in trade_map:
                volume = parse_num(trade_map[cb_code][CB_TRADE["volume"]])
            premium = None
            if cb_price and stock_price and conversion_price and conversion_price > 0:
                parity = (stock_price / conversion_price) * 100
                if parity > 0:
                    premium = (cb_price / parity - 1) * 100
            converted_pct = None
            if original_issue and outstanding and original_issue > 0:
                converted_pct = (1 - outstanding / original_issue) * 100
            listing_date = listing_map.get(cb_code)
            stock_code = derive_stock_code(cb_code)
            cbs.append({
                "cb_code": cb_code,
                "cb_name": cb_name,
                "cb_price": cb_price,
                "stock_price": stock_price,
                "premium": premium,
                "volume": volume,
                "conversion_price": conversion_price,
                "converted_pct": converted_pct,
                "maturity_date": maturity_date,
                "listing_date": listing_date,
                "stock_code": stock_code,
            })
        except (IndexError, ValueError, AttributeError):
            continue
    if len(cbs) < 100:
        raise ValueError(f"Only found {len(cbs)} CBs, expected >= 100")
    print(f"→ 全部CB: {len(cbs)} 筆")
    return cbs


def fetch_recent_cbs(days=90):
    """取得近N天上市的CB"""
    listing_map, _ = fetch_issuance_dates()
    cutoff = datetime.now() - timedelta(days=days)
    recent = {}
    for code, date_str in listing_map.items():
        try:
            if datetime.strptime(date_str, "%Y-%m-%d") >= cutoff:
                recent[code] = {"listing_date": date_str}
        except ValueError:
            continue
    print(f"→ 近{days}天CB: {len(recent)} 筆")
    return recent


def _parse_short_rows(rows, data_date):
    """ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ±ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂ§ÃÂÃÂ£ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¼ÃÂÃÂ¯ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ TWSE/TPEX ÃÂÃÂ§ÃÂÃÂÃÂÃÂ row ÃÂÃÂ©ÃÂÃÂÃÂÃÂ£ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ short_map"""
    def ti(s):
        try:
            return round(int(str(s).replace(',', '').strip() or '0') / 1000)
        except:
            return 0
    short_map = {}
    for row in rows:
        code = row[0]
        if not code or code == 'ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂ':
            continue
        prev    = ti(row[8])
        today_v = ti(row[12])
        short_map[code] = {
            'name':         row[1],
            'short_prev':   prev,
            'short_today':  today_v,
            'short_change': today_v - prev,
            'short_sell':   ti(row[9]),
            'short_cover':  ti(row[10]),
            'increasing':   (today_v - prev) > 0,
            'data_date':    data_date,
        }
    return short_map


def fetch_short_data():
    print("[3/3] ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ (TWSE + TPEX)...")
    short_map = {}
    data_date = 'N/A'

    # ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ TWSE TWT93UÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¸ÃÂÃÂ¶ date ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
    for delta in range(0, 5):
        try_date = (TODAY - timedelta(days=delta)).strftime('%Y%m%d')
        try:
            url = 'https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U'
            r = requests.get(url, params={'date': try_date, 'response': 'json'},
                             headers=HEADERS, timeout=20)
            d = r.json()
            if d.get('stat') == 'OK' and d.get('data'):
                twse_map = _parse_short_rows(d['data'], try_date)
                short_map.update(twse_map)
                data_date = try_date
                print(f"  ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ TWSE: {len(twse_map)} ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ{try_date}ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ")
                break
        except Exception as e:
            print(f"  ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ  TWSE {try_date}: {e}")

    # ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ TPEX SBLÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¦ÃÂÃÂ«ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂªÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¿ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
    try:
        url = 'https://www.tpex.org.tw/www/zh-tw/margin/sbl'
        r = requests.get(url, headers=HEADERS, timeout=20)
        d = r.json()
        if d.get('stat') == 'ok' and d.get('tables'):
            rows = d['tables'][0]['data']
            tpex_date = d.get('date', data_date)
            tpex_map = _parse_short_rows(rows, tpex_date)
            # TPEX ÃÂÃÂ¨ÃÂÃÂ£ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ ÃÂÃÂ´ÃÂÃÂ¦ÃÂÃÂ²ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¦ÃÂÃÂ«ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂ TWSE ÃÂÃÂ¥ÃÂÃÂ·ÃÂÃÂ²ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ
            added = 0
            for code, v in tpex_map.items():
                if code not in short_map:
                    short_map[code] = v
                    added += 1
            print(f"  ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ TPEX: {len(tpex_map)} ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¥ÃÂÃÂ¢ÃÂÃÂ {added} ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¦ÃÂÃÂ«ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ{tpex_date}ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ")
            if data_date == 'N/A':
                data_date = tpex_date
    except Exception as e:
        print(f"  ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ  TPEX: {e}")

    if short_map:
        print(f"  ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂ: {len(short_map)} ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂ¥ÃÂÃÂ¨")
        return short_map, data_date

    print("  ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ  ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ")
    return {}, 'N/A'


# ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
# 4. ÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ§ÃÂÃÂ®ÃÂÃÂÃÂÃÂ¤ÃÂÃÂºÃÂÃÂ¤ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂªÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ©ÃÂÃÂÃÂÃÂ±ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¤ÃÂÃÂºÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ«ÃÂÃÂ©ÃÂÃÂÃÂÃÂ±ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ«ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ
# ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
def trading_days_between(start_str, end_date=None):
    if not start_str:
        return None
    if end_date is None:
        end_date = TODAY
    try:
        start = datetime.strptime(start_str, '%Y-%m-%d').date()
        if start > end_date:
            return -1
        count = 0
        cur = start
        while cur <= end_date:
            if cur.weekday() < 5:  # ÃÂÃÂ©ÃÂÃÂÃÂÃÂ±ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂ=0 ÃÂÃÂ©ÃÂÃÂÃÂÃÂ±ÃÂÃÂ¤ÃÂÃÂºÃÂÃÂ=4
                count += 1
            cur += timedelta(days=1)
        return count - 1  # ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¶ÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂ©ÃÂÃÂ§ÃÂÃÂ®ÃÂÃÂ Day 0
    except:
        return None

def calendar_days_to(maturity_str, end_date=None):
    if end_date is None:
        end_date = TODAY
    try:
        mat = datetime.strptime(maturity_str, '%Y-%m-%d').date()
        return (mat - end_date).days
    except:
        return 0


# ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
# 5. ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¼ÃÂÃÂ¯
# ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
def evaluate_s1(cb, short_map, recent_map):
    """ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂCBAS ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ3 ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ"""
    rec = recent_map.get(cb['cb_code'], {})
    listing_date = rec.get('listing_date') or cb.get('listing_date')
    if not listing_date:
        return None  # ÃÂÃÂ¦ÃÂÃÂ²ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¦ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ·

    td = trading_days_between(listing_date)
    if td is None or td < 0:
        return {'signal': 'ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ³ÃÂÃÂ¥ÃÂÃÂ°ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¸ÃÂÃÂ', 'cls': 'info', 'td': td,
                'c1': False, 'c2': False, 'c3': None}

    cbp  = cb.get('cb_price') or 0
    sc   = cb['stock_code']
    sh   = short_map.get(sc)

    c1 = 4 <= td <= 8         # ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ D4-D8
    c2 = cbp >= 98             # ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶ÃÂÃÂ¤ÃÂÃÂºÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂCB ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¥ 98
    if sh is None:
        c3 = None              # ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂºÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ
    else:
        c3 = sh['increasing']  # ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¥ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ 

    n_ok = sum(x for x in [c1, c2] if x) + (1 if c3 else 0)

    if not c1:
        if td <= 3:
            sig, cls = f'ÃÂÃÂ¨ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¯ÃÂÃÂ D{td}', 'watch'
        elif 8 < td <= 20:
            sig, cls = f'ÃÂÃÂ¥ÃÂÃÂÃÂÃÂºÃÂÃÂ¥ÃÂÃÂ ÃÂÃÂ´ D{td}', 'sell'
        else:
            sig, cls = 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ', 'neutral'
    elif c1 and c2 and c3:
        sig, cls = f'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¨ÃÂÃÂ²ÃÂÃÂ·ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ D{td} (3/3)', 'buy'
    elif c1 and c2 and c3 is None:
        sig, cls = f'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂº D{td}', 'watch'
    elif c1 and c2:
        sig, cls = f'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¦ÃÂÃÂÃÂÃÂªÃÂÃÂ¥ÃÂÃÂ¢ÃÂÃÂ D{td}', 'sell'
    else:
        sig, cls = f'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ D{td}', 'neutral'

    return {'signal': sig, 'cls': cls, 'td': td,
            'c1': c1, 'c2': c2, 'c3': c3,
            'short_today':  sh['short_today']  if sh else None,
            'short_change': sh['short_change'] if sh else None,
            'listing_date': listing_date}


def evaluate_s2(cb, short_map):
    """ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂºÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ4 ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ"""
    prem  = cb.get('premium_rate') or 0
    conv  = cb.get('converted_pct') or 0
    dtm   = calendar_days_to(cb.get('maturity_date', ''))
    sc    = cb['stock_code']
    sh    = short_map.get(sc)

    d1 = prem <= 2            # ÃÂÃÂ¦ÃÂÃÂºÃÂÃÂ¢ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹ ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ 2%
    d2 = conv < 60            # ÃÂÃÂ¥ÃÂÃÂ·ÃÂÃÂ²ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ < 60%
    d3 = dtm >= 90            # ÃÂÃÂ¨ÃÂÃÂ·ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¥ 90 ÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂ©
    d4 = sh['increasing'] if sh else None  # ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¥ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ 

    if sh is None:
        short_today = None; short_change = None
    else:
        short_today = sh['short_today']; short_change = sh['short_change']

    if d1 and d2 and d3 and d4:
        sig, cls = 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ© (4/4)', 'buy'
    elif d1 and d2 and d3 and d4 is None:
        sig, cls = 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂº (3+?/4)', 'watch'
    elif d1 and d2 and d3:
        sig, cls = 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¦ÃÂÃÂÃÂÃÂªÃÂÃÂ¥ÃÂÃÂ¢ÃÂÃÂ (3/4)', 'sell'
    elif prem <= 5 and d2 and d3:
        sig, cls = 'ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¨ÃÂÃÂ¿ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ', 'watch'
    else:
        sig, cls = 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ', 'neutral'

    return {'signal': sig, 'cls': cls,
            'c1': d1, 'c2': d2, 'c3': d3, 'c4': d4,
            'days_to_mat': dtm,
            'short_today': short_today, 'short_change': short_change}


# ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
# 6. ÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ HTML
# ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
def chk(ok, na=False):
    if na:  return '<span class="chk chk-na">?</span>'
    return '<span class="chk chk-y">ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ</span>' if ok else '<span class="chk chk-n">ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ</span>'

def fmt(v, d=1):
    if v is None: return 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ'
    try: return f'{float(v):.{d}f}'
    except: return 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ'

def sc_fmt(v):
    if v is None: return 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ'
    return f'+{v}' if v > 0 else str(v)

def sc_cls(v):
    if v is None or v == 0: return ''
    return 'short-up' if v > 0 else 'short-dn'

def generate_html(all_cbs, recent_map, short_map, short_date):
    # ÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ§ÃÂÃÂ®ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂ
    results = []
    for cb in all_cbs:
        s1 = evaluate_s1(cb, short_map, recent_map)
        s2 = evaluate_s2(cb, short_map)
        results.append({**cb, 's1': s1, 's2': s2})

    # ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ©ÃÂÃÂ¡ÃÂÃÂ
    s1_items = [r for r in results if r['s1'] and r['s1']['td'] is not None and r['s1']['td'] >= 0 and r['s1']['td'] <= 20]
    s1_items.sort(key=lambda x: x['s1']['td'])
    s2_items = sorted(results, key=lambda x: (
        0 if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in x['s2']['signal'] else 1 if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in x['s2']['signal'] else 2 if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' not in x['s2']['signal'] and x['s2']['signal'] != 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' else 3,
        x.get('premium_rate') or 99
    ))

    s1_buy   = sum(1 for r in results if r['s1'] and 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in r['s1']['signal'])
    s1_pend  = sum(1 for r in results if r['s1'] and 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in r['s1']['signal'])
    s2_buy   = sum(1 for r in results if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in r['s2']['signal'])
    s2_pend  = sum(1 for r in results if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in r['s2']['signal'])

    # ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ S1 rows ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
    s1_rows_html = ''
    for r in s1_items:
        s1 = r['s1']
        cbas = 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ' if s1['td'] >= 6 else f'D6ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ'
        s1_rows_html += f"""<tr class="{'row-buy' if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in s1['signal'] else 'row-watch' if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in s1['signal'] else 'row-sell' if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in s1['signal'] else ''}">
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="center">D{s1['td']}</td>
  <td class="center">{cbas}</td>
  <td class="center cond">{chk(s1['c1'])} ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ<br>{chk(s1['c2'])} CBÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹ÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂ¨ÃÂÃÂ<br>{chk(s1['c3'], s1['c3'] is None)} ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ</td>
  <td class="num">{fmt(s1.get('short_today'),0)}ÃÂÃÂ¥ÃÂÃÂ¼ÃÂÃÂµ</td>
  <td class="num {sc_cls(s1.get('short_change'))}">{sc_fmt(s1.get('short_change'))}</td>
  <td class="center"><span class="badge {s1['cls']}">{s1['signal']}</span></td>
</tr>"""

    # ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ S2 rows ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
    s2_rows_html = ''
    for r in s2_items:
        s2 = r['s2']
        pc = 'prem-neg' if (r.get('premium_rate') or 0) < 0 else ''
        s2_rows_html += f"""<tr class="{'row-buy' if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in s2['signal'] else 'row-watch' if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in s2['signal'] or 'ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¨ÃÂÃÂ¿ÃÂÃÂ' in s2['signal'] else ''}">
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="num {pc}">{fmt(r.get('premium_rate'))}%</td>
  <td class="num">{fmt(r.get('stock_price'))}</td>
  <td class="num">{fmt(r.get('conversion_price'))}</td>
  <td class="center cond">{chk(s2['c1'])} ÃÂÃÂ¤ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂºÃÂÃÂ¢ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹<br>{chk(s2['c2'])} ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂ¯ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¾ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ½ÃÂÃÂ<br>{chk(s2['c3'])} ÃÂÃÂ¨ÃÂÃÂ·ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ£ÃÂÃÂ<br>{chk(s2['c4'], s2['c4'] is None)} ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ</td>
  <td class="num">{s2['days_to_mat']}ÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂ©</td>
  <td class="num">{fmt(s2.get('short_today'),0)}ÃÂÃÂ¥ÃÂÃÂ¼ÃÂÃÂµ</td>
  <td class="num {sc_cls(s2.get('short_change'))}">{sc_fmt(s2.get('short_change'))}</td>
  <td class="center"><span class="badge {s2['cls']}">{s2['signal']}</span></td>
</tr>"""

    # ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ All rows ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
    all_rows_html = ''
    for r in results:
        s1 = r['s1']
        s2 = r['s2']
        s1sig = s1['signal'] if s1 else 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ'
        s1cls = s1['cls'] if s1 else 'neutral'
        pc = 'prem-neg' if (r.get('premium_rate') or 0) < 0 else ''
        sh = short_map.get(r['stock_code'])
        all_rows_html += f"""<tr>
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="num {pc}">{fmt(r.get('premium_rate'))}%</td>
  <td class="num">{fmt(r.get('stock_price'))}</td>
  <td class="num">{fmt(r.get('conversion_price'))}</td>
  <td>{r.get('maturity_date','ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ')}</td>
  <td class="num">{fmt(sh['short_today'],0) if sh else 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ'}ÃÂÃÂ¥ÃÂÃÂ¼ÃÂÃÂµ</td>
  <td class="num {sc_cls(sh['short_change'] if sh else None)}">{sc_fmt(sh['short_change'] if sh else None)}</td>
  <td class="center"><span class="badge {s1cls}">{s1sig}</span></td>
  <td class="center"><span class="badge {s2['cls']}">{s2['signal']}</span></td>
</tr>"""

    CSS = """
:root{--bl:#2563eb;--gr:#16a34a;--rd:#dc2626;--am:#d97706;--bg:#f8fafc;--brd:#e2e8f0;--txt:#1e293b;--mu:#64748b}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:var(--bg);color:var(--txt);font-size:13px}
.hdr{background:linear-gradient(135deg,#1e40af,#3b82f6);color:#fff;padding:18px 24px 14px}
.hdr h1{font-size:18px;font-weight:700;margin-bottom:3px}
.hdr .sub{font-size:11px;opacity:.8}
.stats{display:flex;gap:10px;padding:14px 24px;background:#fff;border-bottom:1px solid var(--brd);flex-wrap:wrap}
.sc{background:var(--bg);border:1px solid var(--brd);border-radius:8px;padding:10px 16px;min-width:110px}
.sc .n{font-size:24px;font-weight:700;color:var(--bl)}.sc .l{font-size:11px;color:var(--mu);margin-top:2px}
.sc.gr .n{color:var(--gr)}.sc.am .n{color:var(--am)}
.tabs{display:flex;padding:0 24px;border-bottom:2px solid var(--brd);background:#fff}
.tab{padding:11px 20px;cursor:pointer;border-bottom:3px solid transparent;font-size:12px;font-weight:600;color:var(--mu);margin-bottom:-2px}
.tab.active{border-bottom-color:var(--bl);color:var(--bl)}
.pane{display:none;padding:18px 24px}.pane.active{display:block}
.ttl{font-size:14px;font-weight:700;margin-bottom:4px}
.desc{font-size:12px;color:var(--mu);margin-bottom:12px;line-height:1.6}
.tag{display:inline-block;background:#dbeafe;color:#1d4ed8;border-radius:4px;padding:1px 6px;font-size:11px;font-weight:600;margin-right:3px}
.box{background:#eff6ff;border:1px solid #bfdbfe;border-radius:7px;padding:9px 13px;font-size:11.5px;color:#1e40af;margin-bottom:12px;line-height:1.7}
.box.warn{background:#fffbeb;border-color:#fde68a;color:#92400e}
table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.06)}
th{background:#f1f5f9;font-weight:700;color:var(--mu);font-size:10.5px;text-transform:uppercase;padding:9px 10px;text-align:left;border-bottom:2px solid var(--brd);white-space:nowrap}
td{padding:7px 10px;border-bottom:1px solid var(--brd);vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#f8fafc}
.num{text-align:right;font-variant-numeric:tabular-nums}
.center{text-align:center}
tr.row-buy td{background:#f0fdf4}
tr.row-buy:hover td{background:#dcfce7}
tr.row-watch td{background:#fefce8}
tr.row-sell td{background:#fff7ed}
.badge{display:inline-block;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:700;white-space:nowrap}
.badge.buy{background:#dcfce7;color:#15803d}
.badge.watch{background:#fef9c3;color:#854d0e}
.badge.sell{background:#ffedd5;color:#c2410c}
.badge.info{background:#dbeafe;color:#1d4ed8}
.badge.neutral{background:#f1f5f9;color:#94a3b8}
.cond{font-size:11px;line-height:2;white-space:nowrap}
.chk{display:inline-block;width:15px;height:15px;border-radius:50%;font-size:9px;text-align:center;line-height:15px;font-weight:700;margin-right:2px}
.chk-y{background:#dcfce7;color:#15803d}
.chk-n{background:#fee2e2;color:#dc2626}
.chk-na{background:#fef9c3;color:#854d0e}
.short-up{color:#16a34a;font-weight:700}
.short-dn{color:#dc2626;font-weight:700}
.prem-neg{color:#16a34a;font-weight:700}
.ft{text-align:center;color:var(--mu);font-size:11px;padding:16px;border-top:1px solid var(--brd)}
@media(max-width:768px){.hdr,.stats,.pane{padding-left:14px;padding-right:14px}.tabs{padding:0 14px;overflow-x:auto}th,td{padding:7px 7px}}
"""

    html = f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂµÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¡ÃÂÃÂ¨ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¿</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>ÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂ ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂµÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¡ÃÂÃÂ¨ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¿</h1>
  <div class="sub">ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ´ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ{TODAY}</div>
</div>
<div class="stats">
  <div class="sc"><div class="n">{len(all_cbs)}</div><div class="l">ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ©ÃÂÃÂÃÂÃÂ¨CBÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¸</div></div>
  <div class="sc gr"><div class="n">{s1_buy}</div><div class="l">S1ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶ÃÂÃÂ¨ÃÂÃÂ²ÃÂÃÂ·ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥</div></div>
  <div class="sc am"><div class="n">{s1_pend}</div><div class="l">S1ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂº</div></div>
  <div class="sc gr"><div class="n">{s2_buy}</div><div class="l">S2ÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ©(4/4)</div></div>
  <div class="sc am"><div class="n">{s2_pend}</div><div class="l">S2ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂº</div></div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('s1',this)">ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂCBASÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¸ÃÂÃÂ</div>
  <div class="tab" onclick="showTab('s2',this)">ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂºÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ{len(all_cbs)}ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ</div>
  <div class="tab" onclick="showTab('all',this)">ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ©ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂµ</div>
</div>
<div id="pane-s1" class="pane active">
  <div class="ttl">ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂCBAS ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ­ÃÂÃÂ¥ÃÂÃÂ£ÃÂÃÂ</div>
  <div class="desc">ÃÂÃÂ¦ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¤ÃÂÃÂºÃÂÃÂºÃÂÃÂ¨ÃÂÃÂ²ÃÂÃÂ·CB ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂºÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂ¥ÃÂÃÂ¨ (D1ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ5) ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ D6 CBASÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ§ÃÂÃÂ£ ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ£ÃÂÃÂ ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂ½ÃÂÃÂ<br>
    <span class="tag">ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶1</span>ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂºÃÂÃÂ¤ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥
    <span class="tag">ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶2</span>CBÃÂÃÂ§ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹ÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ®ÃÂÃÂÃÂÃÂ¦ÃÂÃÂ°ÃÂÃÂ´ÃÂÃÂ¦ÃÂÃÂºÃÂÃÂ
    <span class="tag">ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶3</span>ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ©ÃÂÃÂ¤ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¡ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ </div>
  <div class="box"><b>ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ ÃÂÃÂ¨ÃÂÃÂªÃÂÃÂªÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ</b>
    <span class="chk chk-y">ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ</span>ÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂ¨ÃÂÃÂ &nbsp;
    <span class="chk chk-n">ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ</span>ÃÂÃÂ¦ÃÂÃÂÃÂÃÂªÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂ¨ÃÂÃÂ &nbsp;
    <span class="chk chk-na">?</span>ÃÂÃÂ¨ÃÂÃÂ©ÃÂÃÂ²ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂÃÂÃÂ®ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂºÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂTWSE TWT93U ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¦ÃÂÃÂ­ÃÂÃÂ¤ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ<br>
    ÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¾ÃÂÃÂÃÂÃÂ¦ÃÂÃÂºÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂTWSEÃÂÃÂ£ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂ£ÃÂÃÂ¥ÃÂÃÂÃÂÃÂºÃÂÃÂ©ÃÂÃÂ¤ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¡ÃÂÃÂÃÂÃÂ£ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂ¯ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¥ÃÂÃÂ¾ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂªÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ´ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ«ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ£ÃÂÃÂÃÂÃÂ</div>
  <table><thead><tr>
    <th>CBÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ£ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ</th><th>CBÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂ¨ÃÂÃÂ±</th><th>ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂ¥ÃÂÃÂ¨</th><th class="num">CBÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹</th>
    <th class="center">ÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂ©ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¸</th><th class="center">CBAS</th>
    <th class="center">ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶1/2/3</th>
    <th class="num">ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ©ÃÂÃÂ¤ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¡ÃÂÃÂ</th><th class="num">ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¨ÃÂÃÂ®ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ</th><th class="center">ÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂ</th>
  </tr></thead><tbody>{s1_rows_html}</tbody></table>
</div>
<div id="pane-s2" class="pane">
  <div class="ttl">ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂºÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ©ÃÂÃÂÃÂÃÂ¨ {len(all_cbs)} ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¯ CBÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ</div>
  <div class="desc">ÃÂÃÂ¨ÃÂÃÂ²ÃÂÃÂ·CB + ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂºÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂ¥ÃÂÃÂ¨ ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¾ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂ¥ÃÂÃÂ¨ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ£ÃÂÃÂ ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ©<br>
    <span class="tag">ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶1</span>ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂºÃÂÃÂ¢ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹ÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ½ÃÂÃÂ <span class="tag">ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶2</span>ÃÂÃÂ¥ÃÂÃÂ·ÃÂÃÂ²ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂ¯ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¾ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ½ÃÂÃÂ
    <span class="tag">ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶3</span>ÃÂÃÂ¨ÃÂÃÂ·ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ£ÃÂÃÂ <span class="tag">ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶4</span>ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¥ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ </div>
  <div class="box warn"><b>ÃÂÃÂ¦ÃÂÃÂ³ÃÂÃÂ¨ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ</b>ÃÂÃÂ¦ÃÂÃÂºÃÂÃÂ¢ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹ÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ©ÃÂÃÂ¡ÃÂÃÂ¯ÃÂÃÂ§ÃÂÃÂ¤ÃÂÃÂº<span style="color:#16a34a;font-weight:700">ÃÂÃÂ§ÃÂÃÂ¶ÃÂÃÂ ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ²</span>ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ²ÃÂÃÂ ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¼ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ£ÃÂÃÂ¨ÃÂÃÂ¡ÃÂÃÂ¨CBÃÂÃÂ¤ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¼ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¼ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ©ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂºÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¤ÃÂÃÂ§ÃÂÃÂ£ÃÂÃÂÃÂÃÂ
    ÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂ¢ÃÂÃÂºÃÂÃÂ¨ÃÂÃÂªÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂ+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¦ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¶ÃÂÃÂ³ÃÂÃÂ£ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ©ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¶ÃÂÃÂ£ÃÂÃÂÃÂÃÂ</div>
  <table><thead><tr>
    <th>CBÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ£ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ</th><th>CBÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂ¨ÃÂÃÂ±</th><th>ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂ¥ÃÂÃÂ¨</th><th class="num">CBÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹</th>
    <th class="num">ÃÂÃÂ¦ÃÂÃÂºÃÂÃÂ¢ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹ÃÂÃÂ§ÃÂÃÂÃÂÃÂ</th><th class="num">ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹</th><th class="num">ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹</th>
    <th class="center">ÃÂÃÂ¦ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ¶1/2/3/4</th>
    <th class="num">ÃÂÃÂ¨ÃÂÃÂ·ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ</th><th class="num">ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ©ÃÂÃÂ¤ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¡ÃÂÃÂ</th><th class="num">ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¨ÃÂÃÂ®ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ</th>
    <th class="center">ÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂ</th>
  </tr></thead><tbody>{s2_rows_html}</tbody></table>
</div>
<div id="pane-all" class="pane">
  <div class="ttl">ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ©ÃÂÃÂÃÂÃÂ¨ {len(all_cbs)} ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂµ</div>
  <div class="desc">ÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¾ÃÂÃÂÃÂÃÂ¦ÃÂÃÂºÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂthefew.twÃÂÃÂ¯ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂTWSE TWT93U {short_date}</div>
  <table><thead><tr>
    <th>CBÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ£ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ</th><th>CBÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂ¨ÃÂÃÂ±</th><th>ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂ¥ÃÂÃÂ¨</th><th class="num">CBÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹</th>
    <th class="num">ÃÂÃÂ¦ÃÂÃÂºÃÂÃÂ¢ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹ÃÂÃÂ§ÃÂÃÂÃÂÃÂ</th><th class="num">ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹</th><th class="num">ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¹</th>
    <th>ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ°ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥</th><th class="num">ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ©ÃÂÃÂ¤ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¡ÃÂÃÂ</th><th class="num">ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¨ÃÂÃÂ®ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ</th>
    <th class="center">S1</th><th class="center">S2</th>
  </tr></thead><tbody>{all_rows_html}</tbody></table>
</div>
<div class="ft">ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¬ÃÂÃÂ¥ÃÂÃÂ·ÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ·ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¾ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ­ÃÂÃÂ¸ÃÂÃÂ§ÃÂÃÂ¿ÃÂÃÂÃÂÃÂ§ÃÂÃÂ ÃÂÃÂÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂ¶ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¦ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ»ÃÂÃÂºÃÂÃÂ¨ÃÂÃÂ­ÃÂÃÂ°ÃÂÃÂ£ÃÂÃÂÃÂÃÂ<br>
ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¾ÃÂÃÂÃÂÃÂ¦ÃÂÃÂºÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂTWSEÃÂÃÂ£ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂ£ÃÂÃÂ¥ÃÂÃÂÃÂÃÂºÃÂÃÂ©ÃÂÃÂ¤ÃÂÃÂÃÂÃÂ©ÃÂÃÂ¡ÃÂÃÂ(TWT93U)ÃÂÃÂ£ÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¦ÃÂÃÂ¯ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¥ÃÂÃÂ¾ÃÂÃÂÃÂÃÂ§ÃÂÃÂ´ÃÂÃÂ17:30ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ´ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂ£ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ®ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ§ÃÂÃÂ¤ÃÂÃÂ»ÃÂÃÂ£ÃÂÃÂ¨ÃÂÃÂ¡ÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂ©ÃÂÃÂ²ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂÃÂÃÂ®ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¾ÃÂÃÂ§ÃÂÃÂ©ÃÂÃÂºÃÂÃÂ£ÃÂÃÂÃÂÃÂ</div>
<script>
function showTab(id,el){{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('pane-'+id).classList.add('active');
}}
</script></body></html>"""
    return html


# ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
# 7. ÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂ»ÃÂÃÂ§ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¼ÃÂÃÂ
# ÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ
def main():
    print(f"\n=== ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¨ÃÂÃÂ½ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂµÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ {TODAY} ===")
    all_cbs    = fetch_all_cbs()
    recent_map = fetch_recent_cbs()
    short_map, short_date = fetch_short_data()

    # ÃÂÃÂ¨ÃÂÃÂ£ÃÂÃÂÃÂÃÂ¤ÃÂÃÂ¸ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂÃÂÃÂ¥ÃÂÃÂ¾ÃÂÃÂ recent_map ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¤ÃÂÃÂ½ÃÂÃÂµÃÂÃÂ¥ÃÂÃÂÃÂÃÂ° all_cbsÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ
    rec_ld = {cb_code: d.get('listing_date') for cb_code, d in recent_map.items()}
    for cb in all_cbs:
        if cb['cb_code'] in rec_ld:
            cb['listing_date'] = rec_ld[cb['cb_code']]

    html = generate_html(all_cbs, recent_map, short_map, short_date)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\nÃÂÃÂ¢ÃÂÃÂÃÂÃÂ ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¨ÃÂÃÂ¡ÃÂÃÂ¨ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¿ÃÂÃÂ¥ÃÂÃÂ·ÃÂÃÂ²ÃÂÃÂ§ÃÂÃÂÃÂÃÂ¢ÃÂÃÂ§ÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ{OUTPUT_HTML}")
    print(f"   ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¨ÃÂÃÂ©ÃÂÃÂÃÂÃÂ¨CB: {len(all_cbs)} ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂ")
    print(f"   ÃÂÃÂ¨ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸+ÃÂÃÂ¥ÃÂÃÂÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¸ÃÂÃÂ¨ÃÂÃÂ³ÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ: {len(short_map)} ÃÂÃÂ¦ÃÂÃÂÃÂÃÂ¯ÃÂÃÂ¨ÃÂÃÂÃÂÃÂ¡ÃÂÃÂ§ÃÂÃÂ¥ÃÂÃÂ¨ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ{short_date}ÃÂÃÂ¯ÃÂÃÂ¼ÃÂÃÂ")

    # ÃÂÃÂ§ÃÂÃÂµÃÂÃÂ±ÃÂÃÂ¨ÃÂÃÂ¨ÃÂÃÂ
    s1_buy = s2_buy = 0
    for cb in all_cbs:
        s1 = evaluate_s1(cb, short_map, recent_map)
        s2 = evaluate_s2(cb, short_map)
        if s1 and 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in s1['signal']: s1_buy += 1
        if 'ÃÂÃÂ¢ÃÂÃÂÃÂÃÂ' in s2['signal']: s2_buy += 1
    print(f"   S1ÃÂÃÂ¨ÃÂÃÂ²ÃÂÃÂ·ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥: {s1_buy} ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂ | S2ÃÂÃÂ¥ÃÂÃÂ¥ÃÂÃÂÃÂÃÂ¥ÃÂÃÂÃÂÃÂ©: {s2_buy} ÃÂÃÂ§ÃÂÃÂ­ÃÂÃÂ")

if __name__ == '__main__':
    main()
