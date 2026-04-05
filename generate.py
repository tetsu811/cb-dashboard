#!/usr/bin/env python3
"""
Ã¥ÂÂ¯Ã¨Â½ÂÃ¥ÂÂµÃ§Â­ÂÃ§ÂÂ¥Ã¥ÂÂÃ¨Â¡Â¨Ã¦ÂÂ¿
- CB Ã¨Â³ÂÃ¦ÂÂÃ¯Â¼Âthefew.tw/cbÃ¯Â¼ÂÃ¥ÂÂ¨Ã©ÂÂ¨ 400+ Ã§Â­ÂÃ¯Â¼Â+ /cb/recentÃ¯Â¼ÂÃ¥ÂÂ«Ã¦ÂÂÃ§ÂÂÃ¦ÂÂ¥Ã¯Â¼ÂÃ§Â­ÂÃ§ÂÂ¥Ã¤Â¸ÂÃ§ÂÂ¨Ã¯Â¼Â
- Ã¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¯Â¼ÂTWSE TWT93UÃ¯Â¼ÂÃ¦Â¯ÂÃ¦ÂÂ¥Ã§ÂÂ¤Ã¥Â¾ÂÃ¨ÂÂªÃ¥ÂÂÃ¦ÂÂ´Ã¦ÂÂ°Ã¯Â¼ÂÃ§ÂÂ¡Ã©ÂÂÃ§ÂÂ»Ã¥ÂÂ¥Ã¯Â¼Â
- Ã§Â­ÂÃ§ÂÂ¥Ã¤Â¸ÂÃ¯Â¼ÂCBASÃ¦ÂÂ°Ã¤Â¸ÂÃ¥Â¸ÂÃ¯Â¼ÂÃ©ÂÂÃ¦ÂÂÃ§ÂÂÃ¦ÂÂ¥Ã¯Â¼ÂÃ¤Â¾ÂÃ¨ÂÂª /cb/recentÃ¯Â¼Â
- Ã§Â­ÂÃ§ÂÂ¥Ã¤ÂºÂÃ¯Â¼ÂÃ¨Â½ÂÃ¦ÂÂÃ¥Â¥ÂÃ¥ÂÂ©Ã¯Â¼ÂÃ¥ÂÂ¨Ã©ÂÂ¨ CBÃ¯Â¼Â
"""

import requests
import json
import os
import re
import time
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from playwright.sync_api import sync_playwright

# Ã¢ÂÂÃ¢ÂÂ Ã¨Â·Â¯Ã¥Â¾ÂÃ¨Â¨Â­Ã¥Â®ÂÃ¯Â¼ÂGitHub Actions Ã§ÂÂ¨Ã¯Â¼ÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_HTML = os.path.join(BASE_DIR, 'index.html')

TODAY = date.today()
TODAY_STR = TODAY.strftime('%Y%m%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36',
    'Accept-Language': 'zh-TW,zh;q=0.9',
    'Referer': 'https://thefew.tw/',
}

# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
# # Ã¥ÂÂ±Ã§ÂÂ¨Ã¯Â¼ÂÃ¨Â§Â£Ã¦ÂÂÃ¦ÂÂ¸Ã¥Â­ÂÃ¯Â¼ÂÃ¥ÂÂ«Ã¨Â²Â Ã¦ÂÂ¸Ã¯Â¼Â
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def parse_num(txt):
    m = re.match(r'^(-?[\d.]+)', txt.strip())
    return float(m.group(1)) if m else None


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
# 1. Ã¦ÂÂÃ¥ÂÂ¨Ã©ÂÂ¨ CBÃ¯Â¼ÂPlaywright Ã¨Â¼ÂÃ¥ÂÂ¥ thefew.tw/cbÃ¯Â¼ÂÃ¥ÂÂÃ¥Â¾Â 400+ Ã§Â­ÂÃ¥Â®ÂÃ¦ÂÂ´Ã¨Â³ÂÃ¦ÂÂÃ¯Â¼Â
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def fetch_all_cbs():
    print("[1/3] Ã¦ÂÂÃ¥ÂÂÃ¥ÂÂ¨Ã©ÂÂ¨CB (thefew.tw/cb) Ã¢ÂÂ Playwright...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = ctx.new_page()
            page.goto('https://thefew.tw/cb', wait_until='networkidle', timeout=120000)
            page.wait_for_timeout(5000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, 'html.parser')
        rows = soup.select('#cb-table tbody tr')
        data = []
        for tr in rows:
            cells = tr.select('td')
            if len(cells) != 8:
                continue
            code_div = cells[0].select_one('div[class*="w-1/3"]')
            name_div = cells[0].select_one('div[class*="w-2/3"]')
            cb_code  = code_div.get_text(strip=True) if code_div else ''
            cb_name  = name_div.get_text(strip=True) if name_div else ''
            if not cb_code or len(cb_code) < 4:
                continue
            data.append({
                'cb_code':          cb_code,
                'cb_name':          cb_name,
                'stock_code':       cb_code[:4],
                'cb_price':         parse_num(cells[1].get_text()),
                'conv_val':         parse_num(cells[2].get_text()),
                'premium_rate':     parse_num(cells[3].get_text().replace('%','')),
                'stock_price':      parse_num(cells[4].get_text()),
                'conversion_price': parse_num(cells[5].get_text()),
                'converted_pct':    parse_num(cells[6].get_text().replace('%','')) or 0.0,
                'maturity_date':    cells[7].get_text(strip=True),
                'listing_date':     None,
            })
        print(f"  Ã¢ÂÂ Ã¥ÂÂ¨Ã©ÂÂ¨CB: {len(data)} Ã§Â­Â")
        if len(data) < 50:
            raise ValueError(f"Ã¨Â³ÂÃ¦ÂÂÃ¤Â¸ÂÃ¨Â¶Â³Ã¯Â¼ÂÃ¥ÂÂ {len(data)} Ã§Â­ÂÃ¯Â¼ÂÃ©Â ÂÃ¦ÂÂ 400+Ã¯Â¼Â")
        return data
    except Exception as e:
        print(f"  Ã¢ÂÂ  Ã§ÂÂ¡Ã¦Â³ÂÃ¦ÂÂÃ¥ÂÂ thefew.tw/cb: {e}")
        raise


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
# 2. Ã¦ÂÂÃ¨Â¿ÂÃ¦ÂÂCBÃ¯Â¼Âthefew.tw/cb/recent Ã¢ÂÂ Ã¥ÂÂ«Ã¦ÂÂÃ§ÂÂÃ¦ÂÂ¥Ã¯Â¼ÂÃ§Â­ÂÃ§ÂÂ¥Ã¤Â¸ÂÃ¥Â¿ÂÃ©ÂÂÃ¯Â¼Â
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def fetch_recent_cbs():
    print("[2/3] Ã¦ÂÂÃ¥ÂÂÃ¨Â¿ÂÃ¦ÂÂCB (thefew.tw/cb/recent) Ã¢ÂÂ Playwright...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = ctx.new_page()
            page.goto('https://thefew.tw/cb/recent', wait_until='networkidle', timeout=120000)
            page.wait_for_timeout(5000)
            html = page.content()
            browser.close()
        soup = BeautifulSoup(html, 'html.parser')

        # Ã¦ÂÂ¾Ã¥ÂÂ«Ã¦ÂÂÃ§ÂÂÃ¦ÂÂ¥Ã§ÂÂ JSON Ã¨Â³ÂÃ¦ÂÂÃ¯Â¼ÂÃ¥ÂµÂÃ¥ÂÂ¨Ã©Â ÂÃ©ÂÂ¢ script Ã¦ÂÂ table Ã¨Â£Â¡Ã¯Â¼Â
        data = []
        rows = soup.select('table tbody tr')
        for tr in rows:
            cells = tr.select('td')
            if len(cells) < 7:
                continue
            # Ã§ÂµÂÃ¦Â§ÂÃ¥ÂÂ¯Ã¨ÂÂ½Ã¤Â¸ÂÃ¥ÂÂÃ¯Â¼ÂÃ¤Â¾ÂÃ¥Â¯Â¦Ã©ÂÂÃ©Â ÂÃ©ÂÂ¢Ã¨ÂªÂ¿Ã¦ÂÂ´
            texts = [c.get_text(strip=True) for c in cells]
            # Ã¥ÂÂÃ¨Â©Â¦Ã¦ÂÂ cb_codeÃ¯Â¼ÂÃ©ÂÂÃ¥Â¸Â¸Ã¥ÂÂ¨Ã§Â¬Â¬Ã¤Â¸ÂÃ¦Â¬ÂÃ¯Â¼Â
            code_match = re.match(r'(\d{4,6})', texts[0])
            if not code_match:
                continue
            cb_code = code_match.group(1)

            # Ã¦ÂÂ¾Ã¦ÂÂÃ§ÂÂÃ¦ÂÂ¥Ã¯Â¼ÂÃ¦Â Â¼Ã¥Â¼Â YYYY-MM-DDÃ¯Â¼Â
            listing_date = None
            for t in texts:
                m = re.search(r'(\d{4}-\d{2}-\d{2})', t)
                if m:
                    listing_date = m.group(1)
                    break

            data.append({
                'cb_code':       cb_code,
                'stock_code':    cb_code[:4],
                'listing_date':  listing_date,
                'raw':           texts,
            })

        if data:
            print(f"  Ã¢ÂÂ Ã¨Â¿ÂÃ¦ÂÂCB: {len(data)} Ã§Â­Â")
            return {d['cb_code']: d for d in data}
        else:
            raise ValueError("Ã¨Â§Â£Ã¦ÂÂÃ¥ÂÂ° 0 Ã§Â­Â")

    except Exception as e:
        print(f"  Ã¢ÂÂ  Ã§ÂÂ¡Ã¦Â³ÂÃ¦ÂÂÃ¥ÂÂ /cb/recent: {e}")
        return {}


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
# 3. Ã¦ÂÂÃ¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¨Â³Â£Ã¥ÂÂºÃ©Â¤ÂÃ©Â¡Â
#    TWSE TWT93UÃ¯Â¼ÂÃ¤Â¸ÂÃ¥Â¸ÂÃ¨ÂÂ¡Ã§Â¥Â¨ 1,262 Ã¦ÂÂ¯Ã¯Â¼Âdate Ã¥ÂÂÃ¦ÂÂ¸Ã¯Â¼Â
#    TPEX SBL   Ã¯Â¼ÂÃ¤Â¸ÂÃ¦Â«ÂÃ¨ÂÂ¡Ã§Â¥Â¨  903 Ã¦ÂÂ¯Ã¯Â¼ÂÃ¨ÂÂªÃ¥ÂÂÃ¦ÂÂÃ¦ÂÂ°Ã¦ÂÂ¥Ã¯Â¼Â
#    Ã¥ÂÂ©Ã¨ÂÂÃ¦Â¬ÂÃ¤Â½ÂÃ§ÂÂ¸Ã¥ÂÂÃ¯Â¼ÂÃ¥ÂÂ®Ã¤Â½ÂÃ¯Â¼ÂÃ¨ÂÂ¡Ã¯Â¼Â/1000 = Ã¥Â¼ÂµÃ¯Â¼ÂÃ¯Â¼Â
#    [0]Ã¤Â»Â£Ã¨ÂÂ [1]Ã¥ÂÂÃ§Â¨Â±
#    Ã¨ÂÂÃ¨Â³Â: [2-7]
#    Ã¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸: [8]Ã¥ÂÂÃ¦ÂÂ¥Ã©Â¤ÂÃ©Â¡Â [9]Ã§ÂÂ¶Ã¦ÂÂ¥Ã¨Â³Â£Ã¥ÂÂº [10]Ã§ÂÂ¶Ã¦ÂÂ¥Ã©ÂÂÃ¥ÂÂ¸ [11]Ã¨ÂªÂ¿Ã¦ÂÂ´ [12]Ã¤Â»ÂÃ¦ÂÂ¥Ã©Â¤ÂÃ©Â¡Â [13]Ã©ÂÂÃ©Â¡Â
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def _parse_short_rows(rows, data_date):
    """Ã¥ÂÂ±Ã§ÂÂ¨Ã¨Â§Â£Ã¦ÂÂÃ©ÂÂÃ¨Â¼Â¯Ã¯Â¼ÂÃ¦ÂÂ TWSE/TPEX Ã§ÂÂ row Ã©ÂÂ£Ã¥ÂÂÃ¨Â½ÂÃ¦ÂÂ short_map"""
    def ti(s):
        try:
            return round(int(str(s).replace(',', '').strip() or '0') / 1000)
        except:
            return 0
    short_map = {}
    for row in rows:
        code = row[0]
        if not code or code == 'Ã¥ÂÂÃ¨Â¨Â':
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
    print("[3/3] Ã¦ÂÂÃ¥ÂÂÃ¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¨Â³ÂÃ¦ÂÂ (TWSE + TPEX)...")
    short_map = {}
    data_date = 'N/A'

    # Ã¢ÂÂÃ¢ÂÂ TWSE TWT93UÃ¯Â¼ÂÃ¤Â¸ÂÃ¥Â¸ÂÃ¯Â¼ÂÃ©ÂÂÃ¥Â¸Â¶ date Ã¥ÂÂÃ¦ÂÂ¸Ã¯Â¼ÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
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
                print(f"  Ã¢ÂÂ TWSE: {len(twse_map)} Ã¦ÂÂ¯Ã¯Â¼Â{try_date}Ã¯Â¼Â")
                break
        except Exception as e:
            print(f"  Ã¢ÂÂ  TWSE {try_date}: {e}")

    # Ã¢ÂÂÃ¢ÂÂ TPEX SBLÃ¯Â¼ÂÃ¤Â¸ÂÃ¦Â«ÂÃ¯Â¼ÂÃ¨ÂÂªÃ¥ÂÂÃ¨Â¿ÂÃ¥ÂÂÃ¦ÂÂÃ¦ÂÂ°Ã¦ÂÂ¥Ã¯Â¼ÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
    try:
        url = 'https://www.tpex.org.tw/www/zh-tw/margin/sbl'
        r = requests.get(url, headers=HEADERS, timeout=20)
        d = r.json()
        if d.get('stat') == 'ok' and d.get('tables'):
            rows = d['tables'][0]['data']
            tpex_date = d.get('date', data_date)
            tpex_map = _parse_short_rows(rows, tpex_date)
            # TPEX Ã¨Â£ÂÃ¤Â¸ÂÃ¥Â¸ÂÃ¥Â Â´Ã¦Â²ÂÃ¦ÂÂÃ§ÂÂÃ¤Â¸ÂÃ¦Â«ÂÃ¨ÂÂ¡Ã¯Â¼ÂÃ¤Â¸ÂÃ¨Â¦ÂÃ¨ÂÂ TWSE Ã¥Â·Â²Ã¦ÂÂÃ¨Â³ÂÃ¦ÂÂÃ¯Â¼Â
            added = 0
            for code, v in tpex_map.items():
                if code not in short_map:
                    short_map[code] = v
                    added += 1
            print(f"  Ã¢ÂÂ TPEX: {len(tpex_map)} Ã¦ÂÂ¯Ã¯Â¼ÂÃ¦ÂÂ°Ã¥Â¢Â {added} Ã¦ÂÂ¯Ã¤Â¸ÂÃ¦Â«ÂÃ¯Â¼Â{tpex_date}Ã¯Â¼Â")
            if data_date == 'N/A':
                data_date = tpex_date
    except Exception as e:
        print(f"  Ã¢ÂÂ  TPEX: {e}")

    if short_map:
        print(f"  Ã¢ÂÂ Ã¥ÂÂÃ¨Â¨Â: {len(short_map)} Ã¦ÂÂ¯Ã¨ÂÂ¡Ã§Â¥Â¨")
        return short_map, data_date

    print("  Ã¢ÂÂ  Ã§ÂÂ¡Ã¨ÂÂÃ¥ÂÂ¸Ã¨Â³ÂÃ¦ÂÂ")
    return {}, 'N/A'


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
# 4. Ã¨Â¨ÂÃ§Â®ÂÃ¤ÂºÂ¤Ã¦ÂÂÃ¦ÂÂ¥Ã¦ÂÂ¸Ã¯Â¼ÂÃ¥ÂÂªÃ¦ÂÂ¸Ã©ÂÂ±Ã¤Â¸ÂÃ¯Â½ÂÃ¤ÂºÂÃ¯Â¼ÂÃ¤Â¸ÂÃ¥ÂÂ«Ã©ÂÂ±Ã¦ÂÂ«Ã¯Â¼Â
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
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
            if cur.weekday() < 5:  # Ã©ÂÂ±Ã¤Â¸Â=0 Ã©ÂÂ±Ã¤ÂºÂ=4
                count += 1
            cur += timedelta(days=1)
        return count - 1  # Ã¦ÂÂÃ§ÂÂÃ§ÂÂ¶Ã¥Â¤Â©Ã§Â®Â Day 0
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


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
# 5. Ã§Â­ÂÃ§ÂÂ¥Ã¨Â¨ÂÃ¨ÂÂÃ©ÂÂÃ¨Â¼Â¯
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def evaluate_s1(cb, short_map, recent_map):
    """Ã§Â­ÂÃ§ÂÂ¥Ã¤Â¸ÂÃ¯Â¼ÂCBAS Ã¦ÂÂ°Ã¤Â¸ÂÃ¥Â¸ÂÃ¯Â¼Â3 Ã¦Â¢ÂÃ¤Â»Â¶Ã¯Â¼Â"""
    rec = recent_map.get(cb['cb_code'], {})
    listing_date = rec.get('listing_date') or cb.get('listing_date')
    if not listing_date:
        return None  # Ã¦Â²ÂÃ¦ÂÂÃ¦ÂÂÃ§ÂÂÃ¦ÂÂ¥Ã¯Â¼ÂÃ§ÂÂ¡Ã¦Â³ÂÃ¥ÂÂ¤Ã¦ÂÂ·

    td = trading_days_between(listing_date)
    if td is None or td < 0:
        return {'signal': 'Ã¥ÂÂ³Ã¥Â°ÂÃ¤Â¸ÂÃ¥Â¸Â', 'cls': 'info', 'td': td,
                'c1': False, 'c2': False, 'c3': None}

    cbp  = cb.get('cb_price') or 0
    sc   = cb['stock_code']
    sh   = short_map.get(sc)

    c1 = 4 <= td <= 8         # Ã¦Â¢ÂÃ¤Â»Â¶Ã¤Â¸ÂÃ¯Â¼ÂÃ¦ÂÂÃ§ÂÂÃ¦ÂÂ¥ D4-D8
    c2 = cbp >= 98             # Ã¦Â¢ÂÃ¤Â»Â¶Ã¤ÂºÂÃ¯Â¼ÂCB Ã¢ÂÂ¥ 98
    if sh is None:
        c3 = None              # Ã§ÂÂ¡Ã¨ÂÂÃ¥ÂÂ¸Ã¨Â³ÂÃ¦ÂÂÃ¯Â¼ÂÃ§ÂÂÃ§ÂÂÃ¤Â¸ÂÃ¥ÂÂ¯Ã¦ÂÂ¾Ã§Â©ÂºÃ¯Â¼Â
    else:
        c3 = sh['increasing']  # Ã¦Â¢ÂÃ¤Â»Â¶Ã¤Â¸ÂÃ¯Â¼ÂÃ¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¥Â¢ÂÃ¥ÂÂ 

    n_ok = sum(x for x in [c1, c2] if x) + (1 if c3 else 0)

    if not c1:
        if td <= 3:
            sig, cls = f'Ã¨Â§ÂÃ¥Â¯Â D{td}', 'watch'
        elif 8 < td <= 20:
            sig, cls = f'Ã¥ÂÂºÃ¥Â Â´ D{td}', 'sell'
        else:
            sig, cls = 'Ã¢ÂÂ', 'neutral'
    elif c1 and c2 and c3:
        sig, cls = f'Ã¢ÂÂ Ã¨Â²Â·Ã¥ÂÂ¥ D{td} (3/3)', 'buy'
    elif c1 and c2 and c3 is None:
        sig, cls = f'Ã¢ÂÂ Ã¤Â¸ÂÃ¥ÂÂ¯Ã¦ÂÂ¾Ã§Â©Âº D{td}', 'watch'
    elif c1 and c2:
        sig, cls = f'Ã¢ÂÂ Ã¨ÂÂÃ¥ÂÂ¸Ã¦ÂÂªÃ¥Â¢Â D{td}', 'sell'
    else:
        sig, cls = f'Ã¢ÂÂ D{td}', 'neutral'

    return {'signal': sig, 'cls': cls, 'td': td,
            'c1': c1, 'c2': c2, 'c3': c3,
            'short_today':  sh['short_today']  if sh else None,
            'short_change': sh['short_change'] if sh else None,
            'listing_date': listing_date}


def evaluate_s2(cb, short_map):
    """Ã§Â­ÂÃ§ÂÂ¥Ã¤ÂºÂÃ¯Â¼ÂÃ¨Â½ÂÃ¦ÂÂÃ¥Â¥ÂÃ¥ÂÂ©Ã¯Â¼Â4 Ã¦Â¢ÂÃ¤Â»Â¶Ã¯Â¼Â"""
    prem  = cb.get('premium_rate') or 0
    conv  = cb.get('converted_pct') or 0
    dtm   = calendar_days_to(cb.get('maturity_date', ''))
    sc    = cb['stock_code']
    sh    = short_map.get(sc)

    d1 = prem <= 2            # Ã¦ÂºÂ¢Ã¥ÂÂ¹ Ã¢ÂÂ¤ 2%
    d2 = conv < 60            # Ã¥Â·Â²Ã¨Â½ÂÃ¦ÂÂ < 60%
    d3 = dtm >= 90            # Ã¨Â·ÂÃ¥ÂÂ°Ã¦ÂÂ Ã¢ÂÂ¥ 90 Ã¥Â¤Â©
    d4 = sh['increasing'] if sh else None  # Ã¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¥Â¢ÂÃ¥ÂÂ 

    if sh is None:
        short_today = None; short_change = None
    else:
        short_today = sh['short_today']; short_change = sh['short_change']

    if d1 and d2 and d3 and d4:
        sig, cls = 'Ã¢ÂÂ Ã¥Â¥ÂÃ¥ÂÂ© (4/4)', 'buy'
    elif d1 and d2 and d3 and d4 is None:
        sig, cls = 'Ã¢ÂÂ Ã¤Â¸ÂÃ¥ÂÂ¯Ã¦ÂÂ¾Ã§Â©Âº (3+?/4)', 'watch'
    elif d1 and d2 and d3:
        sig, cls = 'Ã¢ÂÂ Ã¨ÂÂÃ¥ÂÂ¸Ã¦ÂÂªÃ¥Â¢Â (3/4)', 'sell'
    elif prem <= 5 and d2 and d3:
        sig, cls = 'Ã¦ÂÂ¥Ã¨Â¿ÂÃ¥Â¥ÂÃ¥ÂÂ©Ã¥ÂÂ', 'watch'
    else:
        sig, cls = 'Ã¢ÂÂ', 'neutral'

    return {'signal': sig, 'cls': cls,
            'c1': d1, 'c2': d2, 'c3': d3, 'c4': d4,
            'days_to_mat': dtm,
            'short_today': short_today, 'short_change': short_change}


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
# 6. Ã§ÂÂÃ¦ÂÂ HTML
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def chk(ok, na=False):
    if na:  return '<span class="chk chk-na">?</span>'
    return '<span class="chk chk-y">Ã¢ÂÂ</span>' if ok else '<span class="chk chk-n">Ã¢ÂÂ</span>'

def fmt(v, d=1):
    if v is None: return 'Ã¢ÂÂ'
    try: return f'{float(v):.{d}f}'
    except: return 'Ã¢ÂÂ'

def sc_fmt(v):
    if v is None: return 'Ã¢ÂÂ'
    return f'+{v}' if v > 0 else str(v)

def sc_cls(v):
    if v is None or v == 0: return ''
    return 'short-up' if v > 0 else 'short-dn'

def generate_html(all_cbs, recent_map, short_map, short_date):
    # Ã¨Â¨ÂÃ§Â®ÂÃ¦ÂÂÃ¦ÂÂÃ¨Â¨ÂÃ¨ÂÂ
    results = []
    for cb in all_cbs:
        s1 = evaluate_s1(cb, short_map, recent_map)
        s2 = evaluate_s2(cb, short_map)
        results.append({**cb, 's1': s1, 's2': s2})

    # Ã¥ÂÂÃ©Â¡Â
    s1_items = [r for r in results if r['s1'] and r['s1']['td'] is not None and r['s1']['td'] >= 0 and r['s1']['td'] <= 20]
    s1_items.sort(key=lambda x: x['s1']['td'])
    s2_items = sorted(results, key=lambda x: (
        0 if 'Ã¢ÂÂ' in x['s2']['signal'] else 1 if 'Ã¢ÂÂ' in x['s2']['signal'] else 2 if 'Ã¢ÂÂ' not in x['s2']['signal'] and x['s2']['signal'] != 'Ã¢ÂÂ' else 3,
        x.get('premium_rate') or 99
    ))

    s1_buy   = sum(1 for r in results if r['s1'] and 'Ã¢ÂÂ' in r['s1']['signal'])
    s1_pend  = sum(1 for r in results if r['s1'] and 'Ã¢ÂÂ' in r['s1']['signal'])
    s2_buy   = sum(1 for r in results if 'Ã¢ÂÂ' in r['s2']['signal'])
    s2_pend  = sum(1 for r in results if 'Ã¢ÂÂ' in r['s2']['signal'])

    # Ã¢ÂÂÃ¢ÂÂ S1 rows Ã¢ÂÂÃ¢ÂÂ
    s1_rows_html = ''
    for r in s1_items:
        s1 = r['s1']
        cbas = 'Ã¢ÂÂ Ã¥ÂÂ¯Ã¦ÂÂ' if s1['td'] >= 6 else f'D6Ã¥ÂÂ¯Ã¦ÂÂ'
        s1_rows_html += f"""<tr class="{'row-buy' if 'Ã¢ÂÂ' in s1['signal'] else 'row-watch' if 'Ã¢ÂÂ' in s1['signal'] else 'row-sell' if 'Ã¢ÂÂ' in s1['signal'] else ''}">
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="center">D{s1['td']}</td>
  <td class="center">{cbas}</td>
  <td class="center cond">{chk(s1['c1'])} Ã¦ÂÂÃ§ÂÂÃ¥ÂÂÃ¦ÂÂ<br>{chk(s1['c2'])} CBÃ¥ÂÂ¹Ã©ÂÂÃ¦Â¨Â<br>{chk(s1['c3'], s1['c3'] is None)} Ã¨ÂÂ+Ã¥ÂÂÃ¢ÂÂ</td>
  <td class="num">{fmt(s1.get('short_today'),0)}Ã¥Â¼Âµ</td>
  <td class="num {sc_cls(s1.get('short_change'))}">{sc_fmt(s1.get('short_change'))}</td>
  <td class="center"><span class="badge {s1['cls']}">{s1['signal']}</span></td>
</tr>"""

    # Ã¢ÂÂÃ¢ÂÂ S2 rows Ã¢ÂÂÃ¢ÂÂ
    s2_rows_html = ''
    for r in s2_items:
        s2 = r['s2']
        pc = 'prem-neg' if (r.get('premium_rate') or 0) < 0 else ''
        s2_rows_html += f"""<tr class="{'row-buy' if 'Ã¢ÂÂ' in s2['signal'] else 'row-watch' if 'Ã¢ÂÂ' in s2['signal'] or 'Ã¦ÂÂ¥Ã¨Â¿Â' in s2['signal'] else ''}">
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="num {pc}">{fmt(r.get('premium_rate'))}%</td>
  <td class="num">{fmt(r.get('stock_price'))}</td>
  <td class="num">{fmt(r.get('conversion_price'))}</td>
  <td class="center cond">{chk(s2['c1'])} Ã¤Â½ÂÃ¦ÂºÂ¢Ã¥ÂÂ¹<br>{chk(s2['c2'])} Ã¨Â½ÂÃ¦ÂÂÃ¦Â¯ÂÃ¤Â¾ÂÃ¤Â½Â<br>{chk(s2['c3'])} Ã¨Â·ÂÃ¥ÂÂ°Ã¦ÂÂÃ¥ÂÂÃ¨Â£Â<br>{chk(s2['c4'], s2['c4'] is None)} Ã¨ÂÂ+Ã¥ÂÂÃ¢ÂÂ</td>
  <td class="num">{s2['days_to_mat']}Ã¥Â¤Â©</td>
  <td class="num">{fmt(s2.get('short_today'),0)}Ã¥Â¼Âµ</td>
  <td class="num {sc_cls(s2.get('short_change'))}">{sc_fmt(s2.get('short_change'))}</td>
  <td class="center"><span class="badge {s2['cls']}">{s2['signal']}</span></td>
</tr>"""

    # Ã¢ÂÂÃ¢ÂÂ All rows Ã¢ÂÂÃ¢ÂÂ
    all_rows_html = ''
    for r in results:
        s1 = r['s1']
        s2 = r['s2']
        s1sig = s1['signal'] if s1 else 'Ã¢ÂÂ'
        s1cls = s1['cls'] if s1 else 'neutral'
        pc = 'prem-neg' if (r.get('premium_rate') or 0) < 0 else ''
        sh = short_map.get(r['stock_code'])
        all_rows_html += f"""<tr>
  <td><b>{r['cb_code']}</b></td><td>{r['cb_name']}</td><td>{r['stock_code']}</td>
  <td class="num">{fmt(r.get('cb_price'))}</td>
  <td class="num {pc}">{fmt(r.get('premium_rate'))}%</td>
  <td class="num">{fmt(r.get('stock_price'))}</td>
  <td class="num">{fmt(r.get('conversion_price'))}</td>
  <td>{r.get('maturity_date','Ã¢ÂÂ')}</td>
  <td class="num">{fmt(sh['short_today'],0) if sh else 'Ã¢ÂÂ'}Ã¥Â¼Âµ</td>
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
<title>Ã¥ÂÂ¯Ã¨Â½ÂÃ¥ÂÂµÃ§Â­ÂÃ§ÂÂ¥Ã¥ÂÂÃ¨Â¡Â¨Ã¦ÂÂ¿</title>
<style>{CSS}</style></head><body>
<div class="hdr">
  <h1>Ã°ÂÂÂ Ã¥ÂÂ¯Ã¨Â½ÂÃ¥ÂÂµÃ§Â­ÂÃ§ÂÂ¥Ã¥ÂÂÃ¨Â¡Â¨Ã¦ÂÂ¿</h1>
  <div class="sub">Ã¦ÂÂ´Ã¦ÂÂ°Ã¯Â¼Â{TODAY}</div>
</div>
<div class="stats">
  <div class="sc"><div class="n">{len(all_cbs)}</div><div class="l">Ã¥ÂÂ¨Ã©ÂÂ¨CBÃ¦ÂÂ¸</div></div>
  <div class="sc gr"><div class="n">{s1_buy}</div><div class="l">S1Ã¥ÂÂ¨Ã¦Â¢ÂÃ¤Â»Â¶Ã¨Â²Â·Ã¥ÂÂ¥</div></div>
  <div class="sc am"><div class="n">{s1_pend}</div><div class="l">S1Ã¤Â¸ÂÃ¥ÂÂ¯Ã¦ÂÂ¾Ã§Â©Âº</div></div>
  <div class="sc gr"><div class="n">{s2_buy}</div><div class="l">S2Ã¥Â¥ÂÃ¥ÂÂ©(4/4)</div></div>
  <div class="sc am"><div class="n">{s2_pend}</div><div class="l">S2Ã¤Â¸ÂÃ¥ÂÂ¯Ã¦ÂÂ¾Ã§Â©Âº</div></div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('s1',this)">Ã§Â­ÂÃ§ÂÂ¥Ã¤Â¸ÂÃ¯Â¼ÂCBASÃ¦ÂÂ°Ã¤Â¸ÂÃ¥Â¸Â</div>
  <div class="tab" onclick="showTab('s2',this)">Ã§Â­ÂÃ§ÂÂ¥Ã¤ÂºÂÃ¯Â¼ÂÃ¨Â½ÂÃ¦ÂÂÃ¥Â¥ÂÃ¥ÂÂ©Ã¯Â¼Â{len(all_cbs)}Ã§Â­ÂÃ¯Â¼Â</div>
  <div class="tab" onclick="showTab('all',this)">Ã¥ÂÂ¨Ã©ÂÂ¨Ã¥ÂÂ¯Ã¨Â½ÂÃ¥ÂÂµ</div>
</div>
<div id="pane-s1" class="pane active">
  <div class="ttl">Ã§Â­ÂÃ§ÂÂ¥Ã¤Â¸ÂÃ¯Â¼ÂCBAS Ã¦ÂÂ°Ã¤Â¸ÂÃ¥Â¸ÂÃ§ÂÂ­Ã¥Â£Â</div>
  <div class="desc">Ã¦Â³ÂÃ¤ÂºÂºÃ¨Â²Â·CB Ã¢ÂÂ Ã¦ÂÂ¾Ã§Â©ÂºÃ¨ÂÂ¡Ã§Â¥Â¨ (D1Ã¢ÂÂ5) Ã¢ÂÂ D6 CBASÃ¦ÂÂÃ¨Â§Â£ Ã¢ÂÂ Ã¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¥ÂÂÃ¨Â£Â Ã¢ÂÂ Ã¨ÂÂ¡Ã¥ÂÂ¹Ã¥ÂÂÃ¥Â½Â<br>
    <span class="tag">Ã¦Â¢ÂÃ¤Â»Â¶1</span>Ã¦ÂÂÃ§ÂÂÃ¥ÂÂÃ¦ÂÂÃ¤ÂºÂ¤Ã¦ÂÂÃ¦ÂÂ¥
    <span class="tag">Ã¦Â¢ÂÃ¤Â»Â¶2</span>CBÃ§ÂÂ¾Ã¥ÂÂ¹Ã©ÂÂÃ¤Â¸ÂÃ¥Â®ÂÃ¦Â°Â´Ã¦ÂºÂ
    <span class="tag">Ã¦Â¢ÂÃ¤Â»Â¶3</span>Ã¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã©Â¤ÂÃ©Â¡ÂÃ¥Â¢ÂÃ¥ÂÂ </div>
  <div class="box"><b>Ã¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸ Ã¨ÂªÂªÃ¦ÂÂÃ¯Â¼Â</b>
    <span class="chk chk-y">Ã¢ÂÂ</span>Ã©ÂÂÃ¦Â¨Â &nbsp;
    <span class="chk chk-n">Ã¢ÂÂ</span>Ã¦ÂÂªÃ©ÂÂÃ¦Â¨Â &nbsp;
    <span class="chk chk-na">?</span>Ã¨Â©Â²Ã¨ÂÂ¡Ã§ÂÂ®Ã¥ÂÂÃ¤Â¸ÂÃ¥ÂÂ¯Ã¦ÂÂ¾Ã§Â©ÂºÃ¯Â¼ÂTWSE TWT93U Ã§ÂÂ¡Ã¦Â­Â¤Ã¨ÂÂ¡Ã¨Â¨ÂÃ©ÂÂÃ¯Â¼Â<br>
    Ã¨Â³ÂÃ¦ÂÂÃ¤Â¾ÂÃ¦ÂºÂÃ¯Â¼ÂTWSEÃ£ÂÂÃ¨ÂÂÃ¥ÂÂ¸Ã¥ÂÂÃ¥ÂÂ¸Ã¨Â³Â£Ã¥ÂÂºÃ©Â¤ÂÃ©Â¡ÂÃ£ÂÂÃ¦Â¯ÂÃ¦ÂÂ¥Ã§ÂÂ¤Ã¥Â¾ÂÃ¨ÂÂªÃ¥ÂÂÃ¦ÂÂ´Ã¦ÂÂ°Ã¯Â¼ÂÃ¥ÂÂÃ¦ÂÂÃ¥ÂÂÃ¥ÂÂ«Ã¨ÂÂÃ¥ÂÂ¸Ã¥ÂÂÃ¥ÂÂÃ¥ÂÂ¸Ã£ÂÂ</div>
  <table><thead><tr>
    <th>CBÃ¤Â»Â£Ã¨ÂÂ</th><th>CBÃ¥ÂÂÃ§Â¨Â±</th><th>Ã¨ÂÂ¡Ã§Â¥Â¨</th><th class="num">CBÃ¥ÂÂ¹</th>
    <th class="center">Ã¥Â¤Â©Ã¦ÂÂ¸</th><th class="center">CBAS</th>
    <th class="center">Ã¦Â¢ÂÃ¤Â»Â¶1/2/3</th>
    <th class="num">Ã¨ÂÂ+Ã¥ÂÂÃ©Â¤ÂÃ©Â¡Â</th><th class="num">Ã¦ÂÂ¥Ã¨Â®ÂÃ¥ÂÂ</th><th class="center">Ã¨Â¨ÂÃ¨ÂÂ</th>
  </tr></thead><tbody>{s1_rows_html}</tbody></table>
</div>
<div id="pane-s2" class="pane">
  <div class="ttl">Ã§Â­ÂÃ§ÂÂ¥Ã¤ÂºÂÃ¯Â¼ÂÃ¨Â½ÂÃ¦ÂÂÃ¥Â¥ÂÃ¥ÂÂ©Ã¯Â¼ÂÃ¥ÂÂ¨Ã©ÂÂ¨ {len(all_cbs)} Ã¦ÂÂ¯ CBÃ¯Â¼Â</div>
  <div class="desc">Ã¨Â²Â·CB + Ã¦ÂÂ¾Ã§Â©ÂºÃ¨ÂÂ¡Ã§Â¥Â¨ Ã¢ÂÂ Ã§Â­ÂÃ¥Â¾ÂÃ¨Â½ÂÃ¦ÂÂ Ã¢ÂÂ Ã¨Â½ÂÃ¦ÂÂÃ¨ÂÂ¡Ã§Â¥Â¨Ã¥ÂÂÃ¨Â£Â Ã¢ÂÂ Ã¥Â¥ÂÃ¥ÂÂ©<br>
    <span class="tag">Ã¦Â¢ÂÃ¤Â»Â¶1</span>Ã¨Â½ÂÃ¦ÂÂÃ¦ÂºÂ¢Ã¥ÂÂ¹Ã§ÂÂÃ¤Â½Â <span class="tag">Ã¦Â¢ÂÃ¤Â»Â¶2</span>Ã¥Â·Â²Ã¨Â½ÂÃ¦ÂÂÃ¦Â¯ÂÃ¤Â¾ÂÃ¤Â½Â
    <span class="tag">Ã¦Â¢ÂÃ¤Â»Â¶3</span>Ã¨Â·ÂÃ¥ÂÂ°Ã¦ÂÂÃ¦ÂÂ¥Ã¥ÂÂÃ¨Â£Â <span class="tag">Ã¦Â¢ÂÃ¤Â»Â¶4</span>Ã¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¥Â¢ÂÃ¥ÂÂ </div>
  <div class="box warn"><b>Ã¦Â³Â¨Ã¦ÂÂÃ¯Â¼Â</b>Ã¦ÂºÂ¢Ã¥ÂÂ¹Ã§ÂÂÃ©Â¡Â¯Ã§Â¤Âº<span style="color:#16a34a;font-weight:700">Ã§Â¶Â Ã¨ÂÂ²</span>Ã¯Â¼ÂÃ¨Â²Â Ã¥ÂÂ¼Ã¯Â¼ÂÃ¤Â»Â£Ã¨Â¡Â¨CBÃ¤Â½ÂÃ¦ÂÂ¼Ã¨Â½ÂÃ¦ÂÂÃ¥ÂÂ¹Ã¥ÂÂ¼Ã¯Â¼ÂÃ¥Â¥ÂÃ¥ÂÂ©Ã§Â©ÂºÃ©ÂÂÃ¦ÂÂÃ¥Â¤Â§Ã£ÂÂ
    Ã©ÂÂÃ§Â¢ÂºÃ¨ÂªÂÃ¯Â¼ÂÃ¨ÂÂ+Ã¥ÂÂÃ¦ÂÂ¯Ã¥ÂÂ¦Ã¥ÂÂÃ¨Â¶Â³Ã£ÂÂÃ¦ÂÂÃ§ÂÂ¡Ã¦ÂÂÃ¥ÂÂÃ¨Â½ÂÃ¦ÂÂÃ©ÂÂÃ¥ÂÂ¶Ã£ÂÂ</div>
  <table><thead><tr>
    <th>CBÃ¤Â»Â£Ã¨ÂÂ</th><th>CBÃ¥ÂÂÃ§Â¨Â±</th><th>Ã¨ÂÂ¡Ã§Â¥Â¨</th><th class="num">CBÃ¥ÂÂ¹</th>
    <th class="num">Ã¦ÂºÂ¢Ã¥ÂÂ¹Ã§ÂÂ</th><th class="num">Ã¨ÂÂ¡Ã¥ÂÂ¹</th><th class="num">Ã¨Â½ÂÃ¦ÂÂÃ¥ÂÂ¹</th>
    <th class="center">Ã¦Â¢ÂÃ¤Â»Â¶1/2/3/4</th>
    <th class="num">Ã¨Â·ÂÃ¥ÂÂ°Ã¦ÂÂ</th><th class="num">Ã¨ÂÂ+Ã¥ÂÂÃ©Â¤ÂÃ©Â¡Â</th><th class="num">Ã¦ÂÂ¥Ã¨Â®ÂÃ¥ÂÂ</th>
    <th class="center">Ã¨Â¨ÂÃ¨ÂÂ</th>
  </tr></thead><tbody>{s2_rows_html}</tbody></table>
</div>
<div id="pane-all" class="pane">
  <div class="ttl">Ã¥ÂÂ¨Ã©ÂÂ¨ {len(all_cbs)} Ã¦ÂÂ¯Ã¥ÂÂ¯Ã¨Â½ÂÃ¥ÂÂµ</div>
  <div class="desc">Ã¨Â³ÂÃ¦ÂÂÃ¤Â¾ÂÃ¦ÂºÂÃ¯Â¼Âthefew.twÃ¯Â½ÂÃ¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¯Â¼ÂTWSE TWT93U {short_date}</div>
  <table><thead><tr>
    <th>CBÃ¤Â»Â£Ã¨ÂÂ</th><th>CBÃ¥ÂÂÃ§Â¨Â±</th><th>Ã¨ÂÂ¡Ã§Â¥Â¨</th><th class="num">CBÃ¥ÂÂ¹</th>
    <th class="num">Ã¦ÂºÂ¢Ã¥ÂÂ¹Ã§ÂÂ</th><th class="num">Ã¨ÂÂ¡Ã¥ÂÂ¹</th><th class="num">Ã¨Â½ÂÃ¦ÂÂÃ¥ÂÂ¹</th>
    <th>Ã¥ÂÂ°Ã¦ÂÂÃ¦ÂÂ¥</th><th class="num">Ã¨ÂÂ+Ã¥ÂÂÃ©Â¤ÂÃ©Â¡Â</th><th class="num">Ã¦ÂÂ¥Ã¨Â®ÂÃ¥ÂÂ</th>
    <th class="center">S1</th><th class="center">S2</th>
  </tr></thead><tbody>{all_rows_html}</tbody></table>
</div>
<div class="ft">Ã¦ÂÂ¬Ã¥Â·Â¥Ã¥ÂÂ·Ã¥ÂÂÃ¤Â¾ÂÃ¥Â­Â¸Ã§Â¿ÂÃ§Â ÂÃ§Â©Â¶Ã¯Â¼ÂÃ¤Â¸ÂÃ¦Â§ÂÃ¦ÂÂÃ¦ÂÂÃ¨Â³ÂÃ¥Â»ÂºÃ¨Â­Â°Ã£ÂÂ<br>
Ã¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¨Â³ÂÃ¦ÂÂÃ¤Â¾ÂÃ¦ÂºÂÃ¯Â¼ÂTWSEÃ£ÂÂÃ¨ÂÂÃ¥ÂÂ¸Ã¥ÂÂÃ¥ÂÂ¸Ã¨Â³Â£Ã¥ÂÂºÃ©Â¤ÂÃ©Â¡Â(TWT93U)Ã£ÂÂÃ¯Â¼ÂÃ¦Â¯ÂÃ¦ÂÂ¥Ã§ÂÂ¤Ã¥Â¾ÂÃ§Â´Â17:30Ã¦ÂÂ´Ã¦ÂÂ°Ã£ÂÂÃ¤Â¸ÂÃ¥ÂÂ¨Ã¥ÂÂÃ¥ÂÂ®Ã¥ÂÂ§Ã¤Â»Â£Ã¨Â¡Â¨Ã¨Â©Â²Ã¨ÂÂ¡Ã§ÂÂ®Ã¥ÂÂÃ¤Â¸ÂÃ¥ÂÂ¯Ã¦ÂÂ¾Ã§Â©ÂºÃ£ÂÂ</div>
<script>
function showTab(id,el){{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('pane-'+id).classList.add('active');
}}
</script></body></html>"""
    return html


# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
# 7. Ã¤Â¸Â»Ã§Â¨ÂÃ¥Â¼Â
# Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
def main():
    print(f"\n=== Ã¥ÂÂ¯Ã¨Â½ÂÃ¥ÂÂµÃ§Â­ÂÃ§ÂÂ¥Ã¦ÂÂÃ¦ÂÂ {TODAY} ===")
    all_cbs    = fetch_all_cbs()
    recent_map = fetch_recent_cbs()
    short_map, short_date = fetch_short_data()

    # Ã¨Â£ÂÃ¤Â¸ÂÃ¦ÂÂÃ§ÂÂÃ¦ÂÂ¥Ã¯Â¼ÂÃ¥Â¾Â recent_map Ã¥ÂÂÃ¤Â½ÂµÃ¥ÂÂ° all_cbsÃ¯Â¼Â
    rec_ld = {cb_code: d.get('listing_date') for cb_code, d in recent_map.items()}
    for cb in all_cbs:
        if cb['cb_code'] in rec_ld:
            cb['listing_date'] = rec_ld[cb['cb_code']]

    html = generate_html(all_cbs, recent_map, short_map, short_date)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\nÃ¢ÂÂ Ã¥ÂÂÃ¨Â¡Â¨Ã¦ÂÂ¿Ã¥Â·Â²Ã§ÂÂ¢Ã§ÂÂÃ¯Â¼Â{OUTPUT_HTML}")
    print(f"   Ã¥ÂÂ¨Ã©ÂÂ¨CB: {len(all_cbs)} Ã§Â­Â")
    print(f"   Ã¨ÂÂÃ¥ÂÂ¸+Ã¥ÂÂÃ¥ÂÂ¸Ã¨Â³ÂÃ¦ÂÂ: {len(short_map)} Ã¦ÂÂ¯Ã¨ÂÂ¡Ã§Â¥Â¨Ã¯Â¼Â{short_date}Ã¯Â¼Â")

    # Ã§ÂµÂ±Ã¨Â¨Â
    s1_buy = s2_buy = 0
    for cb in all_cbs:
        s1 = evaluate_s1(cb, short_map, recent_map)
        s2 = evaluate_s2(cb, short_map)
        if s1 and 'Ã¢ÂÂ' in s1['signal']: s1_buy += 1
        if 'Ã¢ÂÂ' in s2['signal']: s2_buy += 1
    print(f"   S1Ã¨Â²Â·Ã¥ÂÂ¥: {s1_buy} Ã§Â­Â | S2Ã¥Â¥ÂÃ¥ÂÂ©: {s2_buy} Ã§Â­Â")

if __name__ == '__main__':
    main()
