"""Screenshot etf_card.html into etf_card.png.

Kept separate from generate_etf.py so a missing browser never blocks the
dashboard build — the card is a nice-to-have, the data is not.
"""
import os
import sys

CARD_HTML = 'etf_card.html'
CARD_PNG = 'etf_card.png'

if not os.path.exists(CARD_HTML):
    print(f'{CARD_HTML} missing — nothing to render')
    sys.exit(0)

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print('playwright not installed — skipping card render')
    sys.exit(0)

with sync_playwright() as p:
    browser = p.chromium.launch()
    # 2x for a crisp image on phones; width matches the card's fixed 1040px body.
    page = browser.new_page(viewport={'width': 1040, 'height': 600},
                            device_scale_factor=2)
    page.goto('file://' + os.path.abspath(CARD_HTML))
    page.wait_for_timeout(400)
    # full_page 的高度下限是 viewport，冷清的日子會在圖片下方留一大片空白。
    # 先量實際內容高度再把 viewport 調成一樣，圖就剛好貼齊內容。
    height = page.evaluate(
        'Math.ceil(document.body.getBoundingClientRect().height)')
    page.set_viewport_size({'width': 1040, 'height': max(height, 200)})
    page.screenshot(path=CARD_PNG)
    browser.close()

print(f'wrote {CARD_PNG} ({os.path.getsize(CARD_PNG)/1024:.0f}KB)')
