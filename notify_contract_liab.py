#!/usr/bin/env python3
"""
合約負債策略 — LINE 通知腳本

每次跑與「上次已通知狀態」比對，僅在以下變化時發送：
  · top 10 名單有變動（新進榜或掉榜）
  · top 20 任一檔狀態翻轉 (✅↔🚨, ⏳→✅, etc.)
  · 月變動 (current month != last_notified month) — 強制發每月摘要

環境變數：
  LINE_CHANNEL_ACCESS_TOKEN  必要 — LINE Messaging API token
  LINE_CHANNEL_SECRET        必要

用法：
  python notify_contract_liab.py            # 偵測變化才發
  python notify_contract_liab.py --force    # 強制發送
  python notify_contract_liab.py --test     # 測試訊息
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LATEST_JSON = ROOT / 'contract_liab_latest.json'
NOTIFY_STATE = ROOT / 'contract_liab_notify_state.json'
ENV_FILE = ROOT / '.env'

# 通知 top N
TOP_N_NOTIFY = 10
STATUS_WATCH_N = 20   # 監測前 20 名的狀態翻轉
DASHBOARD_URL = 'https://tetsu811.com/合約負債策略/'


def load_env():
    """讀 .env 補上環境變數（local 開發用，CI 用 secrets）。"""
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text().splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())


def load_line_token():
    load_env()
    # 也試 cb 策略目錄的 .env
    alt = Path.home() / '可轉債策略訊號' / '.env'
    if alt.exists() and not os.environ.get('LINE_CHANNEL_ACCESS_TOKEN'):
        for line in alt.read_text().splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())
    tok = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
    if not tok:
        print('ERROR: LINE_CHANNEL_ACCESS_TOKEN not set', file=sys.stderr)
        sys.exit(2)
    return tok


def fmt_pct(v, digits=0):
    if v is None: return '—'
    return f'{v * 100:+.{digits}f}%'


def fmt_twd(v):
    if v is None: return '—'
    if abs(v) >= 1e8: return f'{v / 1e8:.1f}億'
    if abs(v) >= 1e4: return f'{v / 1e4:.0f}萬'
    return f'{v:,.0f}'


def build_state(latest):
    """從 latest.json 萃取通知用 state。"""
    ranked = latest.get('ranked', [])
    top_n = ranked[:TOP_N_NOTIFY]
    return {
        'quarter': latest.get('quarter'),
        'top_codes': [it['code'] for it in top_n],
        'top_status': {
            it['code']: (it.get('q1_progress') or {}).get('status_key', 'na')
            for it in ranked[:STATUS_WATCH_N]
        },
        'month': datetime.now().strftime('%Y-%m'),
    }


def diff_states(curr, prev):
    """比對 state，回傳 events list。"""
    events = []
    if not prev:
        events.append(('init', '首次執行，發送初始榜單'))
        return events

    # 月份變動 → 強制每月摘要
    if curr['month'] != prev.get('month'):
        events.append(('monthly', f'月份切換 {prev.get("month")} → {curr["month"]}'))

    # 季度變動
    if curr['quarter'] != prev.get('quarter'):
        events.append(('quarterly', f'季度資料更新 {prev.get("quarter")} → {curr["quarter"]}'))

    # top 10 名單變動
    new_in = set(curr['top_codes']) - set(prev.get('top_codes', []))
    out = set(prev.get('top_codes', [])) - set(curr['top_codes'])
    if new_in:
        events.append(('new_entry', f'新進前 10：{", ".join(sorted(new_in))}'))
    if out:
        events.append(('drop', f'掉出前 10：{", ".join(sorted(out))}'))

    # 狀態翻轉
    flips = []
    prev_status = prev.get('top_status', {})
    for code, st in curr['top_status'].items():
        ps = prev_status.get(code)
        if ps and ps != st:
            # 重要轉折：⏳→✅、✅→🚨
            if (ps == 'wait' and st == 'verified') or \
               (ps == 'verified' and st == 'warn') or \
               (ps == 'warn' and st == 'verified') or \
               (ps == 'mild' and st == 'warn'):
                flips.append((code, ps, st))
    if flips:
        events.append(('status_flip', flips))

    return events


def build_message(latest, events):
    """組 LINE 推播訊息。"""
    ranked = latest.get('ranked', [])
    quarter = latest.get('quarter', '')
    now = datetime.now().strftime('%Y-%m-%d %H:%M')

    # Header
    msg = '📊 合約負債策略每月更新\n'
    msg += f'資料季度：{quarter} · {now}\n\n'

    # 變化摘要
    has_alert = any(e[0] in ('status_flip', 'quarterly', 'new_entry') for e in events)
    # 過濾掉 'forced' 等非顯示事件
    display_events = [e for e in events if e[0] != 'forced']
    if display_events:
        msg += '🔔 本次變化\n'
        for evt_type, payload in display_events:
            if evt_type == 'init':
                msg += f'  · {payload}\n'
            elif evt_type == 'monthly':
                msg += f'  · 📅 {payload}\n'
            elif evt_type == 'quarterly':
                msg += f'  · 📈 {payload}\n'
            elif evt_type == 'new_entry':
                msg += f'  · 🆕 {payload}\n'
            elif evt_type == 'drop':
                msg += f'  · ⬇️ {payload}\n'
            elif evt_type == 'status_flip':
                for code, ps, st in payload[:5]:
                    name = next((it.get('name', '') for it in ranked if it['code'] == code), '')
                    icon = {'verified': '✅', 'warn': '🚨', 'mild': '🟢', 'wait': '⏳'}
                    msg += f'  · {code} {name}: {icon.get(ps, ps)} → {icon.get(st, st)}\n'
        msg += '\n'
    elif any(e[0] == 'forced' for e in events):
        msg += '🔔 手動強制發送（無自動 diff）\n\n'

    # Top 5 picks
    msg += f'🏆 前 5 名（共 {len(ranked)} 檔通過篩選）\n'
    for i, it in enumerate(ranked[:5], 1):
        q1 = it.get('q1_progress') or {}
        cyoy = q1.get('cum_yoy')
        cyoy_str = fmt_pct(cyoy) if cyoy is not None else '—'
        status = q1.get('status_label', '')
        cl_b = it['cl'] / 1e8
        msg += (f'{i}. {it["code"]} {it.get("name", "")} · '
                f'CL {cl_b:.1f}億 ({fmt_pct(it["cl_yoy_raw"] or it["cl_yoy"])})\n'
                f'   {status} Q+1月營收 {cyoy_str}\n')

    # 警訊清單（前 20 名中的 🚨）
    warns = [it for it in ranked[:20]
             if (it.get('q1_progress') or {}).get('status_key') == 'warn']
    if warns:
        msg += f'\n🚨 警訊（top 20 中 {len(warns)} 檔）\n'
        for it in warns[:5]:
            q1 = it.get('q1_progress') or {}
            cyoy = q1.get('cum_yoy')
            msg += (f'  · {it["code"]} {it.get("name", "")} '
                    f'累計 {fmt_pct(cyoy)} (CL/TA {(it["cl_to_assets"] or 0) * 100:.0f}%)\n')

    # 連結
    msg += f'\n🔗 完整 dashboard\n{DASHBOARD_URL}'

    return msg, has_alert


def send_line(message):
    try:
        from linebot import LineBotApi
        from linebot.models import TextSendMessage
        from linebot.exceptions import LineBotApiError
    except ImportError:
        print('ERROR: line-bot-sdk not installed. Run: pip install line-bot-sdk', file=sys.stderr)
        return False
    token = load_line_token()
    try:
        bot = LineBotApi(token)
        bot.broadcast(TextSendMessage(text=message))
        print(f'✓ LINE broadcast sent ({len(message)} chars)')
        return True
    except LineBotApiError as e:
        print(f'ERROR: LINE API error: {e}', file=sys.stderr)
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--force', action='store_true', help='強制發送，忽略 diff')
    ap.add_argument('--test', action='store_true', help='測試訊息')
    ap.add_argument('--dry-run', action='store_true', help='只印訊息不發送')
    args = ap.parse_args()

    if args.test:
        msg = (f'🧪 合約負債策略系統測試\n'
               f'時間: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
               f'通知通道正常運作。\n\n🔗 {DASHBOARD_URL}')
        if args.dry_run:
            print(msg)
        else:
            send_line(msg)
        return

    if not LATEST_JSON.exists():
        print('ERROR: contract_liab_latest.json not found. Run generate_contract_liab.py first.',
              file=sys.stderr)
        sys.exit(1)

    latest = json.loads(LATEST_JSON.read_text('utf-8'))
    curr = build_state(latest)

    prev = None
    if NOTIFY_STATE.exists():
        try:
            prev = json.loads(NOTIFY_STATE.read_text('utf-8'))
        except Exception:
            pass

    events = diff_states(curr, prev) if not args.force else [('forced', '強制發送')]

    if not events and not args.force:
        print('→ no changes detected, skipping LINE notification')
        return

    msg, has_alert = build_message(latest, events)
    print(f'=== Message ({len(msg)} chars) ===')
    print(msg)

    if args.dry_run:
        print('\n(dry-run, not sending)')
        return

    if send_line(msg):
        # 更新 state
        NOTIFY_STATE.write_text(
            json.dumps(curr, ensure_ascii=False, separators=(',', ':')), 'utf-8')
        print(f'→ updated {NOTIFY_STATE.name}')


if __name__ == '__main__':
    main()
