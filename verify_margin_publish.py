#!/usr/bin/env python3
import argparse
import json
import time
from pathlib import Path

import requests


def main():
    parser = argparse.ArgumentParser(description='Verify the public margin JSON has the newly generated trading date.')
    parser.add_argument('--local', default='margin_strategy_latest.json')
    parser.add_argument('--url', required=True)
    parser.add_argument('--attempts', type=int, default=30)
    parser.add_argument('--interval', type=int, default=10)
    args = parser.parse_args()

    expected = json.loads(Path(args.local).read_text())['as_of']
    last = None
    for attempt in range(1, args.attempts + 1):
        try:
            response = requests.get(
                args.url,
                params={'verify': int(time.time())},
                headers={'Cache-Control': 'no-cache'},
                timeout=30,
            )
            response.raise_for_status()
            published = response.json().get('as_of')
            last = published
            print(f'attempt={attempt} expected={expected} published={published}')
            if published == expected:
                print(f'Public margin data verified: {published}')
                return
        except (requests.RequestException, ValueError) as exc:
            last = str(exc)
            print(f'attempt={attempt} verification error={exc}')
        if attempt < args.attempts:
            time.sleep(args.interval)
    raise SystemExit(f'Public margin data did not update to {expected}; last result: {last}')


if __name__ == '__main__':
    main()
