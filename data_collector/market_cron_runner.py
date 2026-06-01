#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
from pathlib import Path

import requests

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def trigger_sync(base_url: str, token: str, mode: str, run_mode: str, dry_run: bool, limit=None):
    url = f"{base_url.rstrip('/')}/market_data/trigger_sync"
    headers = {
        'Authorization': f'Token {token}',
        'Content-Type': 'application/json',
    }
    payload = {
        'mode': mode,
        'run_mode': run_mode,
        'dry_run': dry_run,
    }
    if limit is not None:
        payload['limit'] = int(limit)

    resp = requests.post(url, headers=headers, json=payload, timeout=600)
    try:
        body = resp.json()
    except Exception:
        body = {'raw_text': resp.text}

    return {
        'status_code': resp.status_code,
        'body': body,
    }


def main():
    parser = argparse.ArgumentParser(description='通过后端接口触发行情同步（供 crontab 调用）')
    parser.add_argument(
        '--base-url', default=os.getenv('MARKET_SYNC_BASE_URL', 'http://127.0.0.1:5000'))
    parser.add_argument(
        '--token', default=os.getenv('MARKET_SYNC_TOKEN', 'serveraly'))
    parser.add_argument('--mode', default='all',
                        choices=['all', 'plan', 'watchlist'])
    parser.add_argument('--run-mode', dest='run_mode', default='intraday_30m',
                        choices=['manual', 'intraday_30m', 'close_confirm'])
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()

    result = trigger_sync(
        base_url=args.base_url,
        token=args.token,
        mode=args.mode,
        run_mode=args.run_mode,
        dry_run=args.dry_run,
        limit=args.limit,
    )

    print(json.dumps(result, ensure_ascii=False, indent=2))

    # 非 2xx 时返回非零码，便于 crontab 告警。
    if result['status_code'] < 200 or result['status_code'] >= 300:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
