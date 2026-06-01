from module.strava_sync import sync_strava_activities
from data_collector.akshare_market_sync import sync_market_data
import argparse
import json
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main():
    parser = argparse.ArgumentParser(description='数据采集入口（Strava/AKShare）')
    parser.add_argument('--source', default='strava',
                        choices=['strava', 'akshare'])
    parser.add_argument('--mode', default='incremental')
    parser.add_argument('--run-mode', dest='run_mode', default='manual')
    parser.add_argument('--start-date', dest='start_date')
    parser.add_argument('--end-date', dest='end_date')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if args.source == 'strava':
        result = sync_strava_activities(
            sync_mode=args.mode,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    else:
        result = sync_market_data(
            mode=args.mode,
            run_mode=args.run_mode,
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
