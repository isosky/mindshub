from module.strava_sync import sync_strava_activities
import argparse
import json
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main():
    parser = argparse.ArgumentParser(description='同步 Strava 跑步和骑行数据到 MySQL')
    parser.add_argument('--mode', default='incremental',
                        choices=['incremental', 'full', 'range'])
    parser.add_argument('--start-date', dest='start_date')
    parser.add_argument('--end-date', dest='end_date')
    args = parser.parse_args()

    result = sync_strava_activities(
        sync_mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
