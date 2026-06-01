# 行情同步调度示例（crontab）

本文给出通过后端接口触发的定时任务方案。

## 1. 前置条件

- 后端服务已运行（默认 `http://127.0.0.1:5000`）。
- 可用 token：`serveraly`（可改为环境变量）。
- 执行脚本：`data_collector/market_cron_runner.py`。

## 2. 建议环境变量

```bash
export MARKET_SYNC_BASE_URL="http://127.0.0.1:5000"
export MARKET_SYNC_TOKEN="serveraly"
```

## 3. 盘中每 30 分钟（交易时段，午休跳过）

```cron
30 9 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
0 10 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
30 10 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
0 11 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
30 11 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
0 13 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
30 13 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
0 14 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
30 14 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
0 15 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode intraday_30m >> logs/market_sync_intraday.log 2>&1
```

说明：午休时段（11:30-13:00）没有任务。

## 4. 收盘确认任务（15:30）

```cron
30 15 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode close_confirm >> logs/market_sync_close.log 2>&1
```

## 5. 手工验证命令

```bash
cd /path/to/mindshub
/path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode plan --run-mode manual --dry-run --limit 1
```

## 6. 说明

- 采集任务日志入库：`market_sync_job_log`。
- `run_mode=intraday_30m` 在非交易时段会自动跳过。
- 若后端接口返回非 2xx，脚本会返回非零退出码，便于 crontab 告警。
