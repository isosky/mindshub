# 市场行情同步调度手册

本文档描述如何通过 `data_collector/market_cron_runner.py` 调用后端接口，完成定时同步与运维排障。

## 1. 脚本与接口关系

- 调度脚本：`data_collector/market_cron_runner.py`
- 实际触发接口：`POST /market_data/trigger_sync`
- 认证方式：`Authorization: Token <token>`

脚本职责：

1. 组装同步参数并发起 HTTP 请求
2. 打印 JSON 结果
3. 遇到非 2xx 返回码时退出码为 1（便于调度系统告警）

## 2. 参数说明

脚本参数：

- `--base-url`：后端地址，默认 `http://127.0.0.1:5000`
- `--token`：鉴权 token，默认读取环境变量
- `--mode`：`all|plan|watchlist`
- `--run-mode`：`manual|intraday_30m|close_confirm`
- `--dry-run`：仅演练不写库
- `--limit`：限制本次处理标的数量

## 3. 环境变量建议

Linux / macOS：

```bash
export MARKET_SYNC_BASE_URL="http://127.0.0.1:5000"
export MARKET_SYNC_TOKEN="serveraly"
```

Windows PowerShell：

```powershell
$env:MARKET_SYNC_BASE_URL = "http://127.0.0.1:5000"
$env:MARKET_SYNC_TOKEN = "serveraly"
```

## 4. 手工命令（先跑通再上定时）

Linux 示例：

```bash
cd /path/to/mindshub
/path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode manual --dry-run --limit 5
```

Windows 示例：

```powershell
Set-Location e:/todo/mindshub
./.venv/Scripts/python.exe data_collector/market_cron_runner.py --mode all --run-mode manual --dry-run --limit 5
```

## 5. crontab 建议（Linux）

### 5.1 盘中每 30 分钟

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

说明：午休时段不调度。

### 5.2 收盘确认任务

```cron
30 15 * * 1-5 cd /path/to/mindshub && /path/to/mindshub/.venv/bin/python data_collector/market_cron_runner.py --mode all --run-mode close_confirm >> logs/market_sync_close.log 2>&1
```

## 6. 运维排障清单

1. 看脚本退出码
- 非 0 表示接口非 2xx 或脚本异常

2. 看后端任务日志表
- 表：`market_sync_job_log`
- 重点字段：`status`、`failed_symbols`、`error_summary`、`detail_json`

3. 常见状态解释
- `success`：全部成功
- `partial_success`：部分标的失败
- `failed`：整体失败
- `skipped`：按规则跳过（如非交易时段）

4. 建议先用 `--dry-run`
- 新环境或改参数时先 dry-run
- 确认返回结构正常后再去掉 dry-run

## 7. 与前端运维页协同

前端 `#/market_sync_ops` 与本脚本调用的是同一后端接口。

- 页面适合人工操作和查看日志
- crontab 适合固定节奏自动运行
- 二者日志最终都落到 `market_sync_job_log`
