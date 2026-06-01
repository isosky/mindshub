# AKShare 行情与指标入库设计（investdb）

## 1. 目标

建设稳定的行情采集与指标入库链路，为投资复盘图表提供统一数据源。

当前覆盖：

- 日线 K 线（股票/ETF/指数）
- 技术指标（MA5/10/20/60、RSI6/14、MACD）
- 观察池维护
- 同步任务日志可观测

## 2. 数据职责边界

1. `investdb` 仅保存行情与指标数据
- `stock_daily_kline_indicator`
- `market_watchlist`
- `market_sync_job_log`

2. `summary` / `summary_test` 仅保存复盘业务数据
- 计划、修改、执行、复盘总结

3. 页面组装职责
- 左侧图表数据来自 `investdb`
- 右侧复盘业务数据来自 `summary`

## 3. 表结构（当前）

### 3.1 行情宽表 `stock_daily_kline_indicator`

主键与约束：

- 主键：`id`
- 唯一键：`(symbol_code, trade_date, adjust_type)`
- 固定复权：`adjust_type='qfq'`

核心字段：

- 标的信息：`symbol_code`、`symbol_type`
- 价格：`open_price/high_price/low_price/close_price/prev_close_price`
- 交易：`volume/amount/turnover_rate`
- 涨跌：`change_amount/change_pct`
- 指标：`ma5/ma10/ma20/ma60/rsi6/rsi14/dif/dea/macd_hist`

### 3.2 观察池表 `market_watchlist`

- 唯一键：`symbol_code`
- 状态字段：`enabled`（1 启用，0 停用）
- 类型字段：`symbol_type`（`stock|etf|index`）

### 3.3 同步日志表 `market_sync_job_log`

- 运行模式：`manual|intraday_30m|close_confirm`
- 状态：`running|success|failed|partial_success|skipped`
- 统计字段：`total_symbols/success_symbols/failed_symbols/upsert_rows`
- 明细：`detail_json`

## 4. 采集策略（当前实现）

### 4.1 标的来源

- `mode=plan`：来自复盘计划中的标的
- `mode=watchlist`：来自观察池启用标的
- `mode=all`：计划标的 + 观察池标的

### 4.2 时间窗口

1. 计划标的窗口
- 起点：计划开始日期前 2 个月
- 终点：计划结束日期后 30 天

2. 增量回看
- 为减少 EMA/RSI 边界误差，按最新交易日向前回看 `LOOKBACK_DAYS=120`

3. 观察池初始化
- 无历史时默认回拉 `WATCHLIST_INITIAL_DAYS=365`

### 4.3 接口回退与重试

- 每类标的采用主接口 + 备用接口策略
- 网络异常重试 `FETCH_RETRY_TIMES=3`

### 4.4 幂等写入

- 采用批量 upsert
- 基于唯一键覆盖更新，重复执行不会产生脏重复

## 5. 指标计算口径

固定参数：

- MA: 5/10/20/60
- RSI: 6/14
- MACD: 12/26/9（输出 `dif/dea/macd_hist`）

计算位置：

- 在采集链路中完成指标计算后入库
- 前端仅消费结果，不重复计算

## 6. 接口（后端）

### 6.1 同步触发

`POST /market_data/trigger_sync`

入参示例：

```json
{
  "mode": "all",
  "run_mode": "manual",
  "start_date": "2026-05-01",
  "end_date": "2026-05-31",
  "dry_run": true,
  "limit": 30
}
```

### 6.2 同步日志

`POST /market_data/sync_jobs/list`

可筛选：`run_mode`、`status`、`limit`

### 6.3 图表数据

`POST /market_data/kline_indicators`

返回结构包含两层：

- `rows`：逐日明细
- `chart`：前端直接可用数组（`dates/kline/volume/ma/rsi/macd`）

### 6.4 观察池管理

- `POST /market_data/watchlist/list`
- `POST /market_data/watchlist/add`
- `POST /market_data/watchlist/remove`
- `POST /market_data/watchlist/toggle`

## 7. 标的代码规范

统一存储格式：`6位数字.交易所`（例如 `000001.SZ`、`510300.SH`）。

代码标准化支持输入：

- `000001`
- `sz000001`
- `000001.SZ`

最终都会归一到统一格式并据此落库。

## 8. 验收要点

1. `manual + dry_run` 可返回计划执行摘要。
2. 关闭 `dry_run` 后可落库并写入任务日志。
3. 重跑同一窗口，数据量增长符合预期且无重复脏数据。
4. 复盘页图表可基于 `symbol_code + 日期区间` 稳定展示 4 图。

## 9. 已知注意事项

1. AKShare 各接口字段名偶有差异，标准化逻辑已做字段兼容映射。
2. `intraday_30m` 在非交易时段会返回 `skipped`，这是预期行为。
3. 若外部源临时限流，任务可能进入 `partial_success`，可通过 `detail_json` 定位失败标的。
