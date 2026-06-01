# AKShare 行情与指标入库设计（investdb）

## 1. 目标与范围

本设计用于将股票、ETF、指数的日线行情与技术指标数据，从 AKShare 采集并写入 investdb，供投资复盘页面图表区使用（K 线、成交量、RSI、MACD）。

本期目标：
- 打通从数据采集到入库再到查询接口的完整链路。
- 支持两类标的来源：计划标的、观察池标的。
- 提供稳定的增量同步能力与可追踪任务日志。

## 2. 已确认业务口径

- 标的范围：股票 + ETF + 指数。
- 复权口径：固定前复权，不提供切换。
- 计划标的数据窗口：计划开始前 2 个月 到 计划结束后 30 天（自然日）。
- 观察池标的：允许额外维护；按每日任务持续拉取，不受单条计划窗口限制。
- 调度频率：
  - 交易时段每 30 分钟执行一次。
  - 午休时段跳过。
  - 收盘后 15:30 执行一次确认同步。

## 3. 数据职责边界

- investdb：仅存行情与指标数据（价格、成交量、MA、RSI、MACD 等）。
- summary / summary_test：仅存复盘业务数据（计划、修改、执行、复盘）。
- 页面展示由接口层进行数据组装：
  - 左侧图表来自 investdb。
  - 右侧复盘内容来自 summary 业务库。

## 4. 数据模型设计

### 4.1 日线宽表（核心）

表名建议：stock_daily_kline_indicator

字段建议：
- id：bigint，自增主键。
- symbol_code：varchar(32)，统一标的代码（如 000001.SZ / 510300.SH / 000300.SH）。
- symbol_type：varchar(16)，取值建议 stock / etf / index。
- trade_date：date，交易日。
- adjust_type：varchar(16)，固定值 qfq（前复权）。
- open_price：decimal(18,4)
- high_price：decimal(18,4)
- low_price：decimal(18,4)
- close_price：decimal(18,4)
- prev_close_price：decimal(18,4)
- change_amount：decimal(18,4)
- change_pct：decimal(10,4)
- volume：decimal(20,2)
- amount：decimal(20,2)
- turnover_rate：decimal(10,4)
- ma5：decimal(18,4)
- ma10：decimal(18,4)
- ma20：decimal(18,4)
- ma60：decimal(18,4)
- rsi6：decimal(10,4)
- rsi14：decimal(10,4)
- dif：decimal(18,6)
- dea：decimal(18,6)
- macd_hist：decimal(18,6)
- data_source：varchar(32)，默认 akshare。
- created_at：datetime
- updated_at：datetime

约束与索引：
- 唯一键：uk_symbol_date_adjust(symbol_code, trade_date, adjust_type)
- 普通索引：idx_symbol_date(symbol_code, trade_date)
- 普通索引：idx_trade_date(trade_date)

说明：
- 当前固定前复权，adjust_type 仍保留字段，便于未来扩展。
- 以 symbol_code + trade_date + adjust_type 作为幂等写入基础。

### 4.2 观察池配置表

表名建议：market_watchlist

字段建议：
- id：bigint，自增主键。
- symbol_code：varchar(32)
- symbol_name：varchar(64)
- symbol_type：varchar(16)
- enabled：tinyint，1 启用 / 0 停用。
- remark：varchar(255)
- created_by：varchar(64)
- created_at：datetime
- updated_at：datetime

约束与索引：
- 唯一键：uk_symbol(symbol_code)
- 索引：idx_enabled_type(enabled, symbol_type)

说明：
- 当前先落后端表与接口，页面后续开发。

### 4.3 任务日志表

表名建议：market_sync_job_log

字段建议：
- id：bigint，自增主键。
- job_name：varchar(64)
- run_mode：varchar(32)（intraday_30m / close_confirm / manual）
- started_at：datetime
- finished_at：datetime
- status：varchar(16)（running / success / failed / partial_success）
- total_symbols：int
- success_symbols：int
- failed_symbols：int
- upsert_rows：int
- error_summary：text
- detail_json：json
- created_at：datetime

## 5. 采集与计算流程

### 5.1 标的来源拆分

1) 计划标的任务
- 从 summary 业务库读取计划列表与计划周期。
- 计算单标的拉取区间：
  - start = plan_start - 2 个月
  - end = plan_end + 30 天
- 同一 symbol 多计划取并集区间，减少重复拉取。

2) 观察池任务
- 读取 market_watchlist 中 enabled=1 的标的。
- 每日按增量窗口拉取并更新。

### 5.2 拉取策略

- 数据源：AKShare 日线接口（前复权）。
- 增量更新：
  - 每个标的先查本地最大 trade_date。
  - 实际拉取起点 = max_trade_date 向前回看 N 天（建议 120 天）
  - 目的：消除 EMA/RSI 类指标边界误差。
- 收盘确认（15:30）：
  - 对当日及最近窗口再次同步，确保最终数据完整。

### 5.3 指标计算口径（本期固定）

- MA：MA5 / MA10 / MA20 / MA60
- RSI：RSI6 / RSI14
- MACD：12, 26, 9（输出 DIF、DEA、MACD 柱）

说明：
- 参数先固定，后续可配置化。
- 所有计算在采集链路完成后再入库，页面不做指标计算。

### 5.4 入库策略

- 批量 upsert（建议每批 500~2000 行）。
- 幂等写入：遇到唯一键冲突执行 update。
- 单标的失败不阻塞全任务，任务结束输出失败清单。

## 6. 调度设计

建议调度表达（crontab 示例思路，实际以服务器时区为准）：

- 交易时段每 30 分钟：
  - 上午：09:30、10:00、10:30、11:00、11:30
  - 下午：13:00、13:30、14:00、14:30、15:00
- 收盘确认：15:30

实现建议：
- 提供统一触发接口（内部调用），由 crontab 调该接口。
- 接口支持 run_mode 参数：intraday_30m / close_confirm / manual。

## 7. 接口草案

### 7.1 采集触发接口（内部）

- 路径：/market_data/trigger_sync
- 方法：POST
- 入参：
  - run_mode（必填）：intraday_30m / close_confirm / manual
  - symbols（可选）：手工指定标的列表
  - start_date（可选）
  - end_date（可选）
- 出参：
  - job_id
  - accepted（true/false）

### 7.2 图表查询接口（前端使用）

- 路径：/market_data/kline_indicators
- 方法：POST
- 入参：
  - symbol_code
  - start_date
  - end_date
- 出参：
  - 按交易日升序的 K 线、成交量、RSI、MACD、MA 数据

### 7.3 观察池管理接口（页面后续接入）

- 新增观察标的：/market_data/watchlist/add
- 删除观察标的：/market_data/watchlist/remove
- 列表查询：/market_data/watchlist/list
- 启停切换：/market_data/watchlist/toggle

## 8. 异常处理与稳定性

- 网络异常：重试 2~3 次，指数退避。
- 空数据或停牌：记录日志并跳过，不判定全任务失败。
- 数据质量校验：
  - trade_date 非空
  - OHLC 合法性校验（high >= low）
  - 数值字段非法时置空并记录。
- 并发控制：
  - 任务加分布式锁或 DB 锁，避免同 run_mode 重复并发执行。

## 9. 验收标准

- 可按 symbol_code + 日期范围稳定返回 K 线、成交量、RSI、MACD。
- 盘中 30 分钟任务与 15:30 任务可稳定执行并记录日志。
- 重跑同区间无重复脏数据，upsert 幂等。
- 计划标的按窗口拉取，观察池标的按每日增量拉取。
- 数据可支撑复盘页面 4 图容器的联动展示。

## 10. 分阶段实施建议

第一阶段（本期）
- 建表（宽表 + 观察池 + 任务日志）
- 采集任务主流程
- 指标计算与幂等入库
- 触发接口与图表查询接口
- crontab 调度接入

第二阶段（后续）
- 观察池页面维护
- 参数配置化（MA/RSI/MACD）
- 数据质量监控看板
- 更细粒度周期扩展（如 30m / 5m）

## 11. 结论

当前业务规模与使用场景下，MySQL 日线宽表方案可行且性价比高。优先保证链路稳定、口径统一和任务可观测性，后续再按数据规模演进分层模型。