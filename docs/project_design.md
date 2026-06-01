## 项目总览

本项目由后端（Flask）与前端（Vue.js）组成，目标是提供项目/任务管理与数据采集展示的完整应用。后端代码位于 `mindshub` 目录，前端位于 `summary` 目录。

## 执行入口

- 后端：运行 `python -u mindshub/app.py` 启动 Flask 服务。
- 前端：进入 `summary` 目录后运行 `npm install`、`npm run serve` 启动开发服务器。

## 主要模块与职责

- `base`：系统基础设施，包含配置、数据库连接与通用工具（例如 `base/base.py`、`base/config.py`）。
- `module`：核心业务逻辑层，处理任务、项目、人员、交易等（例如 `module/task.py`、`module/project.py`）。
- `routes`：后端路由集合，每个文件为一类 API（如 `task_routes.py`、`project_routes.py`、`transaction_routes.py`）。
- `fund`：与基金相关的业务逻辑，已移动并归档为 `archive/fund`（例如 `archive/fund/fund_total.py`、`archive/fund/fund_estimate.py`）。
- `archive`：历史/采集器代码，包含多个数据采集脚本（`archive/data_collector/*`）。
- `nga_images` 与其他资源目录：图片及静态资源存放。

## 后端路由与 API

- 路由定义分散在 `routes` 目录，由 `app.py` 注册为蓝图（Blueprint）。
- 常见 API 示例：`/getproject`、`/addtask` 等（具体查看各 `*_routes.py` 文件）。

## 数据流与持久化

- 使用关系型数据库（MySQL），数据库连接由 `base` 模块封装。
- 采集器会调用外部 API（例如基金数据源）并以表或 CSV 的形式导入/导出数据。
- 日志保存到项目内的 `logs` 目录，建议配置日志轮转与分级管理。

## 前端结构与交互

- 前端位于 `summary/src`，主入口为 `main.js`，根组件 `App.vue`。
- 页面组件位于 `summary/src/components`（如 `task.vue`、`project.vue` 等），通过 Axios 访问后端 API。

## 关键实现细节与注意事项

- 采集器/爬虫模块：位于 `archive/data_collector`，多数脚本为同步实现并使用 `time.sleep()` 做节流，建议未来考虑异步或任务队列以提升效率。
- 配置管理：数据库、API 的地址和凭据集中在 `base/config.py`，部署前需确认配置安全性与环境区分（开发/生产）。
- 依赖：后端依赖 Flask，前端依赖 Vue.js、Axios。参见 `mindshub/requirements.txt` 与 `summary/package.json`。

## 参考文件位置

- 后端主入口：mindshub/app.py
- 基础模块：mindshub/base/
- 业务模块：mindshub/module/
- 路由集合：mindshub/routes/
- 采集器：mindshub/archive/data_collector/
- 前端代码：summary/

## 专题说明

- Strava 同步模块说明：mindshub/docs/strava_sync_module.md

### Strava 与 activity 模块现状

- Strava 同步模块支持 Run/ Ride 两类活动的抓取，并把活动与路段（跑步/骑行）落入 MySQL；跑步路段 `strava_run_segments` 已验证写入成功，骑行路段 `strava_ride_segments` 的最终回填受 Strava API 限流（429）影响，需待限流窗口恢复后重跑验证。
- 前端将跑步路段分析拆为独立页面 `summary/src/components/activity_run_segment_analysis.vue`，布局为一行三列（左-路段列表，中-趋势图，右-明细表），趋势图采用动态 Y 轴且 X 轴按时间升序渲染（最左为最早时间）。
- 已删除旧的合并页面 `activity_segment_analysis.vue` 与兼容路由 `/activity_segment_analysis`，前端导航仅保留跑步与骑行独立页入口。

## 投资复盘子系统（2026-06 更新）

当前投资相关能力已从“原型说明”进入“前后端可联调”状态，包含 3 个页面与 2 组后端接口：

- 页面：`/investment_review`、`/market_watchlist`、`/market_sync_ops`
- 接口组：`investment_review_routes.py`、`market_data_routes.py`

职责拆分：

- 复盘业务数据（计划、修改、执行、复盘总结）存储在 `summary/summary_test`。
- 行情与指标（K线、MA、RSI、MACD）及同步日志存储在 `investdb`。

详细文档请查看：

- `docs/investment.md`
- `docs/akshare_investdb_design.md`
- `docs/market_data_crontab.md`


