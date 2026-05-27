# Strava 同步模块说明

## 1. 模块目标

该模块用于把 Strava 的跑步和骑行活动同步到 MySQL，供后端接口和后续前端页面调用。

当前实现范围：

- 同步 Run 和 Ride 两类活动
- 跑步额外同步 segment_effort 路段数据
- 骑行额外同步 segment_effort 路段数据，并按骑行路段字典过滤保存
- 同步活动级运动负荷字段 exercise_load_score
- 支持增量同步、全量同步、指定周期同步
- 提供 Flask 接口查询同步结果和已落库数据

## 2. 代码位置

- 核心服务：[module/strava_sync.py](module/strava_sync.py)
- Strava API 访问：[data_collector/strava_api.py](data_collector/strava_api.py)
- Flask 路由：[routes/strava_routes.py](routes/strava_routes.py)
- 骑行路段字典维护路由：[routes/activity_routes.py](routes/activity_routes.py)
- 命令行入口：[data_collector/main.py](data_collector/main.py)
- 建表脚本：[sqls/create_strava_sync_tables.sql](sqls/create_strava_sync_tables.sql)
- 现有库加列脚本：[sqls/alter_strava_activities_add_exercise_load_score.sql](sqls/alter_strava_activities_add_exercise_load_score.sql)
- activity 相关建表脚本：[sqls/create_activity_module_tables.sql](sqls/create_activity_module_tables.sql)

## 3. 职责划分

### 3.1 strava_sync.py

负责同步主流程，包含：

- 读取 Strava 凭据
- 计算同步窗口
- 调用 Strava API 拉取活动和跑步路段
- 调用 Strava API 拉取 Run / Ride 活动的 segment_effort
- 把活动和路段标准化为数据库字段
- 把 Strava 的 suffer_score 映射为 exercise_load_score
- 执行 MySQL upsert
- 记录同步日志
- 提供活动、路段、同步状态查询方法

### 3.2 strava_api.py

负责与 Strava HTTP API 交互，包含：

- 拉取 athlete activities 列表
- 拉取活动详情
- 拉取活动 stream
- 从 segment_efforts 构造活动路段记录
- 在 segment_effort 缺少心率时，尝试用 heartrate stream 补算

### 3.3 strava_routes.py

负责对外暴露 HTTP 接口，当前接口包括：

- POST /sync_strava_activities
- GET /get_strava_sync_status
- POST /query_strava_activities
- POST /query_strava_run_segments

所有接口都走 token 鉴权。

骑行路段字典维护当前复用 [routes/activity_routes.py](routes/activity_routes.py) 提供的接口：

- GET /get_ride_segment_dict
- POST /save_ride_segment_dict
- POST /delete_ride_segment_dict

### 3.4 main.py

负责命令行手动触发同步，适合本地调试、补数、临时执行区间同步。

示例：

```bash
python -m data_collector.main --mode incremental
python -m data_collector.main --mode full
python -m data_collector.main --mode range --start-date 2026-05-01 --end-date 2026-05-20
```

## 4. 配置来源

当前 Strava 凭据来源只有两层：

- 系统环境变量
- [base/config.py](base/config.py)

已移除 [data_collector/.env](data_collector/.env) 读取逻辑，避免出现双份配置来源。

当前支持的变量名：

- STRAVA_ACCESS_TOKEN
- STRAVA_CLIENT_ID
- STRAVA_CLIENT_SECRET
- STRAVA_REFRESH_TOKEN

说明：

- 如果配置了 STRAVA_ACCESS_TOKEN，优先直接使用
- 如果没有 access token，则需要 client_id、client_secret、refresh_token 三项组合

## 5. 数据落库说明

### 5.1 活动主表 strava_activities

用于保存 Run 和 Ride 的主活动信息，核心字段包括：

- activity_id
- activity_type
- activity_name
- start_time
- duration_second
- distance_meter
- elevation_gain
- average_heartrate
- average_power_watt
- average_pace_second_per_km
- exercise_load_score

字段说明：

- duration_second 表示运动时长，单位为秒
- average_power_watt 仅骑行有意义
- average_pace_second_per_km 仅跑步有意义，单位为秒/公里
- exercise_load_score 是活动级运动负荷字段，当前映射自 Strava 返回的 suffer_score
- 该字段对跑步和骑行都尝试保存；如果 Strava 原始活动没有返回 suffer_score，则数据库中保持为空

### 5.2 跑步路段表 strava_run_segments

用于保存跑步活动中的 segment_effort，核心字段包括：

- segment_effort_id
- activity_id
- segment_id
- segment_name
- start_time
- distance_meter
- duration_second
- average_heartrate
- average_pace_second_per_km

### 5.3 同步日志表 strava_sync_logs

用于保存每次同步的执行结果，核心字段包括：

- sync_mode
- start_date
- end_date
- status
- summary_json
- error_message
- created_at

### 5.4 骑行路段字典表 strava_ride_segment_dict

用于维护需要保留的骑行路段名称，核心字段包括：

- id
- segment_name
- is_enabled
- created_at
- updated_at

### 5.5 骑行路段表 strava_ride_segments

用于保存命中字典名称后的骑行 segment_effort，当前保存字段与实际实现一致，包含：

- segment_effort_id
- activity_id
- segment_id
- segment_name
- start_time
- distance_meter
- duration_second
- average_heartrate
- average_power_watt
- average_grade_adjusted_speed
- elevation_gain

保存规则：

- 只有 activity_type = Ride 的 segment_effort 才会进入该表
- 只有 segment_name 命中 strava_ride_segment_dict 启用名称的记录才会落库

### 5.6 现有库升级

如果数据库已经提前建好旧版 strava_activities 表，需要先执行以下加列脚本，再进行全量回填：

- [sqls/alter_strava_activities_add_exercise_load_score.sql](sqls/alter_strava_activities_add_exercise_load_score.sql)

回填方式：

- 执行一次 full 同步
- 通过 upsert 把已有活动的 exercise_load_score 更新进去

## 6. 同步模式

### 6.1 incremental

默认模式。使用数据库中最新的活动时间作为 after 条件，只拉取新增活动。

### 6.2 full

不限制时间窗口，重新拉取所有可访问的活动。数据库侧通过唯一键和 upsert 保证幂等。

### 6.3 range

使用 start_date 和 end_date 构造 after / before，仅同步指定时间区间。

## 7. 查询返回说明

查询方法和接口返回前会统一做序列化处理：

- datetime 转为 yyyy-mm-dd HH:MM:SS 字符串
- Decimal 转为 float

这样可以直接执行 json.dumps，也可以直接被 Flask jsonify 返回。

## 8. 当前已验证结果

本轮开发中已完成的关键验证：

- 增量同步成功执行
- 全量同步成功执行
- 活动表成功落库
- 跑步路段表成功落库
- 同步日志成功记录
- 查询结果已可直接 JSON 序列化
- exercise_load_score 已可通过查询接口返回
- 本次全量回填后，活动总数为 245 条，其中 239 条写入了 exercise_load_score

样例结果：

- Run: Morning Run -> exercise_load_score = 136.0
- Run: Morning Run -> exercise_load_score = 74.0
- Ride: Lunch Ride -> exercise_load_score = 34.0

说明：

- 仍有 6 条活动的 exercise_load_score 为空
- 这通常表示 Strava 原始活动本身没有返回 suffer_score，而不是同步失败

## 9. 遗留项

### 遗留 3

[data_collector/main.py](data_collector/main.py) 当前仍然保留为命令行同步入口。

这不是功能性问题，而是一个保留项：

- 目前它对本地调试、补数、排查同步问题仍然有价值
- 后续如果前端页面和后端接口已经完全覆盖所有触发场景，并且不再需要人工执行同步，可再评估是否下线该入口
- 在那之前，建议继续保留

### 遗留 4

骑行路段字典维护入口已经补齐，并已人工维护至少 1 条启用字典名称。

当前待办：

- 需要在 Strava 限流窗口恢复后，重新执行一次骑行路段回填验证
- 本次验证阻塞原因不是本地代码报错，而是 Strava 详情接口返回 429 Rate Limit Exceeded
- 当前数据库现状为：strava_ride_segments 表记录数仍为 0，尚不能据此判断过滤逻辑是否有误
- 下一次验证时，优先检查同步结果中的 saved_ride_segment_count，并复核 strava_ride_segments 实际落库样例

## 10. 新增需求记录

### 10.1 骑行路段下载

新增需求：

- 下载骑行活动的 segment_effort 数据。

### 10.2 骑行路段过滤保存

保存规则：

- 骑行路段不做全量保存。
- 只保存“路段名称与业务字典名称一致”的骑行路段。

当前阶段说明：

- 该需求已完成后端同步逻辑、字典表结构、页面维护入口的开发。
- 当前实现状态：
	- 已支持拉取骑行活动 segment_effort 数据
	- 已支持按 strava_ride_segment_dict 启用字典名称过滤后保存到 strava_ride_segments
	- 已提供骑行路段字典维护入口，支持新增、启停、删除
- 当前未完成项：
	- 受 Strava 429 限流影响，尚未完成“已维护字典名称命中后成功落库”的最终回填验证

建议实现方向：

- 现阶段保持现有实现不变。
- 待 Strava 限流恢复后，优先重跑 full 或针对已有 Ride 活动做补抓验证。
- 验证重点为 saved_ride_segment_count、strava_ride_segments 实际记录数，以及样例路段名称是否命中字典。