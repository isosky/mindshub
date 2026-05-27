# activity 模块说明

## 1. 模块目标

activity 模块用于承接 Strava 同步后的运动分析能力，当前已落地以下功能：

- 展示本年运动列表，按开始时间倒序分页查询
- 按年、季度、月、周统计运动情况
- 设置本年骑行距离目标、跑步距离目标，并计算完成率与每日还需距离
- 根据 exercise_load_score 计算健康度、疲劳度、状态值，并展示最近 42 天趋势
- 在页面展示 Strava 最近同步状态，并直接复用现有同步接口触发增量、全量、区间同步
- 展示某次跑步的路段明细，并支持跳转到独立跑步路段分析页
- 维护骑行路段字典，供骑行路段按名称过滤保存使用
 
当前开发重点与现状更新：

- 跑步路段分析为首要优化对象，已将跑步独立页 `activity_run_segment_analysis.vue` 布局调整为一行三列（左：路段列表，中：趋势图，右：明细表），并放大明细展示区以提升可读性。
- 趋势图 Y 轴已采用动态刻度（不强制从 0 开始）以避免数据被压扁；具体由组件内 `buildValueAxis` 计算 min/max 并启用 `scale:true`。
- 趋势图 X 轴已修正为按时间升序展示（最左为最早时间、最右为最新时间），仅影响图表展示，列表/明细的原始排序保持不变。
- 旧的统一页 `activity_segment_analysis.vue` 已删除，相关兼容路由 `/activity_segment_analysis` 已移除，前端入口指向独立的跑步/骑行页。
- 后端同步链路已实现跑步与骑行数据落库（不依赖 pandas）；跑步路段 `strava_run_segments` 已成功写入，骑行路段 `strava_ride_segments` 的最终回填仍受 Strava API 429 限流影响，需待限流窗口恢复后重跑验证。

请注意：当前只对跑步独立页的趋势图 X 轴顺序进行了小范围修改，未改动其他页面或路由逻辑。

## 2. 代码位置

- 后端服务：[mindshub/module/activity.py](mindshub/module/activity.py)
- 后端路由：[mindshub/routes/activity_routes.py](mindshub/routes/activity_routes.py)
- 页面组件：[summary/src/components/activity.vue](summary/src/components/activity.vue)
- 跑步路段分析页：[summary/src/components/activity_run_segment_analysis.vue](summary/src/components/activity_run_segment_analysis.vue)
- 前端路由注册：[summary/src/main.js](summary/src/main.js)
- 建表脚本：[mindshub/sqls/create_activity_module_tables.sql](mindshub/sqls/create_activity_module_tables.sql)

## 3. 前台实现状态

### 3.1 activity 主页面

[summary/src/components/activity.vue](summary/src/components/activity.vue) 当前包含 6 个区域：

1. Strava 同步状态区
2. 骑行路段字典维护区
3. 年度目标区
4. 健康度趋势区
5. 统计汇总区
6. 运动列表区

当前已实现交互：

- 增量同步、全量同步、区间同步按钮，直接调用 /sync_strava_activities
- 骑行路段字典的新增或启用、启停切换、删除
- 年度目标保存
- 汇总粒度切换 year / quarter / month / week
- 运动列表分页查询
- 点击 Run 活动后打开右侧抽屉查看跑步路段明细
- 从抽屉进入独立跑步路段分析页

### 3.2 跑步路段分析页

[summary/src/components/activity_run_segment_analysis.vue](summary/src/components/activity_run_segment_analysis.vue) 当前已实现：

- 按日期范围查询
- 按路段关键字查询
- 左侧显示路段汇总列表
- 右侧显示路段趋势图和明细表
- 点击汇总行后，按该路段名称刷新趋势与明细

## 4. routes 实现状态

[mindshub/routes/activity_routes.py](mindshub/routes/activity_routes.py) 当前已提供以下接口，全部走 token 鉴权，并统一返回 {code: 200, data: ...}：

1. GET /get_activity_init_data
2. POST /query_activity_list
3. POST /query_activity_summary
4. GET /get_activity_goal
5. POST /save_activity_goal
6. GET /get_activity_health_metrics
7. POST /query_run_segment_detail
8. POST /query_run_segment_analysis
9. GET /get_ride_segment_dict
10. POST /save_ride_segment_dict
11. POST /delete_ride_segment_dict

说明：

- activity 页面没有单独新增 /sync_activity_strava，前端直接复用现有 /sync_strava_activities
- get_activity_init_data 当前除同步状态、年度目标、概览统计、健康度外，还会一并返回 ride_segment_dict

## 5. module 实现状态

[mindshub/module/activity.py](mindshub/module/activity.py) 当前已实现以下核心函数：

- get_activity_year_goal
- save_activity_goal
- get_ride_segment_dict
- save_ride_segment_dict
- delete_ride_segment_dict
- query_activity_list
- query_activity_summary
- rebuild_activity_daily_load_metrics
- get_activity_health_metrics
- query_run_segment_detail
- query_run_segment_analysis
- get_activity_overview
- get_activity_init_data

当前实现口径：

- 健康度计算采用 CTL / ATL / TSB 思路
- daily_exercise_load 为按自然日汇总的 exercise_load_score
- CTL 使用 42 天指数加权平均
- ATL 使用 7 天指数加权平均
- TSB = CTL - ATL
- 年度目标仅围绕当前年份使用，不做历史年份管理页面

## 6. 数据依赖与表结构

activity 模块当前依赖以下数据表：

- strava_activities
- strava_run_segments
- strava_sync_logs
- activity_year_goals
- activity_daily_load_metrics
- strava_ride_segment_dict
- strava_ride_segments

用途说明：

- activity_year_goals：保存年度骑行与跑步目标
- activity_daily_load_metrics：缓存每日负荷与健康度指标
- strava_ride_segment_dict：保存需要保留的骑行路段名称字典
- strava_ride_segments：保存命中字典名称后的骑行路段数据

## 7. 当前验证状态

当前已完成验证：

- activity 主页面与跑步路段分析页已接入前端路由
- activity routes 与 module 静态检查通过
- 骑行路段字典维护接口已接通页面
- 页面初始化已能返回 ride_segment_dict，并在前台展示

当前未完成验证：

- 受 Strava 429 限流影响，尚未完成“骑行路段字典命中后成功落库”的最终联调验证

## 8. 遗留项

### 遗留 1

骑行路段字典维护已完成，但骑行路段回填验证仍受 Strava 读限流影响。

后续验证重点：

- sync_strava_activities 返回中的 saved_ride_segment_count
- strava_ride_segments 实际记录数
- 实际样例 segment_name 是否命中字典名称