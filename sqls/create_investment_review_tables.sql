-- 投资复盘业务表
-- 执行库：开发环境 summary_test，生产环境 summary
-- 注意：本文件只创建复盘业务表，不创建股票行情与指标表；行情数据继续存放在 invest 库。


-- 投资复盘计划主表：保存一轮完整交易计划的当前有效信息
create table if not exists investment_review_plans (
    id bigint not null auto_increment,
    plan_code varchar(64) default null,
    stock_code varchar(32) not null,
    stock_name varchar(64) not null,
    industry varchar(128) default null,
    record_type varchar(16) not null default 'real',
    plan_type varchar(32) not null default '执行计划',
    trade_direction varchar(16) not null default 'long',
    plan_status varchar(32) not null default 'draft',
    period_start date default null,
    period_end date default null,
    open_strategy varchar(64) default null,
    close_strategy varchar(64) default null,
    reason longtext default null,
    entry_zone varchar(128) default null,
    stop_loss decimal(12,4) default null,
    target_price decimal(12,4) default null,
    market_status longtext default null,
    sector_status longtext default null,
    tags_text varchar(255) default null,
    plan_score tinyint not null default 0,
    initial_plan_snapshot_json longtext default null,
    current_plan_snapshot_json longtext default null,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_investment_review_plans_plan_code (plan_code),
    key idx_investment_review_plans_stock_code (stock_code),
    key idx_investment_review_plans_status_created_at (plan_status, created_at),
    key idx_investment_review_plans_period (period_start, period_end)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


-- 投资复盘计划修改记录表：追加保存每次计划调整，不覆盖原计划
create table if not exists investment_review_plan_modifications (
    id bigint not null auto_increment,
    plan_id bigint not null,
    modification_time datetime not null,
    modification_label varchar(32) default null,
    tag_type varchar(16) default null,
    title varchar(255) not null,
    reason longtext default null,
    updated_plan longtext default null,
    evaluation longtext default null,
    plan_snapshot_json longtext default null,
    is_deleted tinyint not null default 0,
    deleted_at datetime default null,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    key idx_investment_review_plan_modifications_plan_time (plan_id, modification_time),
    key idx_investment_review_plan_modifications_deleted (plan_id, is_deleted, modification_time),
    key idx_investment_review_plan_modifications_created_at (created_at)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


-- 投资复盘执行记录表：保存一轮计划中的每次买入、加仓、减仓、卖出
create table if not exists investment_review_executions (
    id bigint not null auto_increment,
    plan_id bigint not null,
    execution_time datetime not null,
    action varchar(16) not null,
    price decimal(12,4) default null,
    volume decimal(18,4) default null,
    position_ratio decimal(8,4) default null,
    position_text varchar(64) default null,
    note longtext default null,
    is_deleted tinyint not null default 0,
    deleted_at datetime default null,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    key idx_investment_review_executions_plan_time (plan_id, execution_time),
    key idx_investment_review_executions_deleted (plan_id, is_deleted, execution_time),
    key idx_investment_review_executions_action_time (action, execution_time)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


-- 投资复盘总结表：保存结果汇总、主观复盘和情绪记录
create table if not exists investment_review_reviews (
    id bigint not null auto_increment,
    plan_id bigint not null,
    review_status varchar(32) not null default 'pending',
    avg_entry_price decimal(12,4) default null,
    exit_price decimal(12,4) default null,
    realized_pnl_amount decimal(14,2) default null,
    realized_pnl_ratio decimal(10,4) default null,
    max_floating_pnl_amount decimal(14,2) default null,
    max_floating_pnl_ratio decimal(10,4) default null,
    risk_reward_ratio decimal(10,4) default null,
    execution_deviation varchar(255) default null,
    did_well longtext default null,
    did_wrong longtext default null,
    buy_emotion varchar(32) default null,
    hold_emotion varchar(32) default null,
    sell_emotion varchar(32) default null,
    improvement_action longtext default null,
    review_conclusion longtext default null,
    review_snapshot_json longtext default null,
    reviewed_at datetime default null,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_investment_review_reviews_plan_id (plan_id),
    key idx_investment_review_reviews_status_reviewed_at (review_status, reviewed_at)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;