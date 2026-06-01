-- AKShare 行情与指标宽表：用于复盘图表（K线、成交量、RSI、MACD）
create table if not exists stock_daily_kline_indicator (
    id bigint not null auto_increment,
    symbol_code varchar(32) not null,
    symbol_type varchar(16) not null comment 'stock/etf/index',
    trade_date date not null,
    adjust_type varchar(16) not null default 'qfq' comment '前复权固定值:qfq',
    open_price decimal(18,4) default null,
    high_price decimal(18,4) default null,
    low_price decimal(18,4) default null,
    close_price decimal(18,4) default null,
    prev_close_price decimal(18,4) default null,
    change_amount decimal(18,4) default null,
    change_pct decimal(10,4) default null,
    volume decimal(20,2) default null,
    amount decimal(20,2) default null,
    turnover_rate decimal(10,4) default null,
    ma5 decimal(18,4) default null,
    ma10 decimal(18,4) default null,
    ma20 decimal(18,4) default null,
    ma60 decimal(18,4) default null,
    rsi6 decimal(10,4) default null,
    rsi14 decimal(10,4) default null,
    dif decimal(18,6) default null,
    dea decimal(18,6) default null,
    macd_hist decimal(18,6) default null,
    data_source varchar(32) not null default 'akshare',
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_stock_daily_symbol_date_adjust (symbol_code, trade_date, adjust_type),
    key idx_stock_daily_symbol_date (symbol_code, trade_date),
    key idx_stock_daily_trade_date (trade_date)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


-- 观察池配置表：后续由页面维护，当前先建表
create table if not exists market_watchlist (
    id bigint not null auto_increment,
    symbol_code varchar(32) not null,
    symbol_name varchar(64) default null,
    symbol_type varchar(16) not null comment 'stock/etf/index',
    enabled tinyint not null default 1,
    remark varchar(255) default null,
    created_by varchar(64) default null,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_market_watchlist_symbol (symbol_code),
    key idx_market_watchlist_enabled_type (enabled, symbol_type)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


-- 行情同步任务日志表：记录盘中、收盘确认、手工触发执行结果
create table if not exists market_sync_job_log (
    id bigint not null auto_increment,
    job_name varchar(64) not null,
    run_mode varchar(32) not null comment 'intraday_30m/close_confirm/manual',
    started_at datetime not null,
    finished_at datetime default null,
    status varchar(16) not null comment 'running/success/failed/partial_success',
    total_symbols int not null default 0,
    success_symbols int not null default 0,
    failed_symbols int not null default 0,
    upsert_rows int not null default 0,
    error_summary text default null,
    detail_json longtext default null,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    key idx_market_sync_job_log_created_at (created_at),
    key idx_market_sync_job_log_mode_status (run_mode, status)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;
