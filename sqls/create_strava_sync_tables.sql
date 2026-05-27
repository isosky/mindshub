-- Strava 活动主表：统一存跑步和骑行
create table if not exists strava_activities (
    id bigint not null auto_increment,
    activity_id bigint not null,
    activity_type varchar(16) not null,
    activity_name varchar(255) not null,
    start_time datetime not null,
    duration_second int default null,
    distance_meter decimal(10,2) default null,
    elevation_gain decimal(10,2) default null,
    average_heartrate decimal(6,2) default null,
    average_power_watt decimal(8,2) default null,
    average_pace_second_per_km decimal(8,2) default null,
    exercise_load_score decimal(8,2) default null,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_strava_activities_activity_id (activity_id),
    key idx_strava_activities_type_time (activity_type, start_time),
    key idx_strava_activities_start_time (start_time)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


-- Strava 跑步路段明细表：按每次跑步中的 segment_effort 存储
create table if not exists strava_run_segments (
    id bigint not null auto_increment,
    segment_effort_id bigint not null,
    activity_id bigint not null,
    segment_id bigint default null,
    segment_name varchar(255) not null,
    start_time datetime default null,
    distance_meter decimal(10,2) default null,
    duration_second int default null,
    average_heartrate decimal(6,2) default null,
    average_pace_second_per_km decimal(8,2) default null,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_strava_run_segments_effort_id (segment_effort_id),
    key idx_strava_run_segments_activity_id (activity_id),
    key idx_strava_run_segments_segment_id (segment_id),
    key idx_strava_run_segments_start_time (start_time)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


-- Strava 同步日志表：记录每次同步模式、周期和结果摘要
create table if not exists strava_sync_logs (
    id bigint not null auto_increment,
    sync_mode varchar(16) not null,
    start_date varchar(32) default null,
    end_date varchar(32) default null,
    status varchar(16) not null,
    summary_json longtext default null,
    error_message text default null,
    created_at datetime not null default current_timestamp,
    primary key (id),
    key idx_strava_sync_logs_created_at (created_at),
    key idx_strava_sync_logs_mode_status (sync_mode, status)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;