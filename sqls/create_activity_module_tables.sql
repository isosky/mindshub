create table if not exists activity_year_goals (
    id bigint not null auto_increment,
    target_year int not null,
    ride_distance_goal_km decimal(10,2) default 0,
    run_distance_goal_km decimal(10,2) default 0,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_activity_year_goals_target_year (target_year)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


create table if not exists activity_daily_load_metrics (
    id bigint not null auto_increment,
    metric_date date not null,
    daily_exercise_load decimal(10,2) default 0,
    ctl_value decimal(10,2) default 0,
    atl_value decimal(10,2) default 0,
    tsb_value decimal(10,2) default 0,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_activity_daily_load_metrics_metric_date (metric_date)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


create table if not exists strava_ride_segment_dict (
    id bigint not null auto_increment,
    segment_id bigint not null,
    segment_name varchar(255) default null,
    is_enabled tinyint not null default 1,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_strava_ride_segment_dict_segment_id (segment_id)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;


create table if not exists strava_ride_segments (
    id bigint not null auto_increment,
    segment_effort_id bigint not null,
    activity_id bigint not null,
    segment_id bigint default null,
    segment_name varchar(255) not null,
    start_time datetime default null,
    distance_meter decimal(10,2) default null,
    duration_second int default null,
    average_heartrate decimal(6,2) default null,
    average_power_watt decimal(8,2) default null,
    created_at datetime not null default current_timestamp,
    updated_at datetime not null default current_timestamp on update current_timestamp,
    primary key (id),
    unique key uk_strava_ride_segments_effort_id (segment_effort_id),
    key idx_strava_ride_segments_activity_id (activity_id),
    key idx_strava_ride_segments_segment_id (segment_id),
    key idx_strava_ride_segments_start_time (start_time)
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci;