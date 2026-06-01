#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import date, datetime, timedelta
from decimal import Decimal

from base.base import connect_database
from module.strava_sync import get_last_sync_status


CURRENT_YEAR = datetime.now().year


def _serialize_rows(rows):
    result = []
    for row in rows:
        current = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                current[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, date):
                current[key] = value.strftime('%Y-%m-%d')
            elif isinstance(value, Decimal):
                current[key] = float(value)
            else:
                current[key] = value
        result.append(current)
    return result


def _to_float(value, digits=2):
    if value in (None, ''):
        return 0 if digits == 0 else 0.0
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return 0 if digits == 0 else 0.0


def _get_year_range(target_year=None):
    target_year = int(target_year or CURRENT_YEAR)
    start_date = date(target_year, 1, 1)
    end_date = date(target_year, 12, 31)
    return target_year, start_date, end_date


def _get_today_in_year(target_year):
    today = date.today()
    if today.year < target_year:
        return date(target_year, 1, 1)
    if today.year > target_year:
        return date(target_year, 12, 31)
    return today


def _build_activity_filters(activity_type=None, start_date=None, end_date=None, require_exercise_load=False):
    sql = ' from strava_activities where 1=1'
    params = []
    if activity_type:
        sql += ' and activity_type=%s'
        params.append(activity_type)
    if start_date:
        sql += ' and start_time >= %s'
        params.append(start_date)
    if end_date:
        sql += ' and start_time <= %s'
        params.append(end_date)
    if require_exercise_load:
        sql += ' and exercise_load_score is not null'
    return sql, params


def _get_year_activity_distance(target_year):
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select '
        'sum(case when activity_type=%s then distance_meter else 0 end) as run_distance_meter, '
        'sum(case when activity_type=%s then distance_meter else 0 end) as ride_distance_meter '
        'from strava_activities where year(start_time)=%s',
        ['Run', 'Ride', target_year],
    )
    row = cursor.fetchone() or {}
    cursor.close()
    conn.close()
    return {
        'run_distance_km': round(_to_float(row.get('run_distance_meter')) / 1000, 2),
        'ride_distance_km': round(_to_float(row.get('ride_distance_meter')) / 1000, 2),
    }


def get_activity_year_goal(target_year=None):
    target_year, _, year_end = _get_year_range(target_year)
    today = _get_today_in_year(target_year)
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select target_year, ride_distance_goal_km, run_distance_goal_km '
        'from activity_year_goals where target_year=%s limit 1',
        [target_year],
    )
    row = cursor.fetchone() or {
        'target_year': target_year,
        'ride_distance_goal_km': 0,
        'run_distance_goal_km': 0,
    }
    cursor.close()
    conn.close()

    distance_data = _get_year_activity_distance(target_year)
    ride_goal = _to_float(row.get('ride_distance_goal_km'))
    run_goal = _to_float(row.get('run_distance_goal_km'))
    ride_done = distance_data['ride_distance_km']
    run_done = distance_data['run_distance_km']
    remaining_days = max((year_end - today).days + 1, 1)

    ride_remaining = max(ride_goal - ride_done, 0)
    run_remaining = max(run_goal - run_done, 0)
    return {
        'year': target_year,
        'ride_distance_goal_km': ride_goal,
        'run_distance_goal_km': run_goal,
        'ride_distance_done_km': ride_done,
        'run_distance_done_km': run_done,
        'ride_completion_rate': round((ride_done / ride_goal) * 100, 2) if ride_goal > 0 else 0,
        'run_completion_rate': round((run_done / run_goal) * 100, 2) if run_goal > 0 else 0,
        'ride_daily_required_km': round(ride_remaining / remaining_days, 2),
        'run_daily_required_km': round(run_remaining / remaining_days, 2),
    }


def save_activity_goal(ride_distance_goal_km, run_distance_goal_km, target_year=None):
    target_year = int(target_year or CURRENT_YEAR)
    conn, cursor = connect_database()
    cursor.execute(
        'insert into activity_year_goals '
        '(target_year, ride_distance_goal_km, run_distance_goal_km, created_at, updated_at) '
        'values (%s,%s,%s,now(),now()) '
        'on duplicate key update '
        'ride_distance_goal_km=values(ride_distance_goal_km), '
        'run_distance_goal_km=values(run_distance_goal_km), '
        'updated_at=now()',
        [
            target_year,
            _to_float(ride_distance_goal_km),
            _to_float(run_distance_goal_km),
        ],
    )
    conn.commit()
    cursor.close()
    conn.close()
    return get_activity_year_goal(target_year)


def get_ride_segment_dict():
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select id, segment_id, segment_name, is_enabled, created_at, updated_at '
        'from strava_ride_segment_dict order by is_enabled desc, updated_at desc, id desc'
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return _serialize_rows(rows)


def save_ride_segment_dict(segment_id, segment_name=None, is_enabled=1):
    try:
        segment_id = int(segment_id)
    except (TypeError, ValueError):
        raise ValueError('segment_id 不能为空且必须是整数')

    segment_name = (segment_name or '').strip() or None

    conn, cursor = connect_database()
    cursor.execute(
        'insert into strava_ride_segment_dict '
        '(segment_id, segment_name, is_enabled, created_at, updated_at) '
        'values (%s,%s,%s,now(),now()) '
        'on duplicate key update '
        'segment_name=values(segment_name), '
        'is_enabled=values(is_enabled), '
        'updated_at=now()',
        [segment_id, segment_name, 1 if is_enabled else 0],
    )
    conn.commit()
    cursor.close()
    conn.close()
    return {'segment_id': segment_id, 'segment_name': segment_name, 'is_enabled': 1 if is_enabled else 0}


def delete_ride_segment_dict(row_id):
    conn, cursor = connect_database()
    cursor.execute(
        'delete from strava_ride_segment_dict where id=%s', [int(row_id)])
    conn.commit()
    cursor.close()
    conn.close()
    return {'deleted_id': int(row_id)}


def query_activity_list(activity_type=None, start_date=None, end_date=None, require_exercise_load=False, page_num=1, page_size=20):
    page_num = max(int(page_num or 1), 1)
    page_size = max(int(page_size or 20), 1)
    offset = (page_num - 1) * page_size
    select_sql = (
        'select activity_id, activity_type, activity_name, start_time, duration_second, distance_meter, '
        'elevation_gain, average_heartrate, average_power_watt, average_pace_second_per_km, exercise_load_score'
    )
    where_sql, params = _build_activity_filters(
        activity_type=activity_type,
        start_date=start_date,
        end_date=end_date,
        require_exercise_load=require_exercise_load,
    )
    conn, cursor = connect_database(dictionary=True)
    cursor.execute('select count(*) as total' + where_sql, params)
    total = int(cursor.fetchone()['total'])
    cursor.execute(
        select_sql + where_sql + ' order by start_time desc limit %s offset %s',
        params + [page_size, offset],
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {'list': _serialize_rows(rows), 'total': total, 'page_num': page_num, 'page_size': page_size}


def query_activity_summary(granularity='month', target_year=None):
    group_sql_map = {
        'year': 'year(start_time)',
        'quarter': 'concat(year(start_time), "-Q", quarter(start_time))',
        'month': 'date_format(start_time, "%Y-%m")',
        'week': 'concat(year(start_time), "-W", lpad(week(start_time, 1), 2, "0"))',
    }
    group_expr = group_sql_map.get(granularity, group_sql_map['month'])
    where_sql = ''
    tail_sql = f'group by {group_expr} order by min(start_time) desc'
    if granularity == 'week':
        tail_sql = f'group by {group_expr} order by max(start_time) desc limit 5'
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select '
        f'{group_expr} as period_label, '
        'count(*) as activity_count, '
        'sum(case when activity_type=%s then 1 else 0 end) as run_count, '
        'sum(case when activity_type=%s then 1 else 0 end) as ride_count, '
        'sum(case when activity_type=%s then distance_meter else 0 end) / 1000 as run_distance_km, '
        'sum(case when activity_type=%s then distance_meter else 0 end) / 1000 as ride_distance_km, '
        'sum(duration_second) as total_duration_second, '
        'sum(elevation_gain) as total_elevation_gain, '
        'sum(exercise_load_score) as total_exercise_load, '
        'avg(case when activity_type=%s then average_heartrate else null end) as run_average_heartrate, '
        'avg(case when activity_type=%s then average_heartrate else null end) as ride_average_heartrate '
        f'from strava_activities {where_sql}'
        f'{tail_sql}',
        ['Run', 'Ride', 'Run', 'Ride', 'Run', 'Ride'],
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return _serialize_rows(rows)


def rebuild_activity_daily_load_metrics(target_year=None):
    target_year, year_start, year_end = _get_year_range(target_year)
    today = min(date.today(), year_end)
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select date(start_time) as metric_date, sum(exercise_load_score) as daily_exercise_load '
        'from strava_activities '
        'where year(start_time)=%s '
        'group by date(start_time) order by metric_date asc',
        [target_year],
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    load_map = {
        row['metric_date']: _to_float(row.get('daily_exercise_load'))
        for row in rows if row.get('metric_date') is not None
    }
    metric_rows = []
    ctl_value = 0.0
    atl_value = 0.0
    ctl_alpha = 2 / (42 + 1)
    atl_alpha = 2 / (7 + 1)
    current_date = year_start
    while current_date <= today:
        daily_load = _to_float(load_map.get(current_date))
        ctl_value = ctl_value + ctl_alpha * (daily_load - ctl_value)
        atl_value = atl_value + atl_alpha * (daily_load - atl_value)
        tsb_value = ctl_value - atl_value
        metric_rows.append((
            current_date,
            round(daily_load, 2),
            round(ctl_value, 2),
            round(atl_value, 2),
            round(tsb_value, 2),
        ))
        current_date += timedelta(days=1)

    conn, cursor = connect_database()
    cursor.execute(
        'delete from activity_daily_load_metrics where metric_date >= %s and metric_date <= %s',
        [year_start, today],
    )
    if metric_rows:
        cursor.executemany(
            'insert into activity_daily_load_metrics '
            '(metric_date, daily_exercise_load, ctl_value, atl_value, tsb_value, created_at, updated_at) '
            'values (%s,%s,%s,%s,%s,now(),now())',
            metric_rows,
        )
    conn.commit()
    cursor.close()
    conn.close()
    return {'year': target_year, 'metric_count': len(metric_rows)}


def get_activity_health_metrics(target_year=None):
    target_year, year_start, year_end = _get_year_range(target_year)
    today = min(date.today(), year_end)
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select count(*) as total from activity_daily_load_metrics '
        'where metric_date >= %s and metric_date <= %s',
        [year_start, today],
    )
    total = int(cursor.fetchone()['total'])
    cursor.close()
    conn.close()
    if total == 0:
        rebuild_activity_daily_load_metrics(target_year)

    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select metric_date, daily_exercise_load, ctl_value, atl_value, tsb_value '
        'from activity_daily_load_metrics '
        'where metric_date >= %s and metric_date <= %s '
        'order by metric_date asc',
        [year_start, today],
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    serialized_rows = _serialize_rows(rows)
    latest = serialized_rows[-1] if serialized_rows else {
        'metric_date': today.strftime('%Y-%m-%d'),
        'daily_exercise_load': 0,
        'ctl_value': 0,
        'atl_value': 0,
        'tsb_value': 0,
    }
    return {
        'year': target_year,
        'today_load': latest.get('daily_exercise_load', 0),
        'ctl_value': latest.get('ctl_value', 0),
        'atl_value': latest.get('atl_value', 0),
        'tsb_value': latest.get('tsb_value', 0),
        'trend_list': serialized_rows[-42:],
    }


def query_run_segment_detail(activity_id):
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select segment_effort_id, activity_id, segment_id, segment_name, start_time, distance_meter, '
        'duration_second, average_heartrate, average_pace_second_per_km '
        'from strava_run_segments where activity_id=%s order by start_time asc',
        [int(activity_id)],
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return _serialize_rows(rows)


def query_run_segment_analysis(start_date=None, end_date=None, segment_name_keyword=None, selected_segment_name=None):
    where_sql = ' where 1=1'
    params = []
    if start_date:
        where_sql += ' and start_time >= %s'
        params.append(start_date)
    if end_date:
        where_sql += ' and start_time <= %s'
        params.append(end_date)
    if segment_name_keyword:
        where_sql += ' and segment_name like %s'
        params.append(f'%{segment_name_keyword}%')

    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select segment_name, count(*) as effort_count, max(start_time) as latest_start_time, '
        'avg(average_heartrate) as average_heartrate, avg(average_pace_second_per_km) as average_pace_second_per_km '
        'from strava_run_segments ' + where_sql +
        ' group by segment_name order by effort_count desc, latest_start_time desc',
        params,
    )
    summary_rows = cursor.fetchall()

    trend_rows = []
    if summary_rows:
        target_name = selected_segment_name or summary_rows[0]['segment_name']
        cursor.execute(
            'select segment_effort_id, activity_id, segment_name, start_time, duration_second, '
            'average_heartrate, average_pace_second_per_km '
            'from strava_run_segments ' + where_sql +
            ' and segment_name=%s order by start_time desc',
            params + [target_name],
        )
        trend_rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        'segment_summary_list': _serialize_rows(summary_rows),
        'segment_trend_list': _serialize_rows(trend_rows),
    }


def query_ride_segment_analysis(start_date=None, end_date=None, segment_name_keyword=None, selected_segment_name=None):
    where_sql = ' where 1=1'
    params = []
    if start_date:
        where_sql += ' and start_time >= %s'
        params.append(start_date)
    if end_date:
        where_sql += ' and start_time <= %s'
        params.append(end_date)
    if segment_name_keyword:
        where_sql += ' and segment_name like %s'
        params.append(f'%{segment_name_keyword}%')

    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select segment_name, count(*) as effort_count, max(start_time) as latest_start_time, '
        'avg(average_heartrate) as average_heartrate, avg(average_power_watt) as average_power_watt '
        'from strava_ride_segments ' + where_sql +
        ' group by segment_name order by effort_count desc, latest_start_time desc',
        params,
    )
    summary_rows = cursor.fetchall()

    trend_rows = []
    if summary_rows:
        target_name = selected_segment_name or summary_rows[0]['segment_name']
        cursor.execute(
            'select segment_effort_id, activity_id, segment_name, start_time, duration_second, '
            'average_heartrate, average_power_watt '
            'from strava_ride_segments ' + where_sql +
            ' and segment_name=%s order by start_time asc',
            params + [target_name],
        )
        trend_rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        'segment_summary_list': _serialize_rows(summary_rows),
        'segment_trend_list': _serialize_rows(trend_rows),
    }


def get_activity_overview(target_year=None):
    target_year = int(target_year or CURRENT_YEAR)
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select count(*) as activity_count, '
        'sum(case when activity_type=%s then 1 else 0 end) as run_count, '
        'sum(case when activity_type=%s then 1 else 0 end) as ride_count, '
        'sum(case when activity_type=%s then distance_meter else 0 end) / 1000 as run_distance_km, '
        'sum(case when activity_type=%s then distance_meter else 0 end) / 1000 as ride_distance_km, '
        'sum(duration_second) as total_duration_second, '
        'sum(elevation_gain) as total_elevation_gain, '
        'sum(exercise_load_score) as total_exercise_load '
        'from strava_activities where year(start_time)=%s',
        ['Run', 'Ride', 'Run', 'Ride', target_year],
    )
    row = cursor.fetchone() or {}
    cursor.close()
    conn.close()
    return _serialize_rows([row])[0] if row else {}


def get_activity_init_data(target_year=None):
    target_year = int(target_year or CURRENT_YEAR)
    return {
        'sync_status': get_last_sync_status(),
        'year_goal': get_activity_year_goal(target_year),
        'overview': get_activity_overview(target_year),
        'health_metrics': get_activity_health_metrics(target_year),
        'ride_segment_dict': get_ride_segment_dict(),
    }
