#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import os
from decimal import Decimal
from datetime import datetime, time

from base.base import connect_database
from base.config import STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN
from data_collector.strava_api import fetch_strava_activities, fetch_strava_segments_for_activities


SYNC_MODES = {'incremental', 'full', 'range'}


def _parse_datetime(value, end_of_day=False):
    if not value:
        return None

    if isinstance(value, datetime):
        return value

    value = str(value).strip()
    date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']
    for fmt in date_formats:
        try:
            parsed = datetime.strptime(value, fmt)
            if fmt == '%Y-%m-%d' and end_of_day:
                return datetime.combine(parsed.date(), time(23, 59, 59))
            return parsed
        except ValueError:
            continue

    try:
        normalized = value.replace('Z', '+00:00')
        parsed = datetime.fromisoformat(normalized)
        if end_of_day and len(value) <= 10:
            return datetime.combine(parsed.date(), time(23, 59, 59))
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    except ValueError as exc:
        raise ValueError(f'无法解析时间: {value}') from exc


def _format_datetime(value):
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return parsed.strftime('%Y-%m-%d %H:%M:%S')


def _to_epoch(value, end_of_day=False):
    parsed = _parse_datetime(value, end_of_day=end_of_day)
    if parsed is None:
        return None
    return int(parsed.timestamp())


def _to_float(value, digits=2):
    if value in (None, ''):
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    if value in (None, ''):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _pace_from_speed(speed):
    if speed in (None, ''):
        return None
    try:
        speed = float(speed)
    except (TypeError, ValueError):
        return None
    if speed <= 0:
        return None
    return round(1000 / speed, 1)


def _serialize_rows(rows):
    serialized = []
    for row in rows:
        current = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                current[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, Decimal):
                current[key] = float(value)
            else:
                current[key] = value
        serialized.append(current)
    return serialized


def _chunk_list(values, chunk_size=500):
    for start in range(0, len(values), chunk_size):
        yield values[start:start + chunk_size]


def _load_strava_credentials():
    credentials = {
        'access_token': os.getenv('STRAVA_ACCESS_TOKEN'),
        'client_id': os.getenv('STRAVA_CLIENT_ID') or STRAVA_CLIENT_ID,
        'client_secret': os.getenv('STRAVA_CLIENT_SECRET') or STRAVA_CLIENT_SECRET,
        'refresh_token': os.getenv('STRAVA_REFRESH_TOKEN') or STRAVA_REFRESH_TOKEN,
    }
    if not credentials['access_token'] and not (
        credentials['client_id'] and credentials['client_secret'] and credentials['refresh_token']
    ):
        raise ValueError(
            '缺少 Strava 凭据，请配置 access_token 或 client_id/client_secret/refresh_token')
    return credentials


def _build_activity_row(activity):
    activity_type = activity.get('type')
    return {
        'activity_id': _to_int(activity.get('id')),
        'activity_type': activity_type,
        'activity_name': activity.get('name'),
        'start_time': _format_datetime(activity.get('start_date_local') or activity.get('start_date')),
        'duration_second': _to_int(activity.get('moving_time')),
        'distance_meter': _to_float(activity.get('distance')),
        'elevation_gain': _to_float(activity.get('total_elevation_gain')),
        'average_heartrate': _to_float(activity.get('average_heartrate')),
        'average_power_watt': _to_float(activity.get('average_watts')) if activity_type == 'Ride' else None,
        'average_pace_second_per_km': _pace_from_speed(activity.get('average_speed')) if activity_type == 'Run' else None,
        'exercise_load_score': _to_float(activity.get('suffer_score')),
    }


def _build_run_segment_row(segment):
    return {
        'segment_effort_id': _to_int(segment.get('segment_effort_id')),
        'activity_id': _to_int(segment.get('activity_id')),
        'segment_id': _to_int(segment.get('segment_id')),
        'segment_name': segment.get('segment_name'),
        'start_time': _format_datetime(segment.get('start_time')),
        'distance_meter': _to_float(segment.get('distance_meter')),
        'duration_second': _to_int(segment.get('duration_second')),
        'average_heartrate': _to_float(segment.get('average_heartrate')),
        'average_pace_second_per_km': _to_float(segment.get('average_pace_second_per_km'), digits=1),
    }


def _build_ride_segment_row(segment):
    return {
        'segment_effort_id': _to_int(segment.get('segment_effort_id')),
        'activity_id': _to_int(segment.get('activity_id')),
        'segment_id': _to_int(segment.get('segment_id')),
        'segment_name': segment.get('segment_name'),
        'start_time': _format_datetime(segment.get('start_time')),
        'distance_meter': _to_float(segment.get('distance_meter')),
        'duration_second': _to_int(segment.get('duration_second')),
        'average_heartrate': _to_float(segment.get('average_heartrate')),
        'average_power_watt': _to_float(segment.get('average_power_watt')),
    }


def _get_latest_activity_time():
    conn, cursor = connect_database()
    cursor.execute('select max(start_time) from strava_activities')
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if not result or not result[0]:
        return None
    return result[0]


def _get_existing_ids(table_name, id_column, values):
    if not values:
        return set()

    existing_ids = set()
    conn, cursor = connect_database()
    for chunk in _chunk_list(values):
        placeholders = ','.join(['%s'] * len(chunk))
        cursor.execute(
            f'select {id_column} from {table_name} where {id_column} in ({placeholders})',
            chunk,
        )
        existing_ids.update(row[0] for row in cursor.fetchall())
    cursor.close()
    conn.close()
    return existing_ids


def _upsert_activities(rows):
    if not rows:
        return

    sql = (
        'insert into strava_activities '
        '(activity_id, activity_type, activity_name, start_time, duration_second, distance_meter, '
        'elevation_gain, average_heartrate, average_power_watt, average_pace_second_per_km, exercise_load_score, created_at, updated_at) '
        'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now()) '
        'on duplicate key update '
        'activity_type=values(activity_type), '
        'activity_name=values(activity_name), '
        'start_time=values(start_time), '
        'duration_second=values(duration_second), '
        'distance_meter=values(distance_meter), '
        'elevation_gain=values(elevation_gain), '
        'average_heartrate=values(average_heartrate), '
        'average_power_watt=values(average_power_watt), '
        'average_pace_second_per_km=values(average_pace_second_per_km), '
        'exercise_load_score=values(exercise_load_score), '
        'updated_at=now()'
    )
    data = [
        (
            row['activity_id'],
            row['activity_type'],
            row['activity_name'],
            row['start_time'],
            row['duration_second'],
            row['distance_meter'],
            row['elevation_gain'],
            row['average_heartrate'],
            row['average_power_watt'],
            row['average_pace_second_per_km'],
            row['exercise_load_score'],
        )
        for row in rows
    ]
    conn, cursor = connect_database()
    cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()


def _upsert_run_segments(rows):
    if not rows:
        return

    sql = (
        'insert into strava_run_segments '
        '(segment_effort_id, activity_id, segment_id, segment_name, start_time, distance_meter, '
        'duration_second, average_heartrate, average_pace_second_per_km, created_at, updated_at) '
        'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now()) '
        'on duplicate key update '
        'activity_id=values(activity_id), '
        'segment_id=values(segment_id), '
        'segment_name=values(segment_name), '
        'start_time=values(start_time), '
        'distance_meter=values(distance_meter), '
        'duration_second=values(duration_second), '
        'average_heartrate=values(average_heartrate), '
        'average_pace_second_per_km=values(average_pace_second_per_km), '
        'updated_at=now()'
    )
    data = [
        (
            row['segment_effort_id'],
            row['activity_id'],
            row['segment_id'],
            row['segment_name'],
            row['start_time'],
            row['distance_meter'],
            row['duration_second'],
            row['average_heartrate'],
            row['average_pace_second_per_km'],
        )
        for row in rows
    ]
    conn, cursor = connect_database()
    cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()


def _get_enabled_ride_segment_names():
    conn, cursor = connect_database()
    cursor.execute(
        'select segment_name from strava_ride_segment_dict where is_enabled=1')
    names = {row[0] for row in cursor.fetchall() if row[0]}
    cursor.close()
    conn.close()
    return names


def _upsert_ride_segments(rows):
    if not rows:
        return

    sql = (
        'insert into strava_ride_segments '
        '(segment_effort_id, activity_id, segment_id, segment_name, start_time, distance_meter, '
        'duration_second, average_heartrate, average_power_watt, created_at, updated_at) '
        'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now()) '
        'on duplicate key update '
        'activity_id=values(activity_id), '
        'segment_id=values(segment_id), '
        'segment_name=values(segment_name), '
        'start_time=values(start_time), '
        'distance_meter=values(distance_meter), '
        'duration_second=values(duration_second), '
        'average_heartrate=values(average_heartrate), '
        'average_power_watt=values(average_power_watt), '
        'updated_at=now()'
    )
    data = [
        (
            row['segment_effort_id'],
            row['activity_id'],
            row['segment_id'],
            row['segment_name'],
            row['start_time'],
            row['distance_meter'],
            row['duration_second'],
            row['average_heartrate'],
            row['average_power_watt'],
        )
        for row in rows
    ]
    conn, cursor = connect_database()
    cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()


def _insert_sync_log(sync_mode, start_date, end_date, status, summary, error_message=None):
    conn, cursor = connect_database()
    cursor.execute(
        'insert into strava_sync_logs '
        '(sync_mode, start_date, end_date, status, summary_json, error_message, created_at) '
        'values (%s,%s,%s,%s,%s,%s,now())',
        [
            sync_mode,
            start_date,
            end_date,
            status,
            json.dumps(summary, ensure_ascii=False),
            error_message,
        ],
    )
    conn.commit()
    cursor.close()
    conn.close()


def _build_sync_window(sync_mode, start_date=None, end_date=None):
    after = None
    before = None

    if sync_mode == 'incremental':
        latest_time = _get_latest_activity_time()
        if latest_time is not None:
            after = int(latest_time.timestamp())
    elif sync_mode == 'range':
        if not start_date or not end_date:
            raise ValueError('周期同步必须提供 start_date 和 end_date')
        after = _to_epoch(start_date)
        before = _to_epoch(end_date, end_of_day=True)
        if after is None or before is None:
            raise ValueError('周期同步时间格式无效')
        if after > before:
            raise ValueError('start_date 不能晚于 end_date')

    return after, before


def sync_strava_activities(sync_mode='incremental', start_date=None, end_date=None):
    if sync_mode not in SYNC_MODES:
        raise ValueError(f'不支持的同步模式: {sync_mode}')

    summary = {
        'sync_mode': sync_mode,
        'start_date': start_date,
        'end_date': end_date,
        'fetched_activity_count': 0,
        'saved_activity_count': 0,
        'saved_run_count': 0,
        'saved_ride_count': 0,
        'saved_run_segment_count': 0,
        'saved_ride_segment_count': 0,
        'skipped_existing_count': 0,
    }

    try:
        credentials = _load_strava_credentials()
        after, before = _build_sync_window(sync_mode, start_date, end_date)
        activities = fetch_strava_activities(
            access_token=credentials['access_token'],
            client_id=credentials['client_id'],
            client_secret=credentials['client_secret'],
            refresh_token=credentials['refresh_token'],
            after=after,
            before=before,
            activity_types={'Run', 'Ride'},
        )
        summary['fetched_activity_count'] = len(activities)

        activities_to_save = activities
        if sync_mode == 'incremental' and activities:
            existing_ids = _get_existing_ids(
                table_name='strava_activities',
                id_column='activity_id',
                values=[activity.get('id') for activity in activities if activity.get(
                    'id') is not None],
            )
            activities_to_save = [
                activity for activity in activities if activity.get('id') not in existing_ids]
            summary['skipped_existing_count'] = len(
                activities) - len(activities_to_save)

        activity_rows = [_build_activity_row(
            activity) for activity in activities_to_save]
        activity_rows = [
            row for row in activity_rows if row['activity_id'] is not None]
        _upsert_activities(activity_rows)

        run_activities = [
            activity for activity in activities_to_save if activity.get('type') == 'Run']
        ride_activities = [
            activity for activity in activities_to_save if activity.get('type') == 'Ride']
        run_segment_rows = []
        ride_segment_rows = []
        segment_activities = run_activities + ride_activities
        if segment_activities:
            segment_rows = fetch_strava_segments_for_activities(
                segment_activities,
                access_token=credentials['access_token'],
                client_id=credentials['client_id'],
                client_secret=credentials['client_secret'],
                refresh_token=credentials['refresh_token'],
            )
            run_segment_rows = [_build_run_segment_row(
                segment) for segment in segment_rows if segment.get('activity_type') == 'Run']
            run_segment_rows = [
                row for row in run_segment_rows if row['segment_effort_id'] is not None]
            _upsert_run_segments(run_segment_rows)

            enabled_ride_segment_names = _get_enabled_ride_segment_names()
            ride_segment_rows = [_build_ride_segment_row(
                segment) for segment in segment_rows
                if segment.get('activity_type') == 'Ride' and segment.get('segment_name') in enabled_ride_segment_names]
            ride_segment_rows = [
                row for row in ride_segment_rows if row['segment_effort_id'] is not None]
            _upsert_ride_segments(ride_segment_rows)

        summary['saved_activity_count'] = len(activity_rows)
        summary['saved_run_count'] = len(
            [row for row in activity_rows if row['activity_type'] == 'Run'])
        summary['saved_ride_count'] = len(
            [row for row in activity_rows if row['activity_type'] == 'Ride'])
        summary['saved_run_segment_count'] = len(run_segment_rows)
        summary['saved_ride_segment_count'] = len(ride_segment_rows)
        try:
            from module import activity as activity_module
            activity_module.rebuild_activity_daily_load_metrics()
        except Exception:
            pass
        _insert_sync_log(sync_mode, start_date, end_date, 'success', summary)
        return summary
    except Exception as exc:
        _insert_sync_log(sync_mode, start_date, end_date,
                         'failed', summary, error_message=str(exc))
        raise


def get_last_sync_status():
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        'select id, sync_mode, start_date, end_date, status, summary_json, error_message, created_at '
        'from strava_sync_logs order by id desc limit 1'
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        return None
    if row.get('summary_json'):
        row['summary'] = json.loads(row['summary_json'])
    else:
        row['summary'] = None
    row.pop('summary_json', None)
    return _serialize_rows([row])[0]


def query_strava_activities(activity_type=None, start_date=None, end_date=None, limit=200):
    conn, cursor = connect_database(dictionary=True)
    sql = (
        'select activity_id, activity_type, activity_name, start_time, duration_second, distance_meter, '
        'elevation_gain, average_heartrate, average_power_watt, average_pace_second_per_km, exercise_load_score '
        'from strava_activities where 1=1'
    )
    params = []
    if activity_type:
        sql += ' and activity_type=%s'
        params.append(activity_type)
    if start_date:
        sql += ' and start_time >= %s'
        params.append(_format_datetime(start_date) or start_date)
    if end_date:
        sql += ' and start_time <= %s'
        params.append(_format_datetime(end_date) or end_date)
    sql += ' order by start_time desc limit %s'
    params.append(int(limit))
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return _serialize_rows(rows)


def query_strava_run_segments(activity_id=None, limit=500):
    conn, cursor = connect_database(dictionary=True)
    sql = (
        'select segment_effort_id, activity_id, segment_id, segment_name, start_time, distance_meter, '
        'duration_second, average_heartrate, average_pace_second_per_km '
        'from strava_run_segments where 1=1'
    )
    params = []
    if activity_id:
        sql += ' and activity_id=%s'
        params.append(int(activity_id))
    sql += ' order by start_time desc limit %s'
    params.append(int(limit))
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return _serialize_rows(rows)
