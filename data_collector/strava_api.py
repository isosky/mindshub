from datetime import datetime

import requests


REQUEST_TIMEOUT = 30
DEFAULT_ACTIVITY_TYPES = {"Run", "Ride"}


def _parse_datetime(value):
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _slice_heartrate_by_index(stream_hr, start_index, end_index):
    if not stream_hr:
        return []
    if start_index is None or end_index is None:
        return []
    try:
        start_index = int(start_index)
        end_index = int(end_index)
    except (TypeError, ValueError):
        return []
    if start_index < 0 or end_index < start_index:
        return []
    return [value for value in stream_hr[start_index:end_index + 1] if value is not None]


def _slice_heartrate_by_time(detail, effort, stream_time, stream_hr):
    if not stream_time or not stream_hr:
        return []

    activity_start = _parse_datetime(detail.get("start_date"))
    effort_start = _parse_datetime(effort.get("start_date"))
    elapsed_time = effort.get("elapsed_time")

    if activity_start is None or effort_start is None or elapsed_time in (None, ""):
        return []

    try:
        elapsed_time = int(elapsed_time)
    except (TypeError, ValueError):
        return []

    start_offset = int(max(0, (effort_start - activity_start).total_seconds()))
    end_offset = start_offset + elapsed_time
    values = []
    for time_value, heartrate_value in zip(stream_time, stream_hr):
        if time_value is None or heartrate_value is None:
            continue
        if start_offset <= time_value <= end_offset:
            values.append(heartrate_value)
    return values


def _compute_effort_heart_rate(detail, effort, stream_time, stream_hr):
    effort_hr = _slice_heartrate_by_index(
        stream_hr,
        effort.get("start_index"),
        effort.get("end_index"),
    )
    if not effort_hr:
        effort_hr = _slice_heartrate_by_time(
            detail, effort, stream_time, stream_hr)
    if not effort_hr:
        return None, None
    avg_hr = round(sum(effort_hr) / len(effort_hr), 1)
    max_hr = float(max(effort_hr))
    return avg_hr, max_hr


def _request_json(url, headers=None, params=None, method="GET", data=None):
    response = requests.request(
        method=method,
        url=url,
        headers=headers,
        params=params,
        data=data,
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Strava 请求失败: {response.status_code} {response.text}")
    return response.json()


def get_access_token(client_id, client_secret, refresh_token):
    """
    用 refresh_token 换取新的 access_token。
    """
    url = "https://www.strava.com/oauth/token"
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }
    resp = requests.post(url, data=data, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(
            f"刷新 access_token 失败: {resp.status_code} {resp.text}")
    return resp.json()['access_token']


def resolve_access_token(access_token=None, client_id=None, client_secret=None, refresh_token=None):
    if access_token:
        return access_token
    if not (client_id and client_secret and refresh_token):
        raise ValueError(
            "必须提供 access_token 或 client_id/client_secret/refresh_token")
    return get_access_token(client_id, client_secret, refresh_token)


def fetch_strava_activities(access_token=None, client_id=None, client_secret=None, refresh_token=None, after=None, before=None, activity_types=None):
    """拉取 Strava 活动列表，返回按类型过滤后的活动字典列表。"""
    access_token = resolve_access_token(
        access_token=access_token,
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": 200, "page": 1}
    valid_types = set(activity_types or DEFAULT_ACTIVITY_TYPES)
    if after is not None:
        params["after"] = after
    if before is not None:
        params["before"] = before
    activities = []
    while True:
        batch = _request_json(url, headers=headers, params=params)
        if not batch:
            break
        filtered_batch = [
            activity for activity in batch if activity.get("type") in valid_types]
        activities.extend(filtered_batch)
        params["page"] += 1
    return activities


def fetch_activity_detail(activity_id, access_token=None, client_id=None, client_secret=None, refresh_token=None, include_all_efforts=True):
    access_token = resolve_access_token(
        access_token=access_token,
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )

    headers = {"Authorization": f"Bearer {access_token}"}
    detail_url = f"https://www.strava.com/api/v3/activities/{int(activity_id)}"
    params = {"include_all_efforts": "true" if include_all_efforts else "false"}
    return _request_json(detail_url, headers=headers, params=params)


def fetch_activity_streams(activity_id, access_token=None, client_id=None, client_secret=None, refresh_token=None, keys=None):
    access_token = resolve_access_token(
        access_token=access_token,
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    headers = {"Authorization": f"Bearer {access_token}"}
    stream_url = f"https://www.strava.com/api/v3/activities/{int(activity_id)}/streams"
    params = {
        "keys": ",".join(keys or ["time", "heartrate"]),
        "key_by_type": "true",
    }
    return _request_json(stream_url, headers=headers, params=params)


def build_activity_segment_records(activity, detail, stream_data):
    rows = []
    stream_time = (stream_data.get("time") or {}).get("data") or []
    stream_hr = (stream_data.get("heartrate") or {}).get("data") or []
    segment_efforts = detail.get("segment_efforts") or []
    activity_type = activity.get("type")

    for effort in segment_efforts:
        segment = effort.get("segment") or {}
        distance = effort.get("distance")
        elapsed_time = effort.get("elapsed_time")
        average_pace_seconds = None
        if distance and elapsed_time and distance > 0:
            average_pace_seconds = round(
                (float(elapsed_time) / float(distance)) * 1000, 1)

        avg_hr = effort.get("average_heartrate")
        max_hr = effort.get("max_heartrate")
        if avg_hr is None or max_hr is None:
            computed_avg_hr, computed_max_hr = _compute_effort_heart_rate(
                detail=detail,
                effort=effort,
                stream_time=stream_time,
                stream_hr=stream_hr,
            )
            if avg_hr is None:
                avg_hr = computed_avg_hr
            if max_hr is None:
                max_hr = computed_max_hr

        if avg_hr is None:
            avg_hr = detail.get("average_heartrate") or activity.get(
                "average_heartrate")
        if max_hr is None:
            max_hr = detail.get(
                "max_heartrate") or activity.get("max_heartrate")

        rows.append({
            "activity_type": activity_type,
            "segment_effort_id": effort.get("id"),
            "activity_id": activity.get("id"),
            "segment_id": segment.get("id"),
            "segment_name": segment.get("name") or effort.get("name"),
            "start_time": effort.get("start_date_local") or effort.get("start_date"),
            "distance_meter": distance,
            "duration_second": elapsed_time,
            "average_heartrate": avg_hr,
            "average_power_watt": effort.get("average_watts"),
            "max_heartrate": max_hr,
            "average_pace_second_per_km": average_pace_seconds,
        })
    return rows


def fetch_strava_segments_for_activities(activity_records, access_token=None, client_id=None, client_secret=None, refresh_token=None):
    """按活动详情拉取活动路段数据，返回 segment_effort 字典列表。"""
    if not activity_records:
        return []

    rows = []
    for activity in activity_records:
        activity_id = activity.get("id")
        if activity_id in (None, ""):
            continue
        detail = fetch_activity_detail(
            activity_id=activity_id,
            access_token=access_token,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            include_all_efforts=True,
        )
        # Only request streams if some segment_effort is missing heartrate
        segment_efforts = detail.get("segment_efforts") or []
        need_stream = False
        for effort in segment_efforts:
            if effort.get("average_heartrate") is None or effort.get("max_heartrate") is None:
                need_stream = True
                break

        stream_data = {}
        if need_stream:
            try:
                stream_data = fetch_activity_streams(
                    activity_id=activity_id,
                    access_token=access_token,
                    client_id=client_id,
                    client_secret=client_secret,
                    refresh_token=refresh_token,
                    keys=["time", "heartrate"],
                )
            except Exception:
                # If streams fail (rate limit etc.), proceed with detail-only data
                stream_data = {}

        rows.extend(build_activity_segment_records(
            activity, detail, stream_data))
    return rows
