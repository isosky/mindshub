#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Blueprint, jsonify, request

from extensions import auth
from module import activity


bp = Blueprint('activity', __name__, url_prefix='')


@bp.route('/get_activity_init_data')
@auth.login_required
def get_activity_init_data():
    target_year = request.args.get('year')
    res = activity.get_activity_init_data(target_year)
    return jsonify({'code': 200, 'data': res})


@bp.route('/query_activity_list', methods=['POST'])
@auth.login_required
def query_activity_list():
    json_data = request.get_json(force=True) or {}
    res = activity.query_activity_list(
        activity_type=json_data.get('activity_type'),
        start_date=json_data.get('start_date'),
        end_date=json_data.get('end_date'),
        require_exercise_load=json_data.get('require_exercise_load', False),
        page_num=json_data.get('page_num', 1),
        page_size=json_data.get('page_size', 20),
    )
    return jsonify({'code': 200, 'data': res})


@bp.route('/query_activity_summary', methods=['POST'])
@auth.login_required
def query_activity_summary():
    json_data = request.get_json(force=True) or {}
    res = activity.query_activity_summary(
        granularity=json_data.get('granularity', 'month'),
        target_year=json_data.get('year'),
    )
    return jsonify({'code': 200, 'data': res})


@bp.route('/get_activity_goal')
@auth.login_required
def get_activity_goal():
    target_year = request.args.get('year')
    res = activity.get_activity_year_goal(target_year)
    return jsonify({'code': 200, 'data': res})


@bp.route('/save_activity_goal', methods=['POST'])
@auth.login_required
def save_activity_goal():
    json_data = request.get_json(force=True) or {}
    res = activity.save_activity_goal(
        ride_distance_goal_km=json_data.get('ride_distance_goal_km', 0),
        run_distance_goal_km=json_data.get('run_distance_goal_km', 0),
        target_year=json_data.get('year'),
    )
    return jsonify({'code': 200, 'data': res})


@bp.route('/get_activity_health_metrics')
@auth.login_required
def get_activity_health_metrics():
    target_year = request.args.get('year')
    res = activity.get_activity_health_metrics(target_year)
    return jsonify({'code': 200, 'data': res})


@bp.route('/compute_health_metrics', methods=['POST'])
@auth.login_required
def compute_health_metrics():
    json_data = request.get_json(force=True) or {}
    target_year = json_data.get('year')
    # rebuild metrics for the year (will rebuild only missing range)
    res_rebuild = activity.rebuild_activity_daily_load_metrics(target_year)
    # return latest metrics
    res = activity.get_activity_health_metrics(target_year)
    return jsonify({'code': 200, 'data': res})


@bp.route('/query_run_segment_detail', methods=['POST'])
@auth.login_required
def query_run_segment_detail():
    json_data = request.get_json(force=True) or {}
    res = activity.query_run_segment_detail(json_data.get('activity_id'))
    return jsonify({'code': 200, 'data': res})


@bp.route('/query_run_segment_analysis', methods=['POST'])
@auth.login_required
def query_run_segment_analysis():
    json_data = request.get_json(force=True) or {}
    res = activity.query_run_segment_analysis(
        start_date=json_data.get('start_date'),
        end_date=json_data.get('end_date'),
        segment_name_keyword=json_data.get('segment_name_keyword'),
        selected_segment_name=json_data.get('selected_segment_name'),
    )
    return jsonify({'code': 200, 'data': res})


@bp.route('/query_ride_segment_analysis', methods=['POST'])
@auth.login_required
def query_ride_segment_analysis():
    json_data = request.get_json(force=True) or {}
    res = activity.query_ride_segment_analysis(
        start_date=json_data.get('start_date'),
        end_date=json_data.get('end_date'),
        segment_name_keyword=json_data.get('segment_name_keyword'),
        selected_segment_name=json_data.get('selected_segment_name'),
    )
    return jsonify({'code': 200, 'data': res})


@bp.route('/get_ride_segment_dict')
@auth.login_required
def get_ride_segment_dict():
    res = activity.get_ride_segment_dict()
    return jsonify({'code': 200, 'data': res})


@bp.route('/save_ride_segment_dict', methods=['POST'])
@auth.login_required
def save_ride_segment_dict():
    json_data = request.get_json(force=True) or {}
    res = activity.save_ride_segment_dict(
        segment_name=json_data.get('segment_name'),
        is_enabled=json_data.get('is_enabled', 1),
    )
    return jsonify({'code': 200, 'data': res})


@bp.route('/delete_ride_segment_dict', methods=['POST'])
@auth.login_required
def delete_ride_segment_dict():
    json_data = request.get_json(force=True) or {}
    res = activity.delete_ride_segment_dict(json_data.get('id'))
    return jsonify({'code': 200, 'data': res})
