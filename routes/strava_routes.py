#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Blueprint, jsonify, request

from extensions import auth
from module import strava_sync


bp = Blueprint('strava', __name__, url_prefix='')


@bp.route('/sync_strava_activities', methods=['POST'])
@auth.login_required
def sync_strava_activities():
    json_data = request.get_json(force=True) or {}
    sync_mode = json_data.get('mode', 'incremental')
    start_date = json_data.get('start_date')
    end_date = json_data.get('end_date')
    result = strava_sync.sync_strava_activities(
        sync_mode=sync_mode,
        start_date=start_date,
        end_date=end_date,
    )
    return jsonify({'code': 200, 'data': result})


@bp.route('/get_strava_sync_status')
@auth.login_required
def get_strava_sync_status():
    result = strava_sync.get_last_sync_status()
    return jsonify({'code': 200, 'data': result})


@bp.route('/query_strava_activities', methods=['POST'])
@auth.login_required
def query_strava_activities():
    json_data = request.get_json(force=True) or {}
    result = strava_sync.query_strava_activities(
        activity_type=json_data.get('activity_type'),
        start_date=json_data.get('start_date'),
        end_date=json_data.get('end_date'),
        limit=json_data.get('limit', 200),
    )
    return jsonify({'code': 200, 'data': result})


@bp.route('/query_strava_run_segments', methods=['POST'])
@auth.login_required
def query_strava_run_segments():
    json_data = request.get_json(force=True) or {}
    result = strava_sync.query_strava_run_segments(
        activity_id=json_data.get('activity_id'),
        limit=json_data.get('limit', 500),
    )
    return jsonify({'code': 200, 'data': result})
