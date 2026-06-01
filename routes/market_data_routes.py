#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Blueprint, jsonify, request

from extensions import auth
from module import market_data


bp = Blueprint('market_data', __name__, url_prefix='')


@bp.route('/market_data/trigger_sync', methods=['POST'])
@auth.login_required
def trigger_sync():
    json_data = request.get_json(force=True) or {}
    try:
        res = market_data.trigger_market_sync(json_data)
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/market_data/sync_jobs/list', methods=['POST'])
@auth.login_required
def sync_jobs_list():
    json_data = request.get_json(force=True) or {}
    try:
        res = market_data.list_sync_job_logs(
            limit=json_data.get('limit', 20),
            run_mode=json_data.get('run_mode'),
            status=json_data.get('status'),
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/market_data/kline_indicators', methods=['POST'])
@auth.login_required
def kline_indicators():
    json_data = request.get_json(force=True) or {}
    try:
        res = market_data.query_kline_indicators(
            symbol_code=json_data.get('symbol_code'),
            start_date=json_data.get('start_date'),
            end_date=json_data.get('end_date'),
            limit=json_data.get('limit', 1200),
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/market_data/watchlist/list', methods=['POST'])
@auth.login_required
def watchlist_list():
    json_data = request.get_json(force=True) or {}
    try:
        enabled = json_data.get('enabled')
        if enabled not in (None, '', 0, 1, '0', '1'):
            return jsonify({'code': 400, 'message': 'enabled 仅支持 0/1'})
        enabled_int = None
        if enabled in (0, 1, '0', '1'):
            enabled_int = int(enabled)
        res = market_data.list_watchlist(
            enabled=enabled_int,
            symbol_type=json_data.get('symbol_type'),
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/market_data/watchlist/add', methods=['POST'])
@auth.login_required
def watchlist_add():
    json_data = request.get_json(force=True) or {}
    try:
        res = market_data.add_watchlist(
            symbol_code=json_data.get('symbol_code'),
            symbol_type=json_data.get('symbol_type'),
            symbol_name=json_data.get('symbol_name'),
            remark=json_data.get('remark'),
            created_by=json_data.get('created_by'),
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/market_data/watchlist/remove', methods=['POST'])
@auth.login_required
def watchlist_remove():
    json_data = request.get_json(force=True) or {}
    try:
        res = market_data.remove_watchlist(
            symbol_code=json_data.get('symbol_code'),
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/market_data/watchlist/toggle', methods=['POST'])
@auth.login_required
def watchlist_toggle():
    json_data = request.get_json(force=True) or {}
    try:
        enabled = json_data.get('enabled')
        if enabled in (None, ''):
            raise ValueError('enabled 不能为空')
        res = market_data.toggle_watchlist(
            symbol_code=json_data.get('symbol_code'),
            enabled=enabled,
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})
