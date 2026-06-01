#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Blueprint, jsonify, request

from extensions import auth
from module import investment_review


bp = Blueprint('investment_review', __name__, url_prefix='')


@bp.route('/query_investment_review_plan_list', methods=['POST'])
@auth.login_required
def query_investment_review_plan_list():
    json_data = request.get_json(force=True) or {}
    res = investment_review.query_investment_review_plan_list(
        keyword=json_data.get('keyword'),
        plan_status=json_data.get('plan_status'),
    )
    return jsonify({'code': 200, 'data': res})


@bp.route('/get_investment_review_plan_detail', methods=['POST'])
@auth.login_required
def get_investment_review_plan_detail():
    json_data = request.get_json(force=True) or {}
    res = investment_review.get_investment_review_plan_detail(
        plan_id=json_data.get('plan_id'),
        plan_code=json_data.get('plan_code') or json_data.get('currentPlanId'),
    )
    return jsonify({'code': 200, 'data': res})


@bp.route('/save_investment_review_plan_bundle', methods=['POST'])
@auth.login_required
def save_investment_review_plan_bundle():
    json_data = request.get_json(force=True) or {}
    try:
        res = investment_review.save_investment_review_plan_bundle(json_data)
        return jsonify({'code': 200, 'data': res})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/save_investment_review_modification', methods=['POST'])
@auth.login_required
def save_investment_review_modification():
    json_data = request.get_json(force=True) or {}
    try:
        res = investment_review.save_investment_review_modification(
            plan_id=json_data.get('plan_id'),
            plan_code=json_data.get(
                'plan_code') or json_data.get('currentPlanId'),
            payload=json_data.get('modification') or json_data,
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/save_investment_review_execution', methods=['POST'])
@auth.login_required
def save_investment_review_execution():
    json_data = request.get_json(force=True) or {}
    try:
        res = investment_review.save_investment_review_execution(
            plan_id=json_data.get('plan_id'),
            plan_code=json_data.get(
                'plan_code') or json_data.get('currentPlanId'),
            payload=json_data.get('execution') or json_data,
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/save_investment_review_review', methods=['POST'])
@auth.login_required
def save_investment_review_review():
    json_data = request.get_json(force=True) or {}
    try:
        res = investment_review.save_investment_review_review(
            plan_id=json_data.get('plan_id'),
            plan_code=json_data.get(
                'plan_code') or json_data.get('currentPlanId'),
            payload=json_data,
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/delete_investment_review_modification', methods=['POST'])
@auth.login_required
def delete_investment_review_modification():
    json_data = request.get_json(force=True) or {}
    try:
        res = investment_review.delete_investment_review_modification(
            plan_id=json_data.get('plan_id'),
            plan_code=json_data.get(
                'plan_code') or json_data.get('currentPlanId'),
            modification_id=json_data.get(
                'modification_id') or json_data.get('id'),
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})


@bp.route('/delete_investment_review_execution', methods=['POST'])
@auth.login_required
def delete_investment_review_execution():
    json_data = request.get_json(force=True) or {}
    try:
        res = investment_review.delete_investment_review_execution(
            plan_id=json_data.get('plan_id'),
            plan_code=json_data.get(
                'plan_code') or json_data.get('currentPlanId'),
            execution_id=json_data.get('execution_id') or json_data.get('id'),
        )
        return jsonify({'code': 200, 'data': res})
    except ValueError as exc:
        return jsonify({'code': 400, 'message': str(exc)})
    except Exception as exc:
        return jsonify({'code': 500, 'message': str(exc)})
