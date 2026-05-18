#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from extensions import auth
from module import transaction

bp = Blueprint('transaction', __name__, url_prefix='')


@bp.route('/get_transaction', methods=['POST'])
@auth.login_required
def get_transaction():
    json_data = request.get_json(force=True)
    query_all = json_data['query_all']
    res = transaction.get_transaction(query_all)
    return jsonify(res)


@bp.route('/get_transaction_option')
@auth.login_required
def get_transaction_option():
    res = transaction.get_transaction_option()
    return jsonify(res)


@bp.route('/update_transaction', methods=['POST'])
@auth.login_required
def update_transaction():
    json_data = request.get_json(force=True)
    transaction_id = json_data['d_transaction_id']
    level1 = json_data['level1']
    level2 = json_data['level2']
    level3 = json_data['level3']
    d_data_status = json_data['d_data_status']
    merge_data = json_data['merge_data']
    counterparty = json_data['d_counterparty']
    product = json_data['d_product']
    res = transaction.update_transaction(
        transaction_id, level1, level2, level3, d_data_status, merge_data, counterparty)
    return jsonify({"msg": 'ok'})
