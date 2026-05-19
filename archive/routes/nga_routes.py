#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from extensions import auth
from module import nga_post, nga_setting

bp = Blueprint('nga', __name__, url_prefix='')


@bp.route('/getposttabledata')
@auth.login_required
def getposttabledata():
    temp = nga_post.get_nga_post_data()
    return jsonify(temp)


@bp.route('/getreplytabledata', methods=['POST'])
@auth.login_required
def getreplytabledata():
    json_data = request.get_json(force=True)
    tid = json_data['tid']
    temp = nga_post.get_nga_reply_by_tid(tid)
    return jsonify(temp)


# nga_setting endpoints migrated from app.py


@bp.route("/get_nga_specia_post")
@auth.login_required
def get_nga_specia_post():
    temp_data = nga_setting.get_nga_specia_post()
    return jsonify({"data": temp_data})


@bp.route("/add_nga_special_post", methods=['POST'])
@auth.login_required
def add_nga_special_post():
    json_data = request.get_json(force=True)
    tid = json_data['new_nga_special_post_id']
    temp_data = nga_setting.add_nga_special_post(tid)
    return jsonify({"data": temp_data})


@bp.route("/delete_nga_special_post", methods=['POST'])
@auth.login_required
def delete_nga_special_post():
    json_data = request.get_json(force=True)
    tid = json_data['delete_nga_special_post_id']
    temp_data = nga_setting.delete_nga_special_post(tid)
    return jsonify({"data": temp_data})


@bp.route("/get_nga_specia_user")
@auth.login_required
def get_nga_specia_user():
    temp_data = nga_setting.get_nga_specia_user()
    return jsonify({"data": temp_data})


@bp.route("/add_nga_special_user", methods=['POST'])
@auth.login_required
def add_nga_special_user():
    json_data = request.get_json(force=True)
    nga_user_id = json_data['new_nga_special_user_id']
    temp_data = nga_setting.add_nga_special_user(nga_user_id)
    return jsonify({"data": temp_data})


@bp.route("/delete_nga_special_user", methods=['POST'])
@auth.login_required
def delete_nga_special_user():
    json_data = request.get_json(force=True)
    nga_user_id = json_data['delete_nga_special_user_id']
    temp_data = nga_setting.delete_nga_special_user(nga_user_id)
    return jsonify({"data": temp_data})
