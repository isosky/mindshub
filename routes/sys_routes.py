#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, render_template, request
from flask_cors import cross_origin
from extensions import auth, g_tokens
from base import base
from module import task, travel, nga_setting
import json

bp = Blueprint('sys', __name__)


@bp.route('/')
@auth.login_required
def mainroute():
    return render_template('index.html')


@bp.route('/login', methods=['POST'])
def login():
    json_data = request.get_json(force=True)
    user_name = json_data.get('user_name')
    user_pass = json_data.get('user_pass')
    if base.login(user_name, user_pass):
        g_tokens['secret-token-1 ' + user_name] = user_name
        return jsonify({"code": 200, 'token': 'Token secret-token-1 ' + user_name})
    else:
        return jsonify({"code": 404})


# additional sys routes migrated from app.py


@bp.route('/setiswork', methods=['POST'])
@auth.login_required
def setiswork():
    json_data = request.get_json(force=True)
    iswork = json_data['iswork']
    base.setiswork(iswork)
    return jsonify({'result': True})


@bp.route('/getiswork')
@auth.login_required
def getiswork():
    res = base.get_sys_params(2)
    return jsonify({'iswork': res})


@bp.route('/getfirstpage')
@auth.login_required
def getfirstpage():
    res = base.get_homepage()
    return jsonify(res)


@bp.route('/setfirstpage', methods=['POST'])
@auth.login_required
def setfirstpage():
    json_data = request.get_json(force=True)
    firstpage = json_data['firstpage']
    base.setfirstpage(firstpage)
    return jsonify({'result': True})


@bp.route('/gettype')
@auth.login_required
def gettype():
    res = base.get_task_type()
    return jsonify(res)


@bp.route('/getnodirdata')
@auth.login_required
def getnodirdata():
    res = base.getnodirdata()
    return jsonify(res)


@bp.route('/getsubtype')
@auth.login_required
def getsubtype():
    res = task.get_task_type_option()
    return jsonify(res)


@bp.route('/updatesubtupe', methods=['POST'])
@auth.login_required
def updatesubtupe():
    json_data = request.get_json(force=True)
    typenow = json_data['typenow']
    old_sub_type = json_data['old_sub_type']
    new_sub_type = json_data['new_sub_type']
    base.update_sub_type(typenow, old_sub_type, new_sub_type)
    return jsonify({'result': True})


@bp.route('/updatedir', methods=['POST'])
@auth.login_required
def updatedir():
    json_data = request.get_json(force=True)
    sub_dir = json_data['sub_dir']
    new_dir_type = json_data['new_dir_type']
    base.updatedir(sub_dir, new_dir_type)
    return jsonify({'result': True})


@bp.route('/addtype', methods=['POST'])
@auth.login_required
def addtype():
    json_data = request.get_json(force=True)
    typename = json_data['typename']
    typevalue = json_data['typevalue']
    base.add_base_type(typename, typevalue)
    return jsonify({'result': True})


@bp.route('/deletetype', methods=['POST'])
@auth.login_required
def deletetype():
    json_data = request.get_json(force=True)
    typeid = json_data['typeid']
    base.delete_base_type(typeid)
    return jsonify({'result': True})


@bp.route('/getcitydata')
@auth.login_required
def getcitydata():
    res = travel.get_city()
    return jsonify(res)


@bp.route('/addcity', methods=['POST'])
@auth.login_required
def addcity():
    json_data = request.get_json(force=True)
    city_name = json_data['city_name']
    city_lon = json_data['city_lon']
    city_lat = json_data['city_lat']
    res = travel.add_city(city_name, city_lon, city_lat)
    return jsonify(res)


@bp.route('/querycity', methods=['POST'])
@auth.login_required
def querycity():
    json_data = request.get_json(force=True)
    city_name = json_data['city_name']
    city_lon = json_data['city_lon']
    city_lat = json_data['city_lat']
    res = travel.query_city(city_name, city_lon, city_lat)
    return jsonify(res)
