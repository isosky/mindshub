#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from extensions import auth
from module import cycling

bp = Blueprint('cycling', __name__, url_prefix='')


@bp.route('/get_cycling_strava_chart_data')
@auth.login_required
def get_cycling_strava_chart_data():
    dates, distances = cycling.get_strava_summary()
    return jsonify({'dates': dates, 'distances': distances})


@bp.route('/getcycling', methods=['POST'])
@auth.login_required
def getcycling():
    json_data = request.get_json(force=True)
    cycling_type_selected = json_data['cycling_type_selected']
    temp = cycling.get_cycling(cycling_type_selected)
    _temp = ['tabledata', 'yaxis', 'avg_hr', 'max_hr',
             'avg_cadence', 'intensity', 'efficiency', 'adr']
    return jsonify(dict(zip(_temp, temp)))


@bp.route('/get_cycling_name')
@auth.login_required
def get_cycling_name():
    temp = cycling.get_cycling_name()
    return jsonify({'data': temp})
