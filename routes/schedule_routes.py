#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from extensions import auth
from module import schedule, project

bp = Blueprint('schedule', __name__, url_prefix='')


@bp.route('/initschedule')
def initschedule():
    project.cal_project_graph()
    return jsonify(schedule.run_schedule())


@bp.route('/addschedule', methods=['POST'])
@auth.login_required
def addschedule():
    json_data = request.get_json(force=True)
    level1 = json_data['level1']
    level2 = json_data['level2']
    level3 = json_data['level3']
    schedule_type = json_data['schedule_type']
    schedule_frequence = json_data['schedule_frequence']
    task_name = json_data['task_name']
    temp = schedule.add_schedule(
        level1, level2, level3, schedule_type, schedule_frequence, task_name)
    return jsonify({'result': temp})


@bp.route('/getscheduledata')
@auth.login_required
def getscheduledata():
    return jsonify({'data': schedule.get_schedule()})


@bp.route('/getscheduletaskdata', methods=['POST'])
@auth.login_required
def getscheduletaskdata():
    json_data = request.get_json(force=True)
    schedule_id = json_data.get('schedule_id')
    return jsonify({'data': schedule.get_task_by_schedule_id(schedule_id)})


@bp.route('/deleteschedule', methods=['POST'])
@auth.login_required
def deleteschedule():
    json_data = request.get_json(force=True)
    schedule_id = json_data.get('schedule_id')
    return jsonify({'status': schedule.delete_schedule(schedule_id)})
