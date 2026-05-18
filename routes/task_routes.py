#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request
from extensions import auth
from module import task

bp = Blueprint('task', __name__, url_prefix='')


@bp.route('/addtask', methods=['POST'])
@auth.login_required
def add_task():
    json_data = request.get_json(force=True)
    level1 = json_data['level1']
    level2 = json_data['level2']
    level3 = json_data['level3']
    arg_task_name = json_data['task_name']
    arg_edate = json_data['edate']
    arg_person = json_data['person']
    temp = task.add_task(level1, level2, level3,
                         arg_task_name, arg_edate, arg_person)
    return jsonify({'arrays': temp})


@bp.route('/initoption')
@auth.login_required
def initoption():
    temp = task.init_option()
    return jsonify({'task_sub_all_option': temp[0], 'task_level1_option': temp[1], 'level2_level3': temp[2], 'lastchecktime': temp[3], 'dir_sub_all_option': temp[4], 'dir_select_option': temp[5]})


@bp.route('/gettasknow')
@auth.login_required
def get_task_now():
    return jsonify({'arrays': task.get_task_now()})


@bp.route('/finishtask', methods=['POST'])
@auth.login_required
def finishtask():
    json_data = request.get_json(force=True)
    task_id = json_data['task_id']
    finishtaskform = json_data['finishtaskform']
    temp = task.finish_task(task_id, finishtaskform)
    return jsonify({'result': True, 'msg': temp})


@bp.route('/deletetask', methods=['POST'])
@auth.login_required
def deletetask():
    json_data = request.get_json(force=True)
    task_id = json_data['task_id']
    task.delete_task_by_task_id(task_id)
    return jsonify({'result': True})


@bp.route('/querytask', methods=['POST'])
@auth.login_required
def querytask():
    json_data = request.get_json(force=True)
    query = json_data['query']
    level1 = json_data['level1']
    level2 = json_data['level2']
    level3 = json_data['level3']
    ftime = json_data['ftime']
    query_duration = json_data['query_duration']
    isstime = json_data['isstime']
    isqueryall = json_data['isqueryall']
    mode = json_data['mode']
    return jsonify({'arrays': task.query_task(query, level1, level2, level3, ftime, query_duration, isstime, isqueryall, mode)})


@bp.route('/querytask_week')
@auth.login_required
def querytask_week():
    return jsonify({'arrays': task.get_task_this_week()})


@bp.route('/gettreetask', methods=['POST'])
@auth.login_required
def gettreetask():
    json_data = request.get_json(force=True)
    type = json_data['type']
    main = json_data['main']
    sub = json_data['sub']
    temp = task.get_task_by_type(type, main, sub)
    return jsonify({'datas': temp})


@bp.route('/updatetask', methods=['POST'])
@auth.login_required
def updatetask():
    json_data = request.get_json(force=True)
    task_id = json_data['task_id']
    level1 = json_data['level1']
    level2 = json_data['level2']
    level3 = json_data['level3']
    task_name = json_data['task_name']
    etime = json_data['etime']
    status = json_data['dustatus']
    dftime = json_data['dftime']
    task.update_task(task_id, level1, level2, level3,
                     task_name, etime, status, dftime)
    return jsonify({'result': True})


@bp.route('/removetask')
@auth.login_required
def removetask():
    return jsonify({'message': '已从任务中移除' + str(task.removetask()) + '条删除的数据'})


@bp.route('/gettasksummary_bar')
@auth.login_required
def gettasksummary_bar():
    return jsonify(task.get_bar_data_from_task())
