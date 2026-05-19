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


@bp.route('/getprogressdata', methods=['POST'])
@auth.login_required
def getprogressdata():
    return jsonify(task.calculate_process_hours())


@bp.route('/getcalendardata', methods=['POST'])
@auth.login_required
def getcalendardata():
    return jsonify(task.get_task_by_calendar())


@bp.route('/gettreemapdata', methods=['POST'])
@auth.login_required
def gettreemapdata():
    return jsonify(task.get_treemap_data_from_task())


@bp.route('/getsankeydata', methods=['POST'])
@auth.login_required
def getsankeydata():
    return jsonify(task.get_sankey_data_from_task())


@bp.route('/gettimedata')
@auth.login_required
def gettimedata():
    return jsonify(task.get_calendar_data_from_task())


@bp.route('/addprocess', methods=['POST'])
@auth.login_required
def addprocess():
    json_data = request.get_json(force=True)
    task_id = json_data['task_id']
    process_name = json_data['process_name']
    res = task.add_task_process(task_id, process_name)
    return jsonify({'result': res})


@bp.route('/deleteprocess', methods=['POST'])
@auth.login_required
def deleteprocess():
    json_data = request.get_json(force=True)
    process_id = json_data['process_id']
    temp = task.delete_process(process_id)
    return jsonify({'result': temp})


@bp.route('/getprocess', methods=['POST'])
@auth.login_required
def getprocess():
    json_data = request.get_json(force=True)
    task_id = json_data['task_id']
    temp = task.get_process_by_task_id(task_id)
    temp_s = task.get_process_count_by_task_id(task_id)
    return jsonify({'arrays': temp, 'status': temp_s})


@bp.route('/resetprocess', methods=['POST'])
@auth.login_required
def resetprocess():
    json_data = request.get_json(force=True)
    process_id = json_data['process_id']
    return jsonify({'status': task.reset_process_by_id(process_id)})


@bp.route('/finishprocess', methods=['POST'])
@auth.login_required
def finishprocess():
    json_data = request.get_json(force=True)
    process_id = json_data['process_id']
    return jsonify({'status': task.finish_process_by_id(process_id)})


@bp.route('/updateprocess', methods=['POST'])
@auth.login_required
def updateprocess():
    json_data = request.get_json(force=True)
    process_id = json_data['process_id']
    process_name = json_data['process_name']
    return jsonify({'status': task.update_process(process_id, process_name)})


@bp.route('/appendtaskperson', methods=['POST'])
@auth.login_required
def appendtaskperson():
    json_data = request.get_json(force=True)
    task_id = json_data['task_id']
    person_id = json_data['person_id']
    res = task.add_task_person(task_id, person_id)
    return jsonify(res)


@bp.route('/deletetaskperson', methods=['POST'])
@auth.login_required
def deletetaskperson():
    json_data = request.get_json(force=True)
    task_id = json_data['task_id']
    person_id = json_data['person_id']
    res = task.delete_person_by_task_id(task_id, person_id)
    return jsonify(res)


@bp.route('/getfinishtask_data', methods=['POST'])
@auth.login_required
def getfinishtask_data():
    json_data = request.get_json(force=True)
    task_id = json_data['task_id']
    res = task.get_sub_by_task_id(task_id)
    return jsonify(res)


@bp.route('/getrecommendperson', methods=['POST'])
@auth.login_required
def getrecommendperson():
    json_data = request.get_json(force=True)
    level1 = json_data.get('level1')
    level2 = json_data.get('level2')
    temp = task.get_recommended_person_by_type(level1, level2)
    return jsonify(temp)
