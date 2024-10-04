#!/usr/bin/python
# -*- coding: utf-8 -*-

# import getlockscreen as ls
import json

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from flask_httpauth import HTTPTokenAuth

from base import base
from data_collector import fund_collector
from fund import fund_base, fund_estimate, fund_order, fund_total, fund_setting, fund_review
from module import cycling, dft, person, schedule, task, travel, nga_post, nga_setting, project
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler

# TODO import样式统一


app = Flask(__name__)
auth = HTTPTokenAuth(scheme='Token')
CORS(app, resources=r'/*', supports_credentials=True)

g_tokens = {"serveraly": "aly"}

# TODO 将所有的json格式化放到app.py里面


@app.route('/')
@auth.login_required
def mainroute():
    return render_template("index.html")


@auth.verify_token
def verify_token(token):
    if token in g_tokens:
        return True
    return False


@app.route("/login", methods=['POST'])
def login():
    json_data = json.loads(request.get_data())
    user_name = json_data['user_name']
    user_pass = json_data['user_pass']
    # print(user_pass)
    if base.login(user_name, user_pass):
        g_tokens['secret-token-1 '+user_name] = user_name
        # print(g_tokens)
        return jsonify({"code": 200, 'token': 'Token secret-token-1 '+user_name})
    else:
        return jsonify({"code": 404})


@auth.error_handler
def error_handler():
    return jsonify({'code': 401, 'message': '401 Unauthorized Access'})

# 前置


@app.route('/addtask', methods=['POST'])
@auth.login_required
def add_task():
    # print(request.get_data())
    json_data = json.loads(request.get_data())
    level1 = json_data['level1']
    level2 = json_data['level2']
    level3 = json_data['level3']
    arg_task_name = json_data['task_name']
    arg_edate = json_data['edate']
    arg_person = json_data['person']
    # print(arg_type, arg_task_name, arg_edate)
    temp = task.add_task(level1, level2, level3,
                         arg_task_name, arg_edate, arg_person)
    return json.dumps({'arrays': temp})


# TODO 这个函数的返回值需要优化，app.py内的所有的返回值可以优化。
@app.route('/initoption')
@auth.login_required
def initoption():
    temp = task.init_option()
    return json.dumps({'task_sub_all_option': temp[0], 'task_level1_option': temp[1], 'level2_level3': temp[2], 'lastchecktime': temp[3], "dir_sub_all_option": temp[4], "dir_select_option": temp[5]})


@app.route('/gettasknow')
@auth.login_required
def get_task_now():
    return json.dumps({'arrays': task.get_task_now()})


@app.route('/gettimedata')
@auth.login_required
def gettimedata():
    return json.dumps(task.get_calendar_data_from_task())


@app.route('/finishtask', methods=['POST'])
@auth.login_required
def finishtask():
    # print(request.get_data())
    json_data = json.loads(request.get_data())
    task_id = json_data['task_id']
    finishtaskform = json_data['finishtaskform']
    # print(finishtaskform)
    temp = task.finish_task(task_id, finishtaskform)
    return json.dumps({'result': True, 'msg': temp})


@app.route('/deletetask', methods=['POST'])
@auth.login_required
def deletetask():
    # print(request.get_data())
    json_data = json.loads(request.get_data())
    task_id = json_data['task_id']
    task.delete_task_by_task_id(task_id)
    return json.dumps({'result': True})


@app.route('/querytask', methods=['POST'])
@auth.login_required
def querytask():
    json_data = json.loads(request.get_data())
    query = json_data['query']
    type = json_data['type']
    ftime = json_data['ftime']
    sub_type = json_data['sub_type']
    query_duration = json_data['query_duration']
    isstime = json_data['isstime']
    isqueryall = json_data['isqueryall']
    mode = json_data['mode']
    return json.dumps({'arrays': task.query_task(query, type, sub_type, ftime, query_duration, isstime, isqueryall, mode)})


@app.route('/querytask_week')
@auth.login_required
def querytask_week():
    return json.dumps({'arrays': task.get_task_this_week()})


@app.route("/gettreetask", methods=['POST'])
@auth.login_required
def gettreetask():
    json_data = json.loads(request.get_data())
    type = json_data['type']
    main = json_data['main']
    sub = json_data['sub']
    temp = task.get_task_by_type(type, main, sub)
    return json.dumps({"datas": temp})


@app.route('/updatetask', methods=['POST'])
@auth.login_required
def updatetask():
    json_data = json.loads(request.get_data())
    # print(json_data)
    task_id = json_data['task_id']
    level1 = json_data['level1']
    level2 = json_data['level2']
    level3 = json_data['level3']
    task_name = json_data['task_name']
    etime = json_data['etime']
    status = json_data['dustatus']
    task.update_task(task_id, level1, level2, level3, task_name, etime, status)
    return json.dumps({'result': True})


@app.route('/removetask')
@auth.login_required
def removetask():
    return json.dumps({'message': '已从任务中移除'+task.removetask()+'条删除的数据'})


@app.route('/gettasksummary_bar')
@auth.login_required
def gettasksummary_bar():
    return json.dumps(task.get_bar_data_from_task())


@app.route('/addprocess', methods=['POST'])
@auth.login_required
def addprocess():
    json_data = json.loads(request.get_data())
    task_id = json_data['task_id']
    process_name = json_data['process_name']
    if task.add_task_process(task_id, process_name):
        return json.dumps({'result': True})


@app.route('/deleteprocess', methods=['POST'])
@auth.login_required
def deleteprocess():
    json_data = json.loads(request.get_data())
    process_id = json_data['process_id']
    temp = task.delete_process(process_id)
    return json.dumps({'result': temp})


@app.route('/getprocess', methods=['POST'])
@auth.login_required
def getprocess():
    json_data = json.loads(request.get_data())
    task_id = json_data['task_id']
    temp = task.get_process_by_task_id(task_id)
    temp_s = task.get_process_count_by_task_id(task_id)
    return json.dumps({'arrays': temp, 'status': temp_s})


# #####################################
# 定义process的函数
# #####################################

@app.route('/resetprocess', methods=['POST'])
@auth.login_required
def resetprocess():
    json_data = json.loads(request.get_data())
    process_id = json_data['process_id']
    # print(process_id)
    return json.dumps({'status': task.reset_process_by_id(process_id)})


@app.route('/finishprocess', methods=['POST'])
@auth.login_required
def finishprocess():
    json_data = json.loads(request.get_data())
    process_id = json_data['process_id']
    # print(process_id)
    return json.dumps({'status': task.finish_process_by_id(process_id)})


@app.route('/updateprocess', methods=['POST'])
@auth.login_required
def updateprocess():
    json_data = json.loads(request.get_data())
    process_id = json_data['process_id']
    process_name = json_data['process_name']
    # print(process_id)
    return json.dumps({'status': task.update_process(process_id, process_name)})

# #####################################
# 定义schedule的函数
# #####################################


@app.route('/initschedule')
def initschedule():
    return json.dumps(schedule.run_schedule())


@app.route('/addschedule', methods=['POST'])
@auth.login_required
def addschedule():
    json_data = json.loads(request.get_data())
    level1 = json_data['level1']
    level2 = json_data['level2']
    level3 = json_data['level3']
    schedule_type = json_data['schedule_type']
    schedule_frequence = json_data['schedule_frequence']
    task_name = json_data['task_name']
    temp = schedule.add_schedule(level1, level2, level3, schedule_type,
                                 schedule_frequence, task_name)
    return json.dumps({'result': temp})


@app.route('/getscheduledata')
@auth.login_required
def getscheduledata():
    return json.dumps({'data': schedule.get_schedule()})


@app.route('/getscheduletaskdata', methods=['POST'])
@auth.login_required
def getscheduletaskdata():
    json_data = json.loads(request.get_data())
    schedule_id = json_data['schedule_id']
    return json.dumps({'data': schedule.get_task_by_schedule_id(schedule_id)})


@app.route('/forbidschedule', methods=['POST'])
@auth.login_required
def forbidschedule():
    json_data = json.loads(request.get_data())
    schedule_id = json_data['schedule_id']
    return json.dumps({'status': schedule.forbid_schedule(schedule_id)})


@app.route('/deleteschedule', methods=['POST'])
@auth.login_required
def deleteschedule():
    json_data = json.loads(request.get_data())
    schedule_id = json_data['schedule_id']
    return json.dumps({'status': schedule.delete_schedule(schedule_id)})


@app.route('/startschedule', methods=['POST'])
@auth.login_required
def startschedule():
    json_data = json.loads(request.get_data())
    schedule_id = json_data['schedule_id']
    return json.dumps({'status': schedule.enable_schedule(schedule_id)})


@app.route('/modifyschedule', methods=['POST'])
@auth.login_required
def modifyschedule():
    json_data = json.loads(request.get_data())
    schedule_id = json_data['schedule_id']
    level1 = json_data['level1']
    level2 = json_data['level2']
    level3 = json_data['level3']
    schedule_type = json_data['schedule_type']
    schedule_frequence = json_data['schedule_frequence']
    task_name = json_data['schedule_content']
    return json.dumps({'status': schedule.update_schedule(schedule_id, level1, level2, level3, schedule_type, schedule_frequence, task_name)})


# #####################################
# 定义sys的函数
# #####################################

@app.route('/setiswork', methods=['POST'])
@auth.login_required
def setiswork():
    json_data = json.loads(request.get_data())
    iswork = json_data['iswork']
    base.setiswork(iswork)
    return json.dumps({'result': True})


@app.route('/getiswork')
@auth.login_required
def getiswork():
    res = base.get_sys_params(2)
    return json.dumps({'iswork': res})


@app.route('/getfirstpage')
@auth.login_required
@auth.login_required
def getfirstpage():
    res = base.get_homepage()
    return json.dumps(res)


@app.route('/setfirstpage', methods=['POST'])
@auth.login_required
def setfirstpage():
    json_data = json.loads(request.get_data())
    firstpage = json_data['firstpage']
    base.setfirstpage(firstpage)
    return json.dumps({'result': True})


@app.route('/gettype')
@auth.login_required
def gettype():
    res = base.get_task_type()
    return json.dumps(res)


@app.route('/getnodirdata')
@auth.login_required
def getnodirdata():
    res = base.getnodirdata()
    return json.dumps(res)


@app.route('/getsubtype')
@auth.login_required
def getsubtype():
    res = task.get_task_type_option()
    return json.dumps(res)


@app.route('/updatesubtupe', methods=['POST'])
@auth.login_required
def updatesubtupe():
    json_data = json.loads(request.get_data())
    typenow = json_data['typenow']
    old_sub_type = json_data['old_sub_type']
    new_sub_type = json_data['new_sub_type']
    base.update_sub_type(typenow, old_sub_type, new_sub_type)
    return json.dumps({'result': True})


@app.route('/updatedir', methods=['POST'])
@auth.login_required
def updatedir():
    json_data = json.loads(request.get_data())
    sub_dir = json_data['sub_dir']
    new_dir_type = json_data['new_dir_type']
    base.updatedir(sub_dir, new_dir_type)
    return json.dumps({'result': True})


@ app.route('/addtype', methods=['POST'])
@auth.login_required
def addtype():
    json_data = json.loads(request.get_data())
    typename = json_data['typename']
    typevalue = json_data['typevalue']
    base.add_base_type(typename, typevalue)
    return json.dumps({'result': True})


@ app.route('/deletetype', methods=['POST'])
@auth.login_required
def deletetype():
    json_data = json.loads(request.get_data())
    typeid = json_data['typeid']
    base.delete_base_type(typeid)
    return json.dumps({'result': True})


@ app.route('/getcitydata')
@auth.login_required
def getcitydata():
    res = travel.get_city()
    return json.dumps(res)


@ app.route('/addcity', methods=['POST'])
@auth.login_required
def addcity():
    json_data = json.loads(request.get_data())
    city_name = json_data['city_name']
    city_lon = json_data['city_lon']
    city_lat = json_data['city_lat']
    res = travel.add_city(city_name, city_lon, city_lat)
    return json.dumps(res)


@ app.route('/querycity', methods=['POST'])
@auth.login_required
def querycity():
    json_data = json.loads(request.get_data())
    city_name = json_data['city_name']
    city_lon = json_data['city_lon']
    city_lat = json_data['city_lat']
    res = travel.query_city(city_name, city_lon, city_lat)
    return json.dumps(res)

# #####################################
# 定义nga_setting的函数
# #####################################


@app.route("/get_nga_specia_post")
@auth.login_required
def get_nga_specia_post():
    temp_data = nga_setting.get_nga_specia_post()
    return json.dumps({"data": temp_data})


@app.route("/add_nga_special_post", methods=['POST'])
@auth.login_required
def add_nga_special_post():
    json_data = json.loads(request.get_data())
    tid = json_data['new_nga_special_post_id']
    temp_data = nga_setting.add_nga_special_post(tid)
    return json.dumps({"data": temp_data})


@app.route("/delete_nga_special_post", methods=['POST'])
@auth.login_required
def delete_nga_special_post():
    json_data = json.loads(request.get_data())
    tid = json_data['delete_nga_special_post_id']
    temp_data = nga_setting.delete_nga_special_post(tid)
    return json.dumps({"data": temp_data})


@app.route("/get_nga_specia_user")
@auth.login_required
def get_nga_specia_user():
    temp_data = nga_setting.get_nga_specia_user()
    return json.dumps({"data": temp_data})


@app.route("/add_nga_special_user", methods=['POST'])
@auth.login_required
def add_nga_special_user():
    json_data = json.loads(request.get_data())
    nga_user_id = json_data['new_nga_special_user_id']
    temp_data = nga_setting.add_nga_special_user(nga_user_id)
    return json.dumps({"data": temp_data})


@app.route("/delete_nga_special_user", methods=['POST'])
@auth.login_required
def delete_nga_special_user():
    json_data = json.loads(request.get_data())
    nga_user_id = json_data['delete_nga_special_user_id']
    temp_data = nga_setting.delete_nga_special_user(nga_user_id)
    return json.dumps({"data": temp_data})


@ app.route('/getpersonoptions')
@auth.login_required
def getpersonoptions():
    res = person.getpersonoptions()
    return json.dumps(res)


@ app.route('/getperson')
@auth.login_required
def getperson():
    res = person.get_person()
    return json.dumps(res)


@ app.route('/getperson_option')
@auth.login_required
def getperson_option():
    res = person.get_person_count()
    return json.dumps(res)


@ app.route('/addperson', methods=['POST'])
@auth.login_required
def addperson():
    json_data = json.loads(request.get_data())
    company = json_data['company']
    department = json_data['department']
    person_name = json_data['person_name']
    post = json_data['post']
    force = json_data['force']
    temp = person.add_person(
        company, department, person_name, post, force)
    return json.dumps(temp)


@ app.route('/deleteperson', methods=['POST'])
@auth.login_required
def deleteperson():
    json_data = json.loads(request.get_data())
    personid = json_data['personid']
    person.delete_person(personid)
    return json.dumps({'result': True})


@ app.route('/getperson_data', methods=['POST'])
@auth.login_required
def getperson_data():
    json_data = json.loads(request.get_data())
    task_id = json_data['task_id']
    res = task.get_person_by_task_id(task_id)
    return json.dumps(res)


@ app.route('/getfinishtask_data', methods=['POST'])
@auth.login_required
def getfinishtask_data():
    json_data = json.loads(request.get_data())
    task_id = json_data['task_id']
    res = task.get_sub_by_task_id(task_id)
    return json.dumps(res)


@ app.route('/appendtaskperson', methods=['POST'])
@auth.login_required
def appendtaskperson():
    json_data = json.loads(request.get_data())
    # print(json_data)
    task_id = json_data['task_id']
    person_id = json_data['person_id']
    res = task.add_task_person(task_id, person_id)
    return json.dumps(res)


@ app.route('/deletetaskperson', methods=['POST'])
@auth.login_required
def deletetaskperson():
    json_data = json.loads(request.get_data())
    task_id = json_data['task_id']
    person_id = json_data['person_id']
    res = task.delete_person_by_task_id(task_id, person_id)
    return json.dumps(res)


@ app.route('/getrecommendperson', methods=['POST'])
@auth.login_required
def getrecommendperson():
    json_data = json.loads(request.get_data())
    type = json_data['type']
    sub_type = json_data['sub_type']
    temp = task.get_recommended_person_by_type(type, sub_type)
    return json.dumps(temp)


# #####################################
# 定义fund_base的函数
# #####################################
@ app.route('/get_fund_info')
@auth.login_required
def get_fund_info():
    temp, temp1 = fund_base.get_fund_info()
    return json.dumps({'data': temp, 'listdata': temp1})


# #####################################
# 定义fund_order的函数
# #####################################
@ app.route('/get_order_data')
@auth.login_required
def get_order_data():
    temp = fund_order.get_order_data()
    return json.dumps({"data": temp})


@app.route("/get_funds_for_cost_update")
@auth.login_required
def get_funds_for_cost_update():
    temp, temp_dict = fund_order.get_funds_for_cost_update()
    return json.dumps({"data": temp, 'data_list': temp_dict})


@app.route("/get_fund_base")
@auth.login_required
def get_fund_base():
    temp_data = fund_order.get_fund_base()
    return json.dumps({"data": temp_data})


@app.route("/add_new_fund", methods=['POST'])
@auth.login_required
def add_new_fund():
    json_data = json.loads(request.get_data())
    new_fund_code = json_data['new_fund_code']
    new_fund_name = json_data['new_fund_name']
    msg = fund_order.add_new_fund(new_fund_code, new_fund_name)
    return json.dumps({"msg": msg})


@app.route("/get_cost_info")
@auth.login_required
def get_cost_info():
    temp = fund_order.get_cost_info()
    return json.dumps({"data": temp})


@app.route("/get_fund_closed_net_value")
@auth.login_required
def get_fund_closed_net_value():
    temp = fund_order.get_fund_closed_net_value()
    return json.dumps({"data": temp})


@ app.route('/commitorders', methods=['POST'])
@auth.login_required
def commitorders():
    json_data = json.loads(request.get_data())
    ordertype = json_data['ordertype']
    orderform = json_data['orderform']
    print(ordertype, orderform)
    if ordertype:
        temp = fund_order.add_buy_order(orderform)
    else:
        temp = fund_order.add_sell_order(orderform)
    return json.dumps({'res': temp})


@app.route("/update_fund_cost", methods=['POST'])
@auth.login_required
def update_fund_cost():
    json_data = json.loads(request.get_data())
    fund_code = json_data['fund_code']
    cost = json_data['cost']
    res = fund_order.update_fund_cost(fund_code, cost)
    return json.dumps(res)


@app.route("/fund_update_once")
@auth.login_required
def fund_update_once():
    fund_order.fund_update_once()
    return json.dumps({'res': "ok"})

# #####################################
# 定义fund_collector的函数
# #####################################


@app.route("/getfundnow", methods=['POST'])
@auth.login_required
def getfundnow():
    json_data = json.loads(request.get_data())
    click_fund_code = json_data['click_fund_code']
    res = fund_collector.collect_fund_net_estimate(
        click_fund_code)
    return json.dumps(res)


@app.route("/collect_all_fund_net")
@auth.login_required
def collect_all_fund_net():
    res = fund_collector.collect_all_fund_net_estimate()
    return json.dumps(res)


# #####################################
# 定义fund_estimate的函数
# #####################################


@app.route("/get_fund_estimate_data")
@auth.login_required
def get_fund_estimate_data():
    res = fund_estimate.get_fund_estimate_data()
    return json.dumps(res)


@ app.route('/getestimatebuydata', methods=['POST'])
@auth.login_required
async def getestimatebuydata():
    fund_code = json.loads(request.get_data())['fund_code']
    esd, y, x = fund_estimate.getestimatebuydata(fund_code=fund_code)
    return json.dumps({"data": esd, "y": y, "x": x})


# #####################################
# 定义fund_total的函数
# #####################################


@ app.route('/get_fund_review', methods=['POST'])
@auth.login_required
def get_fund_review():
    json_data = json.loads(request.get_data())
    reviewform = json_data['reviewform']
    oneday = json_data['reviewform']
    temp = fund_total.get_fund_review(reviewform, getoneday=oneday)
    if temp:
        return json.dumps({"response_code": 200, 'res': temp})
    else:
        return json.dumps({"response_code": 404, "res": temp})


@ app.route('/add_fund_review', methods=['POST'])
@auth.login_required
def add_fund_review():
    json_data = json.loads(request.get_data())
    reviewform = json_data['reviewform']
    if "isupdate" in json_data.keys():
        temp = fund_total.add_fund_review(reviewform, True)
        return json.dumps({'res': temp})
    temp = fund_total.add_fund_review(reviewform)
    return json.dumps({'res': temp})


@ app.route('/get_fund_total_data')
@auth.login_required
def get_fund_total_data():
    temp = fund_total.get_fund_total_data()
    return json.dumps({"data": temp})


@ app.route('/getfryfundtable')
@auth.login_required
def getfryfundtable():
    temp = fund_total.get_fund_total_data(getfry=True)
    return json.dumps({"data": temp})


@ app.route('/getfundalldata')
@auth.login_required
def getfundalldata():
    temp = fund_total.get_fund_total_data(getall=True)
    return json.dumps({"data": temp})


@app.route('/get_fund_remain_chart_data', methods=['POST'])
@auth.login_required
def get_fund_remain_chart_data():
    json_data = json.loads(request.get_data())
    fund_code = json_data['fund_code']
    res = fund_total.get_fund_remain_chart_data(fund_code)
    return json.dumps(res)


@app.route('/get_fund_total_chart_data', methods=['POST'])
@auth.login_required
def get_fund_total_chart_data():
    json_data = json.loads(request.get_data())
    fund_code = json_data['fund_code']
    res = fund_total.get_fund_total_chart_data(fund_code)
    return json.dumps(res)


@app.route('/get_fund_treemap_label')
@auth.login_required
def get_fund_treemap_label():
    res = fund_total.get_fund_treemap_label()
    return json.dumps(res)


# #####################################
# 定义fund_review的函数
# #####################################


@ app.route('/getfundcalendar', methods=['POST'])
@auth.login_required
def getfundcalendar():
    json_data = json.loads(request.get_data())

    mode = json_data['mode']
    if mode == 'fund':
        fund_code = json_data['fund_code']
        temp, temp1, sform = fund_review.getfundcalendar(fund_code)
        return json.dumps({'data': temp, 'bs_data': temp1, "sform": sform})
    if mode == 'author':
        temp, temp1, sform = fund_review.getfundcalendarbyauthor()
        return json.dumps({'data': temp, 'bs_data': temp1, "sform": sform})


@ app.route('/getreviewtabledata', methods=['POST'])
@auth.login_required
def getreviewtabledata():
    json_data = json.loads(request.get_data())
    fund_code = json_data['fund_code']
    fund_review_time = json_data['fund_review_time']
    temp = fund_review.getreviewtabledata(fund_code, fund_review_time)
    return json.dumps(temp)


@ app.route('/getfunder')
@auth.login_required
def getfunder():
    temp, temp_list = fund_review.getfunder()
    return json.dumps({"data": temp, "data_list": temp_list})


@ app.route('/getfundlabel')
@auth.login_required
def getfundlabel():
    temp = fund_review.getfundlabel()
    return json.dumps(temp)


@ app.route('/commitfunderreview', methods=['POST'])
@auth.login_required
def commitfunderreview():
    json_data = json.loads(request.get_data())
    funderreviewform = json_data['funderreviewform']
    temp = fund_review.commitfunderreview(funderreviewform)
    return json.dumps({'res': temp})


@ app.route('/getfunderreview', methods=['POST'])
@auth.login_required
def getfunderreview():
    json_data = json.loads(request.get_data())
    funder_id = json_data['funder_id']
    temp = fund_review.getfunderreview(funder_id)
    return json.dumps(temp)


# #####################################
# 定义fishtang的函数
# #####################################


@ app.route('/get_fund_base_label')
@auth.login_required
def get_fund_base_label():
    temp, temp_option = fund_setting.get_fund_base_label()
    return json.dumps({"data": temp, "data_option": temp_option})


@ app.route('/get_fund_customer_label_option')
@auth.login_required
def get_fund_customer_label_option():
    option_data, table_data = fund_setting.get_fund_customer_label_option()
    temp = {'option_data': option_data, 'table_data': table_data}
    return json.dumps(temp)


@ app.route('/delete_fund_customer_label', methods=['POST'])
@auth.login_required
def delete_fund_customer_label():
    json_data = json.loads(request.get_data())
    fund_label_id = json_data['fund_operation_label_id']
    fund_setting.delete_fund_customer_label(fund_label_id)
    return json.dumps({"data": "ok"})


@ app.route('/add_fund_customer_label', methods=['POST'])
@auth.login_required
def add_fund_customer_label():
    json_data = json.loads(request.get_data())
    fund_customer_label_selected = json_data['fund_customer_label_selected']
    fund_customer_fund_selected = json_data['fund_customer_fund_selected']
    temp = fund_setting.add_fund_customer_label(
        fund_customer_label_selected, fund_customer_fund_selected)
    return json.dumps({'res': temp})


@ app.route('/add_fund_author', methods=['POST'])
@auth.login_required
def add_fund_author():
    json_data = json.loads(request.get_data())
    new_author = json_data['new_author']
    apps_selected = json_data['apps_selected']
    isfirm = json_data['isfirm']
    temp = fund_setting.add_fund_author(new_author, apps_selected, isfirm)
    return json.dumps(temp)


@ app.route('/add_new_label', methods=['POST'])
@auth.login_required
def add_new_label():
    json_data = json.loads(request.get_data())
    new_fund_label = json_data['new_fund_label']
    temp = fund_setting.add_new_label(new_fund_label)
    return json.dumps({'res': temp})


@ app.route('/add_fund_label', methods=['POST'])
@auth.login_required
def add_fund_label():
    json_data = json.loads(request.get_data())
    fund_base_label_selected = json_data['fund_base_label_selected']
    fund_had_code_selected = json_data['fund_had_code_selected']
    temp = fund_setting.add_fund_label(
        fund_base_label_selected, fund_had_code_selected)
    return json.dumps({'res': temp})


@ app.route('/get_fund_base_label_data')
@auth.login_required
def get_fund_base_label_data():
    temp = fund_setting.get_fund_base_label_data()
    return json.dumps(temp)


@ app.route('/get_author_app_option')
@auth.login_required
def get_author_app_option():
    temp = fund_setting.get_author_app_option()
    return json.dumps(temp)


@ app.route('/get_fund_author_data')
@auth.login_required
def get_fund_author_data():
    temp = fund_setting.get_fund_author_data()
    return json.dumps(temp)


# #####################################
# 定义count的函数
# #####################################


@ app.route('/getprogressdata', methods=['POST'])
@auth.login_required
def getprogressdata():
    return json.dumps(task.calculate_process_hours())


@ app.route('/getcalendardata', methods=['POST'])
@auth.login_required
def getcalendardata():
    return json.dumps(task.get_task_by_calendar())


@ app.route('/gettreemapdata', methods=['POST'])
@auth.login_required
def gettreemapdata():
    return json.dumps(task.get_treemap_data_from_task())


@ app.route('/getsankeydata', methods=['POST'])
@auth.login_required
def getsankeydata():
    return json.dumps(task.get_sankey_data_from_task())


@ app.route('/getscatterdata', methods=['POST'])
@auth.login_required
def getscatterdata():
    json_data = json.loads(request.get_data())
    type = json_data['type']
    sub_type = json_data['sub_type']
    person_id = json_data['person_id']
    return json.dumps(person.get_scatter_data_from_task(type, sub_type, person_id))


# #####################################
# 定义tarvel的函数
# #####################################

@ app.route('/gettravel')
@auth.login_required
def gettravel():
    temp = travel.get_travel()
    return json.dumps(temp)


# #####################################
# 定义cycling的函数
# #####################################

@ app.route('/getcycling', methods=['POST'])
@auth.login_required
def getcycling():
    json_data = json.loads(request.get_data())
    cycling_type_selected = json_data['cycling_type_selected']
    temp = cycling.get_cycling(cycling_type_selected)
    _temp = ['tabledata', 'yaxis', 'avg_hr', 'max_hr',
             'avg_cadence', 'intensity', 'efficiency', 'adr']
    return json.dumps(dict(zip(_temp, temp)))


@ app.route('/get_cycling_name')
@auth.login_required
def get_cycling_name():
    temp = cycling.get_cycling_name()
    return json.dumps({'data': temp})


@ app.route('/addcyctraindata', methods=['POST'])
@auth.login_required
def addcyctraindata():
    json_data = json.loads(request.get_data())
    cyctraindata = json_data['cyctraindata']
    temp = cycling.add_cycling(cyctraindata)
    # _temp = ['tabledata', 'yaxis', 'avg_hr', 'max_hr', 'avg_cadence', 'intensity', 'efficiency', 'adr']
    # return json.dumps(dict(zip(_temp, temp)))
    return json.dumps({'result': temp})


# #####################################
# 定义dft的函数
# #####################################

@ app.route('/getdftdata')
@auth.login_required
def getdftdata():
    temp = dft.get_dft()
    unread_count = dft.get_dft_data_unread_count()
    return json.dumps({'data': temp, 'unread_count': unread_count})


@ app.route('/getappendixdata', methods=['POST'])
@auth.login_required
def getappendixdata():
    json_data = json.loads(request.get_data())
    doc_id = json_data['doc_id']
    temp = dft.get_appendix_by_doc_id(doc_id)
    return json.dumps({'data': temp})


@ app.route('/commitdft', methods=['POST'])
@auth.login_required
def commitdft():
    json_data = json.loads(request.get_data())
    dftform = json_data['dftform']
    mode = json_data['mode']
    if mode == 'add':
        temp = dft.add_dft(dftform, isbyupdate=False)
    else:
        temp = dft.update_dft(dftform)
    return json.dumps(temp)


@ app.route('/deletedft', methods=['POST'])
@auth.login_required
def deletedft():
    json_data = json.loads(request.get_data())
    doc_id = json_data['doc_id']
    temp = dft.delete_dft(doc_id)
    return json.dumps({'res': temp})


@ app.route('/getdftdir')
@auth.login_required
def getdftdir():
    return json.dumps(dft.get_dft_option())

# #####################################
# 定义project的函数
# #####################################


@ app.route('/getproject')
@auth.login_required
def getproject():
    res = project.get_project()
    return json.dumps(res)


@ app.route('/get_task_by_project_id', methods=['POST'])
@auth.login_required
def get_task_by_project_id():
    json_data = json.loads(request.get_data())
    project_id = json_data['project_id']
    project_task = project.get_task_by_project_id(project_id)
    project_pie_data = project.get_project_task_piechart_by_project_id(
        project_id)
    project_bar_data = project.get_project_task_barchart_by_project_id(
        project_id)
    return json.dumps({"project_task_data": project_task, 'project_pie_data': project_pie_data, 'project_bar_data': project_bar_data})


@ app.route('/get_person_by_project_id', methods=['POST'])
@auth.login_required
def get_person_by_project_id():
    json_data = json.loads(request.get_data())
    project_id = json_data['project_id']
    project_person = project.get_person_by_project_id(project_id)
    project_person_graph = project.get_project_person_graph_data(project_id)
    return json.dumps({"project_person_data": project_person, "project_person_graph": project_person_graph})


@ app.route('/update_project_desc', methods=['POST'])
@auth.login_required
def update_project_desc():
    json_data = json.loads(request.get_data())
    project_id = json_data['project_id']
    project_desc = json_data['project_desc']
    project.update_project_desc(project_id, project_desc)
    return json.dumps({"msg": 'ok'})


@ app.route('/update_project_detail', methods=['POST'])
@auth.login_required
def update_project_detail():
    json_data = json.loads(request.get_data())
    project_id = json_data['project_id']
    project_detail = project.update_project_detail(project_id)
    return json.dumps(project_detail)

# #####################################
# 定义process的函数
# #####################################


@ app.route('/getposttabledata')
@auth.login_required
def getposttabledata():
    temp = nga_post.get_nga_post_data()
    return json.dumps(temp)


@ app.route('/getreplytabledata', methods=['POST'])
@auth.login_required
def getreplytabledata():
    json_data = json.loads(request.get_data())
    tid = json_data['tid']
    temp = nga_post.get_nga_reply_by_tid(tid)
    return json.dumps(temp)


LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
LOG_FILE = 'logs/app.log'
LOG_LEVEL = 'INFO'


# 创建按天滚动的文件处理器
rolling_handler = TimedRotatingFileHandler(
    LOG_FILE, when='midnight', interval=1)
rolling_handler.setLevel(LOG_LEVEL)
rolling_handler.setFormatter(Formatter(LOG_FORMAT))


if __name__ == '__main__':
    # ls.getlocksreen()
    # TODO 使用logging模块
    app.logger.addHandler(rolling_handler)
    app.run(host='0.0.0.0', port=5000, debug=True)
