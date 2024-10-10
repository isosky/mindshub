#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime, date
import time
import json

from base.base import connect_database, get_base_type, get_sys_params


def formatdate(data: dict):
    for i in data:
        for k, v in i.items():
            if isinstance(v, date):
                i[k] = v.strftime('%Y-%m-%d')
            if isinstance(v, datetime):
                i[k] = v.strftime('%Y-%m-%d %H:%M:%S')
    return data


def get_project():
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        "select * from project order by update_time")
    res = cursor.fetchall()
    res = formatdate(res)
    conn.close()
    return res


def update_project_desc(project_id, project_desc):
    conn, cursor = connect_database()
    cursor.execute(
        "insert into project_detail (project_id,project_desc) select project_id,project_desc from project where project_id=%s", [project_id])
    conn.commit()
    cursor.execute("update project set project_desc =%s,update_time=now() where project_id=%s", [
                   project_desc, project_id])
    conn.commit()
    conn.close()


def get_task_by_project_id(project_id):
    dict_stauts = ['待做', '及时', '待做逾期', '超时', '删除']
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        "select stime,etime,ftime,task_name,status from task where project_id =%s order by task_id desc", [project_id])
    res = cursor.fetchall()
    # res = formatdate(res)
    for i in res:
        for k, v in i.items():
            if isinstance(v, date) or isinstance(v, datetime):
                i[k] = v.strftime('%Y-%m-%d')
            if k == 'status':
                i[k] = dict_stauts[i[k]-1]

    conn.close()
    return res


def get_person_by_project_id(project_id):
    conn, cursor = connect_database()
    cursor.execute(
        "select person_id,person_name,company,department,post from person")
    _person = {}
    for i in cursor:
        _person[i[0]] = {'person_name': i[1],
                         'company': i[2], 'department': i[3], 'post': i[4]}
    cursor.execute(
        "select person_id,avg(score_activity) as aa,avg(score_critical) as ac from task_person_score group by person_id")
    _person_all_score = {}
    for i in cursor:
        _person_all_score[i[0]] = [round(i[1], 2), round(i[2], 2)]
    res = []
    cursor.execute(
        "select person_id,count(distinct task_id),avg(score_activity),avg(score_critical) from task_person_score where project_id=%s group by person_id;", [project_id])
    for i in cursor:
        if i[0] == 0:
            continue
        res.append(
            {'person_id': i[0], 'person_name': _person[i[0]]['person_name'], 'company': _person[i[0]]['company'], 'department': _person[i[0]]['department'], 'post': _person[i[0]]['post'],
             'task_count': i[1], 'project_activity': round(i[2], 2), 'project_critical': round(i[3], 2), 'all_activity': _person_all_score[i[0]][0], 'all_critical': _person_all_score[i[0]][1]})
    return res


def cal_project_graph():
    conn, cursor = connect_database()
    # todo test
    cursor.execute(
        "select distinct project_id from project where project_id not in (select project_id from project_graph_json)")
    for i in cursor:
        print('初始处理：'+str(i))
        cal_project_person_graph_data(i[0])
    cursor.execute(
        "select distinct a.project_id from project_graph_json a,task b where a.project_id=b.project_id and b.ftime>a.update_time;")
    for i in cursor:
        print('增量处理：'+str(i))
        cal_project_person_graph_data(i[0])
    conn.close()


def get_project_person_graph_data(project_id):
    conn, cursor = connect_database()
    cursor.execute(
        "select nodes,links,categories,legend from project_graph_json where project_id=%s", [project_id])
    temp = cursor.fetchone()
    conn.close()
    return {'nodes': json.loads(temp[0]), 'links': json.loads(temp[1]), 'categories': json.loads(temp[2]), 'legend': json.loads(temp[3])}


def cal_project_person_graph_data(project_id):
    conn, cursor = connect_database()
    cursor.execute(
        "select count(distinct task_id) from task_person_score where project_id=%s", [project_id])
    project_task_count = cursor.fetchone()[0]
    symbolsize = {}
    cursor.execute(
        "select company,count(distinct task_id) as tn from task_person_score where project_id=%s group by company;", [project_id])
    for i in cursor:
        if i[0] not in symbolsize:
            symbolsize[i[0]] = {
                'ss': max(int(i[1]/project_task_count*100), 30)}
    cursor.execute(
        "select company,department,count(distinct task_id) as tn from task_person_score where project_id=%s group by company,department;", [project_id])
    for i in cursor:
        symbolsize[i[0]][i[1]] = {
            'ss': max(int(i[2]/project_task_count*100), 20)}

    cursor.execute(
        "select company,department,post,count(distinct task_id) as tn from task_person_score where project_id=%s group by company,department,post;", [project_id])
    for i in cursor:
        symbolsize[i[0]][i[1]][i[2]] = {
            'ss': max(int(i[3]/project_task_count*100), 15)}

    cursor.execute(
        "select company,department,post,person_id,count(distinct task_id) as tn from task_person_score where project_id=%s group by company,department,post,person_id;", [project_id])
    for i in cursor:
        symbolsize[i[0]][i[1]][i[2]][i[3]] = {
            'ss': max(int(i[4]/project_task_count*100), 5)}

    # return symbolsize
    cursor.execute(
        "select distinct person_id,person_name,company,department,post from task_person_score where project_id=%s;", [project_id])
    _temp_dict = {}
    person_dict = {}
    for i in cursor:
        if i[2] not in _temp_dict:
            _temp_dict[i[2]] = {i[3]: {i[4]: [i[0]]}}
        else:
            if i[3] not in _temp_dict[i[2]]:
                _temp_dict[i[2]][i[3]] = {i[4]: [i[0]]}
            else:
                if i[4] not in _temp_dict[i[2]][i[3]]:
                    _temp_dict[i[2]][i[3]][i[4]] = [i[0]]
                else:
                    _temp_dict[i[2]][i[3]][i[4]].append(i[0])
        person_dict[i[0]] = i[1]
    nodes = [{'id': 0, 'name': '项目', 'symbolSize': 100, 'category': '项目'}]
    links = []
    id = 1
    # cstr = '0ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    for company in _temp_dict:
        _company_id = id
        nodes.append({'id': id, 'name': company,
                     'symbolSize': symbolsize[company]['ss'], 'category': company})
        links.append({"source": 0, "target": _company_id})
        id += 1
        for department in _temp_dict[company]:
            nodes.append({'id': id, 'name': department,
                         'symbolSize': symbolsize[company][department]['ss'], 'category': company})
            _department_id = id
            id += 1
            links.append({"source": _company_id, "target": _department_id})
            for post in _temp_dict[company][department]:
                nodes.append(
                    {'id': id, 'name': post, 'symbolSize': symbolsize[company][department][post]['ss'], 'category': company})
                _post_id = id
                id += 1
                links.append({"source": _department_id, "target": _post_id})
                for person_id in _temp_dict[company][department][post]:
                    nodes.append({'id': id, 'name': person_dict[person_id],
                                 'symbolSize': symbolsize[company][department][post][person_id]['ss'], 'category': company})
                    links.append({"source": _post_id, "target": id})
                    id += 1
    categories = [{'name': '项目'}]
    categories.extend([{'name': x} for x in _temp_dict])
    legend = ['项目']
    legend.extend(list(_temp_dict.keys()))
    cursor.execute(
        "select count(*) from project_graph_json where project_id=%s", [project_id])
    if cursor.fetchone()[0] > 0:
        cursor.execute("update project_graph_json set nodes=%s,links=%s,categories=%s,legend=%s,update_time=now() where project_id=%s", [
                       json.dumps(nodes), json.dumps(links), json.dumps(categories), json.dumps(legend), project_id])
        conn.commit()
    else:
        cursor.execute("insert into project_graph_json (project_id,nodes,links,categories,legend) values (%s,%s,%s,%s,%s) ", [
                       project_id, json.dumps(nodes), json.dumps(links), json.dumps(categories), json.dumps(legend)])
    conn.commit()
    conn.close()
    # return {'nodes': nodes, 'links': links, 'categories': categories, 'legend': legend}


def get_project_task_barchart_by_project_id(project_id):
    conn, cursor = connect_database()
    cursor.execute(
        "select DATE_FORMAT(ftime,'%Y-%m') as ym,status,count(*) as c from task where ftime is not null and project_id = %s group by DATE_FORMAT(ftime,'%Y-%m'),status order by ym,status;", [project_id])
    ym = []
    # ym_status = [2,4,5]
    _bar_data = {2: [], 4: [], 5: []}
    bar_data = {}

    for i in cursor:
        if i[0] not in ym:
            ym.append(i[0])
            for k in _bar_data.keys():
                _bar_data[k].append(0)
        _bar_data[i[1]].pop()
        _bar_data[i[1]].append(i[2])
    bar_data['完成'] = _bar_data[2]
    bar_data['超时'] = _bar_data[4]
    bar_data['删除'] = _bar_data[5]
    conn.close()
    return {'ym': ym, 'bar_data': bar_data}


def get_project_task_piechart_by_project_id(project_id):
    dict_stauts = ['待做', '及时', '待做逾期', '超时', '删除']
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        "select status as name,count(*) as value from task where project_id =%s group by status", [project_id])
    _pie_summary_data = {}
    sum_task = 0
    finish_sums = 0
    overdue_sums = 0
    for i in cursor:
        i['name'] = dict_stauts[i['name']-1]
        sum_task += i['value']
        if i['name'] in ['待做逾期', '超时']:
            overdue_sums += i['value']
        if i['name'] in ['及时', '超时']:
            finish_sums += i['value']
        _pie_summary_data[i['name']] = i['value']
    if sum_task != 0:
        overdue_percent = round(overdue_sums / sum_task * 100, 2)
        finish_percent = round(finish_sums/sum_task*100, 2)
    else:
        overdue_percent = 0
        finish_percent = 0
    pie_summary_data = []
    _dict_stauts = ['超时', '待做逾期', '待做', '及时', '删除']
    for i in _dict_stauts:
        if i in _pie_summary_data:
            pie_summary_data.append({'value': _pie_summary_data[i], 'name': i})
        else:
            pie_summary_data.append({'value': 0, 'name': i})

    conn.close()
    return {'pie_summary_data': pie_summary_data, 'percent': [finish_percent, overdue_percent], 'sum_task': sum_task}


def update_project_detail(project_id):
    conn, cursor = connect_database()
    cursor.execute(
        "select level1,level2,level3,project_desc,create_time,update_time from project where project_id=%s", [project_id])
    temp = cursor.fetchone()
    project_name = '-'.join(['' if x is None else x for x in temp[:3]])
    # print(project_name)
    project_start_time = temp[4].strftime('%Y-%m-%d %H:%M:%S')
    project_last_time = temp[5].strftime('%Y-%m-%d %H:%M:%S')
    project_desc = temp[3]
    conn.close()
    return {'project_name': project_name, 'project_start_time': project_start_time, 'project_last_time': project_last_time, 'project_desc': project_desc}
