#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import time

from base.base import connect_database, get_base_type, get_sys_params
from base.config import myself_name
from module.travel import init_travel


def add_task(level1: str, level2: str, level3: str, task_name: str, etime: datetime.date, person_arrays: list) -> list:
    # TODO bug 添加任务的时候，如果过期了，status不应该使用默认值
    conn, cursor = connect_database()
    temp_base_type = get_base_type()
    cursor.execute("insert into task (level1,level2,level3,task_name,etime,iswork) values (%s,%s,%s,%s,%s,%s)", [
        level1, level2, level3, task_name, etime, temp_base_type[level1]])
    conn.commit()
    cursor.execute("select max(task_id) from task")
    task_id = cursor.fetchone()[0]
    if person_arrays:
        for i in person_arrays:
            cursor.execute(
                "insert into task_person values (%s,%s)", [task_id, i])
    conn.commit()
    conn.close()
    if level1 == '项目':
        check_project(level1, level2, level3)
    return get_task_now()


def check_project(level1, level2, level3):
    conn, cursor = connect_database()
    if level3 is None:
        cursor.execute(
            "select count(*) from project where level2=%s and level3 is null", [level2])
    else:
        cursor.execute(
            "select count(*) from project where level2=%s and level3=%s ", [level2, level3])
    count = cursor.fetchone()[0]
    if count == 0:
        cursor.execute("insert into project (level1, level2, level3,update_time) values (%s,%s,%s,now())", [
                       level1, level2, level3])
        conn.commit()
    conn.close()


def get_task_now():
    iswork = get_sys_params(2)
    conn, cursor = connect_database()
    cursor.execute(
        "select task_id,level1,level2,task_name,etime,stime,isfinish,status from task where isfinish=0 and isabandon=0 and iswork>=%s order by etime,task_id", [iswork])
    temp_data = cursor.fetchall()
    result = []
    process = count_task_process_number()
    person = count_task_person_number()
    for row in temp_data:
        temp = {'task_id': row[0], 'level1': row[1], 'level2': row[2],
                'task_name': row[3], 'etime': row[4].strftime('%m-%d'), 'stime': row[5].strftime('%Y-%m-%d %H:%M:%S'), 'tetime': row[4].strftime('%Y-%m-%d'), 'isfinish': row[6], 'status': row[7]}
        if row[0] in process.keys():
            temp['num_process'] = process[row[0]]
        if row[0] in person.keys():
            temp['num_person'] = person[row[0]]
        result.append(temp)
    conn.close()
    return result


def count_task_process_number():
    conn, cursor = connect_database()
    cursor.execute(
        "select task_id,count(*) from task_process where isfinish=1 group by task_id")
    temp_data = cursor.fetchall()
    pfa = dict(temp_data)
    cursor.execute(
        "select task_id,count(*) from task_process group by task_id")
    temp_data = cursor.fetchall()
    pff = dict(temp_data)
    result = {}
    for i in pff.keys():
        if i in pfa.keys():
            result[i] = str(pfa[i]) + '/'+str(pff[i])
        else:
            result[i] = '0/'+str(pff[i])
    conn.close()
    return result


def count_task_person_number():
    conn, cursor = connect_database()
    cursor.execute("select task_id,count(*) from task_person group by task_id")
    temp_data = cursor.fetchall()
    result = dict(temp_data)
    conn.close()
    return result


def init_option():
    iswork = get_sys_params(2)
    conn, cursor = connect_database()
    # 获得所有
    result = {}
    level2_level3 = {}
    result_all = []

    t = cal_begin()
    cursor.execute(
        "select level1,count(*) from task where isabandon=0 and iswork>=%s and stime>=DATE_SUB(%s, INTERVAL 7 DAY) group by level1 order by 2 desc", [iswork, t])
    for i in cursor:
        result_all.append({'value': i[0], 'label': i[0]})
        result[i[0]] = []

    cursor.execute("select name from sys_cfg where type = 'type'")
    for i in cursor:
        if i[0] not in result.keys():
            result_all.append({'value': i[0], 'label': i[0]})
            result[i[0]] = []

    # 得到近7天高频的
    cursor.execute(
        "select level1,level2,count(*) from task where isabandon=0 and iswork>=%s and stime>=%s group by level1,level2 order by 3 desc", [iswork, t])
    for row in cursor:
        result[row[0]].append(row[1])

    # 将多的加回来
    cursor.execute(
        "select level1,level2,count(*) from task where isabandon=0 and iswork>=%s group by level1,level2 order by 3 desc", [iswork])
    for row in cursor:
        if row[1] not in result[row[0]]:
            result[row[0]].append(row[1])

    # 得到近7天高频的
    cursor.execute(
        "select level2,level3,count(*) from task where isabandon=0 and iswork>=%s and stime>=%s group by level2,level3 order by 3 desc", [iswork, t])
    for row in cursor:
        if row[0] not in level2_level3:
            level2_level3[row[0]] = []
        level2_level3[row[0]].append(row[1])

    # 将多的加回来
    cursor.execute(
        "select level2,level3,count(*) from task where isabandon=0 and iswork>=%s group by level2,level3 order by 3 desc", [iswork])
    for row in cursor:
        if row[0] not in level2_level3:
            level2_level3[row[0]] = []
        else:
            if row[1] not in level2_level3[row[0]]:
                level2_level3[row[0]].append(row[1])

    cursor.execute("select value from sys_cfg where id=1")
    lastchecktime = cursor.fetchone()[0]

    dir = {}
    dir_all = []
    cursor.execute(
        "select skill_level1,skill_level2,sum(hours) from task_person_skill group by skill_level1,skill_level2 order by 3 desc; ")
    for row in cursor:
        if row[0] not in dir.keys():
            dir[row[0]] = [row[1]]
            dir_all.append({"value": row[0], "label": row[0]})
        else:
            dir[row[0]].append(row[1])

    conn.close()
    return [result, result_all, level2_level3, lastchecktime, dir, dir_all]


def cal_begin():
    return datetime.date.today() - datetime.timedelta(days=14)


def get_calendar_data_from_task():
    iswork = get_sys_params(2)
    conn, cursor = connect_database()
    today = datetime.date.today()
    start_time = datetime.datetime.strftime(cal_begin(), "%Y-%m-%d")
    end_time = datetime.datetime.strftime(today, "%Y-%m-%d")
    print(start_time, end_time)
    date_list = generate_date_list(start_time, end_time)

    finish_task = {}
    cursor.execute(
        "select  DATE_FORMAT(ftime,'%Y-%m-%d'),count(*) from task where isfinish =1 and isabandon=0 and iswork>=%s and ftime>=%s and status in (2,4)  group by  DATE_FORMAT(ftime,'%Y-%m-%d')", [iswork, start_time])
    for row in cursor:
        finish_task[row[0]] = row[1]
    # print(finish_task)

    todo_task = {}
    cursor.execute(
        "select  DATE_FORMAT(etime,'%Y-%m-%d'),count(*) from task where isfinish =1 and isabandon=0 and iswork>=%s and etime>=%s and etime<=%s group by  DATE_FORMAT(etime,'%Y-%m-%d')", [iswork, start_time, end_time])
    for row in cursor:
        todo_task[row[0]] = row[1]
    # print(todo_task)

    overdue_task = {}
    cursor.execute(
        "select  DATE_FORMAT(ftime,'%Y-%m-%d'),count(*) from task where isfinish =1 and status=4 and isabandon=0 and iswork>=0 group by  DATE_FORMAT(ftime,'%Y-%m-%d')")
    for row in cursor:
        overdue_task[row[0]] = row[1]
    # print(overdue_task)

    normal_task = {}
    cursor.execute(
        "select DATE_FORMAT(ftime,'%Y-%m-%d'),count(*) from task where isfinish =1 and isabandon=0  and etime=DATE_FORMAT(ftime,'%Y-%m-%d') and iswork>=%s and ftime>=%s group by DATE_FORMAT(ftime,'%Y-%m-%d')", [iswork, start_time])
    for row in cursor:
        normal_task[row[0]] = row[1]
    # print(normal_task)

    r = [start_time, end_time]

    result = []
    result_desc = []
    for i in date_list:
        if i not in finish_task:
            finish_task[i] = 0
        if i not in todo_task:
            todo_task[i] = 0
        if i not in overdue_task:
            overdue_task[i] = 0
        if i not in normal_task:
            normal_task[i] = 0
        result.append([i, finish_task[i]])
        result_desc.append([i, str(finish_task[i])+'/' +
                            str(todo_task[i]), str(overdue_task[i]) + '/' + str(normal_task[i]) + '/' + str(finish_task[i] - overdue_task[i] - normal_task[i])])
    # print(result)

    cursor.close()
    conn.close()
    return {'result': result, 'result_desc': result_desc, 'range': r}


def generate_date_list(datestart: str, dateend: str) -> list:
    datestart = datetime.datetime.strptime(datestart, '%Y-%m-%d')
    dateend = datetime.datetime.strptime(dateend, '%Y-%m-%d')
    date_list = []
    date_list.append(datestart.strftime('%Y-%m-%d'))
    while datestart < dateend:
        # 日期叠加一天
        datestart += datetime.timedelta(days=+1)
        # 日期转字符串存入列表
        date_list.append(datestart.strftime('%Y-%m-%d'))
    return date_list


def finish_task(task_id: int, finishtaskform: dict):
    conn, cursor = connect_database()
    # print(finishtaskform)
    # 格式化成2016-03-20 11:45:39形式
    ftime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    cursor.execute(
        "select etime,level1,level2,level3,project_id from task where task_id=%s", [task_id])
    temp = cursor.fetchone()
    etime = temp[0].strftime('%Y-%m-%d') + ' 23:59:59'
    level1 = temp[1]
    level2 = temp[2]
    level3 = temp[3]
    project_id = temp[4]
    etime_ts = time.mktime(time.strptime(etime, "%Y-%m-%d %H:%M:%S"))
    now_ts = time.time()
    if finishtaskform['desc'] == '':
        input_finish = '已完成'
    else:
        input_finish = finishtaskform['desc']
    if now_ts > etime_ts:
        status = 4
    else:
        status = 2
    if finishtaskform['isok']:
        is_score = 1
    else:
        is_score = 2
    cursor.execute("update task set ftime=%s ,isfinish=1,status=%s,hours=%s,is_score=%s where task_id =%s ", [
        ftime, status, finishtaskform['hours'], is_score, task_id])
    res = len(get_process_by_task_id(task_id))
    if res == 0:
        cursor.execute("insert into task_process (task_id,process_name,isfinish) values (%s,%s,1)", [
            task_id, input_finish])
    conn.commit()
    #   关闭所有进程
    cursor.execute(
        "update task_process set isfinish=1,ftime=now() where task_id=%s", [task_id])

    # TODO 规范前台命名skill_level1 skill_level2
    if finishtaskform['isok']:
        tp_skill = []
        for i in finishtaskform['dirtable']:
            for p in finishtaskform['peoples']:
                tp_skill.append(
                    [task_id, project_id, p['person_id'], i['dir'], i['sub_dir'], i['hours']])
        tps = [tuple(x) for x in tp_skill]
        cursor.executemany(
            "insert into task_person_skill (task_id,project_id,person_id,skill_level1,skill_level2,hours) values (%s,%s,%s,%s,%s,%s)", tps)
        conn.commit()
        tp_score = []
        for i in finishtaskform['peoples']:
            tp_score.append([task_id, project_id, i['person_id'], level1,
                             level2, level3, i['score_activity'], i['score_critical']])
        tps = [tuple(x) for x in tp_score]
        cursor.executemany(
            "insert into task_person_score (task_id,project_id,person_id,level1,level2,level3,score_activity,score_critical) values (%s,%s,%s,%s,%s,%s,%s,%s)", tps)
        conn.commit()
        cursor.execute(
            "update task_person_skill a inner join person b on a.person_id=b.person_id set a.company=b.company,a.department=b.department,a.post=b.post where a.task_id=%s", [task_id])
        cursor.execute(
            "update task_person_score a inner join person b on a.person_id=b.person_id set a.company=b.company,a.department=b.department,a.post=b.post where a.task_id=%s", [task_id])
        conn.commit()
    conn.commit()
    conn.close()
    temp = None
    if level2 == '出行' or level2 == '出差':
        temp = init_travel()

    # 完成项目相关信息的录入
    if level1 == '项目':
        update_project_by_task(task_id)

    return temp


def update_project_by_task(task_id):
    conn, cursor = connect_database()
    cursor.execute("select project_id from task where task_id =%s", [task_id])
    project_id = cursor.fetchone()[0]
    if project_id:
        cursor.execute(
            "select count(*) from task where project_id = %s", [project_id])
        task_count = cursor.fetchone()[0]
        cursor.execute(
            "select count(distinct person_id) from task a,task_person b where a.task_id=b.task_id  and  a.project_id = %s", [project_id])
        person_count = cursor.fetchone()[0]
        cursor.execute("update project set task_count=%s ,person_count =%s,update_time=now() where project_id=%s", [
                       task_count, person_count, project_id])
        conn.commit()
    conn.close()


def get_process_by_task_id(task_id: int) -> list:
    conn, cursor = connect_database()
    cursor.execute(
        "select DATE_FORMAT(stime,'%Y-%m-%d'),process_name,isfinish,process_id,task_id from task_process where task_id=%s order by 1 desc", [task_id])
    result = []
    for row in cursor:
        result.append({'stime': row[0], 'process_name': row[1],
                       'isfinish': row[2], 'process_id': row[3], 'task_id': row[4]})
    conn.close()
    return result


def delete_task_by_task_id(task_id: int):
    conn, cursor = connect_database()
    # 格式化成2016-03-20 11:45:39形式
    cursor.execute(
        "update task set isabandon=1,status=5 where task_id =%s ", [task_id])
    cursor.execute("delete from task_person where task_id=%s", [task_id])
    cursor.execute("delete from task_person_score where task_id=%s", [task_id])
    conn.commit()
    conn.close()


def query_task(query, level1, level2, ftime, query_duration, isstime, isqueryall, mode):
    # print(query, level1, level2, ftime,
    #       query_duration, isstime, isqueryall, mode)
    # print(ftime, '|', query_duration, '|', isstime)
    iswork = get_sys_params(2)
    conn, cursor = connect_database()
    query = '%'+query+'%'
    sql = "select task_id,level1,level2,level3,task_name,etime,stime,isfinish,status,project_id from task where isabandon=0 and task_name like %s"
    if not isqueryall:
        sql += " and isfinish = 0 "
    params_list = [query]
    if level1 != '':
        sql += " and level1=%s "
        params_list.append(level1)
    if level2 != '':
        sql += " and level2=%s "
        params_list.append(level2)
    # if ftime != '':
    #     sql += " and ftime like %s"
    #     ftime = '%'+ftime+'%'
    #     params_list.append(ftime)
    # if query_duration:
    if isstime and query_duration:
        sql += " and stime between %s and %s"
        params_list.extend(query_duration)
        if ftime != '' and ftime is not None:
            sql += " and ftime like %s"
            ftime = '%'+ftime+'%'
            params_list.append(ftime)
    if not isstime and ftime:
        sql += " and ftime like %s"
        ftime = '%'+ftime+'%'
        params_list.append(ftime)
    if not isstime and query_duration != []:
        sql += " and ftime between %s and %s"
        params_list.extend(query_duration)

    sql += " and iswork>=%s "
    params_list.append(iswork)
    if mode == 'graph':
        t = cal_begin()
        sql += " and (stime>=%s or status in (1,3) or etime>=%s)"
        params_list.append(t)
        params_list.append(t)
    sql += " order by etime,task_id"
    # print('*'*10)
    # print(sql)
    # print(params_list)
    cursor.execute(sql, params_list)
    # 得到所有进展清单
    process = count_task_process_number()
    person = count_task_person_number()
    result = []
    for row in cursor:
        temp = {'task_id': row[0], 'project_id': row[9], 'level1': row[1], 'level2': row[2], 'level3': row[3],
                'task_name': row[4], 'etime': row[5].strftime('%m-%d'), 'stime': row[6].strftime('%Y-%m-%d %H:%M:%S'), 'tetime': row[5].strftime('%Y-%m-%d'), 'isfinish': row[7], 'status': row[8]}
        if row[0] in process.keys():
            temp['num_process'] = process[row[0]]
        else:
            temp['num_process'] = ''
        if row[0] in person.keys():
            temp['num_person'] = person[row[0]]
        else:
            temp['num_person'] = ''
        # print(temp)
        result.append(temp)
    # temp = cursor
    conn.close()
    return result


# 只查询本周
def get_task_this_week():
    iswork = get_sys_params(2)
    conn, cursor = connect_database()
    sql = "select task_id,level1,level2,task_name,etime,stime,isfinish,status from task where isabandon=0 and iswork >= %s AND etime <= %s AND isfinish =0 order by etime"
    today = datetime.datetime.today()
    etime = datetime.datetime.strftime(
        today + datetime.timedelta(7 - today.weekday() - 1), "%Y-%m-%d")
    cursor.execute(sql, [iswork, etime])
    process = count_task_process_number()
    person = count_task_person_number()
    result = []
    for row in cursor:
        temp = {'task_id': row[0], 'level1': row[1], 'level2': row[2],
                'task_name': row[3], 'etime': row[4].strftime('%m-%d'), 'stime': row[5].strftime('%Y-%m-%d %H:%M:%S'), 'tetime': row[4].strftime('%Y-%m-%d'), 'isfinish': row[6], 'status': row[7]}
        if row[0] in process.keys():
            temp['num_process'] = process[row[0]]
        if row[0] in person.keys():
            temp['num_person'] = person[row[0]]
        result.append(temp)
    cursor.close()
    conn.close()
    return result


def get_task_by_type(level1, main, sub):
    conn, cursor = connect_database()
    if level1 == 'level1':
        sql = ' level1 = %s and level2=%s '
    else:
        sql = ' dir =%s and sub_dir=%s '
    cursor.execute("select task_name,hours,ftime,task_id from task where task_id in (select distinct task_id from task_person_score where " +
                   sql+") order by ftime desc ", [main, sub])
    res = []
    for i in cursor:
        res.append({'task_name': i[0], 'hours': i[1],
                    'ftime': i[2], "task_id": i[3]})
    return res


def update_task(task_id, level1, level2, level3, task_name, etime, status):
    conn, cursor = connect_database()
    # print(task_id, level2, task_name, etime)
    if status == 1 or status == 3:
        etime_ts = time.mktime(time.strptime(
            etime + ' 23:59:59', "%Y-%m-%d %H:%M:%S"))
        now_ts = time.time()
        if now_ts > etime_ts:
            status = 3
        else:
            status = 1
    cursor.execute(
        "insert into task_his  select *,now() from task where task_id=%s", [task_id])
    if level1 == '项目':
        new_project_id = get_project_id_by_level(level2, level3)
    else:
        new_project_id = None
    cursor.execute("update task set level1=%s, level2=%s ,level3=%s ,project_id=%s, task_name=%s , etime=%s,status=%s where task_id =%s ", [
        level1, level2, level3, new_project_id, task_name, etime, status, task_id])
    conn.commit()
    conn.close()


def get_project_id_by_level(level2, level3):
    conn, cursor = connect_database()
    if level3 is None:
        cursor.execute(
            "select project_id from project where level2=%s and level3 is null", [level2])
    else:
        cursor.execute(
            "select project_id from project where level2=%s and level3=%s ", [level2, level3])
    project_id = cursor.fetchone()[0]
    conn.close()
    return project_id


def get_project_id_by_task(task_id):
    conn, cursor = connect_database()
    cursor.execute(
        "select project_id from task where task_id=%s", [task_id])
    project_id = cursor.fetchone()[0]
    conn.close()
    return project_id


def get_bar_data_from_task():
    iswork = get_sys_params(2)
    t = cal_begin()
    conn, cursor = connect_database()
    cursor.execute(
        "select level1,level2,count(*) from task where iswork>=%s and (stime>=%s or status in (1,3) or ftime>=%s) group by level1,level2 order by 3,1,2", [iswork, t, t])
    yAxisdata = []
    for i in cursor:
        yAxisdata.append(i[0]+'-'+i[1])
    # print(yAxisdata)

    # 柱形堆叠图
    yAxistodo = {}
    yAxistodooverdue = {}
    yAxisnormal = {}
    yAxisoverdue = {}
    yAxisabandon = {}

    cursor.execute(
        "select level1,level2,status,count(*) from task where iswork>=%s and (stime>=%s or status in (1,3) or ftime>=%s)  group by level1,level2,status order by 1,2,3", [iswork, t, t])
    for i in cursor:
        if i[2] == 1:
            yAxistodo[i[0]+'-'+i[1]] = i[3]
        if i[2] == 2:
            yAxisnormal[i[0]+'-'+i[1]] = i[3]
        if i[2] == 3:
            yAxistodooverdue[i[0] + '-' + i[1]] = i[3]
        if i[2] == 4:
            yAxisoverdue[i[0]+'-'+i[1]] = i[3]
        if i[2] == 5:
            yAxisabandon[i[0] + '-' + i[1]] = i[3]

    yAxistodo_list = []
    yAxisnormal_list = []
    yAxisoverdue_list = []
    yAxistodooverdue_list = []
    yAxisabandon_list = []

    for level2 in yAxisdata:
        if level2 not in yAxistodo.keys():
            yAxistodo_list.append(0)
        else:
            yAxistodo_list.append(yAxistodo[level2])

        if level2 not in yAxisnormal.keys():
            yAxisnormal_list.append(0)
        else:
            yAxisnormal_list.append(yAxisnormal[level2])

        if level2 not in yAxisoverdue.keys():
            yAxisoverdue_list.append(0)
        else:
            yAxisoverdue_list.append(yAxisoverdue[level2])

        if level2 not in yAxistodooverdue.keys():
            yAxistodooverdue_list.append(0)
        else:
            yAxistodooverdue_list.append(yAxistodooverdue[level2])

        if level2 not in yAxisabandon.keys():
            yAxisabandon_list.append(0)
        else:
            yAxisabandon_list.append(yAxisabandon[level2])

    sum_todo = sum(yAxistodo_list)
    sum_normal = sum(yAxisnormal_list)
    sum_overdue = sum(yAxisoverdue_list)
    sum_todooverdue = sum(yAxistodooverdue_list)
    sum_abandon = sum(yAxisabandon_list)

    sum_task = sum_todo + sum_normal + sum_overdue + sum_todooverdue + sum_abandon

    if sum_task != 0:
        overdue_percent = round(
            (sum_overdue+sum_todooverdue) / sum_task * 100, 2)
        finish_percent = round((sum_overdue+sum_normal)/sum_task*100, 2)
    else:
        overdue_percent = 0
        finish_percent = 0

    cursor.execute(
        " select level1, count(*) from task where iswork>=%s and (stime>=%s or status in (1,3) or ftime>=%s) group by level1 order by 2 desc", [iswork, t, t])
    pie_type_data = []
    for i in cursor:
        pie_type_data.append({'name': i[0], 'value': i[1]})

    cursor.execute(
        "select iswork,count(*) from task  where iswork>=%s and (stime>=%s or status in (1,3) or ftime>=%s) group by iswork order by 2 desc", [iswork, t, t])
    pie_type_data_c = []
    for i in cursor:
        if i[0]:
            pie_type_data_c.append({'name': '工作', 'value': i[1]})
        else:
            pie_type_data_c.append({'name': '非工作', 'value': i[1]})

    # 饼图数据
    pie_summary_data = [{'value': sum_overdue, 'name': '逾期完成'}, {'value': sum_todooverdue, 'name': '待做逾期'}, {'value': sum_todo, 'name': '待做'}, {
        'value': sum_normal, 'name': '正常'}, {'value': sum_abandon, 'name': '作废'}]

    # pie_summary_data = sorted(
    #     pie_summary_data, key=lambda e: e.__getitem__('value'), reverse=True)
    # print(pie_summary_data)

    # 柱形堆叠图数据
    result = {'sum_task': sum_task, 'percent': [finish_percent, overdue_percent], 'yAxisdata': yAxisdata, 'yAxistodo_list': yAxistodo_list,
              'yAxisnormal_list': yAxisnormal_list, 'yAxisoverdue_list': yAxisoverdue_list, 'yAxistodooverdue_list': yAxistodooverdue_list,
              'yAxisabandon_list': yAxisabandon_list, 'pie_summary_data': pie_summary_data, 'pie_type_data': pie_type_data, 'pie_type_data_c': pie_type_data_c}
    # print('tongji')
    conn.close()
    return result


# #####################################
# 定义process的函数
# #####################################

# TODO 和下面的合并成一个函数吧
def reset_process_by_id(process_id):
    conn, cursor = connect_database()
    cursor.execute("update task_process set isfinish=0,ftime=now() where process_id=%s",
                   [process_id])
    conn.commit()
    conn.close()
    return True


def finish_process_by_id(process_id):
    conn, cursor = connect_database()
    cursor.execute("update task_process set isfinish=1,ftime=now() where process_id=%s",
                   [process_id])
    conn.commit()
    conn.close()
    return True


def add_task_process(task_id: int, process_name: str) -> bool:
    conn, cursor = connect_database()
    # TODO 判断是否完成
    cursor.execute("select isfinish from task where task_id=%s", [task_id])
    temp = cursor.fetchone()[0]
    stime = datetime.datetime.now()
    if temp:
        cursor.execute("insert into task_process (task_id,process_name,isfinish,stime) values (%s,%s,%s,%s)", [
            task_id, process_name, 1, stime])
    else:
        cursor.execute("insert into task_process (task_id,process_name,stime) values (%s,%s,%s)", [
            task_id, process_name, stime])
    conn.commit()
    conn.close()
    return True


def delete_process(process_id: int) -> bool:
    conn, cursor = connect_database()
    cursor.execute(
        "delete from task_process where process_id =%s", [process_id])
    conn.commit()
    conn.close()
    return True


def update_process(process_id, process_name):
    conn, cursor = connect_database()
    # etime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    cursor.execute("update task_process set process_name=%s where process_id=%s", [
        process_name, process_id])
    conn.commit()
    conn.close()
    return True


def get_process_count_by_task_id(task_id: int) -> dict:
    res = {}
    res['k'] = task_id
    conn, cursor = connect_database()
    cursor.execute(
        "select '%s' ,count(*) as c from task_process where task_id='%s' and isfinish=1" % (task_id, task_id))
    f = dict(cursor.fetchall())

    cursor.execute(
        "select '%s' , count(*) as c from task_process where task_id='%s' " % (task_id, task_id))
    a = dict(cursor.fetchall())
    res['num_process'] = str(f[str(task_id)]) + '/' + str(a[str(task_id)])
    conn.close()
    return res


def get_task_type_option():
    conn, cursor = connect_database()
    temp = []
    cursor.execute(
        "select distinct level1,level2 from task")
    for i in cursor:
        temp.append({'level1': i[0], 'subtype': i[1]})
    conn.commit()
    conn.close()
    return temp


# TODO 去重
def get_person_by_task_id(task_id):
    # TODO 加备注
    conn, cursor = connect_database()
    cursor.execute(
        "select person_id,AVG(score_activity) as all_activity,AVG(score_critical) as all_critical from task_person_score group by person_id")
    temp = cursor.fetchall()
    all_person_data = {}
    for row in temp:
        all_person_data[row[0]] = {
            "all_activity": round(row[1], 2), "all_critical": round(row[2], 2)}

    # TODO bug,没考虑3级的情况如何处理
    cursor.execute(
        "select person_id,AVG(score_activity) as sub_activity,AVG(score_critical) as sub_critical from task_person_score a , (select level1,level2 from task where  task_id=%s) b where a.level1=b.level1 and a.level2=b.level2 group by a.person_id;", [task_id])
    temp = cursor.fetchall()
    sub_person_data = {}
    for row in temp:
        sub_person_data[row[0]] = {
            "sub_activity": round(row[1], 2), "sub_critical": round(row[2], 2)}

    cursor.execute(
        "select person_id,company,person_name,person_py from person where person_id in (select person_id from task_person where task_id =%s)", [task_id])
    temp = cursor.fetchall()
    if 1 in sub_person_data.keys():
        res = [{"person_id": 0, "company": 'all', "person_name": myself_name, 'person_py': 'wtr', 'score_activity': "5", 'score_critical': "5",
                'all_activity': all_person_data[1]['all_activity'], 'all_critical': all_person_data[1]['all_critical'], 'sub_activity': sub_person_data[1]["sub_activity"], 'sub_critical': sub_person_data[1]["sub_critical"]}]
    else:
        res = [{"person_id": 0, "company": 'all', "person_name": myself_name, 'person_py': 'wtr', 'score_activity': "5", 'score_critical': "5",
                'all_activity': all_person_data[1]['all_activity'], 'all_critical': all_person_data[1]['all_critical'], 'sub_activity': 5, 'sub_critical': 5}]
    for row in temp:
        if row[0] in sub_person_data.keys():
            res.append({"person_id": row[0], "company": row[1], "person_name": row[2],
                        'person_py': row[3], 'score_activity': "2.5", 'score_critical': "2.5", 'all_activity': all_person_data[row[0]]['all_activity'], 'all_critical': all_person_data[row[0]]['all_critical'], 'sub_activity': sub_person_data[row[0]]["sub_activity"], 'sub_critical': sub_person_data[row[0]]["sub_critical"]})
        else:
            res.append({"person_id": row[0], "company": row[1], "person_name": row[2],
                        'person_py': row[3], 'score_activity': "2.5", 'score_critical': "2.5", 'all_activity': 5, 'all_critical': 5, 'sub_activity': 5, 'sub_critical': 5})
    conn.commit()
    conn.close()
    return {'arrays': res, 'num_person': len(res), 'task_id': task_id}


def get_sub_by_task_id(task_id):
    conn, cursor = connect_database()
    cursor.execute(
        "select skill_level1,skill_level2,hours from task_person_skill where person_id=0 and task_id=%s", [task_id])
    res = []
    for i in cursor:
        res.append({'dir': i[0], 'sub_dir': i[1], 'hours': i[2]})
    conn.commit()
    conn.close()
    return res


def add_task_person(task_id: int, person_id: list):
    conn, cursor = connect_database()
    project_id = get_project_id_by_task(task_id)
    cursor.execute(
        "select person_id from task_person where task_id=%s ", [task_id])
    temp = list(cursor.fetchall())
    temp = [x[0] for x in temp]
    newperson = [x for x in person_id if x not in temp]
    if newperson:
        for i in newperson:
            if i == 0:
                continue
            cursor.execute(
                "insert into task_person (task_id,project_id,person_id) values (%s,%s,%s)", [task_id, project_id, i])
    conn.commit()
    conn.close()
    return get_person_by_task_id(task_id)


def delete_person_by_task_id(task_id, person_id):
    conn, cursor = connect_database()
    cursor.execute(
        "delete from task_person where task_id=%s and  person_id=%s ", [task_id, person_id])
    conn.commit()
    conn.close()
    res = get_person_by_task_id(task_id)
    return res


# TODO 感觉这个有个bug，推荐不了相关的人
def get_recommended_person_by_type(level1, level2):
    conn, cursor = connect_database()
    cursor.execute(
        "select b.person_id,c.company,c.person_name,count(*) from task a,task_person b,person c where a.task_id=b.task_id and c.person_id=b.person_id and a.level1=%s and a.level2=%s group by b.person_id,c.company,c.person_name order by 4 desc limit 10; ", [level1, level2])
    personrecommend = []
    for i in cursor:
        personrecommend.append({'id': i[0], 'label': i[1]+'-' + i[2]})
    cursor.execute(
        "select person_id,company,person_name,person_py from person order by 4 ")
    person_list = []
    for i in cursor:
        person_list.append(
            {'key': i[0], 'label': i[1]+'-' + i[2], "person_py": i[3]})
    conn.close()
    return {"personrecommend": personrecommend, 'person_list': person_list}


def calculate_process_hours():
    conn, cursor = connect_database()
    progress_color = {'技术': 0, '领域': 33, '行业': 66, '技能': 100}
    cursor.execute(
        "select skill_level1,skill_level2,sum(hours) from task_person_skill where person_id =0 group by skill_level1,skill_level2 order by sum(hours) desc;")
    temp = []
    for i in cursor:
        temp.append([i[1], round(i[2], 2), progress_color[i[0]]])
    conn.commit()
    conn.close()
    return {'progress_data': temp}


def get_task_by_calendar():
    iswork = get_sys_params(2)
    start_time = '2021-01-01'
    conn, cursor = connect_database()
    finish_task = []
    cursor.execute(
        "select DATE_FORMAT(ftime,'%Y-%m-%d'),count(*) from task where isfinish =1 and isabandon=0 and iswork>=%s and ftime>=%s group by DATE_FORMAT(ftime,'%Y-%m-%d')", [iswork, start_time])
    temp_data = cursor.fetchall()
    for row in temp_data:
        finish_task.append([row[0], row[1]])
    conn.close()
    return {"calendar_data": finish_task}


def get_treemap_data_from_task():
    conn, cursor = connect_database()
    treemap_type_data = {}
    cursor.execute(
        "select level1,level2,sum(hours) from task where hours is not null and is_score=1 group by level1,level2 order by 1;")
    for row in cursor:
        if row[0] not in treemap_type_data.keys():
            treemap_type_data[row[0]] = {
                'name': row[0], 'value': 0, "children": []}
        treemap_type_data[row[0]]['value'] += row[2]
        treemap_type_data[row[0]]['children'].append(
            {'name': row[1], 'value': round(row[2], 2)})

    res_treemap_type_data = list(treemap_type_data.values())

    treemap_dir_data = {}
    cursor.execute(
        "select skill_level1,skill_level2,sum(hours) from task_person_skill where person_id=0 group by skill_level1,skill_level2 order by 1;")
    for row in cursor:
        if row[0] not in treemap_dir_data.keys():
            treemap_dir_data[row[0]] = {
                'name': row[0], 'value': 0, "children": []}
        treemap_dir_data[row[0]]['value'] += row[2]
        treemap_dir_data[row[0]]['children'].append(
            {'name': row[1], 'value': round(row[2], 2)})

    res_treemap_dir_data = list(treemap_dir_data.values())

    conn.close()
    return {'treemap_type_data': res_treemap_type_data, "treemap_dir_data": res_treemap_dir_data}


def get_sankey_data_from_task():
    conn, cursor = connect_database()
    nodes_depth = {'技术': 1, '行业': 2, '领域': 3, '技能': 4}
    nodes = []

    cursor.execute("select distinct level2 from task_person_skill;")
    for row in cursor:
        nodes.append({'name': row[0], 'depth': 0})

    cursor.execute(
        "select distinct skill_level1,skill_level2 from task_person_skill;")
    for row in cursor:
        nodes.append({'name': row[0]+'-'+row[1], 'depth': nodes_depth[row[0]]})

    # print(nodes)

    links = []
    # 考虑A到B、C、D
    cursor.execute(
        "select level2,skill_level1,skill_level2,sum(hours) from task_person_skill where person_id=0 GROUP BY level2,skill_level2;")
    for row in cursor:
        links.append(
            {"source": row[0], "target": row[1]+'-'+row[2], 'value': row[3]})

    # # 考虑B到C、D
    # cursor = cursor.execute(
    #     "select a.sub_dir,b.sub_dir,sum(a.hours) from task_person_score a,task_person_score b where a.task_id=b.task_id and a.dir='技术' and b.dir!='技术' GROUP BY a.sub_dir,b.sub_dir ;")
    # for row in cursor:
    #     links.append({"source": row[0], "target": row[1], 'value': row[2]})

    # # 考虑C到D
    # cursor = cursor.execute(
    #     "select a.sub_dir,b.sub_dir,sum(a.hours) from task_person_score a,task_person_score b where a.task_id=b.task_id and a.dir='领域' and b.dir='行业' GROUP BY a.sub_dir,b.sub_dir ;")
    # for row in cursor:
    #     links.append({"source": row[0], "target": row[1], 'value': row[2]})

    conn.close()
    return {"nodes": nodes, 'links': links}


if __name__ == "__main__":
    print('good')
