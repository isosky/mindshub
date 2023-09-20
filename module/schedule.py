#!/usr/bin/python
# -*- coding: utf-8 -*-

import calendar
from base.base import connect_database, get_base_type
import datetime


# TODO 准备开始弄个新module
def add_schedule(type, sub_type, schedule_type, schedule_frequence, task_name):
    # sql = 'insert into schedule (type,sub_type,task_name) values (%s,%s,%s)'
    conn, cursor = connect_database()
    cursor.execute("insert into schedule (type,sub_type,task_name,schedule_type,schedule_frequence) values (%s,%s,%s,%s,%s)", [
        type, sub_type, task_name, schedule_type, schedule_frequence])
    s_id = cursor.lastrowid
    nexttime = get_next_schedule_time(schedule_type, schedule_frequence, None)
    # if schedule_type == 'month':
    cursor.execute("update schedule set nexttime=%s where schedule_id=%s", [
        nexttime, s_id])
    conn.commit()
    run_schedule(force=True)
    conn.close()
    return True


def get_next_schedule_time(schedule_type, schedule_frequence, nexttime):
    # 月任务的写法应该是
    # 1;3 每月1号，3号
    # 1:1;2:2 第一个周一，第二个周二
    # print(schedule_type, schedule_frequence, nexttime)
    day31s = [1, 3, 5, 7, 8, 10, 12]
    # 判断计算哪个时间
    if nexttime:
        n = max(datetime.date.today(), nexttime)
    else:
        n = datetime.date.today()
    # 得到下个月
    if n.month == 12:
        nn = n.replace(year=n.year+1)
        nn = nn.replace(month=1)
    else:
        if n.month == 1 and n.day > 28:
            nn = n.replace(day=28)
            nn = nn.replace(month=2)
        elif n.day >= 30 and (n.month+1 not in day31s):
            nn = n.replace(day=30)
            nn = nn.replace(month=n.month+1)
        else:
            nn = n.replace(month=n.month+1)
    if schedule_type == 'week':
        t_sf = schedule_frequence.split(',')
        t_sf = [int(x) for x in t_sf]
        temp = []
        for wd in t_sf:
            temp.extend(get_dates_by_weekday(n.year, n.month, wd))
            temp.extend(get_dates_by_weekday(nn.year, nn.month, wd))
    if schedule_type == 'month':
        t_sf = schedule_frequence.split(';')
        temp = []
        for wd in t_sf:
            if ':' not in wd:
                td = n.replace(day=int(wd))
                temp.append(td)
                td = nn.replace(day=int(wd))
                temp.append(td)
            else:
                weeks, days = wd.split(':')
                weeks = int(weeks)
                days = [int(x) for x in days.split(',')]
                for i in days:
                    alldd = get_dates_by_weekday(n.year, n.month, i)
                    allddnn = get_dates_by_weekday(nn.year, nn.month, i)
                    if weeks > 0:
                        temp.append(alldd[weeks-1])
                        temp.append(allddnn[weeks-1])
                    else:
                        temp.append(alldd[weeks])
                        temp.append(allddnn[weeks])
        # print(temp)
    temp = [x for x in temp if x > n]
    # print(td.strftime("%Y-%m-%d"))
    return min(temp)


def run_schedule(force=False):
    type_work = get_base_type()
    conn, cursor = connect_database()
    cursor.execute("select value from sys_cfg where id=1")
    lastcheck = cursor.fetchone()[0]
    print('last check time is : '+lastcheck)
    d = datetime.date.today().strftime("%Y-%m-%d")
    if d != lastcheck or force:
        cursor.execute(
            "select * from schedule where isabandon=0 and (lasttime is null or nexttime<DATE_ADD(NOW(), INTERVAL 10 DAY))")
        res = []
        for i in cursor:
            temp = {'schedule_id': i[0], 'type': i[1], 'sub_type': i[2], 'task_name': i[3],
                    'schedule_type': i[4], 'schedule_frequence': i[5], 'nexttime': i[8]}
            res.append(temp)
        # 添加定时任务
        for i in res:
            # print(i['subject'],i['subsub'],i['content'],i['nexttime']+' 00:00:00')
            cursor.execute("insert into task (type,sub_type,task_name,etime,iswork) values (%s,%s,%s,%s,%s)", [
                i['type'], i['sub_type'], i['task_name'], i['nexttime'], type_work[i['type']]])
            newtaskid = cursor.lastrowid
            cursor.execute("insert into schedule_task (schedule_id,task_id,etime) values (%s,%s,%s)", [
                i['schedule_id'], newtaskid, i['nexttime']])
            nexttime = i['nexttime'].strftime('%Y-%m-%d').split('-')
            nexttime = datetime.date(year=int(nexttime[0]), month=int(
                nexttime[1]), day=int(nexttime[2]))
            newnexttime = get_next_schedule_time(
                i['schedule_type'], i['schedule_frequence'], nexttime)
            cursor.execute("update schedule set nexttime =%s,lasttime = %s where schedule_id=%s", [
                newnexttime, d, i['schedule_id']])
            conn.commit()
        cursor.execute("update sys_cfg set value =%s where id=1", [d])
        cursor.execute(
            "update task set status=3 where etime<date(now()) and isfinish=0 and isabandon=0")
        conn.commit()

        cursor.execute(
            "insert into base_log (check_time) values (NOW())")
        conn.commit()
        # 删除无效的task
        # TODO 带验证删除
        # deleterows = removetask()
        conn.close()
        return {'status': 1, 'message': '新增了：'+str(len(res)) + '条计划任务，可查看详情。lastchecktime:' + lastcheck}
    else:
        return {'status': 0, 'message': '今日已检查', 'lastchecktime': lastcheck}


def get_dates_by_weekday(year, month, weekday):
    c = calendar.Calendar()
    monthcal = c.monthdatescalendar(year, month)
    result = [day for week in monthcal for day in week if day.weekday(
    ) == weekday-1 and day.month == month]
    # result = [day for week in monthcal for day in week if day.weekday(
    # ) == weekday-1 and day.month == month][weeks]
    return result


def delete_schedule(schedule_id):
    conn, cursor = connect_database()
    cursor.execute("update schedule set isdelete=1 where schedule_id=%s",
                   [schedule_id])
    conn.commit()
    conn.close()
    return True


def update_schedule(schedule_id, type, sub_type, schedule_type, schedule_frequence, task_name):
    conn, cursor = connect_database()
    netxtime = get_next_schedule_time(schedule_type, schedule_frequence, None)
    cursor.execute("update schedule set type=%s,sub_type=%s,schedule_type=%s,schedule_frequence=%s,nexttime=%s,task_name=%s where schedule_id=%s", [
        type, sub_type, schedule_type, schedule_frequence, netxtime, task_name, schedule_id])
    cursor.execute(
        "update task set task_name=%s where task_id in (select task_id from schedule_task where schedule_id=%s)", [task_name, schedule_id])
    conn.commit()
    conn.close()
    return True


def enable_schedule(schedule_id):
    conn, cursor = connect_database()
    # get schedule_type, schedule_frequence
    cursor.execute("select schedule_type, schedule_frequence from schedule where schedule_id=%s", [
        schedule_id])
    schedule_type, schedule_frequence = cursor.fetchone()
    nexttime = get_next_schedule_time(schedule_type, schedule_frequence, None)
    lasttime = datetime.datetime.strftime(
        datetime.datetime.today(), "%Y-%m-%d")
    cursor.execute("update schedule set isabandon=0,lasttime=%s,nexttime=%s where schedule_id=%s",
                   [lasttime, nexttime, schedule_id])
    conn.commit()
    conn.close()
    return True


def forbid_schedule(schedule_id):
    conn, cursor = connect_database()
    cursor.execute("update schedule set isabandon=1 where schedule_id=%s",
                   [schedule_id])
    conn.commit()
    conn.close()
    return True


def get_task_by_schedule_id(schedule_id):
    conn, cursor = connect_database()
    params_list = []
    sql = "select a.schedule_id,b.task_name,a.task_id,a.addtime,a.etime from schedule_task a,schedule b where a.schedule_id =b.schedule_id "
    if schedule_id != '':
        sql += ' and a.schedule_id=%s'
        params_list.append(schedule_id)
    sql += " order by a.addtime desc"
    cursor.execute(sql, params_list)
    res = []
    for i in cursor:
        temp = {'schedule_id': i[0], 'task_name': i[1],
                'task_id': i[2], 'addtime': i[3].strftime('%Y-%m-%d'), 'etime': i[4].strftime('%Y-%m-%d')}
        res.append(temp)
    # print(res)
    conn.close()
    return res


def get_schedule():
    conn, cursor = connect_database()
    cursor.execute(
        "select * from schedule where isdelete=0 order by schedule_id desc")
    res = []
    for i in cursor:
        if i[7]:
            lasttime = i[7].strftime('%Y-%m-%d')
        else:
            lasttime = i[7]
        temp = {'schedule_id': i[0], 'type': i[1], 'sub_type': i[2], 'task_name': i[3],
                'schedule_type': i[4], 'schedule_frequence': i[5], 'lasttime': lasttime, 'nexttime': i[8].strftime('%Y-%m-%d'), 'isabandon': i[9]}
        res.append(temp)
    conn.close()
    return res
