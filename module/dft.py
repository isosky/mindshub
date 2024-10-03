#!/usr/bin/python
# -*- coding: utf-8 -*-

import copy
from base.base import connect_database


def add_dft(dftform, isbyupdate):
    conn, cursor = connect_database()
    if not isbyupdate:
        cursor.execute(
            "insert into reading_dft (author,title,author_com,author_base,url_org,isread,tags,publish_time,reading_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)", [
                dftform['author'], dftform['title'], dftform['author_com'], dftform['author_base'], dftform[
                    'url_org'], dftform['isread'], ','.join(dftform['tags']), dftform['publish_time'], dftform['reading_time']
            ])
        conn.commit()
        cursor.execute("select max(doc_id) from reading_dft")
        doc_id = cursor.fetchone()[0]
    # 得到doc_id
    else:
        doc_id = dftform['doc_id']
    if len(dftform['appendixtable']) > 0:
        for i in dftform['appendixtable']:
            cursor.execute("insert into reading_dft_appendix (doc_id,tags,title,url) values (%s,%s,%s,%s)", [
                doc_id, ','.join(i['tags']), i['title'], i['url']])
        conn.commit()

    # 如果任务没完成
    if not dftform['isread']:
        return {"res": True}
    # 添加任务
    etime = dftform['reading_time'].split(' ')[0]
    cursor.execute("insert into task (level1,level2,task_name,etime,ftime,iswork,isfinish,status,is_score,hours) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", [
        '学习', 'dft', dftform['title'], etime, dftform['reading_time'], 1, 1, 2, 1, dftform['hours']])
    conn.commit()

    # 添加人员
    cursor.execute("select max(task_id) from task")
    task_id = cursor.fetchone()[0]
    cursor.execute("update reading_dft set task_id=%s where doc_id=%s",
                   [task_id, doc_id])
    cursor.execute("insert into task_person values (%s,%s)", [task_id, 0])
    cursor.execute("insert into task_process (task_id,process_name,isfinish) values (%s,%s,1)", [
        task_id, '已完成'])
    conn.commit()
    #   关闭所有进程
    cursor.execute(
        "update task_process set isfinish=1,ftime=now() where task_id=%s", [task_id])
    res = update_dft_task_process(
        cursor, task_id, dftform['tags'], dftform['hours'])
    conn.commit()
    conn.close()
    return {"res": res}


def delete_dft(doc_id):
    conn, cursor = connect_database()
    cursor.execute(
        "insert into reading_dft_his  select * from reading_dft where doc_id=%s;", [doc_id])
    conn.commit()
    cursor.execute("delete from reading_dft where doc_id=%s;", [doc_id])
    conn.commit()
    conn.close()
    return {"res": True}


def update_dft(dftform):
    # print(dftform)
    res = True
    conn, cursor = connect_database()
    doc_id = dftform['doc_id']
    # 根据isread情况来看怎么处理
    if dftform['isread']:
        temp = cursor.execute("select tags,task_id from reading_dft where doc_id = %s", [
            doc_id])
        tags_old, task_id = temp.fetchone()
        if task_id is None:
            add_dft(dftform=dftform, isbyupdate=True)
        else:
            temp_data = cursor.execute(
                "select hours from task where task_id = %s ", [task_id])
            hours_old = temp_data.fetchone()[0]
            #   如果时间和标签发生了修改
            if tags_old != dftform['tags'] or hours_old != dftform['hours']:
                cursor.execute("delete from task_person_score where task_id=%s",
                               [dftform['task_id']])
                conn.commit()
                res = update_dft_task_process(
                    cursor, dftform['task_id'], dftform['tags'], dftform['hours'])
                if hours_old != dftform['hours']:
                    cursor.execute("update task set hours=%s where task_id=%s",
                                   [dftform['hours'], task_id])

    #   修改链接数组，这个不管是否可以阅读，都可以进行修改
    if len(dftform['appendixtable']) > 0:
        cursor.execute('delete from reading_dft_appendix where doc_id=%s',
                       [doc_id])
        conn.commit()
        for i in dftform['appendixtable']:
            cursor.execute("insert into reading_dft_appendix (doc_id,tags,title,url) values (%s,%s,%s,%s)", [
                doc_id, i['tags'], i['title'], i['url']])
        conn.commit()

    dftform_new = copy.deepcopy(dftform)
    del dftform_new['doc_id']
    del dftform_new['task_id']
    del dftform_new['doc_appendix_num']
    del dftform_new['hours']
    del dftform_new['appendixtable']
    del dftform_new['tagslabel']
    dftform_new['tags'] = ','.join(dftform_new['tags'])
    for k, v in dftform_new.items():
        cursor.execute("update reading_dft set "+k +
                       " =%s where doc_id=%s", [v, doc_id])
    conn.commit()
    conn.close()
    return {"res": res}


def get_appendix_by_doc_id(doc_id):
    conn, cursor = connect_database()
    cursor = conn.cursor(named_tuple=True)
    temp = cursor.execute(
        "select * from reading_dft_appendix where doc_id=%s", [doc_id])
    res = []
    if temp:
        for i in temp:
            res.append(dict(i))
    conn.close()
    return res


def get_dft_data_unread_count():
    conn, cursor = connect_database()
    cursor.execute("select count(*) as c from reading_dft where isread=0;")
    data = cursor.fetchone()[0]
    conn.close()
    return data


def get_dft_option():
    conn, cursor = connect_database()
    res = []
    cursor.execute(
        "select skill_level1,sum(hours) from task_person_skill group by skill_level1 order by 2 desc")
    for i in cursor:
        res.append({"value": i[0], "label": i[0]})
    conn.commit()
    conn.close()
    return {"res": res}


def update_dft_task_process(cur, task_id, tags, hours):
    # 得到所有的类型
    res = True
    cur.execute(
        "select DISTINCT skill_level2 ,skill_level1 from task_person_skill order by 1;")
    sub_dict = {}
    for i in cur:
        sub_dict[i[0]] = i[1]
    now_tags = tags
    for i in now_tags:
        if i not in sub_dict.keys():
            res = False
            dir = None
        else:
            dir = sub_dict[i]
        cur.execute(
            "insert into task_person_score (task_id,person_id,level1,level2,skill_level1,skill_level2,hours,score_activity,score_critical) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)", [task_id, 0, '学习', 'dft', dir, i, hours, 5, 5])
    return res


def get_dft():
    conn, cursor = connect_database()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "select doc_id,count(*) as c from reading_dft_appendix group by doc_id")
    doc_appendix_num = {}
    for i in cursor:
        doc_appendix_num[i['doc_id']] = i['c']

    cursor.execute("select task_id,hours from task where level2='dft'")
    hours = {}
    for i in cursor:
        ti = dict(i)
        hours[ti['task_id']] = ti['hours']

    cursor.execute(
        "select * from reading_dft where is_delete=0 order by isread,add_time")
    res = []
    for i in cursor:
        temp_row = dict(i)
        if temp_row['doc_id'] in doc_appendix_num.keys():
            temp_row['doc_appendix_num'] = doc_appendix_num[temp_row['doc_id']]
        else:
            temp_row['doc_appendix_num'] = 0
        if temp_row['task_id']:
            temp_row['hours'] = hours[temp_row['task_id']]
        else:
            temp_row['hours'] = 0
        if temp_row['isread'] == 0:
            temp_row['isread'] = False
        else:
            temp_row['isread'] = True
        if temp_row['reading_time']:
            temp_row['reading_time'] = temp_row['reading_time'].strftime(
                '%Y-%m-%d')
        temp_row['appendixtable'] = get_appendix_by_doc_id(temp_row['doc_id'])
        temp_row['add_time'] = temp_row['add_time'].strftime(
            '%Y-%m-%d %H:%M:%S')
        temp_row['publish_time'] = temp_row['publish_time'].strftime(
            '%Y-%m-%d')
        temp_row['tagslabel'] = temp_row['tags']
        # print(temp_row['tags'])
        if temp_row['tags'] == '':
            temp_row['tags'] = []
        else:
            temp_row['tags'] = temp_row['tags'].split(",")

        res.append(temp_row)
    conn.close()
    return res
