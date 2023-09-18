#!/usr/bin/python
# -*- coding: utf-8 -*-

from pypinyin import Style, pinyin
from base.base import connect_database


def delete_person(personid):
    conn, cursor = connect_database()
    cursor.execute("delete from person where person_id=%s", [personid])
    conn.commit()
    conn.close()


def get_company():
    conn, cursor = connect_database()
    company_selector = []
    cursor.execute(
        "select company,count(*) from person group by company order by 2 desc")
    for i in cursor:
        company_selector.append({'value': i[0], 'label': i[0]})

    person_post_selector = []
    cursor.execute(
        "select person_post,count(*) from person group by person_post order by 2 desc")
    for i in cursor:
        person_post_selector.append({'value': i[0], 'label': i[0]})
    conn.close()
    return {"person_post_selector": person_post_selector, "company_selector": company_selector}


def get_person():
    conn, cursor = connect_database()
    temp = []
    cursor.execute(
        "select person_id,company,person_name,person_post,person_py from person order by 5 ")
    for i in cursor:
        temp.append({'person_id': i[0], 'company': i[1],
                     'person_name': i[2], 'person_post': i[3], 'person_py': i[4]})
    conn.commit()
    conn.close()
    return temp


def get_person_count():
    conn, cursor = connect_database()
    temp = []
    cursor.execute(
        "select a.person_id,company,person_name,count(*) from person a left join task_person b  on a.person_id=b.person_id group by a.person_id,a.company,a.person_name order by 4 desc")
    for i in cursor:
        temp.append({'value': i[0], 'label': i[1]+'-' + i[2]})
    conn.commit()
    conn.close()
    return temp


def add_person(company, person_name, person_post, force: bool):
    try:
        conn, cursor = connect_database()
        cursor.execute("select count(*) from person where company=%s and person_name=%s", [company, person_name])
        temp = cursor.fetchone()[0]
        if temp > 0 and not force:
            return {"msg": '姓名重复'}
        content = pinyin(person_name, style=Style.FIRST_LETTER)
        person_py = ''.join([x[0] for x in content])
        cursor.execute("insert into person (company,person_name,person_post,person_py) values (%s,%s,%s,%s)", [
            company, person_name, person_post, person_py])
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        return {"msg": False}
    return {"msg": True}


def get_scatter_data_from_task(type: str, sub_type: str, person_id: int) -> dict:
    conn, cursor = connect_database()
    sql = ''
    if person_id != '':
        sql = 'and c.person_id = ' + str(person_id)+' '
    else:
        if type != '':
            sql = sql + " and a.type = '"+type+"'"
        if sub_type != '':
            sql = sql + " and a.sub_type = '"+sub_type+"'"
    scatter_data = {}
    # print("select person_id,avg(a.score_activity) as score_activity,avg(a.score_critical) as score_critical from task_person_score a,task b where a.task_id=b.task_id  " +
    #       sql + " and a.person_id!=0 group by person_id")
    # TODO 这个地方对删除的任务没做判断，后续需要加上
    if person_id == '':
        # 如果没有指定人，就差近30天了，指定了人就查这个人的所有了
        temp_sql = "select c.person_name,avg(a.score_activity) as score_activity,avg(a.score_critical) as score_critical from task_person_score a,task b,person c where b.ftime>DATE_SUB(now(), INTERVAL 30 DAY) and a.person_id = c.person_id and  a.task_id=b.task_id  " + \
            sql + " and a.person_id!=0 group by a.person_id"
    else:
        temp_sql = "select a.type||'-'||a.sub_type as type_name,avg(a.score_activity) as score_activity,avg(a.score_critical) as score_critical from task_person_score a,task b,person c where a.person_id = c.person_id and  a.task_id=b.task_id  " + \
            sql + " and a.person_id!=0 group by a.person_id,a.type,a.sub_type"
    # print(temp_sql)
    cursor.execute(temp_sql)
    for i in cursor:
        if i[0] not in scatter_data.keys():
            scatter_data[i[0]] = {
                'name': i[0],
                'datas': []
            }
        scatter_data[i[0]]['datas'].append(
            [round(i[1]-5, 2), round(i[2]-5, 2)])
    conn.close()
    return {'scatter_data': scatter_data}
