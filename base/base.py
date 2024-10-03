#!/usr/bin/python
# -*- coding: utf-8 -*-

import mysql.connector
from base.config import dbconfig


def connect_database(dictionary=False):
    # 建立数据库连接
    conn = mysql.connector.connect(**dbconfig)
    # 创建游标对象
    cursor = conn.cursor(dictionary=dictionary)
    return conn, cursor


def login(user_name, user_pwd):
    conn, cursor = connect_database()
    # 执行查询语句
    sql = "SELECT * FROM user WHERE user_name = %s"
    value = (user_name,)
    cursor.execute(sql, value)
    # 获取查询结果
    result = cursor.fetchall()
    # 关闭游标和数据库连接
    cursor.close()
    conn.close()
    if len(result) > 0:
        return True
    else:
        return False


def get_sys_params(id: int):
    conn, cursor = connect_database()
    cursor.execute("select value from sys_cfg where id=%s", [id])
    temp_data = cursor.fetchall()
    iswork = temp_data[0][0]
    cursor.close
    conn.close()
    return iswork


def add_base_type(typename, typevalue):
    conn, cursor = connect_database()
    cursor.execute("insert into sys_cfg ('type','name','value') values ('type',%s,%s)", [
        typename, typevalue])
    conn.commit()
    conn.close()


def delete_base_type(typeid):
    conn, cursor = connect_database()
    cursor.execute("delete from sys_cfg where id=%s", [typeid])
    conn.commit()
    conn.close()


def get_base_type():
    conn, cursor = connect_database()
    temp_dict = {}
    cursor.execute("select name,value from sys_cfg where type='type'")
    temp_data = cursor.fetchall()
    for i in temp_data:
        temp_dict[i[0]] = int(i[1])
    cursor.close()
    conn.close()
    return temp_dict


def get_homepage():
    conn, cursor = connect_database()
    cursor.execute("select value from sys_cfg where id=4")
    i = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return {'firstpage': i}


def getnodirdata():
    conn, cursor = connect_database()
    temp = []
    cursor.execute(
        "select skill_level1,skill_level2 from task_person_skill where skill_level1 is null")
    for i in cursor:
        temp.append({'dir': i[0], 'sub_dir': i[1]})
    conn.commit()
    conn.close()
    return temp


def get_task_type():
    conn, cursor = connect_database()
    temp = []
    cursor.execute(
        "select id, name,value from sys_cfg where type = 'type'")
    for i in cursor:
        temp.append({'type_id': i[0], 'name': i[1], 'value': i[2]})
    conn.commit()
    conn.close()
    return temp


def setfirstpage(fp):
    conn, cursor = connect_database()
    cursor.execute("update sys_cfg set value =%s where id=4", [fp])
    conn.commit()
    conn.close()


def setiswork(isw):
    conn, cursor = connect_database()
    c = conn.cursor()
    if isw == True:
        isw = 1
    else:
        isw = 0
    cursor.execute("update sys_cfg set value = %s where id=2", [isw])
    conn.commit()
    conn.close()


def updatedir(sub_dir, new_dir_type):
    conn, cursor = connect_database()
    cursor.execute("update task_person_score set dir=%s where sub_dir=%s and dir is null", [
        new_dir_type, sub_dir])
    conn.commit()
    conn.close()


def update_sub_type(typenow, old_sub_type, new_sub_type):
    conn, cursor = connect_database()
    temp = cursor.execute("select task_id from task where type=%s and sub_type=%s ", [
        typenow, old_sub_type])
    temp_task_ids = list(temp)
    cursor.executemany(
        "insert into task_his  select *,datetime('now','localtime') from task where task_id=%s", temp_task_ids)
    cursor.execute("update task set sub_type=%s where type=%s and sub_type=%s", [
        new_sub_type, typenow, old_sub_type])
    cursor.execute("update task_person_score set sub_type=%s where type=%s and sub_type=%s", [
        new_sub_type, typenow, old_sub_type])
    conn.commit()
    conn.close()
