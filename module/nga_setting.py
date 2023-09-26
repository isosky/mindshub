#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.base import connect_database
from data_collector import nga_collector
from datetime import datetime, date


def add_nga_special_post(tid):
    '''
    添加nga特别关注的帖子，并立马抓取第一页
    '''
    conn, cursor = connect_database()
    # 判断一下是否已经添加过
    cursor.execute("select count(*) from nga_special_post where tid=%s", [tid])
    temp = cursor.fetchone()[0]
    if temp == 0:
        cursor.execute("insert into nga_special_post (tid) values (%s)", [tid])
        conn.commit()
        nga_collector.collect_nga_one_page(0, tid, 1, 1, special=True)
    else:
        cursor.execute("update nga_special_post set is_delete=0 where tid=%s", [tid])
        conn.commit()
        # TODO 考虑下再抓下一页
        # cursor.execute("select * from nga_post_page_list where status")
    conn.close()
    return 'ok'


def get_nga_specia_post():
    conn, cursor = connect_database(dictionary=True)
    cursor.execute("select a.tid,b.post_name from nga_special_post a left join nga_post b on a.tid=b.tid where is_delete=0")
    temp = cursor.fetchall()
    return temp


def get_nga_specia_user():
    conn, cursor = connect_database(dictionary=True)
    cursor.execute("select a.nga_user_id,b.nga_user_name from nga_special_user a left join nga_user b on a.nga_user_id=b.nga_user_id where is_delete=0")
    temp = cursor.fetchall()
    return temp


def delete_nga_special_post(tid):
    '''
    逻辑删除特别关注的帖子
    '''
    conn, cursor = connect_database()
    cursor.execute("update nga_special_post set is_delete=1 where tid=%s", [tid])
    conn.commit()
    conn.close()
    return 'ok'


def add_nga_special_user(nga_user_id):
    conn, cursor = connect_database()
    cursor.execute("select count(*) from nga_special_user where nga_user_id=%s", [nga_user_id])
    temp = cursor.fetchone()[0]
    if temp == 0:
        cursor.execute("insert into nga_special_user (nga_user_id) values (%s)", [nga_user_id])
    else:
        cursor.execute("update nga_special_user set is_delete=0,datetime=now() where nga_user_id=%s", [nga_user_id])
    conn.commit()
    conn.close()


def delete_nga_special_user(nga_user_id):
    conn, cursor = connect_database()
    cursor.execute("update nga_special_user set is_delete=1,datetime=now() where nga_user_id=%s", [nga_user_id])
    conn.commit()
    conn.close()
