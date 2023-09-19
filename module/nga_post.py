#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.base import connect_database
from datetime import datetime, date


def getposttabledata(rowcount=100, offset=0):
    conn, cursor = connect_database(dictionary=True)
    cursor.execute("select tid from nga_special_post")
    sdata = cursor.fetchall()
    sdata = {str(x['tid']): '' for x in sdata}
    cursor.execute(
        "select a.*,v.mrt,ifnull(nu.nga_user_name,a.nga_user_id) as user_name from nga_post a left join v_mrt v on a.tid=v.tid left join nga_user nu on a.nga_user_id=nu.nga_user_id order by v.mrt  desc limit %s offset %s;", [rowcount, offset])
    temp = cursor.fetchall()
    for i in temp:
        if i['tid'] in sdata:
            i['st'] = 'special'
        else:
            i['st'] = ''
        for k, v in i.items():
            if isinstance(v, date):
                i[k] = v.strftime('%Y-%m-%d')
            if isinstance(v, datetime):
                i[k] = v.strftime('%Y-%m-%d %H:%M:%S')
    return temp


def getreplytabledata(tid):
    conn, cursor = connect_database(dictionary=True)
    cursor.execute("select nga_user_id from nga_special_user")
    sdata = cursor.fetchall()
    sdata = {str(x['nga_user_id']): '' for x in sdata}
    cursor.execute(
        "select a.*,ifnull(nu.nga_user_name,a.nga_user_id) as nga_user_name from nga_post_reply a left join nga_user nu on a.nga_user_id=nu.nga_user_id where a.tid=%s order by a.reply_time;", [tid])
    temp = cursor.fetchall()
    for i in temp:
        if str(i['nga_user_id']) in sdata:
            i['st'] = 'special'
        else:
            i['st'] = ''
        for k, v in i.items():
            if isinstance(v, date):
                i[k] = v.strftime('%Y-%m-%d')
            if isinstance(v, datetime):
                i[k] = v.strftime('%Y-%m-%d %H:%M:%S')
    return temp
