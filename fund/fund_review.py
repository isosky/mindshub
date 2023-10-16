#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.base import connect_database
from datetime import date, datetime
import math
import json


def getfundcalendar(fund_code):
    conn, cursor = connect_database()
    temp_bs = {}
    cursor.execute(
        "select trade_time,transaction_type,transaction_methods from fund_orders where fund_code=%s", [fund_code])
    for i in cursor:
        if i[1] == '0':
            bs_label = '卖'
        elif i[2] == 'w':
            bs_label = '定投'
        else:
            bs_label = '买'
        if i[0] not in temp_bs.keys():
            temp_bs[i[0]] = bs_label
        else:
            temp_bs[i[0]] = temp_bs[i[0]]+'|'+bs_label

    cursor.execute(
        "select DATE_FORMAT(fund_review_time, '%Y-%m-%d') from fund_review where fund_code=%s and funder_id=1", [fund_code])
    temp = cursor.fetchall()
    temp_review = [x[0] for x in temp]
    cursor.execute(
        "select DATE_FORMAT(a.fund_review_time, '%Y-%m-%d') from fund_review a,fund_label b where a.fund_label = b.fund_label and a.fund_code is null and b.fund_code=%s", [fund_code])
    temp = cursor.fetchall()
    temp_label = [x[0] for x in temp]
    temp_one_label = cursor.execute(
        "select fund_code,GROUP_CONCAT(fund_label,'|') as label from fund_label where fund_code =%s group by fund_code", [fund_code])
    temp_one_label = cursor.fetchone()
    if temp_one_label:
        temp_one_label = temp_one_label[1]
    else:
        temp_one_label = '未分类'
    temp_label_all = {}
    for i in temp_review:
        if i in temp_label:
            temp_label_all[i] = '复|点'
        else:
            temp_label_all[i] = '复'
    temp_label = [x for x in temp_label if x not in temp_review]
    for i in temp_label:
        temp_label_all[i] = '点'

    cursor.execute(
        "select DATE_FORMAT(net_value_date, '%Y-%m-%d'),equity_return from fund_net_history where fund_code=%s order by net_value_date desc ", [fund_code])
    temp_fund_time = []
    res = []
    bs_list = []
    for i in cursor:
        temp_fund_time.append(i[0])
        res.append([i[0], i[1]])
        if i[0] in temp_bs.keys():
            if i[0] in temp_label_all:
                bs_list.append([i[0], temp_bs[i[0]], temp_label_all[i[0]]])
            else:
                bs_list.append([i[0], temp_bs[i[0]], ''])
        else:
            if i[0] in temp_label_all:
                bs_list.append([i[0], '', temp_label_all[i[0]]])
            else:
                bs_list.append([i[0], '', ''])
    for i in temp_label_all.keys():
        if i not in temp_fund_time:
            res.append([i, 0])
            bs_list.append([i, '', temp_label_all[i]])
    cursor.execute(
        "select cumulative_profit,holding_profit,holding_return_rate from fund_total where fund_code=%s", [fund_code])
    temp_sform = cursor.fetchone()
    sform = {'earn_history': temp_sform[0],
             'earn_sum': temp_sform[1], 'earn_percent': temp_sform[2], 'fund_label': temp_one_label}
    cursor.close()
    conn.close()
    return res, bs_list, sform


def getfundcalendarbyauthor():
    conn, cursor = connect_database()
    cursor.execute(
        "select fund_review_time from fund_review where funder_id=1")
    temp = cursor.fetchall()
    temp_review = [x[0] for x in temp]
    cursor.execute(
        "select fund_review_time from fund_review where funder_id !=1")
    temp = cursor.fetchall()
    temp_comment = [x[0] for x in temp]
    temp_label_all = {}
    for i in temp_review:
        if i in temp_comment:
            temp_label_all[i] = '复|点'
        else:
            temp_label_all[i] = '复'
    temp_comment_extra = [x for x in temp_comment if x not in temp_review]
    for i in temp_comment_extra:
        temp_label_all[i] = '点'

    # TODO 这个为啥固定要001887
    cursor.execute(
        "select DATE_FORMAT(fund_time, '%Y-%m-%d'),equity_return from fund_net_history where fund_code='001887' order by fund_time desc ")
    temp_fund_time = []
    res = []
    bs_list = []
    for i in cursor:
        temp_fund_time.append(i[0])
        res.append([i[0], 0])
        if i[0] in temp_label_all.keys():
            bs_list.append([i[0], '', temp_label_all[i[0]]])
        else:
            bs_list.append([i[0], '', ''])
    for i in temp_label_all.keys():
        if i not in temp_fund_time:
            res.append([i, 0])
            bs_list.append([i, '', temp_label_all[i]])
    cursor.close()
    conn.close()
    return res, bs_list, []


def getreviewtabledata(fund_code, fund_review_time):
    conn, cursor = connect_database(dictionary=True)
    if fund_code != '':
        cursor.execute("select a.*,b.apps,b.isfirm,b.funder_name,0 as isind,b.funder_id from fund_review a,fund_funder b  where a.funder_id=b.funder_id and a.fund_code=%s and a.fund_review_time=%s",
                       [fund_code, fund_review_time])
        temp = cursor.fetchall()
    else:
        cursor.execute("select a.*,b.apps,b.isfirm,b.funder_name,0 as isind,b.funder_id from fund_review a,fund_funder b  where a.funder_id=b.funder_id and a.fund_review_time=%s",
                       [fund_review_time])
        temp = cursor.fetchall()
    cursor.execute("select a.fund_label,c.apps,a.fr_id,b.fund_code,b.fund_name,a.fund_review,a.fund_review_attitude,a.fund_review_time,a.funder_id,c.funder_name,c.isfirm,a.operation,1 as isind from fund_review a,fund_label b,fund_funder c where a.funder_id=c.funder_id and  a.fund_label = b.fund_label and a.fund_code is null and b.fund_code=%s and a.fund_review_time=%s;",
                   [fund_code, fund_review_time])
    temp_ind = cursor.fetchall()
    temp.extend(temp_ind)
    for i in temp:
        for k, v in i.items():
            if isinstance(v, date):
                i[k] = v.strftime('%Y-%m-%d')
            if isinstance(v, datetime):
                i[k] = v.strftime('%Y-%m-%d %H:%M:%S')
    conn.close()
    return temp


def getfunder():
    conn, cursor = connect_database()
    cursor.execute(
        "select funder_id,funder_name,apps,isfirm from fund_funder where funder_id!=1")
    res = []
    res1 = {}
    for i in cursor:
        if i[3] == 0:
            label = i[2]+'-' + i[1]+'-' + '非'
        else:
            label = i[2]+'-' + i[1]+'-' + '实盘'
        res.append({"value": i[0], "label": label})
        res1[i[0]] = label
    conn.close()
    return res, res1


def getfundlabel():
    conn, cursor = connect_database()
    cursor.execute("select distinct fund_label from fund_label")
    res = []
    for i in cursor:
        res.append({"value": i[0], "label": i[0]})
    conn.close()
    return res


# TODO 看是否合并
def commitfunderreview(funderreviewform, user_name='w'):
    conn, cursor = connect_database()
    temp_fund_label = []
    for i in funderreviewform['funder_table']:
        if 'fr_id' not in i.keys():
            cursor.execute("insert into fund_review (fund_review_time,fund_label,fund_review,fund_review_attitude,operation,funder_id) values (%s,%s,%s,%s,%s,%s)",
                           [funderreviewform['fund_review_time'], i['fund_label'], i['fund_review'], i['fund_review_attitude'], i['operation'],
                            funderreviewform['funder_id']])
            conn.commit()
        else:
            # cursor.execute("insert into fund_review_his (fr_id_old,fund_code,fund_name,fund_review_time,fund_review,fund_review_attitude,funder_id,user_name,operation,fund_label) select fr_id,fund_code,fund_name,fund_review_time,fund_review,fund_review_attitude,funder_id,user_name,operation,fund_label from fund_review where fr_id=%s", [
            #     i['fr_id']])
            # conn.commit()
            cursor.execute("update fund_review set fund_label=%s,fund_review=%s,fund_review_attitude=%s,operation=%s where fr_id=%s",
                           [i['fund_label'], i['fund_review'], i['fund_review_attitude'], i['operation'], i['fr_id']])
            conn.commit()
        temp_fund_label.append(i['fund_label'])
    temp = getfundlabel()
    temp = [x['value'] for x in temp]
    new_label = [x for x in temp_fund_label if x not in temp]
    if new_label != []:
        for i in new_label:
            cursor.execute("insert into fund_label (fund_label) values (%s)", [i])
        conn.commit()
    conn.close()


def getfunderreview(funder_id):
    conn, cursor = connect_database()
    cursor.execute(
        "select fr_id,fund_review_time,fund_label,fund_review,fund_review_attitude,operation from fund_review where funder_id=%s", [funder_id])
    res = {}
    for i in cursor:
        temp_date = i[1].strftime("%Y-%m-%d")
        if temp_date not in res.keys():
            res[temp_date] = [{"fr_id": i[0], "fund_label":i[2], "fund_review":i[3],
                               "fund_review_attitude":i[4], "operation":i[5]}]
        else:
            res[temp_date].append({"fr_id": i[0], "fund_label": i[2], "fund_review": i[3],
                                   "fund_review_attitude": i[4], "operation": i[5]})
    conn.close()
    return res
