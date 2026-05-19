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
        "select DATE_FORMAT(a.fund_review_time, '%Y-%m-%d') from fund_review a,fund_label b where a.funder_id=b.fund_label and a.fund_code is null and b.fund_code=%s", [fund_code])
    temp = cursor.fetchall()
    temp_label = [x[0] for x in temp]
    temp_one_label = cursor.execute(
        "select fund_code,GROUP_CONCAT(fund_label,'|') as label from fund_label where fund_code =%s group by fund_code", [fund_code])
    temp_one_label = temp_one_label.fetchone()
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
