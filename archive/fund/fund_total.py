#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.base import connect_database
from datetime import date, datetime
import math
import json


def get_fund_total_data(getfry=False, getall=False):
    conn, cursor = connect_database()
    cursor = conn.cursor(dictionary=True)
    # TODO 具体前台的交互，还得重新设计下
    cursor.execute(
        "select fund_code,GROUP_CONCAT(fund_label,'|') as label from (select fund_code,fund_label  from fund_label where fund_code is not null order by fund_label ) g group by fund_code")
    labels = {}
    for i in cursor:
        labels[i['fund_code']] = i['label'][:-1]
    if getall:
        cursor.execute(
            "select * from fund_total order by cumulative_profit desc")
    else:
        cursor.execute(
            "select * from fund_total where holding_fraction>0 order by cumulative_profit desc")
    temp = cursor.fetchall()
    res = []
    for i in temp:
        if i['cost'] is None and not getall:
            continue
        if i['fund_code'] in labels.keys():
            fl = labels[i["fund_code"]]
        else:
            fl = '未分类'

        res.append({'fund_name': i['fund_name'], 'fund_code': i['fund_code'], 'holding_amount': i['holding_amount'], "yesterday_profit": i['yesterday_profit'],
                    "cumulative_profit": i['cumulative_profit'], "holding_return_rate": i['holding_return_rate'],
                    "holding_profit": i['holding_profit'], "cost": i['cost'], "holding_fraction": round(i['holding_fraction'], 2), "fund_label": fl})
    res.sort(key=lambda x: x["cumulative_profit"], reverse=True)
    if getfry:
        cursor.execute(
            "select fund_code from fund_operation_label where operation_label = '榜一'")
        temp = cursor.fetchall()
        fry_code = []
        for i in temp:
            fry_code.append(i['fund_code'])
        fry_res = []
        for i in res:
            if i['fund_code'] in fry_code:
                fry_res.append(i)
        return fry_res
    conn.close()
    return res
