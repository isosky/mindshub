#!/usr/bin/python
# -*- coding: utf-8 -*-
from base.base import connect_database


def get_fund_estimate_data():
    # 获取标签
    conn, cursor = connect_database()
    cursor.execute(
        "select fund_code,GROUP_CONCAT(fund_label,'|') as label from (select fund_code,fund_label from fund_label where fund_code is not null order by fund_label ) g group by fund_code")
    labels = {}
    for i in cursor:
        labels[i[0]] = i[1][:-1]
    cursor.execute(
        "select fund_code,operation_label from fund_operation_label")
    c_lables = {}
    for i in cursor:
        c_lables[i[0]] = i[1]
    cursor.execute(
        "select fund_code,max(net_value_time) as mgz from fund_gz_detail GROUP BY fund_code;")
    gzdict = {}
    for i in cursor:
        gzdict[i[0]] = i[1]
    res = []
    cursor.execute(
        "select fund_name,fund_code,holding_fraction,yesterday_net_value,holding_amount,holding_profit,holding_return_rate from fund_total where holding_fraction>0;")
    temp_key = ['fund_name', 'fund_code', "holding_fraction", "yesterday_net_value",
                "holding_amount", "holding_profit", "holding_return_rate", 'net_value_estimate', 'net_change', "fund_label", "operation_label", "net_value_time", 'estimate_profit']
    temp_list = cursor.fetchall()
    for i in temp_list:
        # print(i)
        if i[1] in gzdict:
            # TODO 优化下，不要查询这么多
            cursor.execute("select net_value_estimate from fund_gz_detail where fund_code=%s and net_value_time=%s", [
                i[1], gzdict[i[1]]])
            now = cursor.fetchone()[0]
            temp_today = round((now-i[3])*i[2], 2)
            temp_percent = round((now-i[3])/i[3]*100, 2)
            et = gzdict[i[1]].strftime('%Y-%m-%d %H:%M:%S')
        else:
            now = '-'
            temp_today = '-'
            temp_percent = '-'
            et = '-'
        if i[1] in labels:
            fl = labels[i[1]]
        else:
            fl = '未分类'
        if i[1] in c_lables:
            cfl = c_lables[i[1]]
        else:
            cfl = '未分类'
        s = list(i)
        s.extend([now, temp_percent, fl, cfl, et, temp_today])
        res.append(dict(zip(temp_key, s)))
    conn.close()
    return res


def getestimatebuydata(fund_code):
    conn, cursor = connect_database()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("select DATE_FORMAT(trade_time, '%Y-%m-%d') as trade_time,order_amount,remain_volume from fund_orders where remain_volume>0 and fund_code=%s order by trade_time desc", [
        fund_code])
    temp = cursor.fetchall()
    cursor.execute(
        "select net_value_time,net_rate from fund_gz_detail where date(net_value_time) in (select max(date(net_value_time)) from fund_gz_detail where fund_code=%s) and fund_code=%s order by net_value_time", [fund_code, fund_code])
    y_axis = []
    x_axis = []
    for i in cursor:
        y_axis.append(i['net_value_time'].strftime('%Y-%m-%d %H:%M:%S'))
        x_axis.append(i['net_rate'])
    conn.close()
    return temp, y_axis, x_axis
