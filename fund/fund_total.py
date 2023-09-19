#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.base import connect_database
import math


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
    # print(res)

    if getfry:
        cursor.execute("select fund_code from fund_operation_lable where operation_label = '榜一'")
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


def get_fund_remain_chart_data(fund_code):
    conn, cursor = connect_database()
    xaxis = []
    xaxisdata = []
    yaxisdata = []
    cursor.execute(
        "select order_date,remain_volume,unit_net_value from fund_orders where remain_volume >0 and fund_code = %s order by order_date", [fund_code])
    temp = cursor.fetchall()
    if temp == []:
        return "404"
    for i in temp:
        xaxis.append(i[0].strftime("%Y-%m-%d"))
        xaxisdata.append(i[1])
        yaxisdata.append(i[2])

    cursor.execute(
        "select net_value from fund_net_history where fund_code=%s order by net_value_date desc limit 1;", [fund_code])
    ymarkline = cursor.fetchone()[0]
    xmax = math.ceil(max(xaxisdata)*1.1/10)*10
    xinterval = round(xmax/5, 2)
    ymin = math.floor(round(min(ymarkline, min(yaxisdata))*0.9, 2)*10)/10
    ymax = round(max(ymarkline, max(yaxisdata))*1.1, 2)
    yinterval = 0.02
    while ymax >= ymin+yinterval*5:
        yinterval += 0.02
    yinterval = round(yinterval, 2)
    ymax = round(ymin+yinterval*5, 2)
    conn.close()
    details = get_fund_remain_detail(fund_code=fund_code)
    return {"xaxis": xaxis, "xaxisdata": xaxisdata, "yaxisdata": yaxisdata, "ymarkline": ymarkline, "xmax": xmax, "xinterval": xinterval, "ymin": ymin, "ymax": ymax, "yinterval": yinterval, "details": details}


def get_fund_remain_detail(fund_code):
    conn, cursor = connect_database()
    cursor.execute(
        "select net_value from fund_net_history where fund_code=%s order by net_value_date desc limit 1;", [fund_code])
    ymarkline = cursor.fetchone()[0]
    cursor.execute(
        "select cost from fund_total where fund_code=%s;", [fund_code])
    cost = cursor.fetchone()[0]
    # 取出所有的remain
    cursor.execute(
        "select order_date,remain_volume from fund_orders where remain_volume>0 and fund_code=%s order by trade_time;", [fund_code])
    res = {}
    res_remain_share = 0
    res_sum = 0
    res_earn = 0
    for i in cursor:
        res_remain_share += round(i[1], 2)
        res_sum += round(i[1]*cost, 2)
        res_earn += round(i[1]*(ymarkline-cost), 2)
        res_earn_percent = str(round(res_earn/res_sum*100, 2))+'%'
        res[i[0].strftime("%Y-%m-%d")] = {'res_remain_share': round(
            res_remain_share, 2), 'res_earn': round(res_earn, 2), "res_earn_percent": res_earn_percent}
    conn.close()
    return res


# get_remain_chart_data_by_fund_code

def get_fund_total_chart_data(fund_code):
    conn, cursor = connect_database()
    xaxisdata = []
    seriesdata = []
    cursor.execute(
        "select net_value_date,net_value,equity_return from fund_net_history where fund_code=%s order by net_value_date", [fund_code])
    temp = cursor.fetchall()
    maxdatatime = temp[-1][0].strftime("%Y-%m-%d")
    for i in temp:
        xaxisdata.append(i[0].strftime("%Y-%m-%d"))
        seriesdata.append([i[0].strftime("%Y-%m-%d"), i[1], i[2]])
    # if fishdata:
    #     t = xaxisdata[-15]
    if len(xaxisdata) > 60:
        t = xaxisdata[-60]
    else:
        t = xaxisdata[0]
    mps = []
    cursor.execute(
        "select trade_time,unit_net_value,transaction_type,remain_volume,transaction_methods from fund_orders where fund_code = %s", [fund_code])
    temp = cursor.fetchall()
    if temp:
        for i in temp:
            if i[2] == '1' and i[3] == 0:
                itemStyle = {"color": "rgb(185, 184, 189)"}
            if i[2] == '1' and i[3] > 0 and i[4] == 's':
                itemStyle = {"color": "rgb(158, 220, 253)"}
            if i[2] == '1' and i[3] > 0 and i[4] == 'w':
                itemStyle = {"color": "rgb(64, 158, 255)"}
            if i[2] == '0':
                itemStyle = {"color": "rgb(247, 71, 23)"}
            mps.append(
                {"symbolSize": 50, "value": '买' if i[2] == '1' else '卖', "xAxis": i[0].strftime("%Y-%m-%d"), "yAxis": i[1], "itemStyle": itemStyle})

        cursor.execute(
            "select cost from fund_total where fund_code=%s", [fund_code])
        fc = cursor.fetchone()[0]
        mkl = {"yAxis": fc, "name": "Cost"}
        conn.close()
        return {"maxdatatime": maxdatatime, 'xAxisdata': xaxisdata, 'seriesdata': seriesdata, 'xaxisrange': t, 'mps': mps, "mkl": mkl}
    else:
        conn.close()
        return {"maxdatatime": maxdatatime, 'xAxisdata': xaxisdata, 'seriesdata': seriesdata, 'xaxisrange': t, 'mps': [], "mkl": []}
