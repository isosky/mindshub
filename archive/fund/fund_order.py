
#!/usr/bin/python
# -*- coding: utf-8 -*-
# 交易相关，含核算，归档
import datetime
from base.base import connect_database
from datetime import date, datetime
from mindshub.archive.data_collector import fund_collector


# TODO 优化这个函数
def get_fund_details(fund_code, time=None):
    if time == None:
        time = datetime.today()
    conn, cursor = connect_database()
    cursor = conn.cursor(dictionary=True)
    if type(time) == datetime.time and type(time) == datetime.date:
        time = time.strftime("%Y-%m-%d")
    print(fund_code, time)
    cursor.execute(
        "select * from fund_orders where fund_code=%s and order_date<=%s", [fund_code, time])
    temp = cursor.fetchall()
    if temp:
        buy_sum = 0
        sell_sum = 0
        fund_shares = 0
        for i in temp:
            if i['transaction_type'] == '1':
                fund_shares += round(i['transaction_amount'], 2)
                buy_sum += round(i['order_amount'], 2)
            else:
                fund_shares -= round(i['transaction_amount'], 2)
                sell_sum += round(i['order_amount'], 2)
        conn.close()
        return round(fund_shares, 2), round(buy_sum, 2), round(sell_sum, 2)
    conn.close()


def add_buy_order(orderform):
    conn, cursor = connect_database()
    cursor.execute("select fund_name from fund_base where fund_code =%s",
                   [orderform['fund_code']])
    fund_name = cursor.fetchone()[0]
    if orderform['buytype']:
        methods = 'w'
    else:
        methods = 's'
    # print(orderform)
    cursor.execute("insert into fund_orders (fund_code,fund_name,trade_time,transaction_amount,unit_net_value,order_amount,order_date,transaction_type,transaction_methods,is_fry,remain_volume) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                   [orderform['fund_code'], fund_name, orderform['trade_time'], orderform['fund_shares'],
                    orderform['fund_prices'],  orderform['order_sum'], orderform['check_time'], 1, methods, orderform['isfry'], orderform['fund_shares']])
    conn.commit()
    fund_shares, buy_sum, sell_sum = get_fund_details(
        fund_code=orderform['fund_code'])
    # 如果第一次买这个基金，在fund_total表里面新增一条数据
    cursor.execute("select count(*) from fund_total where fund_code = %s",
                   [orderform['fund_code']])
    temp = cursor.fetchone()[0]
    if temp == 0:
        cursor.execute("insert into fund_total (fund_code,fund_name,cost,cost_update_time) values (%s,%s,%s,%s)", [
            orderform['fund_code'], fund_name, orderform['fund_prices'], orderform['trade_time']])
        conn.commit()

        cursor.execute("update fund_total set holding_fraction=%s,total_purchase_amount=%s where fund_code=%s", [
            fund_shares, buy_sum, orderform['fund_code']])
        conn.commit()
        return "ok"
    else:
        # 如果曾经卖空过，然后再买的
        cursor.execute("select count(*) from fund_total where fund_code=%s and cost is null",
                       [orderform['fund_code']])
        temp = cursor.fetchone()[0]
        if temp == 1:
            cursor.execute("update fund_total set cost=%s,cost_update_time=%s,holding_fraction=%s where fund_code=%s", [
                orderform['fund_prices'], orderform['trade_time'], fund_shares, orderform['fund_code']])
            conn.commit()
            conn.close()
            return "ok"
        else:
            cursor.execute("update fund_total set holding_fraction=%s where fund_code=%s", [
                fund_shares, orderform['fund_code']])
            conn.commit()
            conn.close()
            return "ok"


def add_sell_order(orderform):
    conn, cursor = connect_database()
    cursor.execute("select fund_name from fund_base where fund_code =%s",
                   [orderform['fund_code']])
    fund_name = cursor.fetchone()[0]
    cursor.execute("insert into fund_orders (fund_code,fund_name,trade_time,transaction_amount,unit_net_value,order_amount,order_date,transaction_type,is_fry) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                   [orderform['fund_code'], fund_name, orderform['trade_time'], orderform['fund_shares'],
                    orderform['fund_prices'], orderform['order_sum'], orderform['check_time'], 0, orderform['isfry']])

    conn.commit()
    fund_shares, buy_sum, sell_sum = get_fund_details(
        fund_code=orderform['fund_code'])
    cursor.execute("update fund_total set holding_fraction =%s,update_time=now() where fund_code=%s", [
        fund_shares,  orderform['fund_code']])
    conn.commit()
    conn.close()
    return "ok"


def add_orders_ralations(src_order, relate_order):
    conn, cursor = connect_database()
    cursor.execute("update fund_orders set relate_id=%s where order_id=%s",
                   [relate_order, src_order])
    cursor.execute("update fund_orders set relate_id=%s where order_id=%s",
                   [src_order, relate_order])
    conn.commit()
    conn.close()
    return "ok"
